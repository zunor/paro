// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use std::cell::RefCell;
use std::rc::Rc;

pub use nom::branch::alt;
pub use nom::branch::permutation;
pub use nom::combinator::consumed;
pub use nom::combinator::map;
pub use nom::combinator::not;
pub use nom::combinator::value;
pub use nom::multi::many1;
use nom::sequence::terminated;
use nom::Offset;
use nom::Parser;
use nom_rule::rule;
use pratt::PrattError;
use pratt::PrattParser;
use pratt::Precedence;

pub fn parser_fn<'a, O, P>(mut parser: P) -> impl FnMut(Input<'a>) -> IResult<'a, O>
where
    P: nom::Parser<Input<'a>, Output = O, Error = Error<'a>>,
{
    move |input| parser.parse(input)
}

/// Hot recursive parser entrypoints re-check stack at fine granularity.
///
/// Local debug runs now record remaining stack at every guarded hotspot through
/// `stacker::remaining_stack()`. On the current parser recursion paths the per-step delta stays
/// far below 256 KiB, so a 256 KiB red zone leaves headroom before the next checkpoint while
/// avoiding the old one-shot 8 MiB/16 MiB over-allocation strategy.
pub(crate) const STACK_RED_ZONE: usize = 256 * 1024;
pub(crate) const STACK_GROW_SIZE: usize = 2 * 1024 * 1024;

#[cfg(debug_assertions)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ParserStackStats {
    pub samples: usize,
    pub min_remaining: usize,
    pub max_step_delta: usize,
}

#[cfg(debug_assertions)]
thread_local! {
    static PARSER_STACK_STATS: RefCell<ParserStackStats> = const { RefCell::new(ParserStackStats {
        samples: 0,
        min_remaining: usize::MAX,
        max_step_delta: 0,
    }) };
    static LAST_REMAINING_STACK: RefCell<Option<usize>> = const { RefCell::new(None) };
}

#[cfg(debug_assertions)]
#[inline(always)]
fn record_parser_stack_sample() {
    let Some(remaining) = stacker::remaining_stack() else {
        return;
    };

    PARSER_STACK_STATS.with(|stats| {
        let mut stats = stats.borrow_mut();
        stats.samples += 1;
        stats.min_remaining = stats.min_remaining.min(remaining);
    });

    LAST_REMAINING_STACK.with(|last| {
        let mut last = last.borrow_mut();
        if let Some(previous) = *last {
            let delta = previous.saturating_sub(remaining);
            PARSER_STACK_STATS.with(|stats| {
                let mut stats = stats.borrow_mut();
                stats.max_step_delta = stats.max_step_delta.max(delta);
            });
        }
        *last = Some(remaining);
    });
}

#[cfg(all(debug_assertions, feature = "internal-testing"))]
pub fn reset_parser_stack_stats() {
    PARSER_STACK_STATS.with(|stats| {
        *stats.borrow_mut() = ParserStackStats {
            samples: 0,
            min_remaining: usize::MAX,
            max_step_delta: 0,
        };
    });
    LAST_REMAINING_STACK.with(|last| {
        *last.borrow_mut() = None;
    });
}

#[cfg(all(debug_assertions, feature = "internal-testing"))]
pub fn parser_stack_stats_snapshot() -> ParserStackStats {
    PARSER_STACK_STATS.with(|stats| {
        let stats = *stats.borrow();
        if stats.samples == 0 {
            ParserStackStats::default()
        } else {
            stats
        }
    })
}

#[inline(always)]
pub(crate) fn maybe_grow_parser_stack<R>(callback: impl FnOnce() -> R) -> R {
    #[cfg(debug_assertions)]
    record_parser_stack_sample();

    stacker::maybe_grow(STACK_RED_ZONE, STACK_GROW_SIZE, callback)
}

use crate::ast::quote::QuotedIdent;
use crate::ast::ColumnID;
use crate::ast::Identifier;
use crate::ast::IdentifierType;
use crate::ast::SchemaRef;
use crate::ast::SetType;
use crate::ast::TableReference;
use crate::parser::input::Input;
use crate::parser::input::WithSpan;
use crate::parser::token::*;
use crate::parser::Error;
use crate::parser::ErrorKind;
use crate::Range;
use crate::Span;

pub type IResult<'a, Output> = nom::IResult<Input<'a>, Output, Error<'a>>;

pub fn match_text(text: &'static str) -> impl FnMut(Input) -> IResult<&Token> {
    move |i| match i.tokens.first().filter(|token| token.text() == text) {
        Some(token) => Ok((i.slice(1..), token)),
        _ => Err(nom::Err::Error(Error::from_error_kind(
            i,
            ErrorKind::ExpectText(text),
        ))),
    }
}

pub fn match_token(kind: TokenKind) -> impl FnMut(Input) -> IResult<&Token> {
    move |i| match i.tokens.first().filter(|token| token.kind == kind) {
        Some(token) => Ok((i.slice(1..), token)),
        _ => Err(nom::Err::Error(Error::from_error_kind(
            i,
            ErrorKind::ExpectToken(kind),
        ))),
    }
}

pub fn any_token(i: Input<'_>) -> IResult<'_, &Token<'_>> {
    match i.tokens.first().filter(|token| token.kind != EOI) {
        Some(token) => Ok((i.slice(1..), token)),
        _ => Err(nom::Err::Error(Error::from_error_kind(
            i,
            ErrorKind::Other("expected any token but reached the end"),
        ))),
    }
}

pub fn lambda_params(i: Input) -> IResult<Vec<Identifier>> {
    let single_param = map(rule! {#ident}, |param| vec![param]);
    let multi_params = map(
        rule! { "(" ~ #comma_separated_list1(ident) ~ ")" },
        |(_, params, _)| params,
    );
    rule!(
        #single_param
        | #multi_params
    )
    .parse(i)
}

pub fn ident(i: Input) -> IResult<Identifier> {
    non_reserved_identifier(|token| token.is_reserved_ident(false))(i)
}

pub fn grant_ident(i: Input) -> IResult<Identifier> {
    non_reserved_identifier(|token| token.is_grant_reserved_ident(false, true))(i)
}

pub fn plain_ident(i: Input) -> IResult<Identifier> {
    plain_identifier(|token| token.is_reserved_ident(false))(i)
}

pub fn ident_after_as(i: Input) -> IResult<Identifier> {
    non_reserved_identifier(|token| token.is_reserved_ident(true))(i)
}

pub fn function_name(i: Input) -> IResult<Identifier> {
    non_reserved_identifier(|token| token.is_reserved_function_name())(i)
}

pub fn stage_name(i: Input) -> IResult<Identifier> {
    let anonymous_stage = map(consumed(rule! { "~" }), |(span, _)| {
        Identifier::from_name(transform_span(span.tokens), "~")
    });

    rule!(
        #plain_ident
        | #anonymous_stage
    )
    .parse(i)
}

fn plain_identifier(
    is_reserved_keyword: fn(&TokenKind) -> bool,
) -> impl FnMut(Input) -> IResult<Identifier> {
    move |i| {
        map(
            rule! {
                Ident
                | #non_reserved_keyword(is_reserved_keyword)
            },
            |token| Identifier {
                span: transform_span(std::slice::from_ref(token)),
                name: token.text().to_string(),
                quote: None,
                ident_type: IdentifierType::None,
            },
        )
        .parse(i)
    }
}

fn quoted_identifier(i: Input) -> IResult<Identifier> {
    match_token(LiteralString)(i).and_then(|(i2, token)| {
        if token
            .text()
            .chars()
            .next()
            .filter(|c| crate::parser::input::is_ident_quote(*c))
            .is_some()
        {
            let QuotedIdent(ident, quote) = token.text().parse().map_err(|_| {
                nom::Err::Error(Error::from_error_kind(
                    i,
                    ErrorKind::Other("invalid identifier"),
                ))
            })?;
            Ok((
                i2,
                Identifier {
                    span: transform_span(std::slice::from_ref(token)),
                    name: ident,
                    quote: Some(quote),
                    ident_type: IdentifierType::None,
                },
            ))
        } else {
            Err(nom::Err::Error(Error::from_error_kind(
                i,
                ErrorKind::ExpectToken(Ident),
            )))
        }
    })
}

fn identifier_hole(i: Input) -> IResult<Identifier> {
    check_template_mode(map(
        consumed(rule! {
            IDENTIFIER ~ ^"(" ~ #template_hole ~ ^")"
        }),
        |(span, (_, _, name, _))| Identifier {
            span: transform_span(span.tokens),
            name,
            quote: None,
            ident_type: IdentifierType::Hole,
        },
    ))(i)
}

fn identifier_variable(i: Input) -> IResult<Identifier> {
    map(
        consumed(rule! {
            IDENTIFIER ~ ^"(" ~ ^#variable_ident ~ ^")"
        }),
        |(span, (_, _, name, _))| Identifier {
            span: transform_span(span.tokens),
            name,
            quote: None,
            ident_type: IdentifierType::Variable,
        },
    )
    .parse(i)
}

fn non_reserved_identifier(
    is_reserved_keyword: fn(&TokenKind) -> bool,
) -> impl FnMut(Input) -> IResult<Identifier> {
    move |i| {
        rule!(
            #plain_identifier(is_reserved_keyword)
            | #quoted_identifier
            | #identifier_hole
            | #identifier_variable
        )
        .parse(i)
    }
}

fn non_reserved_keyword(
    is_reserved_keyword: fn(&TokenKind) -> bool,
) -> impl FnMut(Input) -> IResult<&Token> {
    move |i: Input| match i
        .tokens
        .first()
        .filter(|token| token.kind.is_keyword() && !is_reserved_keyword(&token.kind))
    {
        Some(token) => Ok((i.slice(1..), token)),
        _ => Err(nom::Err::Error(Error::from_error_kind(
            i,
            ErrorKind::ExpectToken(Ident),
        ))),
    }
}

pub fn schema_ref(i: Input) -> IResult<SchemaRef> {
    map(dot_separated_idents_1_to_2, |(database, schema)| {
        SchemaRef { database, schema }
    })
    .parse(i)
}

pub fn set_type(i: Input) -> IResult<SetType> {
    map(
        rule! {
           (GLOBAL | SESSION | VARIABLE | LOCAL)?
        },
        |res| match res {
            Some(token) => match token.kind {
                GLOBAL => SetType::SettingsGlobal,
                SESSION => SetType::SettingsSession,
                VARIABLE => SetType::Variable,
                LOCAL => SetType::SettingsLocal,
                _ => unreachable!(),
            },
            None => SetType::SettingsSession,
        },
    )
    .parse(i)
}

pub fn table_reference_only(i: Input) -> IResult<TableReference> {
    map(
        consumed(rule! {
            #dot_separated_idents_1_to_3
        }),
        |(span, (database, schema, table))| TableReference::Table {
            span: transform_span(span.tokens),
            database,
            schema,
            table,
            alias: None,
            temporal: None,
            with_options: None,
            pivot: None,
            unpivot: None,
            sample: None,
        },
    )
    .parse(i)
}

pub fn column_reference_only(i: Input) -> IResult<(TableReference, Identifier)> {
    map(
        consumed(rule! {
            #dot_separated_idents_2_to_4
        }),
        |(span, (database, schema, table, column))| {
            (
                TableReference::Table {
                    span: transform_span(span.tokens),
                    database,
                    schema,
                    table,
                    alias: None,
                    temporal: None,
                    with_options: None,
                    pivot: None,
                    unpivot: None,
                    sample: None,
                },
                column,
            )
        },
    )
    .parse(i)
}

pub fn column_id(i: Input) -> IResult<ColumnID> {
    alt((column_position, column_row, column_ident)).parse(i)
}

pub fn column_position(i: Input) -> IResult<ColumnID> {
    map_res(rule! { ColumnPosition }, |token| {
        let name = token.text().to_string();
        let pos = name[1..]
            .parse::<usize>()
            .map_err(|e| nom::Err::Failure(e.into()))?;
        if pos == 0 {
            return Err(nom::Err::Failure(ErrorKind::Other(
                "column position must be greater than 0",
            )));
        }
        Ok(ColumnID::Position(crate::ast::ColumnPosition {
            pos,
            name,
            span: Some(token.span),
        }))
    })
    .parse(i)
}

pub fn column_row(i: Input) -> IResult<ColumnID> {
    // ROW could be a column name for compatibility
    map_res(rule! {ROW}, |token| {
        Ok(ColumnID::Name(Identifier::from_name(
            transform_span(std::slice::from_ref(token)),
            "row",
        )))
    })
    .parse(i)
}

pub fn column_ident(i: Input) -> IResult<ColumnID> {
    map_res(rule! { #ident }, |ident| Ok(ColumnID::Name(ident))).parse(i)
}

pub fn variable_ident(i: Input) -> IResult<String> {
    map(rule! { IdentVariable }, |t| t.text()[1..].to_string()).parse(i)
}

/// Parse one to two idents separated by a dot, fulfilling from the right.
///
/// Example: `database.schema`
pub fn dot_separated_idents_1_to_2(i: Input) -> IResult<(Option<Identifier>, Identifier)> {
    map(
        rule! {
           #ident ~ ( "." ~ #ident )?
        },
        |res| match res {
            (ident1, None) => (None, ident1),
            (ident0, Some((_, ident1))) => (Some(ident0), ident1),
        },
    )
    .parse(i)
}

/// Parse one to three idents separated by a dot, fulfilling from the right.
///
/// Example: `database.schema.table`
pub fn dot_separated_idents_1_to_3(
    i: Input,
) -> IResult<(Option<Identifier>, Option<Identifier>, Identifier)> {
    map(
        rule! {
            #ident ~ ( "." ~ #ident ~ ( "." ~ #ident )? )?
        },
        |res| match res {
            (ident2, None) => (None, None, ident2),
            (ident1, Some((_, ident2, None))) => (None, Some(ident1), ident2),
            (ident0, Some((_, ident1, Some((_, ident2))))) => (Some(ident0), Some(ident1), ident2),
        },
    )
    .parse(i)
}

/// Parse two to four idents separated by a dot, fulfilling from the right.
///
/// Example: `database.schema.table.column`
pub fn dot_separated_idents_2_to_4(
    i: Input,
) -> IResult<(
    Option<Identifier>,
    Option<Identifier>,
    Identifier,
    Identifier,
)> {
    map(
        rule! {
            #ident ~ "." ~ #ident ~ ( "." ~ #ident ~ ( "." ~ #ident )? )?
        },
        |res| match res {
            (ident2, _, ident3, None) => (None, None, ident2, ident3),
            (ident1, _, ident2, Some((_, ident3, None))) => (None, Some(ident1), ident2, ident3),
            (ident0, _, ident1, Some((_, ident2, Some((_, ident3))))) => {
                (Some(ident0), Some(ident1), ident2, ident3)
            }
        },
    )
    .parse(i)
}

pub fn comma_separated_list0<'a, T>(
    item: impl nom::Parser<Input<'a>, Output = T, Error = Error<'a>>,
) -> impl FnMut(Input<'a>) -> IResult<'a, Vec<T>> {
    separated_list0(match_text(","), item)
}

pub fn comma_separated_list0_ignore_trailing<'a, T>(
    item: impl nom::Parser<Input<'a>, Output = T, Error = Error<'a>>,
) -> impl nom::Parser<Input<'a>, Output = Vec<T>, Error = Error<'a>> {
    nom::multi::separated_list0(match_text(","), item)
}

pub fn comma_separated_list1_ignore_trailing<'a, T>(
    item: impl nom::Parser<Input<'a>, Output = T, Error = Error<'a>>,
) -> impl nom::Parser<Input<'a>, Output = Vec<T>, Error = Error<'a>> {
    nom::multi::separated_list1(match_text(","), item)
}

pub fn semicolon_terminated_list1<'a, T>(
    item: impl nom::Parser<Input<'a>, Output = T, Error = Error<'a>>,
) -> impl nom::Parser<Input<'a>, Output = Vec<T>, Error = Error<'a>> {
    many1(terminated(item, match_text(";")))
}

pub fn comma_separated_list1<'a, T>(
    item: impl nom::Parser<Input<'a>, Output = T, Error = Error<'a>>,
) -> impl nom::Parser<Input<'a>, Output = Vec<T>, Error = Error<'a>> {
    separated_list1(match_text(","), item)
}

/// A fork of `separated_list0` from nom, but never forgive parser error
/// after a separator is encountered, and always forgive the first element
/// failure.
pub fn separated_list0<I, O, O2, E, F, G>(
    mut sep: G,
    mut f: F,
) -> impl FnMut(I) -> nom::IResult<I, Vec<O>, E>
where
    I: Clone + nom::Input,
    F: nom::Parser<I, Output = O, Error = E>,
    G: nom::Parser<I, Output = O2, Error = E>,
    E: nom::error::ParseError<I>,
{
    move |mut i: I| {
        let mut res = Vec::new();

        match f.parse(i.clone()) {
            Err(_) => return Ok((i, res)),
            Ok((i1, o)) => {
                res.push(o);
                i = i1;
            }
        }

        loop {
            let len = i.input_len();
            match sep.parse(i.clone()) {
                Err(nom::Err::Error(_)) => return Ok((i, res)),
                Err(e) => return Err(e),
                Ok((i1, _)) => {
                    // infinite loop check: the parser must always consume
                    if i1.input_len() == len {
                        return Err(nom::Err::Error(E::from_error_kind(
                            i1,
                            nom::error::ErrorKind::SeparatedList,
                        )));
                    }

                    match f.parse(i1.clone()) {
                        Err(e) => return Err(e),
                        Ok((i2, o)) => {
                            res.push(o);
                            i = i2;
                        }
                    }
                }
            }
        }
    }
}

/// A fork of `separated_list1` from nom, but never forgive parser error
/// after a separator is encountered.
pub fn separated_list1<I, O, O2, E, F, G>(
    mut sep: G,
    mut f: F,
) -> impl FnMut(I) -> nom::IResult<I, Vec<O>, E>
where
    I: Clone + nom::Input,
    F: nom::Parser<I, Output = O, Error = E>,
    G: nom::Parser<I, Output = O2, Error = E>,
    E: nom::error::ParseError<I>,
{
    move |mut i: I| {
        let mut res = Vec::new();

        // Parse the first element
        match f.parse(i.clone()) {
            Err(e) => return Err(e),
            Ok((i1, o)) => {
                res.push(o);
                i = i1;
            }
        }

        loop {
            let len = i.input_len();
            match sep.parse(i.clone()) {
                Err(nom::Err::Error(_)) => return Ok((i, res)),
                Err(e) => return Err(e),
                Ok((i1, _)) => {
                    // infinite loop check: the parser must always consume
                    if i1.input_len() == len {
                        return Err(nom::Err::Error(E::from_error_kind(
                            i1,
                            nom::error::ErrorKind::SeparatedList,
                        )));
                    }

                    match f.parse(i1.clone()) {
                        Err(e) => return Err(e),
                        Ok((i2, o)) => {
                            res.push(o);
                            i = i2;
                        }
                    }
                }
            }
        }
    }
}

/// A fork of `map_res` from nom, but doesn't require `FromExternalError`.
pub fn map_res<'a, O1, O2, F, G>(
    mut parser: F,
    mut f: G,
) -> impl FnMut(Input<'a>) -> IResult<'a, O2>
where
    F: nom::Parser<Input<'a>, Output = O1, Error = Error<'a>>,
    G: FnMut(O1) -> Result<O2, nom::Err<ErrorKind>>,
{
    move |input: Input| {
        let i = input;
        let bt = i.backtrace.clone();
        let (rest, o1) = parser.parse(input)?;
        match f(o1) {
            Ok(o2) => Ok((rest, o2)),
            Err(nom::Err::Error(e)) => {
                i.backtrace.restore(bt);
                Err(nom::Err::Error(Error::from_error_kind(i, e)))
            }
            Err(nom::Err::Failure(e)) => {
                i.backtrace.restore(bt);
                Err(nom::Err::Failure(Error::from_error_kind(i, e)))
            }
            Err(nom::Err::Incomplete(_)) => unreachable!(),
        }
    }
}

/// Try to find an error pattern that user may have made, and hint them with suggestion.
pub fn error_hint<'a, O, F>(
    mut match_error: F,
    message: &'static str,
) -> impl FnMut(Input<'a>) -> IResult<'a, ()>
where
    F: nom::Parser<Input<'a>, Output = O, Error = Error<'a>>,
{
    move |input: Input| match match_error.parse(input) {
        Ok(_) => Err(nom::Err::Error(Error::from_error_kind(
            input,
            ErrorKind::Other(message),
        ))),
        Err(_) => Ok((input, ())),
    }
}

pub fn transform_span(tokens: &[Token]) -> Span {
    Some(Range {
        start: tokens.first().unwrap().span.start,
        end: tokens.last().unwrap().span.end,
    })
}

pub(crate) trait IterProvider<'a> {
    type Item;
    type Iter: Iterator<Item = Self::Item> + ExactSizeIterator;

    fn create_iter(self, span: Rc<RefCell<Option<Input<'a>>>>) -> Self::Iter;
}

impl<'a, T> IterProvider<'a> for Vec<WithSpan<'a, T>>
where
    T: Clone,
{
    type Item = WithSpan<'a, T>;
    type Iter = ErrorSpan<'a, T, std::vec::IntoIter<WithSpan<'a, T>>>;

    fn create_iter(self, span: Rc<RefCell<Option<Input<'a>>>>) -> Self::Iter {
        ErrorSpan::new(self.into_iter(), span)
    }
}

pub(crate) struct ErrorSpan<'a, T, I: Iterator<Item = WithSpan<'a, T>>> {
    iter: I,
    span: Rc<RefCell<Option<Input<'a>>>>,
}

impl<'a, T, I: Iterator<Item = WithSpan<'a, T>>> ErrorSpan<'a, T, I> {
    fn new(iter: I, span: Rc<RefCell<Option<Input<'a>>>>) -> Self {
        Self { iter, span }
    }
}

impl<'a, T, I: Iterator<Item = WithSpan<'a, T>>> Iterator for ErrorSpan<'a, T, I> {
    type Item = WithSpan<'a, T>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter
            .next()
            .inspect(|item| *self.span.borrow_mut() = Some(item.span))
    }
}

impl<'a, T, I: Iterator<Item = WithSpan<'a, T>>> ExactSizeIterator for ErrorSpan<'a, T, I> {}

pub fn run_pratt_parser<'a, I, P, E, T>(
    mut parser: P,
    parsers: T,
    rest: Input<'a>,
    input: Input<'a>,
) -> IResult<'a, P::Output>
where
    E: std::fmt::Debug,
    P: PrattParser<I, Input = WithSpan<'a, E>, Error = &'static str>,
    I: Iterator<Item = P::Input> + ExactSizeIterator,
    T: IterProvider<'a, Item = P::Input, Iter = I>,
{
    let span = Rc::new(RefCell::new(None));
    let mut iter = parsers.create_iter(span.clone()).peekable();
    let expr = parser
        .parse_input(&mut iter, Precedence(0))
        .map_err(|err| {
            // Rollback parsing footprint on unused expr elements.
            input.backtrace.clear();

            let err_kind = match err {
                PrattError::EmptyInput => ErrorKind::Other("expecting an operand"),
                PrattError::UnexpectedNilfix(i) => {
                    *span.borrow_mut() = Some(i.span);
                    ErrorKind::Other("unable to parse the element")
                }
                PrattError::UnexpectedPrefix(i) => {
                    *span.borrow_mut() = Some(i.span);
                    ErrorKind::Other("unable to parse the prefix operator")
                }
                PrattError::UnexpectedInfix(i) => {
                    *span.borrow_mut() = Some(i.span);
                    ErrorKind::Other("missing lhs or rhs for the binary operator")
                }
                PrattError::UnexpectedPostfix(i) => {
                    *span.borrow_mut() = Some(i.span);
                    ErrorKind::Other("unable to parse the postfix operator")
                }
                PrattError::UserError(err) => ErrorKind::Other(err),
            };

            let span = span
                .take()
                // It's safe to slice one more token because input must contain EOI.
                .unwrap_or_else(|| rest.slice(..1));

            nom::Err::Error(Error::from_error_kind(span, err_kind))
        })?;
    if let Some(elem) = iter.peek() {
        // Rollback parsing footprint on unused expr elements.
        input.backtrace.clear();
        Ok((input.slice(input.offset(&elem.span)..), expr))
    } else {
        Ok((rest, expr))
    }
}

pub fn check_template_mode<'a, O, F>(mut parser: F) -> impl FnMut(Input<'a>) -> IResult<'a, O>
where
    F: nom::Parser<Input<'a>, Output = O, Error = Error<'a>>,
{
    move |input: Input| {
        parser.parse(input).and_then(|(i, res)| {
            if input.mode.is_template() {
                Ok((i, res))
            } else {
                i.backtrace.clear();
                let error = Error::from_error_kind(
                    input,
                    ErrorKind::Other("variable is only available in SQL template"),
                );
                Err(nom::Err::Failure(error))
            }
        })
    }
}

pub fn template_hole(i: Input) -> IResult<String> {
    check_template_mode(map(
        rule! {
            ":" ~ ^#plain_ident
        },
        |(_, name)| name.name,
    ))(i)
}

macro_rules! declare_experimental_feature {
    ($check_fn_name: ident, $feature_name: literal) => {
        pub fn $check_fn_name<'a, O, F>(
            _is_exclusive: bool,
            mut parser: F,
        ) -> impl FnMut(Input<'a>) -> IResult<'a, O>
        where
            F: nom::Parser<Input<'a>, Output = O, Error = Error<'a>>,
        {
            move |input: Input| {
                // All features are enabled by default in PostgreSQL dialect
                parser.parse(input)
            }
        }
    };
}

declare_experimental_feature!(check_experimental_chain_function, "chain function");
declare_experimental_feature!(check_experimental_list_comprehension, "list comprehension");
