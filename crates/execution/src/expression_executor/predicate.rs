use paro_common::vector::{SelectionVector, Vector};

#[inline]
pub(super) fn map_row(input_sel: Option<&SelectionVector>, row_idx: usize) -> usize {
    input_sel.map_or(row_idx, |sel| sel.get(row_idx))
}

pub(super) fn select_all_rows(
    input_sel: Option<&SelectionVector>,
    count: usize,
    output: &mut SelectionVector,
) -> usize {
    output.set_len(count);
    for row_idx in 0..count {
        output.set(row_idx, map_row(input_sel, row_idx));
    }
    output.set_len(count);
    count
}

pub(super) fn copy_selection(input: &SelectionVector, output: &mut SelectionVector) -> usize {
    let count = input.len();
    output.set_len(count);
    for row_idx in 0..count {
        output.set(row_idx, input.get(row_idx));
    }
    output.set_len(count);
    count
}

pub(super) fn scan_bool_selection(
    values: &Vector,
    input_sel: Option<&SelectionVector>,
    count: usize,
    output: &mut SelectionVector,
) -> usize {
    output.set_len(count);
    let mut selected = 0;
    for row_idx in 0..count {
        if !values.is_null(row_idx) && matches!(values.get_bool(row_idx), Some(true)) {
            output.set(selected, map_row(input_sel, row_idx));
            selected += 1;
        }
    }
    output.set_len(selected);
    selected
}

pub(super) fn scan_false_bool_selection(
    values: &Vector,
    input_sel: Option<&SelectionVector>,
    count: usize,
    output: &mut SelectionVector,
) -> usize {
    output.set_len(count);
    let mut selected = 0;
    for row_idx in 0..count {
        if !values.is_null(row_idx) && matches!(values.get_bool(row_idx), Some(false)) {
            output.set(selected, map_row(input_sel, row_idx));
            selected += 1;
        }
    }
    output.set_len(selected);
    selected
}

pub(super) fn scan_null_selection(
    values: &Vector,
    input_sel: Option<&SelectionVector>,
    count: usize,
    select_nulls: bool,
    output: &mut SelectionVector,
) -> usize {
    output.set_len(count);
    let mut selected = 0;
    for row_idx in 0..count {
        if values.is_null(row_idx) == select_nulls {
            output.set(selected, map_row(input_sel, row_idx));
            selected += 1;
        }
    }
    output.set_len(selected);
    selected
}

pub(super) fn mark_selected_rows(
    input_sel: Option<&SelectionVector>,
    count: usize,
    selected: &SelectionVector,
    marks: &mut Vec<u8>,
) {
    if marks.len() < count {
        marks.resize(count, 0);
    }
    marks[..count].fill(0);

    let mut cursor = 0;
    for row_idx in 0..count {
        if cursor >= selected.len() {
            break;
        }
        let physical_row = map_row(input_sel, row_idx);
        if selected.get(cursor) == physical_row {
            marks[row_idx] = 1;
            cursor += 1;
        }
    }
}

pub(super) fn accumulate_selected_rows(
    input_sel: Option<&SelectionVector>,
    count: usize,
    selected: &SelectionVector,
    marks: &mut Vec<u8>,
) {
    if marks.len() < count {
        marks.resize(count, 0);
    }

    let mut cursor = 0;
    for row_idx in 0..count {
        if cursor >= selected.len() {
            break;
        }
        let physical_row = map_row(input_sel, row_idx);
        if selected.get(cursor) == physical_row {
            marks[row_idx] = 1;
            cursor += 1;
        }
    }
}

pub(super) fn build_marked_selection(
    input_sel: Option<&SelectionVector>,
    count: usize,
    marks: &[u8],
    output: &mut SelectionVector,
) -> usize {
    output.set_len(count);
    let mut selected = 0;
    for row_idx in 0..count {
        if marks.get(row_idx).copied().unwrap_or_default() != 0 {
            output.set(selected, map_row(input_sel, row_idx));
            selected += 1;
        }
    }
    output.set_len(selected);
    selected
}
