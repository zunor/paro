#[derive(Debug, Clone)]
pub struct ExplainState {
    /// Plan-tree output buffer.
    pub output: String,
    /// Summary lines rendered after the plan tree.
    pub summary_lines: Vec<String>,
    /// Current indentation level (2 spaces per level).
    pub indent: usize,
    /// Whether to display estimated row count.
    pub costs: bool,
    /// Whether verbose mode is enabled.
    pub verbose: bool,
    /// Whether EXPLAIN ANALYZE mode is enabled.
    pub analyze: bool,
}

impl ExplainState {
    pub fn new(analyze: bool) -> Self {
        Self {
            output: String::new(),
            summary_lines: Vec::new(),
            indent: 0,
            costs: true,
            verbose: false,
            analyze,
        }
    }

    pub fn write_line(&mut self, line: &str) {
        self.output.push_str(&"  ".repeat(self.indent));
        self.output.push_str(line);
        self.output.push('\n');
    }

    pub fn write_property(&mut self, label: &str, value: impl AsRef<str>) {
        self.write_line(&format!("{label}: {}", value.as_ref()));
    }

    pub fn write_summary(&mut self, label: &str, value: impl AsRef<str>) {
        self.summary_lines
            .push(format!("{label}: {}", value.as_ref()));
    }

    pub fn into_lines(mut self) -> Vec<String> {
        if self.output.ends_with('\n') {
            self.output.pop();
        }
        let mut lines = if self.output.is_empty() {
            vec![]
        } else {
            self.output.lines().map(str::to_string).collect()
        };
        lines.extend(self.summary_lines);
        lines
    }

    pub fn has_plan_lines(&self) -> bool {
        !self.output.is_empty()
    }

    pub fn ensure_plan_line(&mut self, line: &str) {
        if !self.has_plan_lines() {
            self.write_line(line);
        }
    }
}
