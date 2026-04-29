use contextdb_core::{Error, Result, Value};
use std::collections::BTreeSet;

#[derive(Debug, Clone)]
pub struct RankFormula {
    root: FormulaNode,
    refs: Vec<String>,
}

impl RankFormula {
    pub fn compile(formula: &str) -> Result<Self> {
        Self::compile_for_index("", formula)
    }

    pub fn compile_for_index(index: &str, formula: &str) -> Result<Self> {
        let mut parser = FormulaParser::new(index, formula);
        let root = parser.parse_expr()?;
        parser.skip_ws();
        if !parser.is_eof() {
            return Err(parser.unexpected_at_current());
        }
        let mut refs = BTreeSet::new();
        collect_refs(&root, &mut refs);
        Ok(Self {
            root,
            refs: refs.into_iter().collect(),
        })
    }

    pub fn const_one() -> Self {
        Self {
            root: FormulaNode::Literal(1.0),
            refs: Vec::new(),
        }
    }

    pub fn column_refs(&self) -> &[String] {
        &self.refs
    }

    pub fn eval_with_resolver(
        &self,
        vector_score: f32,
        mut resolver: impl FnMut(&str) -> std::result::Result<Option<f32>, FormulaEvalError>,
    ) -> std::result::Result<Option<f32>, FormulaEvalError> {
        eval_node(&self.root, vector_score, &mut resolver)
    }

    pub fn eval(
        &self,
        anchor: &std::collections::HashMap<String, Value>,
        joined: Option<&std::collections::HashMap<String, Value>>,
        vector_score: f32,
    ) -> std::result::Result<Option<f32>, FormulaEvalError> {
        self.eval_with_resolver(vector_score, |column| {
            let value = anchor
                .get(column)
                .or_else(|| joined.and_then(|row| row.get(column)))
                .unwrap_or(&Value::Null);
            value_to_rank_number(value, column)
        })
    }
}

#[derive(Debug, Clone)]
enum FormulaNode {
    Literal(f32),
    ColRef(String),
    Coalesce(Box<FormulaNode>, f32),
    Mul(Box<FormulaNode>, Box<FormulaNode>),
    Add(Box<FormulaNode>, Box<FormulaNode>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FormulaEvalError {
    UnsupportedType {
        column: String,
        actual: &'static str,
    },
    CorruptJoinedColumn {
        column: String,
    },
}

impl FormulaEvalError {
    pub fn reason(&self) -> String {
        match self {
            FormulaEvalError::UnsupportedType { column, actual } => {
                format!("rank formula column `{column}` has unsupported runtime type {actual}")
            }
            FormulaEvalError::CorruptJoinedColumn { column } => {
                format!("failed to decode joined column `{column}`")
            }
        }
    }
}

fn eval_node(
    node: &FormulaNode,
    vector_score: f32,
    resolver: &mut impl FnMut(&str) -> std::result::Result<Option<f32>, FormulaEvalError>,
) -> std::result::Result<Option<f32>, FormulaEvalError> {
    match node {
        FormulaNode::Literal(value) => Ok(Some(*value)),
        FormulaNode::ColRef(column) if column == "vector_score" => Ok(Some(vector_score)),
        FormulaNode::ColRef(column) => resolver(column),
        FormulaNode::Coalesce(expr, fallback) => {
            Ok(eval_node(expr, vector_score, resolver)?.or(Some(*fallback)))
        }
        FormulaNode::Mul(left, right) => {
            match (
                eval_node(left, vector_score, resolver)?,
                eval_node(right, vector_score, resolver)?,
            ) {
                (Some(left), Some(right)) => Ok(Some(left * right)),
                _ => Ok(None),
            }
        }
        FormulaNode::Add(left, right) => {
            match (
                eval_node(left, vector_score, resolver)?,
                eval_node(right, vector_score, resolver)?,
            ) {
                (Some(left), Some(right)) => Ok(Some(left + right)),
                _ => Ok(None),
            }
        }
    }
}

fn value_to_rank_number(
    value: &Value,
    column: &str,
) -> std::result::Result<Option<f32>, FormulaEvalError> {
    match value {
        Value::Null => Ok(None),
        Value::Float64(value) => Ok(Some(*value as f32)),
        Value::Int64(value) => Ok(Some(*value as f32)),
        Value::Bool(value) => Ok(Some(if *value { 1.0 } else { 0.0 })),
        Value::Text(_) => Err(FormulaEvalError::UnsupportedType {
            column: column.to_string(),
            actual: "TEXT",
        }),
        Value::Json(_) => Err(FormulaEvalError::UnsupportedType {
            column: column.to_string(),
            actual: "JSON",
        }),
        Value::Uuid(_) => Err(FormulaEvalError::UnsupportedType {
            column: column.to_string(),
            actual: "UUID",
        }),
        Value::Vector(_) => Err(FormulaEvalError::UnsupportedType {
            column: column.to_string(),
            actual: "VECTOR",
        }),
        Value::Timestamp(_) => Err(FormulaEvalError::UnsupportedType {
            column: column.to_string(),
            actual: "TIMESTAMP",
        }),
        Value::TxId(_) => Err(FormulaEvalError::UnsupportedType {
            column: column.to_string(),
            actual: "TXID",
        }),
    }
}

fn collect_refs(node: &FormulaNode, refs: &mut BTreeSet<String>) {
    match node {
        FormulaNode::Literal(_) => {}
        FormulaNode::ColRef(column) => {
            refs.insert(column.clone());
        }
        FormulaNode::Coalesce(expr, _) => collect_refs(expr, refs),
        FormulaNode::Mul(left, right) | FormulaNode::Add(left, right) => {
            collect_refs(left, refs);
            collect_refs(right, refs);
        }
    }
}

struct FormulaParser<'a> {
    index: &'a str,
    input: &'a str,
    pos: usize,
}

impl<'a> FormulaParser<'a> {
    fn new(index: &'a str, input: &'a str) -> Self {
        Self {
            index,
            input,
            pos: 0,
        }
    }

    fn parse_expr(&mut self) -> Result<FormulaNode> {
        self.parse_add()
    }

    fn parse_add(&mut self) -> Result<FormulaNode> {
        let mut node = self.parse_mul()?;
        loop {
            self.skip_ws();
            if !self.consume('+') {
                break;
            }
            let right = self.parse_mul()?;
            node = FormulaNode::Add(Box::new(node), Box::new(right));
        }
        Ok(node)
    }

    fn parse_mul(&mut self) -> Result<FormulaNode> {
        let mut node = self.parse_primary()?;
        loop {
            self.skip_ws();
            if !self.consume('*') {
                break;
            }
            let right = self.parse_primary()?;
            node = FormulaNode::Mul(Box::new(node), Box::new(right));
        }
        Ok(node)
    }

    fn parse_primary(&mut self) -> Result<FormulaNode> {
        self.skip_ws();
        if self.is_eof() {
            return Err(self.error(self.position(), "expected expression"));
        }
        if self.consume('(') {
            let expr = self.parse_expr()?;
            self.skip_ws();
            if !self.consume(')') {
                return Err(self.error(self.position(), "expected ')'"));
            }
            return Ok(expr);
        }
        if self.peek() == Some('{') {
            return self.parse_col_ref();
        }
        if self.starts_ident("coalesce") {
            return self.parse_coalesce();
        }
        if self
            .peek()
            .is_some_and(|ch| ch.is_ascii_digit() || ch == '.')
        {
            return self.parse_number().map(FormulaNode::Literal);
        }
        if self.starts_ident("CASE") {
            return Err(self.error(self.position(), "CASE expressions are not supported"));
        }
        if self.starts_ident("SELECT") {
            return Err(self.error(self.position(), "SELECT subqueries are not supported"));
        }
        if self
            .peek()
            .is_some_and(|ch| ch.is_ascii_alphabetic() || ch == '_')
        {
            let start = self.pos;
            let ident = self.read_identifier();
            self.skip_ws();
            if self.peek() == Some('(') {
                return Err(self.error_at(start, "function calls are not supported"));
            }
            return Err(self.error_at(start, &format!("unsupported token `{ident}`")));
        }
        Err(self.unexpected_at_current())
    }

    fn parse_coalesce(&mut self) -> Result<FormulaNode> {
        let start = self.pos;
        self.pos += "coalesce".len();
        self.skip_ws();
        if !self.consume('(') {
            return Err(self.error_at(start, "coalesce requires '('"));
        }
        self.skip_ws();
        if self.is_eof() {
            return Err(self.error(self.input.len() + 2, "coalesce requires expression"));
        }
        let expr = self.parse_expr()?;
        self.skip_ws();
        if !self.consume(',') {
            return Err(self.error(self.input.len() + 2, "coalesce requires fallback literal"));
        }
        let fallback = self.parse_number()?;
        self.skip_ws();
        if !self.consume(')') {
            return Err(self.error(self.input.len() + 2, "coalesce requires closing ')'"));
        }
        Ok(FormulaNode::Coalesce(Box::new(expr), fallback))
    }

    fn parse_col_ref(&mut self) -> Result<FormulaNode> {
        let start = self.pos;
        self.pos += 1;
        let body_start = self.pos;
        while let Some(ch) = self.peek() {
            if ch == '}' {
                let name = &self.input[body_start..self.pos];
                self.pos += 1;
                if let Some(offset) = name.find('.') {
                    return Err(self.error_at(
                        body_start + offset,
                        "table-qualified column references are not supported",
                    ));
                }
                if name.is_empty()
                    || !name
                        .chars()
                        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
                {
                    return Err(self.error_at(start, "invalid column reference"));
                }
                return Ok(FormulaNode::ColRef(name.to_string()));
            }
            self.pos += ch.len_utf8();
        }
        Err(self.error_at(start, "unterminated column reference"))
    }

    fn parse_number(&mut self) -> Result<f32> {
        self.skip_ws();
        let start = self.pos;
        let mut seen_digit = false;
        while let Some(ch) = self.peek() {
            if ch.is_ascii_digit() {
                seen_digit = true;
                self.pos += 1;
            } else if ch == '.' {
                self.pos += 1;
            } else {
                break;
            }
        }
        if !seen_digit {
            return Err(self.error_at(start, "expected number literal"));
        }
        self.input[start..self.pos]
            .parse::<f32>()
            .map_err(|err| self.error_at(start, &format!("invalid number literal: {err}")))
    }

    fn unexpected_at_current(&self) -> Error {
        match self.peek() {
            Some('/') => self.error(self.position(), "unsupported operator `/`"),
            Some('-') => self.error(self.position(), "unsupported operator `-`"),
            Some(ch) => self.error(self.position(), &format!("unexpected token `{ch}`")),
            None => self.error(self.position(), "unexpected end of formula"),
        }
    }

    fn skip_ws(&mut self) {
        while let Some(ch) = self.peek() {
            if !ch.is_whitespace() {
                break;
            }
            self.pos += ch.len_utf8();
        }
    }

    fn consume(&mut self, expected: char) -> bool {
        if self.peek() == Some(expected) {
            self.pos += expected.len_utf8();
            true
        } else {
            false
        }
    }

    fn starts_ident(&self, ident: &str) -> bool {
        self.input[self.pos..]
            .get(..ident.len())
            .is_some_and(|s| s.eq_ignore_ascii_case(ident))
            && self.input[self.pos + ident.len()..]
                .chars()
                .next()
                .is_none_or(|ch| !ch.is_ascii_alphanumeric() && ch != '_')
    }

    fn read_identifier(&mut self) -> &'a str {
        let start = self.pos;
        while let Some(ch) = self.peek() {
            if ch.is_ascii_alphanumeric() || ch == '_' {
                self.pos += ch.len_utf8();
            } else {
                break;
            }
        }
        &self.input[start..self.pos]
    }

    fn peek(&self) -> Option<char> {
        self.input[self.pos..].chars().next()
    }

    fn is_eof(&self) -> bool {
        self.pos >= self.input.len()
    }

    fn position(&self) -> usize {
        self.pos + 1
    }

    fn error_at(&self, zero_based: usize, reason: &str) -> Error {
        self.error(zero_based + 1, reason)
    }

    fn error(&self, position: usize, reason: &str) -> Error {
        Error::RankPolicyFormulaParse {
            index: self.index.to_string(),
            position,
            reason: reason.to_string(),
        }
    }
}
