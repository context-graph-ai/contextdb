use crate::ast::*;
use contextdb_core::{Error, Result};
use pest::Parser;
use pest::iterators::Pair;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "grammar.pest"]
struct ContextDbParser;

pub fn parse(input: &str) -> Result<Statement> {
    let sql = input.trim();

    if starts_with_keywords(sql, &["CREATE", "PROCEDURE"])
        || starts_with_keywords(sql, &["CREATE", "FUNCTION"])
    {
        return Err(Error::StoredProcNotSupported);
    }
    if starts_with_keywords(sql, &["WITH", "RECURSIVE"]) {
        return Err(Error::RecursiveCteNotSupported);
    }
    if contains_keyword_sequence_outside_strings(sql, &["GROUP", "BY"]) {
        return Err(Error::ParseError("GROUP BY is not supported".to_string()));
    }
    if contains_token_outside_strings(sql, "OVER") {
        return Err(Error::WindowFunctionNotSupported);
    }
    if contains_where_match_operator(sql) {
        return Err(Error::FullTextSearchNotSupported);
    }

    let mut pairs = ContextDbParser::parse(Rule::statement, sql)
        .map_err(|e| Error::ParseError(e.to_string()))?;
    let statement = pairs
        .next()
        .ok_or_else(|| Error::ParseError("empty statement".to_string()))?;
    let inner = statement
        .into_inner()
        .next()
        .ok_or_else(|| Error::ParseError("missing statement body".to_string()))?;

    let stmt = match inner.as_rule() {
        Rule::begin_stmt => Statement::Begin,
        Rule::commit_stmt => Statement::Commit,
        Rule::rollback_stmt => Statement::Rollback,
        Rule::create_table_stmt => Statement::CreateTable(build_create_table(inner)?),
        Rule::alter_table_stmt => Statement::AlterTable(build_alter_table(inner)?),
        Rule::drop_table_stmt => Statement::DropTable(build_drop_table(inner)?),
        Rule::create_index_stmt => Statement::CreateIndex(build_create_index(inner)?),
        Rule::drop_index_stmt => Statement::DropIndex(build_drop_index(inner)?),
        Rule::insert_stmt => Statement::Insert(build_insert(inner)?),
        Rule::delete_stmt => Statement::Delete(build_delete(inner)?),
        Rule::update_stmt => Statement::Update(build_update(inner)?),
        Rule::select_stmt => Statement::Select(build_select(inner)?),
        Rule::set_sync_conflict_policy => {
            let policy = inner
                .into_inner()
                .find(|p| p.as_rule() == Rule::conflict_policy_value)
                .ok_or_else(|| Error::ParseError("missing conflict policy value".to_string()))?
                .as_str()
                .to_lowercase();
            Statement::SetSyncConflictPolicy(policy)
        }
        Rule::show_sync_conflict_policy => Statement::ShowSyncConflictPolicy,
        Rule::set_memory_limit => Statement::SetMemoryLimit(build_set_memory_limit(inner)?),
        Rule::show_memory_limit => Statement::ShowMemoryLimit,
        Rule::set_disk_limit => Statement::SetDiskLimit(build_set_disk_limit(inner)?),
        Rule::show_disk_limit => Statement::ShowDiskLimit,
        _ => return Err(Error::ParseError("unsupported statement".to_string())),
    };

    validate_statement(&stmt)?;
    Ok(stmt)
}

fn build_select(pair: Pair<'_, Rule>) -> Result<SelectStatement> {
    let mut ctes = Vec::new();
    let mut body = None;

    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::with_clause => {
                for item in p.into_inner() {
                    match item.as_rule() {
                        Rule::recursive_kw => return Err(Error::RecursiveCteNotSupported),
                        Rule::cte_def => ctes.push(build_cte(item)?),
                        other => return Err(unexpected_rule(other, "build_select.with_clause")),
                    }
                }
            }
            Rule::select_core => body = Some(build_select_core(p)?),
            other => return Err(unexpected_rule(other, "build_select")),
        }
    }

    Ok(SelectStatement {
        ctes,
        body: body.ok_or_else(|| Error::ParseError("missing SELECT body".to_string()))?,
    })
}

fn build_cte(pair: Pair<'_, Rule>) -> Result<Cte> {
    let mut name = None;
    let mut query = None;

    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::identifier if name.is_none() => name = Some(parse_identifier(p.as_str())),
            Rule::select_core => query = Some(build_select_core(p)?),
            other => return Err(unexpected_rule(other, "build_cte")),
        }
    }

    Ok(Cte::SqlCte {
        name: name.ok_or_else(|| Error::ParseError("CTE missing name".to_string()))?,
        query: query.ok_or_else(|| Error::ParseError("CTE missing query".to_string()))?,
    })
}

fn build_select_core(pair: Pair<'_, Rule>) -> Result<SelectBody> {
    let mut distinct = false;
    let mut columns = Vec::new();
    let mut from = Vec::new();
    let mut joins = Vec::new();
    let mut where_clause = None;
    let mut order_by = Vec::new();
    let mut limit = None;

    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::distinct_kw => distinct = true,
            Rule::select_list => {
                columns = build_select_list(p)?;
            }
            Rule::from_clause => {
                from = build_from_clause(p)?;
            }
            Rule::join_clause => {
                joins.push(build_join_clause(p)?);
            }
            Rule::where_clause => {
                where_clause = Some(build_where_clause(p)?);
            }
            Rule::order_by_clause => {
                order_by = build_order_by_clause(p)?;
            }
            Rule::limit_clause => {
                limit = Some(build_limit_clause(p)?);
            }
            other => return Err(unexpected_rule(other, "build_select_core")),
        }
    }

    Ok(SelectBody {
        distinct,
        columns,
        from,
        joins,
        where_clause,
        order_by,
        limit,
    })
}

fn build_select_list(pair: Pair<'_, Rule>) -> Result<Vec<SelectColumn>> {
    let mut cols = Vec::new();

    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::star => cols.push(SelectColumn {
                expr: Expr::Column(ColumnRef {
                    table: None,
                    column: "*".to_string(),
                }),
                alias: None,
            }),
            Rule::select_item => cols.push(build_select_item(p)?),
            other => return Err(unexpected_rule(other, "build_select_list")),
        }
    }

    Ok(cols)
}

fn build_select_item(pair: Pair<'_, Rule>) -> Result<SelectColumn> {
    let mut expr = None;
    let mut alias = None;

    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::expr => expr = Some(build_expr(p)?),
            Rule::identifier => alias = Some(parse_identifier(p.as_str())),
            other => return Err(unexpected_rule(other, "build_select_item")),
        }
    }

    Ok(SelectColumn {
        expr: expr
            .ok_or_else(|| Error::ParseError("SELECT item missing expression".to_string()))?,
        alias,
    })
}

fn build_from_clause(pair: Pair<'_, Rule>) -> Result<Vec<FromItem>> {
    let mut items = Vec::new();
    for p in pair.into_inner() {
        if p.as_rule() == Rule::from_item {
            items.push(build_from_item(p)?);
        }
    }
    Ok(items)
}

fn build_from_item(pair: Pair<'_, Rule>) -> Result<FromItem> {
    let inner = pair
        .into_inner()
        .next()
        .ok_or_else(|| Error::ParseError("missing FROM item".to_string()))?;

    match inner.as_rule() {
        Rule::table_ref => build_table_ref(inner),
        Rule::graph_table => build_graph_table(inner),
        _ => Err(Error::ParseError("invalid FROM item".to_string())),
    }
}

fn build_join_clause(pair: Pair<'_, Rule>) -> Result<JoinClause> {
    let mut join_type = None;
    let mut table = None;
    let mut alias = None;
    let mut on = None;

    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::join_type => {
                join_type = Some(if p.as_str().to_ascii_uppercase().starts_with("LEFT") {
                    JoinType::Left
                } else {
                    JoinType::Inner
                });
            }
            Rule::join_table_ref => {
                let mut inner = p.into_inner();
                table = Some(parse_identifier(inner.next().unwrap().as_str()));
                if let Some(alias_pair) = inner.next() {
                    alias = Some(parse_identifier(alias_pair.as_str()));
                }
            }
            Rule::expr => on = Some(build_expr(p)?),
            other => return Err(unexpected_rule(other, "build_join_clause")),
        }
    }

    Ok(JoinClause {
        join_type: join_type.ok_or_else(|| Error::ParseError("JOIN missing type".to_string()))?,
        table: table.ok_or_else(|| Error::ParseError("JOIN missing table".to_string()))?,
        alias,
        on: on.ok_or_else(|| Error::ParseError("JOIN missing ON expression".to_string()))?,
    })
}

fn build_table_ref(pair: Pair<'_, Rule>) -> Result<FromItem> {
    let mut name = None;
    let mut alias = None;

    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier if name.is_none() => name = Some(parse_identifier(part.as_str())),
            Rule::identifier | Rule::table_alias if alias.is_none() => {
                alias = Some(parse_identifier(part.as_str()))
            }
            other => return Err(unexpected_rule(other, "build_table_ref")),
        }
    }

    let name = name.ok_or_else(|| Error::ParseError("table name missing".to_string()))?;

    Ok(FromItem::Table { name, alias })
}

fn build_graph_table(pair: Pair<'_, Rule>) -> Result<FromItem> {
    let mut graph_name = None;
    let mut pattern = None;
    let mut where_clause = None;
    let mut columns: Vec<GraphTableColumn> = Vec::new();

    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::graph_table_kw => {}
            Rule::identifier if graph_name.is_none() => {
                graph_name = Some(parse_identifier(p.as_str()))
            }
            Rule::graph_match_clause => pattern = Some(build_match_pattern(p)?),
            Rule::graph_where_clause => {
                let expr_pair = p
                    .into_inner()
                    .find(|i| i.as_rule() == Rule::expr)
                    .ok_or_else(|| {
                        Error::ParseError("MATCH WHERE missing expression".to_string())
                    })?;
                where_clause = Some(build_expr(expr_pair)?);
            }
            Rule::columns_clause => columns = build_columns_clause(p)?,
            other => return Err(unexpected_rule(other, "build_graph_table")),
        }
    }

    let graph_name = graph_name
        .ok_or_else(|| Error::ParseError("GRAPH_TABLE requires graph name".to_string()))?;
    let graph_pattern = pattern
        .ok_or_else(|| Error::ParseError("GRAPH_TABLE missing MATCH pattern".to_string()))?;
    let return_cols = columns
        .iter()
        .map(|c| ReturnCol {
            expr: c.expr.clone(),
            alias: Some(c.alias.clone()),
        })
        .collect::<Vec<_>>();

    let match_clause = MatchClause {
        graph_name: Some(graph_name.clone()),
        pattern: graph_pattern,
        where_clause,
        return_cols,
    };

    Ok(FromItem::GraphTable {
        graph_name,
        match_clause,
        columns,
    })
}

fn build_match_pattern(pair: Pair<'_, Rule>) -> Result<GraphPattern> {
    let inner = pair
        .into_inner()
        .find(|p| p.as_rule() == Rule::graph_pattern)
        .ok_or_else(|| Error::ParseError("MATCH pattern missing".to_string()))?;

    let mut nodes_and_edges = inner.into_inner();
    let start_pair = nodes_and_edges
        .next()
        .ok_or_else(|| Error::ParseError("pattern start node missing".to_string()))?;
    let start = build_node_pattern(start_pair)?;

    let mut edges = Vec::new();
    for p in nodes_and_edges {
        if p.as_rule() == Rule::edge_step {
            edges.push(build_edge_step(p)?);
        }
    }

    if edges.is_empty() {
        return Err(Error::ParseError(
            "MATCH requires at least one edge step".to_string(),
        ));
    }

    Ok(GraphPattern { start, edges })
}

fn build_node_pattern(pair: Pair<'_, Rule>) -> Result<NodePattern> {
    let mut alias = None;
    let mut label = None;

    for p in pair.into_inner() {
        if p.as_rule() == Rule::identifier {
            if alias.is_none() {
                alias = Some(parse_identifier(p.as_str()));
            } else if label.is_none() {
                label = Some(parse_identifier(p.as_str()));
            }
        }
    }

    Ok(NodePattern {
        alias: alias.unwrap_or_default(),
        label,
        properties: Vec::new(),
    })
}

fn build_edge_step(pair: Pair<'_, Rule>) -> Result<EdgeStep> {
    let edge = pair
        .into_inner()
        .next()
        .ok_or_else(|| Error::ParseError("edge step missing".to_string()))?;

    let (direction, inner_rule) = match edge.as_rule() {
        Rule::outgoing_edge => (EdgeDirection::Outgoing, edge),
        Rule::incoming_edge => (EdgeDirection::Incoming, edge),
        Rule::both_edge => (EdgeDirection::Both, edge),
        _ => return Err(Error::ParseError("invalid edge direction".to_string())),
    };

    let mut alias = None;
    let mut edge_type = None;
    let mut min_hops = 1_u32;
    let mut max_hops = 1_u32;
    let mut target = None;

    for p in inner_rule.into_inner() {
        match p.as_rule() {
            Rule::edge_bracket => {
                let (a, t) = build_edge_bracket(p)?;
                alias = a;
                edge_type = t;
            }
            Rule::quantifier => {
                let (min, max) = build_quantifier(p)?;
                min_hops = min;
                max_hops = max;
            }
            Rule::node_pattern => target = Some(build_node_pattern(p)?),
            other => return Err(unexpected_rule(other, "build_edge_step")),
        }
    }

    Ok(EdgeStep {
        direction,
        edge_type,
        min_hops,
        max_hops,
        alias,
        target: target.ok_or_else(|| Error::ParseError("edge target node missing".to_string()))?,
    })
}

fn build_edge_bracket(pair: Pair<'_, Rule>) -> Result<(Option<String>, Option<String>)> {
    let mut alias = None;
    let mut edge_type = None;

    for p in pair.into_inner() {
        if p.as_rule() == Rule::edge_spec {
            let raw = p.as_str().trim().to_string();
            let ids: Vec<String> = p
                .into_inner()
                .filter(|i| i.as_rule() == Rule::identifier)
                .map(|i| parse_identifier(i.as_str()))
                .collect();

            if raw.starts_with(':') {
                if let Some(t) = ids.first() {
                    edge_type = Some(t.clone());
                }
            } else if ids.len() == 1 {
                alias = Some(ids[0].clone());
            } else if ids.len() >= 2 {
                alias = Some(ids[0].clone());
                edge_type = Some(ids[1].clone());
            }
        }
    }

    Ok((alias, edge_type))
}

fn build_quantifier(pair: Pair<'_, Rule>) -> Result<(u32, u32)> {
    let inner = pair
        .into_inner()
        .next()
        .ok_or_else(|| Error::ParseError("invalid quantifier".to_string()))?;

    match inner.as_rule() {
        Rule::plus_quantifier | Rule::star_quantifier => Ok((1, 0)),
        Rule::bounded_quantifier => {
            let nums: Vec<u32> = inner
                .into_inner()
                .filter(|p| p.as_rule() == Rule::integer)
                .map(|p| parse_u32(p.as_str(), "invalid quantifier number"))
                .collect::<Result<Vec<_>>>()?;

            if nums.is_empty() {
                return Err(Error::ParseError("invalid quantifier".to_string()));
            }

            let min = nums[0];
            let max = if nums.len() > 1 { nums[1] } else { 0 };
            Ok((min, max))
        }
        _ => Err(Error::ParseError("invalid quantifier".to_string())),
    }
}

fn build_columns_clause(pair: Pair<'_, Rule>) -> Result<Vec<GraphTableColumn>> {
    let mut cols = Vec::new();

    for p in pair.into_inner() {
        if p.as_rule() == Rule::graph_column {
            let mut expr = None;
            let mut alias = None;

            for inner in p.into_inner() {
                match inner.as_rule() {
                    Rule::expr => expr = Some(build_expr(inner)?),
                    Rule::identifier => alias = Some(parse_identifier(inner.as_str())),
                    other => {
                        return Err(unexpected_rule(other, "build_columns_clause.graph_column"));
                    }
                }
            }

            let expr = expr
                .ok_or_else(|| Error::ParseError("COLUMNS item missing expression".to_string()))?;
            let alias = alias.unwrap_or_else(|| match &expr {
                Expr::Column(c) => c.column.clone(),
                _ => "expr".to_string(),
            });
            cols.push(GraphTableColumn { expr, alias });
        }
    }

    Ok(cols)
}

fn build_where_clause(pair: Pair<'_, Rule>) -> Result<Expr> {
    let expr_pair = pair
        .into_inner()
        .find(|p| p.as_rule() == Rule::expr)
        .ok_or_else(|| Error::ParseError("WHERE missing expression".to_string()))?;
    build_expr(expr_pair)
}

fn build_order_by_clause(pair: Pair<'_, Rule>) -> Result<Vec<OrderByItem>> {
    let mut items = Vec::new();
    for p in pair.into_inner() {
        if p.as_rule() == Rule::order_item {
            items.push(build_order_item(p)?);
        }
    }
    Ok(items)
}

fn build_order_item(pair: Pair<'_, Rule>) -> Result<OrderByItem> {
    let mut direction = SortDirection::Asc;
    let mut expr = None;

    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::cosine_expr => {
                let mut it = p.into_inner();
                let left = build_additive_expr(
                    it.next()
                        .ok_or_else(|| Error::ParseError("invalid cosine expr".to_string()))?,
                )?;
                let right = build_additive_expr(
                    it.next()
                        .ok_or_else(|| Error::ParseError("invalid cosine expr".to_string()))?,
                )?;
                expr = Some(Expr::CosineDistance {
                    left: Box::new(left),
                    right: Box::new(right),
                });
                direction = SortDirection::CosineDistance;
            }
            Rule::expr => expr = Some(build_expr(p)?),
            Rule::sort_dir => {
                direction = if p.as_str().eq_ignore_ascii_case("DESC") {
                    SortDirection::Desc
                } else {
                    SortDirection::Asc
                };
            }
            other => return Err(unexpected_rule(other, "build_order_item")),
        }
    }

    Ok(OrderByItem {
        expr: expr
            .ok_or_else(|| Error::ParseError("ORDER BY item missing expression".to_string()))?,
        direction,
    })
}

fn build_limit_clause(pair: Pair<'_, Rule>) -> Result<u64> {
    let num = pair
        .into_inner()
        .find(|p| p.as_rule() == Rule::integer)
        .ok_or_else(|| Error::ParseError("LIMIT missing value".to_string()))?;
    parse_u64(num.as_str(), "invalid LIMIT value")
}

fn build_expr(pair: Pair<'_, Rule>) -> Result<Expr> {
    let inner = pair
        .into_inner()
        .next()
        .ok_or_else(|| Error::ParseError("invalid expression".to_string()))?;
    build_or_expr(inner)
}

fn build_or_expr(pair: Pair<'_, Rule>) -> Result<Expr> {
    let mut inner = pair.into_inner();
    let first = inner
        .next()
        .ok_or_else(|| Error::ParseError("invalid OR expression".to_string()))?;
    let mut expr = build_and_expr(first)?;

    while let Some(op_or_next) = inner.next() {
        if op_or_next.as_rule() == Rule::or_op {
            let rhs_pair = inner
                .next()
                .ok_or_else(|| Error::ParseError("OR missing right operand".to_string()))?;
            let rhs = build_and_expr(rhs_pair)?;
            expr = Expr::BinaryOp {
                left: Box::new(expr),
                op: BinOp::Or,
                right: Box::new(rhs),
            };
        }
    }

    Ok(expr)
}

fn build_and_expr(pair: Pair<'_, Rule>) -> Result<Expr> {
    let mut inner = pair.into_inner();
    let first = inner
        .next()
        .ok_or_else(|| Error::ParseError("invalid AND expression".to_string()))?;
    let mut expr = build_unary_bool_expr(first)?;

    while let Some(op_or_next) = inner.next() {
        if op_or_next.as_rule() == Rule::and_op {
            let rhs_pair = inner
                .next()
                .ok_or_else(|| Error::ParseError("AND missing right operand".to_string()))?;
            let rhs = build_unary_bool_expr(rhs_pair)?;
            expr = Expr::BinaryOp {
                left: Box::new(expr),
                op: BinOp::And,
                right: Box::new(rhs),
            };
        }
    }

    Ok(expr)
}

fn build_unary_bool_expr(pair: Pair<'_, Rule>) -> Result<Expr> {
    let mut not_count = 0usize;
    let mut cmp = None;

    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::not_op => not_count += 1,
            Rule::comparison_expr => cmp = Some(build_comparison_expr(p)?),
            other => return Err(unexpected_rule(other, "build_unary_bool_expr")),
        }
    }

    let mut expr =
        cmp.ok_or_else(|| Error::ParseError("invalid unary boolean expression".to_string()))?;
    for _ in 0..not_count {
        expr = Expr::UnaryOp {
            op: UnaryOp::Not,
            operand: Box::new(expr),
        };
    }
    Ok(expr)
}

fn build_comparison_expr(pair: Pair<'_, Rule>) -> Result<Expr> {
    let mut inner = pair.into_inner();
    let left_pair = inner
        .next()
        .ok_or_else(|| Error::ParseError("comparison missing left operand".to_string()))?;
    let left = build_additive_expr(left_pair)?;

    if let Some(suffix) = inner.next() {
        build_comparison_suffix(left, suffix)
    } else {
        Ok(left)
    }
}

fn build_comparison_suffix(left: Expr, pair: Pair<'_, Rule>) -> Result<Expr> {
    let suffix = pair
        .into_inner()
        .next()
        .ok_or_else(|| Error::ParseError("invalid comparison suffix".to_string()))?;

    match suffix.as_rule() {
        Rule::cmp_suffix => {
            let mut it = suffix.into_inner();
            let op_pair = it
                .next()
                .ok_or_else(|| Error::ParseError("comparison missing operator".to_string()))?;
            let rhs_pair = it
                .next()
                .ok_or_else(|| Error::ParseError("comparison missing right operand".to_string()))?;
            let op = match op_pair.as_str() {
                "=" => BinOp::Eq,
                "!=" | "<>" => BinOp::Neq,
                "<" => BinOp::Lt,
                "<=" => BinOp::Lte,
                ">" => BinOp::Gt,
                ">=" => BinOp::Gte,
                _ => {
                    return Err(Error::ParseError(
                        "unsupported comparison operator".to_string(),
                    ));
                }
            };
            let right = build_additive_expr(rhs_pair)?;
            Ok(Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            })
        }
        Rule::is_null_suffix => {
            let negated = suffix.into_inner().any(|p| p.as_rule() == Rule::not_op);
            Ok(Expr::IsNull {
                expr: Box::new(left),
                negated,
            })
        }
        Rule::like_suffix => {
            let mut negated = false;
            let mut pattern = None;
            for p in suffix.into_inner() {
                match p.as_rule() {
                    Rule::not_op => negated = true,
                    Rule::additive_expr => pattern = Some(build_additive_expr(p)?),
                    other => return Err(unexpected_rule(other, "build_comparison_suffix.like")),
                }
            }
            Ok(Expr::Like {
                expr: Box::new(left),
                pattern: Box::new(
                    pattern.ok_or_else(|| Error::ParseError("LIKE missing pattern".to_string()))?,
                ),
                negated,
            })
        }
        Rule::between_suffix => {
            let mut negated = false;
            let mut vals = Vec::new();
            for p in suffix.into_inner() {
                match p.as_rule() {
                    Rule::not_op => negated = true,
                    Rule::additive_expr => vals.push(build_additive_expr(p)?),
                    other => {
                        return Err(unexpected_rule(other, "build_comparison_suffix.between"));
                    }
                }
            }

            if vals.len() != 2 {
                return Err(Error::ParseError(
                    "BETWEEN requires lower and upper bounds".to_string(),
                ));
            }

            let upper = vals.pop().expect("checked len");
            let lower = vals.pop().expect("checked len");
            let gte = Expr::BinaryOp {
                left: Box::new(left.clone()),
                op: BinOp::Gte,
                right: Box::new(lower),
            };
            let lte = Expr::BinaryOp {
                left: Box::new(left),
                op: BinOp::Lte,
                right: Box::new(upper),
            };
            let between = Expr::BinaryOp {
                left: Box::new(gte),
                op: BinOp::And,
                right: Box::new(lte),
            };

            if negated {
                Ok(Expr::UnaryOp {
                    op: UnaryOp::Not,
                    operand: Box::new(between),
                })
            } else {
                Ok(between)
            }
        }
        Rule::in_suffix => {
            let mut negated = false;
            let mut list = Vec::new();
            let mut subquery = None;

            for p in suffix.into_inner() {
                match p.as_rule() {
                    Rule::not_op => negated = true,
                    Rule::in_contents => {
                        let mut parts = p.into_inner();
                        let first = parts.next().ok_or_else(|| {
                            Error::ParseError("IN list cannot be empty".to_string())
                        })?;
                        match first.as_rule() {
                            Rule::select_core => subquery = Some(build_select_core(first)?),
                            Rule::expr => {
                                list.push(build_expr(first)?);
                                for rest in parts {
                                    if rest.as_rule() == Rule::expr {
                                        list.push(build_expr(rest)?);
                                    }
                                }
                            }
                            _ => return Err(Error::ParseError("invalid IN contents".to_string())),
                        }
                    }
                    other => return Err(unexpected_rule(other, "build_comparison_suffix.in")),
                }
            }

            if let Some(sq) = subquery {
                Ok(Expr::InSubquery {
                    expr: Box::new(left),
                    subquery: Box::new(sq),
                    negated,
                })
            } else {
                Ok(Expr::InList {
                    expr: Box::new(left),
                    list,
                    negated,
                })
            }
        }
        _ => Err(Error::ParseError(
            "unsupported comparison suffix".to_string(),
        )),
    }
}

fn build_additive_expr(pair: Pair<'_, Rule>) -> Result<Expr> {
    let mut inner = pair.into_inner();
    let first = inner
        .next()
        .ok_or_else(|| Error::ParseError("invalid additive expression".to_string()))?;
    let mut expr = build_multiplicative_expr(first)?;

    while let Some(op) = inner.next() {
        let rhs_pair = inner
            .next()
            .ok_or_else(|| Error::ParseError("arithmetic missing right operand".to_string()))?;
        let rhs = build_multiplicative_expr(rhs_pair)?;
        let func = if op.as_str() == "+" { "__add" } else { "__sub" };
        expr = Expr::FunctionCall {
            name: func.to_string(),
            args: vec![expr, rhs],
        };
    }

    Ok(expr)
}

fn build_multiplicative_expr(pair: Pair<'_, Rule>) -> Result<Expr> {
    let mut inner = pair.into_inner();
    let first = inner
        .next()
        .ok_or_else(|| Error::ParseError("invalid multiplicative expression".to_string()))?;
    let mut expr = build_unary_math_expr(first)?;

    while let Some(op) = inner.next() {
        let rhs_pair = inner
            .next()
            .ok_or_else(|| Error::ParseError("arithmetic missing right operand".to_string()))?;
        let rhs = build_unary_math_expr(rhs_pair)?;
        let func = if op.as_str() == "*" { "__mul" } else { "__div" };
        expr = Expr::FunctionCall {
            name: func.to_string(),
            args: vec![expr, rhs],
        };
    }

    Ok(expr)
}

fn build_unary_math_expr(pair: Pair<'_, Rule>) -> Result<Expr> {
    let mut neg_count = 0usize;
    let mut primary = None;

    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::unary_minus => neg_count += 1,
            Rule::primary_expr => primary = Some(build_primary_expr(p)?),
            other => return Err(unexpected_rule(other, "build_unary_math_expr")),
        }
    }

    let mut expr =
        primary.ok_or_else(|| Error::ParseError("invalid unary expression".to_string()))?;
    for _ in 0..neg_count {
        expr = Expr::UnaryOp {
            op: UnaryOp::Neg,
            operand: Box::new(expr),
        };
    }

    Ok(expr)
}

fn build_primary_expr(pair: Pair<'_, Rule>) -> Result<Expr> {
    let mut inner = pair.into_inner();
    let first = inner
        .next()
        .ok_or_else(|| Error::ParseError("invalid primary expression".to_string()))?;

    match first.as_rule() {
        Rule::function_call => build_function_call(first),
        Rule::parameter => Ok(Expr::Parameter(
            first.as_str().trim_start_matches('$').to_string(),
        )),
        Rule::null_lit => Ok(Expr::Literal(Literal::Null)),
        Rule::bool_lit => Ok(Expr::Literal(Literal::Bool(
            first.as_str().eq_ignore_ascii_case("true"),
        ))),
        Rule::float => Ok(Expr::Literal(Literal::Real(parse_f64(
            first.as_str(),
            "invalid float literal",
        )?))),
        Rule::integer => Ok(Expr::Literal(Literal::Integer(parse_i64(
            first.as_str(),
            "invalid integer literal",
        )?))),
        Rule::string => Ok(Expr::Literal(Literal::Text(parse_string_literal(
            first.as_str(),
        )))),
        Rule::vector_lit => {
            let values: Vec<f32> = first
                .into_inner()
                .map(|p| {
                    p.as_str()
                        .parse::<f32>()
                        .map_err(|_| Error::ParseError("invalid vector component".to_string()))
                })
                .collect::<Result<_>>()?;
            Ok(Expr::Literal(Literal::Vector(values)))
        }
        Rule::column_ref => build_column_ref(first),
        Rule::expr => build_expr(first),
        _ => Err(Error::ParseError(
            "unsupported primary expression".to_string(),
        )),
    }
}

fn build_function_call(pair: Pair<'_, Rule>) -> Result<Expr> {
    let mut name = None;
    let mut args = Vec::new();

    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::identifier if name.is_none() => name = Some(parse_identifier(p.as_str())),
            Rule::star => args.push(Expr::Column(ColumnRef {
                table: None,
                column: "*".to_string(),
            })),
            Rule::expr => args.push(build_expr(p)?),
            other => return Err(unexpected_rule(other, "build_function_call")),
        }
    }

    Ok(Expr::FunctionCall {
        name: name.ok_or_else(|| Error::ParseError("function name missing".to_string()))?,
        args,
    })
}

fn build_column_ref(pair: Pair<'_, Rule>) -> Result<Expr> {
    let ids: Vec<String> = pair
        .into_inner()
        .filter(|p| p.as_rule() == Rule::identifier)
        .map(|p| parse_identifier(p.as_str()))
        .collect();

    match ids.as_slice() {
        [column] => Ok(Expr::Column(ColumnRef {
            table: None,
            column: column.clone(),
        })),
        [table, column] => Ok(Expr::Column(ColumnRef {
            table: Some(table.clone()),
            column: column.clone(),
        })),
        _ => Err(Error::ParseError("invalid column reference".to_string())),
    }
}

fn build_create_table(pair: Pair<'_, Rule>) -> Result<CreateTable> {
    let mut name = None;
    let mut if_not_exists = false;
    let mut columns = Vec::new();
    let mut unique_constraints = Vec::new();
    let mut immutable = false;
    let mut state_machine = None;
    let mut dag_edge_types = Vec::new();
    let mut propagation_rules = Vec::new();
    let mut has_propagation = false;
    let mut retain = None;

    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::if_not_exists => if_not_exists = true,
            Rule::identifier if name.is_none() => name = Some(parse_identifier(p.as_str())),
            Rule::table_element => {
                let element = p
                    .into_inner()
                    .next()
                    .ok_or_else(|| Error::ParseError("invalid table element".to_string()))?;
                match element.as_rule() {
                    Rule::column_def => {
                        let (col, inline_sm) = build_column_def(element)?;
                        if col
                            .references
                            .as_ref()
                            .is_some_and(|fk| !fk.propagation_rules.is_empty())
                        {
                            has_propagation = true;
                        }
                        columns.push(col);
                        if let Some(sm) = inline_sm {
                            if state_machine.is_some() {
                                return Err(Error::ParseError(
                                    "duplicate STATE MACHINE clause".to_string(),
                                ));
                            }
                            state_machine = Some(sm);
                        }
                    }
                    Rule::unique_table_constraint => {
                        unique_constraints.push(build_unique_table_constraint(element)?);
                    }
                    other => {
                        return Err(unexpected_rule(other, "build_create_table.table_element"));
                    }
                }
            }
            Rule::table_option => {
                let opt = p
                    .into_inner()
                    .next()
                    .ok_or_else(|| Error::ParseError("invalid table option".to_string()))?;
                match opt.as_rule() {
                    Rule::immutable_option => {
                        if immutable {
                            return Err(Error::ParseError(
                                "duplicate IMMUTABLE clause".to_string(),
                            ));
                        }
                        immutable = true;
                    }
                    Rule::state_machine_option => {
                        if state_machine.is_some() {
                            return Err(Error::ParseError(
                                "duplicate STATE MACHINE clause".to_string(),
                            ));
                        }
                        state_machine = Some(build_state_machine_option(opt)?)
                    }
                    Rule::dag_option => {
                        if !dag_edge_types.is_empty() {
                            return Err(Error::ParseError("duplicate DAG clause".to_string()));
                        }
                        dag_edge_types = build_dag_option(opt)?;
                    }
                    Rule::propagate_edge_option => {
                        has_propagation = true;
                        propagation_rules.push(build_edge_propagation_option(opt)?);
                    }
                    Rule::propagate_state_option => {
                        has_propagation = true;
                        propagation_rules.push(build_vector_propagation_option(opt)?);
                    }
                    Rule::retain_option => {
                        if retain.is_some() {
                            return Err(Error::ParseError("duplicate RETAIN clause".to_string()));
                        }
                        retain = Some(build_retain_option(opt)?);
                    }
                    other => return Err(unexpected_rule(other, "build_create_table.table_option")),
                }
            }
            other => return Err(unexpected_rule(other, "build_create_table")),
        }
    }

    let options_count = [
        immutable,
        state_machine.is_some(),
        !dag_edge_types.is_empty(),
    ]
    .into_iter()
    .filter(|v| *v)
    .count();

    if options_count > 1 {
        return Err(Error::ParseError(
            "IMMUTABLE, STATE MACHINE, and DAG cannot be used together".to_string(),
        ));
    }

    if has_propagation && (immutable || !dag_edge_types.is_empty()) {
        return Err(Error::ParseError(
            "propagation clauses require STATE MACHINE tables".to_string(),
        ));
    }

    if immutable && retain.is_some() {
        return Err(Error::ParseError(
            "IMMUTABLE and RETAIN are mutually exclusive".to_string(),
        ));
    }

    // A column declared both in the STATE MACHINE status position AND IMMUTABLE is
    // contradictory — STATE MACHINE permits transitions, IMMUTABLE refuses them.
    if let Some(sm) = &state_machine
        && let Some(col) = columns.iter().find(|c| c.name == sm.column)
        && col.immutable
    {
        return Err(Error::ParseError(format!(
            "column '{}' cannot be both IMMUTABLE and the STATE MACHINE status column",
            sm.column
        )));
    }

    // Propagation rules that write into a column (edge propagation and
    // FK propagation `PROPAGATE SET <col>`) cannot target a column declared
    // IMMUTABLE on the same table.
    for rule in &propagation_rules {
        if let AstPropagationRule::EdgeState { target_state, .. } = rule
            && let Some(col) = columns.iter().find(|c| c.name == *target_state)
            && col.immutable
        {
            return Err(Error::ParseError(format!(
                "propagation rule cannot target column '{}' declared IMMUTABLE",
                target_state
            )));
        }
    }
    for col in &columns {
        let Some(fk) = &col.references else { continue };
        for rule in &fk.propagation_rules {
            if let AstPropagationRule::FkState { target_state, .. } = rule
                && let Some(target_col) = columns.iter().find(|c| c.name == *target_state)
                && target_col.immutable
            {
                return Err(Error::ParseError(format!(
                    "FK propagation rule cannot target column '{}' declared IMMUTABLE",
                    target_state
                )));
            }
        }
    }

    for columns_in_constraint in &unique_constraints {
        for column_name in columns_in_constraint {
            if !columns.iter().any(|column| column.name == *column_name) {
                return Err(Error::ParseError(format!(
                    "UNIQUE constraint references unknown column '{}'",
                    column_name
                )));
            }
        }
    }

    Ok(CreateTable {
        name: name.ok_or_else(|| Error::ParseError("missing table name".to_string()))?,
        columns,
        unique_constraints,
        if_not_exists,
        immutable,
        state_machine,
        dag_edge_types,
        propagation_rules,
        retain,
    })
}

fn build_alter_table(pair: Pair<'_, Rule>) -> Result<AlterTable> {
    let mut table = None;
    let mut action = None;

    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::identifier if table.is_none() => table = Some(parse_identifier(p.as_str())),
            Rule::alter_action => action = Some(build_alter_action(p)?),
            other => return Err(unexpected_rule(other, "build_alter_table")),
        }
    }

    Ok(AlterTable {
        table: table.ok_or_else(|| Error::ParseError("missing table name".to_string()))?,
        action: action
            .ok_or_else(|| Error::ParseError("missing ALTER TABLE action".to_string()))?,
    })
}

fn build_alter_action(pair: Pair<'_, Rule>) -> Result<AlterAction> {
    let action = pair
        .into_inner()
        .next()
        .ok_or_else(|| Error::ParseError("missing ALTER TABLE action".to_string()))?;

    match action.as_rule() {
        Rule::add_column_action => {
            let (column, _) = action
                .into_inner()
                .find(|part| part.as_rule() == Rule::column_def)
                .ok_or_else(|| {
                    Error::ParseError("ADD COLUMN missing column definition".to_string())
                })
                .and_then(build_column_def)?;
            Ok(AlterAction::AddColumn(column))
        }
        Rule::drop_column_action => {
            let mut column: Option<String> = None;
            let mut cascade = false;
            for part in action.into_inner() {
                match part.as_rule() {
                    Rule::identifier if column.is_none() => {
                        column = Some(parse_identifier(part.as_str()));
                    }
                    Rule::drop_column_modifier => {
                        let token = part.as_str().to_ascii_uppercase();
                        if token == "CASCADE" {
                            cascade = true;
                        }
                        // RESTRICT is the default; no-op.
                    }
                    other => return Err(unexpected_rule(other, "build_alter_action/drop_column")),
                }
            }
            let column = column
                .ok_or_else(|| Error::ParseError("DROP COLUMN missing column name".to_string()))?;
            Ok(AlterAction::DropColumn { column, cascade })
        }
        Rule::rename_column_action => {
            let mut identifiers = action
                .into_inner()
                .filter(|part| part.as_rule() == Rule::identifier)
                .map(|part| parse_identifier(part.as_str()));
            let from = identifiers.next().ok_or_else(|| {
                Error::ParseError("RENAME COLUMN missing source name".to_string())
            })?;
            let to = identifiers.next().ok_or_else(|| {
                Error::ParseError("RENAME COLUMN missing target name".to_string())
            })?;
            Ok(AlterAction::RenameColumn { from, to })
        }
        Rule::set_retain_action => {
            let retain = build_retain_option(action)?;
            Ok(AlterAction::SetRetain {
                duration_seconds: retain.duration_seconds,
                sync_safe: retain.sync_safe,
            })
        }
        Rule::drop_retain_action => Ok(AlterAction::DropRetain),
        Rule::set_table_conflict_policy => {
            let policy = action
                .into_inner()
                .find(|p| p.as_rule() == Rule::conflict_policy_value)
                .ok_or_else(|| Error::ParseError("missing conflict policy value".to_string()))?
                .as_str()
                .to_lowercase();
            Ok(AlterAction::SetSyncConflictPolicy(policy))
        }
        Rule::drop_table_conflict_policy => Ok(AlterAction::DropSyncConflictPolicy),
        _ => Err(Error::ParseError(
            "unsupported ALTER TABLE action".to_string(),
        )),
    }
}

fn build_column_def(pair: Pair<'_, Rule>) -> Result<(ColumnDef, Option<StateMachineDef>)> {
    let mut name = None;
    let mut data_type = None;
    let mut nullable = true;
    let mut primary_key = false;
    let mut unique = false;
    let mut default = None;
    let mut references = None;
    let mut fk_propagation_rules = Vec::new();
    let mut inline_state_machine = None;
    let mut expires = false;
    let mut immutable_flag = false;
    // Track if we saw the type token before column_constraints. If IMMUTABLE appears
    // as the column name (i.e. before the data_type position), Pest will parse it as
    // the identifier rule; we detect that case by the column name.
    let mut column_name_text: Option<String> = None;

    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::identifier if name.is_none() => {
                let ident = parse_identifier(p.as_str());
                column_name_text = Some(ident.clone());
                name = Some(ident);
            }
            Rule::data_type => data_type = Some(build_data_type(p)?),
            Rule::column_constraint => {
                let c = p
                    .into_inner()
                    .next()
                    .ok_or_else(|| Error::ParseError("invalid column constraint".to_string()))?;
                match c.as_rule() {
                    Rule::not_null => {
                        if !nullable {
                            return Err(Error::ParseError(
                                "duplicate NOT NULL constraint".to_string(),
                            ));
                        }
                        nullable = false;
                    }
                    Rule::nullable_marker => {
                        // Explicit NULL — column remains nullable. Idempotent.
                    }
                    Rule::primary_key => {
                        if primary_key {
                            return Err(Error::ParseError(
                                "duplicate PRIMARY KEY constraint".to_string(),
                            ));
                        }
                        primary_key = true;
                    }
                    Rule::unique => {
                        if unique {
                            return Err(Error::ParseError(
                                "duplicate UNIQUE constraint".to_string(),
                            ));
                        }
                        unique = true;
                    }
                    Rule::default_clause => {
                        if default.is_some() {
                            return Err(Error::ParseError("duplicate DEFAULT clause".to_string()));
                        }
                        let expr = c
                            .into_inner()
                            .find(|i| i.as_rule() == Rule::expr)
                            .ok_or_else(|| {
                                Error::ParseError("DEFAULT missing expression".to_string())
                            })?;
                        default = Some(build_expr(expr)?);
                    }
                    Rule::references_clause => {
                        if references.is_some() {
                            return Err(Error::ParseError(
                                "duplicate REFERENCES clause".to_string(),
                            ));
                        }
                        references = Some(build_references_clause(c)?);
                    }
                    Rule::fk_propagation_clause => {
                        fk_propagation_rules.push(build_fk_propagation_clause(c)?);
                    }
                    Rule::expires_constraint => {
                        if expires {
                            return Err(Error::ParseError(
                                "duplicate EXPIRES constraint".to_string(),
                            ));
                        }
                        expires = true;
                    }
                    Rule::immutable_constraint => {
                        if immutable_flag {
                            let col = column_name_text.as_deref().unwrap_or("column");
                            return Err(Error::ParseError(format!(
                                "duplicate IMMUTABLE constraint on column '{col}'"
                            )));
                        }
                        immutable_flag = true;
                    }
                    Rule::state_machine_option => {
                        if inline_state_machine.is_some() {
                            return Err(Error::ParseError(
                                "duplicate STATE MACHINE clause".to_string(),
                            ));
                        }
                        inline_state_machine = Some(build_state_machine_option(c)?);
                    }
                    other => {
                        return Err(unexpected_rule(other, "build_column_def.column_constraint"));
                    }
                }
            }
            other => return Err(unexpected_rule(other, "build_column_def")),
        }
    }

    if !fk_propagation_rules.is_empty() {
        let fk = references.as_mut().ok_or_else(|| {
            Error::ParseError("FK propagation requires REFERENCES constraint".to_string())
        })?;
        fk.propagation_rules = fk_propagation_rules;
    }

    Ok((
        ColumnDef {
            name: name.ok_or_else(|| Error::ParseError("column name missing".to_string()))?,
            data_type: data_type
                .ok_or_else(|| Error::ParseError("column type missing".to_string()))?,
            nullable,
            primary_key,
            unique,
            default,
            references,
            expires,
            immutable: immutable_flag,
        },
        inline_state_machine,
    ))
}

fn build_unique_table_constraint(pair: Pair<'_, Rule>) -> Result<Vec<String>> {
    let columns: Vec<String> = pair
        .into_inner()
        .filter(|part| part.as_rule() == Rule::identifier)
        .map(|part| parse_identifier(part.as_str()))
        .collect();

    if columns.len() < 2 {
        return Err(Error::ParseError(
            "table-level UNIQUE requires at least two columns".to_string(),
        ));
    }

    let mut seen = std::collections::HashSet::new();
    for column in &columns {
        if !seen.insert(column.clone()) {
            return Err(Error::ParseError(format!(
                "duplicate column '{}' in UNIQUE constraint",
                column
            )));
        }
    }

    Ok(columns)
}

fn build_retain_option(pair: Pair<'_, Rule>) -> Result<RetainOption> {
    let mut amount = None;
    let mut unit = None;
    let mut sync_safe = false;

    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::integer => {
                amount = Some(part.as_str().parse::<u64>().map_err(|err| {
                    Error::ParseError(format!(
                        "invalid RETAIN duration '{}': {err}",
                        part.as_str()
                    ))
                })?);
            }
            Rule::retain_unit => unit = Some(part.as_str().to_ascii_uppercase()),
            Rule::sync_safe_option => sync_safe = true,
            other => return Err(unexpected_rule(other, "build_retain_option")),
        }
    }

    let amount = amount.ok_or_else(|| Error::ParseError("RETAIN missing duration".to_string()))?;
    let unit = unit.ok_or_else(|| Error::ParseError("RETAIN missing unit".to_string()))?;
    let duration_seconds = match unit.as_str() {
        "SECONDS" | "SECOND" => amount,
        "MINUTES" | "MINUTE" => amount.saturating_mul(60),
        "HOURS" | "HOUR" => amount.saturating_mul(60 * 60),
        "DAYS" | "DAY" => amount.saturating_mul(24 * 60 * 60),
        _ => {
            return Err(Error::ParseError(format!(
                "unsupported RETAIN unit: {unit}"
            )));
        }
    };

    Ok(RetainOption {
        duration_seconds,
        sync_safe,
    })
}

fn build_references_clause(pair: Pair<'_, Rule>) -> Result<ForeignKey> {
    let ids: Vec<String> = pair
        .into_inner()
        .filter(|p| p.as_rule() == Rule::identifier)
        .map(|p| parse_identifier(p.as_str()))
        .collect();

    if ids.len() < 2 {
        return Err(Error::ParseError(
            "REFERENCES requires table and column".to_string(),
        ));
    }

    Ok(ForeignKey {
        table: ids[0].clone(),
        column: ids[1].clone(),
        propagation_rules: Vec::new(),
    })
}

fn build_fk_propagation_clause(pair: Pair<'_, Rule>) -> Result<AstPropagationRule> {
    let mut trigger_state = None;
    let mut target_state = None;
    let mut max_depth = None;
    let mut abort_on_failure = false;

    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::identifier if trigger_state.is_none() => {
                trigger_state = Some(parse_identifier(p.as_str()))
            }
            Rule::identifier if target_state.is_none() => {
                target_state = Some(parse_identifier(p.as_str()))
            }
            Rule::max_depth_clause => max_depth = Some(parse_max_depth_clause(p)?),
            Rule::abort_on_failure_clause => abort_on_failure = true,
            other => return Err(unexpected_rule(other, "build_fk_propagation_clause")),
        }
    }

    Ok(AstPropagationRule::FkState {
        trigger_state: trigger_state
            .ok_or_else(|| Error::ParseError("FK propagation missing trigger state".to_string()))?,
        target_state: target_state
            .ok_or_else(|| Error::ParseError("FK propagation missing target state".to_string()))?,
        max_depth,
        abort_on_failure,
    })
}

fn build_edge_propagation_option(pair: Pair<'_, Rule>) -> Result<AstPropagationRule> {
    let mut edge_type = None;
    let mut direction = None;
    let mut trigger_state = None;
    let mut target_state = None;
    let mut max_depth = None;
    let mut abort_on_failure = false;

    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::identifier if edge_type.is_none() => {
                edge_type = Some(parse_identifier(p.as_str()))
            }
            Rule::direction_kw => direction = Some(parse_identifier(p.as_str())),
            Rule::identifier if trigger_state.is_none() => {
                trigger_state = Some(parse_identifier(p.as_str()))
            }
            Rule::identifier if target_state.is_none() => {
                target_state = Some(parse_identifier(p.as_str()))
            }
            Rule::max_depth_clause => max_depth = Some(parse_max_depth_clause(p)?),
            Rule::abort_on_failure_clause => abort_on_failure = true,
            other => return Err(unexpected_rule(other, "build_edge_propagation_option")),
        }
    }

    Ok(AstPropagationRule::EdgeState {
        edge_type: edge_type
            .ok_or_else(|| Error::ParseError("EDGE propagation missing edge type".to_string()))?,
        direction: direction
            .ok_or_else(|| Error::ParseError("EDGE propagation missing direction".to_string()))?,
        trigger_state: trigger_state.ok_or_else(|| {
            Error::ParseError("EDGE propagation missing trigger state".to_string())
        })?,
        target_state: target_state.ok_or_else(|| {
            Error::ParseError("EDGE propagation missing target state".to_string())
        })?,
        max_depth,
        abort_on_failure,
    })
}

fn build_vector_propagation_option(pair: Pair<'_, Rule>) -> Result<AstPropagationRule> {
    let trigger_state = pair
        .into_inner()
        .find(|p| p.as_rule() == Rule::identifier)
        .map(|p| parse_identifier(p.as_str()))
        .ok_or_else(|| Error::ParseError("VECTOR propagation missing trigger state".to_string()))?;

    Ok(AstPropagationRule::VectorExclusion { trigger_state })
}

fn parse_max_depth_clause(pair: Pair<'_, Rule>) -> Result<u32> {
    let depth = pair
        .into_inner()
        .find(|p| p.as_rule() == Rule::integer)
        .ok_or_else(|| Error::ParseError("MAX DEPTH missing value".to_string()))?;
    parse_u32(depth.as_str(), "invalid MAX DEPTH value")
}

fn build_data_type(pair: Pair<'_, Rule>) -> Result<DataType> {
    let txt = pair.as_str().to_string();
    let mut inner = pair.into_inner();
    if let Some(v) = inner.find(|p| p.as_rule() == Rule::vector_type) {
        let dim = v
            .into_inner()
            .find(|p| p.as_rule() == Rule::integer)
            .ok_or_else(|| Error::ParseError("VECTOR dimension missing".to_string()))?;
        let dim = parse_u32(dim.as_str(), "invalid VECTOR dimension")?;
        return Ok(DataType::Vector(dim));
    }

    if txt.eq_ignore_ascii_case("UUID") {
        Ok(DataType::Uuid)
    } else if txt.eq_ignore_ascii_case("TEXT") {
        Ok(DataType::Text)
    } else if txt.eq_ignore_ascii_case("INTEGER") || txt.eq_ignore_ascii_case("INT") {
        Ok(DataType::Integer)
    } else if txt.eq_ignore_ascii_case("REAL") || txt.eq_ignore_ascii_case("FLOAT") {
        Ok(DataType::Real)
    } else if txt.eq_ignore_ascii_case("BOOLEAN") || txt.eq_ignore_ascii_case("BOOL") {
        Ok(DataType::Boolean)
    } else if txt.eq_ignore_ascii_case("TIMESTAMP") {
        Ok(DataType::Timestamp)
    } else if txt.eq_ignore_ascii_case("JSON") {
        Ok(DataType::Json)
    } else if txt.eq_ignore_ascii_case("TXID") {
        Ok(DataType::TxId)
    } else {
        Err(Error::ParseError(format!("unsupported data type: {txt}")))
    }
}

fn build_state_machine_option(pair: Pair<'_, Rule>) -> Result<StateMachineDef> {
    let entries = pair
        .into_inner()
        .find(|p| p.as_rule() == Rule::state_machine_entries)
        .ok_or_else(|| Error::ParseError("invalid STATE MACHINE clause".to_string()))?;

    let mut column = None;
    let mut transitions: Vec<(String, Vec<String>)> = Vec::new();

    for entry in entries
        .into_inner()
        .filter(|p| p.as_rule() == Rule::state_machine_entry)
    {
        let has_column_prefix = entry.as_str().contains(':');
        let ids: Vec<String> = entry
            .into_inner()
            .filter(|p| p.as_rule() == Rule::identifier)
            .map(|p| parse_identifier(p.as_str()))
            .collect();

        if ids.len() < 2 {
            return Err(Error::ParseError(
                "invalid STATE MACHINE transition".to_string(),
            ));
        }

        let (from, to_targets) = if has_column_prefix {
            if column.is_none() {
                column = Some(ids[0].clone());
            }
            (ids[1].clone(), ids[2..].to_vec())
        } else {
            (ids[0].clone(), ids[1..].to_vec())
        };

        if let Some((_, existing)) = transitions.iter_mut().find(|(src, _)| src == &from) {
            for t in to_targets {
                if !existing.iter().any(|v| v == &t) {
                    existing.push(t);
                }
            }
        } else {
            transitions.push((from, to_targets));
        }
    }

    Ok(StateMachineDef {
        column: column.unwrap_or_else(|| "status".to_string()),
        transitions,
    })
}

fn build_dag_option(pair: Pair<'_, Rule>) -> Result<Vec<String>> {
    let edge_types = pair
        .into_inner()
        .filter(|p| p.as_rule() == Rule::string)
        .map(|p| parse_string_literal(p.as_str()))
        .collect::<Vec<_>>();

    if edge_types.is_empty() {
        return Err(Error::ParseError(
            "DAG requires at least one edge type".to_string(),
        ));
    }

    Ok(edge_types)
}

fn build_drop_table(pair: Pair<'_, Rule>) -> Result<DropTable> {
    let mut if_exists = false;
    let mut name = None;

    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::if_exists => if_exists = true,
            Rule::identifier => name = Some(parse_identifier(p.as_str())),
            other => return Err(unexpected_rule(other, "build_drop_table")),
        }
    }

    Ok(DropTable {
        name: name.ok_or_else(|| Error::ParseError("missing table name".to_string()))?,
        if_exists,
    })
}

fn build_create_index(pair: Pair<'_, Rule>) -> Result<CreateIndex> {
    let mut name: Option<String> = None;
    let mut table: Option<String> = None;
    let mut columns: Vec<(String, SortDirection)> = Vec::new();

    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::identifier if name.is_none() => {
                name = Some(parse_identifier(p.as_str()));
            }
            Rule::identifier if table.is_none() => {
                table = Some(parse_identifier(p.as_str()));
            }
            Rule::indexed_column => {
                let mut col_name: Option<String> = None;
                let mut direction = SortDirection::Asc;
                for inner in p.into_inner() {
                    match inner.as_rule() {
                        Rule::identifier if col_name.is_none() => {
                            col_name = Some(parse_identifier(inner.as_str()));
                        }
                        Rule::index_sort_direction => {
                            let token = inner.as_str().to_ascii_uppercase();
                            direction = if token == "DESC" {
                                SortDirection::Desc
                            } else {
                                SortDirection::Asc
                            };
                        }
                        other => return Err(unexpected_rule(other, "build_create_index/column")),
                    }
                }
                let col = col_name
                    .ok_or_else(|| Error::ParseError("CREATE INDEX missing column".to_string()))?;
                columns.push((col, direction));
            }
            other => return Err(unexpected_rule(other, "build_create_index")),
        }
    }

    Ok(CreateIndex {
        name: name.ok_or_else(|| Error::ParseError("CREATE INDEX missing name".to_string()))?,
        table: table.ok_or_else(|| Error::ParseError("CREATE INDEX missing table".to_string()))?,
        columns,
    })
}

fn build_drop_index(pair: Pair<'_, Rule>) -> Result<DropIndex> {
    let mut if_exists = false;
    let mut idents: Vec<String> = Vec::new();
    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::if_exists => if_exists = true,
            Rule::identifier => idents.push(parse_identifier(p.as_str())),
            other => return Err(unexpected_rule(other, "build_drop_index")),
        }
    }
    if idents.len() < 2 {
        return Err(Error::ParseError(
            "DROP INDEX requires `<index_name> ON <table>`".to_string(),
        ));
    }
    Ok(DropIndex {
        name: idents[0].clone(),
        table: idents[1].clone(),
        if_exists,
    })
}

fn build_insert(pair: Pair<'_, Rule>) -> Result<Insert> {
    let mut table = None;
    let mut columns = Vec::new();
    let mut values = Vec::new();
    let mut on_conflict = None;
    let mut seen_table = false;

    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::identifier if !seen_table => {
                table = Some(parse_identifier(p.as_str()));
                seen_table = true;
            }
            Rule::identifier => columns.push(parse_identifier(p.as_str())),
            Rule::values_row => values.push(build_values_row(p)?),
            Rule::on_conflict_clause => on_conflict = Some(build_on_conflict(p)?),
            other => return Err(unexpected_rule(other, "build_insert")),
        }
    }

    Ok(Insert {
        table: table.ok_or_else(|| Error::ParseError("INSERT missing table".to_string()))?,
        columns,
        values,
        on_conflict,
    })
}

fn build_values_row(pair: Pair<'_, Rule>) -> Result<Vec<Expr>> {
    pair.into_inner()
        .filter(|p| p.as_rule() == Rule::expr)
        .map(build_expr)
        .collect()
}

fn build_on_conflict(pair: Pair<'_, Rule>) -> Result<OnConflict> {
    let mut columns = Vec::new();
    let mut update_columns = Vec::new();

    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::identifier => columns.push(parse_identifier(p.as_str())),
            Rule::assignment => update_columns.push(build_assignment(p)?),
            other => return Err(unexpected_rule(other, "build_on_conflict")),
        }
    }

    Ok(OnConflict {
        columns,
        update_columns,
    })
}

fn build_assignment(pair: Pair<'_, Rule>) -> Result<(String, Expr)> {
    let mut name = None;
    let mut value = None;

    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::identifier if name.is_none() => name = Some(parse_identifier(p.as_str())),
            Rule::expr => value = Some(build_expr(p)?),
            other => return Err(unexpected_rule(other, "build_assignment")),
        }
    }

    Ok((
        name.ok_or_else(|| Error::ParseError("assignment missing column".to_string()))?,
        value.ok_or_else(|| Error::ParseError("assignment missing value".to_string()))?,
    ))
}

fn build_delete(pair: Pair<'_, Rule>) -> Result<Delete> {
    let mut table = None;
    let mut where_clause = None;

    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::identifier => table = Some(parse_identifier(p.as_str())),
            Rule::where_clause => where_clause = Some(build_where_clause(p)?),
            other => return Err(unexpected_rule(other, "build_delete")),
        }
    }

    Ok(Delete {
        table: table.ok_or_else(|| Error::ParseError("DELETE missing table".to_string()))?,
        where_clause,
    })
}

fn build_update(pair: Pair<'_, Rule>) -> Result<Update> {
    let mut table = None;
    let mut assignments = Vec::new();
    let mut where_clause = None;

    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::identifier if table.is_none() => table = Some(parse_identifier(p.as_str())),
            Rule::assignment => assignments.push(build_assignment(p)?),
            Rule::where_clause => where_clause = Some(build_where_clause(p)?),
            other => return Err(unexpected_rule(other, "build_update")),
        }
    }

    Ok(Update {
        table: table.ok_or_else(|| Error::ParseError("UPDATE missing table".to_string()))?,
        assignments,
        where_clause,
    })
}

fn validate_statement(stmt: &Statement) -> Result<()> {
    if let Statement::Select(sel) = stmt {
        validate_select(sel)?;
    }
    Ok(())
}

fn validate_select(sel: &SelectStatement) -> Result<()> {
    for cte in &sel.ctes {
        if let Cte::SqlCte { query, .. } = cte {
            validate_select_body(query)?;
        }
    }

    validate_select_body(&sel.body)?;

    let cte_names = sel
        .ctes
        .iter()
        .map(|c| match c {
            Cte::SqlCte { name, .. } | Cte::MatchCte { name, .. } => name.as_str(),
        })
        .collect::<Vec<_>>();

    if let Some(expr) = &sel.body.where_clause {
        validate_subquery_expr(expr, &cte_names)?;
    }

    Ok(())
}

fn validate_select_body(body: &SelectBody) -> Result<()> {
    if body
        .order_by
        .iter()
        .any(|o| matches!(o.direction, SortDirection::CosineDistance))
        && body.limit.is_none()
    {
        return Err(Error::UnboundedVectorSearch);
    }

    for from in &body.from {
        if let FromItem::GraphTable { match_clause, .. } = from {
            validate_match_clause(match_clause)?;
        }
    }

    if let Some(expr) = &body.where_clause {
        validate_expr(expr)?;
    }

    Ok(())
}

fn validate_match_clause(mc: &MatchClause) -> Result<()> {
    if mc.graph_name.as_ref().is_none_or(|g| g.trim().is_empty()) {
        return Err(Error::ParseError(
            "GRAPH_TABLE requires graph name".to_string(),
        ));
    }
    if mc.pattern.start.alias.trim().is_empty() {
        return Err(Error::ParseError(
            "MATCH start node alias is required".to_string(),
        ));
    }

    for edge in &mc.pattern.edges {
        if edge.min_hops == 0 && edge.max_hops == 0 {
            return Err(Error::UnboundedTraversal);
        }
        if edge.max_hops == 0 {
            return Err(Error::UnboundedTraversal);
        }
        if edge.min_hops == 0 {
            return Err(Error::ParseError(
                "graph quantifier minimum hop must be >= 1".to_string(),
            ));
        }
        if edge.min_hops > edge.max_hops {
            return Err(Error::ParseError(
                "graph quantifier minimum cannot exceed maximum".to_string(),
            ));
        }
        if edge.max_hops > 10 {
            return Err(Error::BfsDepthExceeded(edge.max_hops));
        }
    }

    if let Some(expr) = &mc.where_clause {
        validate_expr(expr)?;
    }

    Ok(())
}

fn validate_expr(expr: &Expr) -> Result<()> {
    match expr {
        Expr::InSubquery { subquery, .. } => {
            if subquery.from.is_empty() {
                return Err(Error::SubqueryNotSupported);
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            validate_expr(left)?;
            validate_expr(right)?;
        }
        Expr::UnaryOp { operand, .. } => validate_expr(operand)?,
        Expr::InList { expr, list, .. } => {
            validate_expr(expr)?;
            for item in list {
                validate_expr(item)?;
            }
        }
        Expr::Like { expr, pattern, .. } => {
            validate_expr(expr)?;
            validate_expr(pattern)?;
        }
        Expr::IsNull { expr, .. } => validate_expr(expr)?,
        Expr::CosineDistance { left, right } => {
            validate_expr(left)?;
            validate_expr(right)?;
        }
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                validate_expr(arg)?;
            }
        }
        Expr::Column(_) | Expr::Literal(_) | Expr::Parameter(_) => {}
    }
    Ok(())
}

fn validate_subquery_expr(expr: &Expr, cte_names: &[&str]) -> Result<()> {
    match expr {
        Expr::InSubquery { subquery, .. } => {
            if subquery.columns.len() != 1 || subquery.from.is_empty() {
                return Err(Error::SubqueryNotSupported);
            }

            let referenced = subquery.from.iter().find_map(|f| match f {
                FromItem::Table { name, .. } => Some(name.as_str()),
                FromItem::GraphTable { .. } => None,
            });
            if let Some(name) = referenced {
                if cte_names.iter().any(|n| n.eq_ignore_ascii_case(name)) {
                    return Ok(());
                }
                return Ok(());
            }
            return Err(Error::SubqueryNotSupported);
        }
        Expr::BinaryOp { left, right, .. } => {
            validate_subquery_expr(left, cte_names)?;
            validate_subquery_expr(right, cte_names)?;
        }
        Expr::UnaryOp { operand, .. } => validate_subquery_expr(operand, cte_names)?,
        Expr::InList { expr, list, .. } => {
            validate_subquery_expr(expr, cte_names)?;
            for item in list {
                validate_subquery_expr(item, cte_names)?;
            }
        }
        Expr::Like { expr, pattern, .. } => {
            validate_subquery_expr(expr, cte_names)?;
            validate_subquery_expr(pattern, cte_names)?;
        }
        Expr::IsNull { expr, .. } => validate_subquery_expr(expr, cte_names)?,
        Expr::CosineDistance { left, right } => {
            validate_subquery_expr(left, cte_names)?;
            validate_subquery_expr(right, cte_names)?;
        }
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                validate_subquery_expr(arg, cte_names)?;
            }
        }
        Expr::Column(_) | Expr::Literal(_) | Expr::Parameter(_) => {}
    }

    Ok(())
}

fn unexpected_rule(rule: Rule, context: &str) -> Error {
    Error::ParseError(format!("unexpected rule {:?} in {}", rule, context))
}

fn parse_identifier(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.len() >= 2 && trimmed.starts_with('"') && trimmed.ends_with('"') {
        trimmed[1..trimmed.len() - 1].replace("\"\"", "\"")
    } else {
        trimmed.to_string()
    }
}

fn parse_string_literal(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.len() >= 2 && trimmed.starts_with('\'') && trimmed.ends_with('\'') {
        trimmed[1..trimmed.len() - 1].replace("''", "'")
    } else {
        trimmed.to_string()
    }
}

fn parse_u32(s: &str, err: &str) -> Result<u32> {
    s.parse::<u32>()
        .map_err(|_| Error::ParseError(err.to_string()))
}

fn parse_u64(s: &str, err: &str) -> Result<u64> {
    s.parse::<u64>()
        .map_err(|_| Error::ParseError(err.to_string()))
}

fn parse_i64(s: &str, err: &str) -> Result<i64> {
    s.parse::<i64>()
        .map_err(|_| Error::ParseError(err.to_string()))
}

fn parse_f64(s: &str, err: &str) -> Result<f64> {
    s.parse::<f64>()
        .map_err(|_| Error::ParseError(err.to_string()))
}

fn starts_with_keywords(input: &str, words: &[&str]) -> bool {
    let tokens: Vec<&str> = input.split_whitespace().take(words.len()).collect();

    if tokens.len() != words.len() {
        return false;
    }

    tokens
        .iter()
        .zip(words)
        .all(|(a, b)| a.eq_ignore_ascii_case(b))
}

fn contains_token_outside_strings(input: &str, token: &str) -> bool {
    let mut in_str = false;
    let mut chars = input.char_indices().peekable();

    while let Some((idx, ch)) = chars.next() {
        if ch == '\'' {
            if in_str {
                if let Some((_, next_ch)) = chars.peek()
                    && *next_ch == '\''
                {
                    let _ = chars.next();
                    continue;
                }
                in_str = false;
            } else {
                in_str = true;
            }
            continue;
        }

        if in_str {
            continue;
        }

        if is_word_boundary(input, idx.saturating_sub(1))
            && input[idx..].len() >= token.len()
            && input[idx..idx + token.len()].eq_ignore_ascii_case(token)
            && is_word_boundary(input, idx + token.len())
        {
            return true;
        }
    }

    false
}

fn contains_keyword_sequence_outside_strings(input: &str, words: &[&str]) -> bool {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut in_str = false;
    let mut chars = input.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '\'' {
            if in_str {
                if chars.peek() == Some(&'\'') {
                    let _ = chars.next();
                    continue;
                }
                in_str = false;
            } else {
                in_str = true;
            }
            if !current.is_empty() {
                tokens.push(std::mem::take(&mut current));
            }
            continue;
        }

        if in_str {
            continue;
        }

        if ch.is_ascii_alphanumeric() || ch == '_' {
            current.push(ch);
        } else if !current.is_empty() {
            tokens.push(std::mem::take(&mut current));
        }
    }

    if !current.is_empty() {
        tokens.push(current);
    }

    tokens.windows(words.len()).any(|window| {
        window
            .iter()
            .zip(words)
            .all(|(a, b)| a.eq_ignore_ascii_case(b))
    })
}

fn contains_where_match_operator(input: &str) -> bool {
    let mut in_str = false;
    let mut word = String::new();
    let mut seen_where = false;

    for ch in input.chars() {
        if ch == '\'' {
            in_str = !in_str;
            if !word.is_empty() {
                if word.eq_ignore_ascii_case("WHERE") {
                    seen_where = true;
                } else if seen_where && word.eq_ignore_ascii_case("MATCH") {
                    return true;
                }
                word.clear();
            }
            continue;
        }

        if in_str {
            continue;
        }

        if ch.is_ascii_alphanumeric() || ch == '_' {
            word.push(ch);
            continue;
        }

        if !word.is_empty() {
            if word.eq_ignore_ascii_case("WHERE") {
                seen_where = true;
            } else if seen_where && word.eq_ignore_ascii_case("GRAPH_TABLE") {
                // A later graph traversal can legitimately contain MATCH; do not
                // keep a prior WHERE active across that boundary.
                seen_where = false;
            } else if seen_where && word.eq_ignore_ascii_case("MATCH") {
                return true;
            } else if seen_where
                && (word.eq_ignore_ascii_case("GROUP")
                    || word.eq_ignore_ascii_case("ORDER")
                    || word.eq_ignore_ascii_case("LIMIT"))
            {
                seen_where = false;
            }
            word.clear();
        }
    }

    if !word.is_empty() && seen_where && word.eq_ignore_ascii_case("MATCH") {
        return true;
    }

    false
}

fn is_word_boundary(s: &str, idx: usize) -> bool {
    if idx >= s.len() {
        return true;
    }
    !s.as_bytes()[idx].is_ascii_alphanumeric() && s.as_bytes()[idx] != b'_'
}

fn build_set_memory_limit(pair: Pair<'_, Rule>) -> Result<SetMemoryLimitValue> {
    let inner = pair
        .into_inner()
        .find(|p| p.as_rule() == Rule::memory_limit_value)
        .ok_or_else(|| Error::ParseError("missing memory_limit_value".to_string()))?;

    if inner.as_str().eq_ignore_ascii_case("none") {
        return Ok(SetMemoryLimitValue::None);
    }

    let value_inner = inner
        .into_inner()
        .next()
        .ok_or_else(|| Error::ParseError("empty memory_limit_value".to_string()))?;

    match value_inner.as_rule() {
        Rule::size_with_unit => Ok(SetMemoryLimitValue::Bytes(parse_size_with_unit(
            value_inner.as_str(),
        )? as usize)),
        _ => Ok(SetMemoryLimitValue::None),
    }
}

fn build_set_disk_limit(pair: Pair<'_, Rule>) -> Result<SetDiskLimitValue> {
    let inner = pair
        .into_inner()
        .find(|p| p.as_rule() == Rule::disk_limit_value)
        .ok_or_else(|| Error::ParseError("missing disk_limit_value".to_string()))?;

    if inner.as_str().eq_ignore_ascii_case("none") {
        return Ok(SetDiskLimitValue::None);
    }

    let value_inner = inner
        .into_inner()
        .next()
        .ok_or_else(|| Error::ParseError("empty disk_limit_value".to_string()))?;

    match value_inner.as_rule() {
        Rule::size_with_unit => Ok(SetDiskLimitValue::Bytes(parse_size_with_unit(
            value_inner.as_str(),
        )?)),
        _ => Ok(SetDiskLimitValue::None),
    }
}

fn parse_size_with_unit(text: &str) -> Result<u64> {
    let (digits, suffix) = text.split_at(text.len() - 1);
    let base: u64 = digits
        .parse()
        .map_err(|e| Error::ParseError(format!("invalid size number: {e}")))?;
    let multiplier = match suffix {
        "G" | "g" => 1024 * 1024 * 1024,
        "M" | "m" => 1024 * 1024,
        "K" | "k" => 1024,
        _ => return Err(Error::ParseError(format!("unknown size suffix: {suffix}"))),
    };
    Ok(base * multiplier)
}
