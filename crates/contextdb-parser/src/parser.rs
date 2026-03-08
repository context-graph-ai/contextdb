use crate::ast::*;
use contextdb_core::{Error, Result};
use pest::Parser;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "grammar.pest"]
struct ContextDbParser;

pub fn parse(input: &str) -> Result<Statement> {
    ContextDbParser::parse(Rule::statement, input).map_err(|e| Error::ParseError(e.to_string()))?;
    let s = input.trim().trim_end_matches(';').trim();
    let upper = s.to_ascii_uppercase();

    if upper.starts_with("WITH RECURSIVE") {
        return Err(Error::RecursiveCteNotSupported);
    }
    if upper.contains(" OVER ") {
        return Err(Error::WindowFunctionNotSupported);
    }
    if upper.starts_with("CREATE PROCEDURE") || upper.starts_with("CREATE FUNCTION") {
        return Err(Error::StoredProcNotSupported);
    }
    if upper.contains("MATCH") && upper.contains("*") && !upper.contains("..") {
        return Err(Error::UnboundedTraversal);
    }
    if upper.contains("<=>") && !upper.contains(" LIMIT ") {
        return Err(Error::UnboundedVectorSearch);
    }

    if upper.starts_with("BEGIN") {
        return Ok(Statement::Begin);
    }
    if upper.starts_with("COMMIT") {
        return Ok(Statement::Commit);
    }
    if upper.starts_with("ROLLBACK") {
        return Ok(Statement::Rollback);
    }

    if upper.starts_with("CREATE TABLE") {
        return parse_create_table(s);
    }
    if upper.starts_with("DROP TABLE") {
        return parse_drop_table(s);
    }
    if upper.starts_with("CREATE INDEX") {
        return parse_create_index(s);
    }
    if upper.starts_with("INSERT INTO") {
        return parse_insert(s);
    }
    if upper.starts_with("DELETE FROM") {
        return parse_delete(s);
    }
    if upper.starts_with("UPDATE") {
        return parse_update(s);
    }
    if upper.starts_with("WITH") || upper.starts_with("SELECT") {
        return parse_select(s);
    }

    Err(Error::ParseError("unsupported statement".to_string()))
}

fn parse_identifier(token: &str) -> String {
    token.trim_matches('`').trim_matches('"').to_string()
}

fn parse_literal(token: &str) -> Expr {
    let t = token.trim();
    if let Some(stripped) = t.strip_prefix('$') {
        return Expr::Parameter(stripped.to_string());
    }
    if t.starts_with('"') && t.ends_with('"') || t.starts_with('\'') && t.ends_with('\'') {
        return Expr::Literal(Literal::Text(t[1..t.len() - 1].to_string()));
    }
    if t.eq_ignore_ascii_case("null") {
        return Expr::Literal(Literal::Null);
    }
    if t.eq_ignore_ascii_case("true") {
        return Expr::Literal(Literal::Bool(true));
    }
    if t.eq_ignore_ascii_case("false") {
        return Expr::Literal(Literal::Bool(false));
    }
    if let Ok(v) = t.parse::<i64>() {
        return Expr::Literal(Literal::Integer(v));
    }
    if let Ok(v) = t.parse::<f64>() {
        return Expr::Literal(Literal::Real(v));
    }
    Expr::Column(ColumnRef {
        table: None,
        column: t.to_string(),
    })
}

fn parse_data_type(token: &str) -> DataType {
    let t = token.trim().to_ascii_uppercase();
    if t.starts_with("VECTOR(") && t.ends_with(')') {
        let dim = t
            .trim_start_matches("VECTOR(")
            .trim_end_matches(')')
            .parse::<u32>()
            .unwrap_or(0);
        return DataType::Vector(dim);
    }
    match t.as_str() {
        "UUID" => DataType::Uuid,
        "TEXT" => DataType::Text,
        "INTEGER" | "INT" => DataType::Integer,
        "REAL" | "FLOAT" => DataType::Real,
        "BOOLEAN" | "BOOL" => DataType::Boolean,
        "TIMESTAMP" => DataType::Timestamp,
        "JSON" => DataType::Json,
        _ => DataType::Text,
    }
}

fn parse_create_table(s: &str) -> Result<Statement> {
    let open = s
        .find('(')
        .ok_or_else(|| Error::ParseError("invalid CREATE TABLE".to_string()))?;
    let close = s
        .rfind(')')
        .ok_or_else(|| Error::ParseError("invalid CREATE TABLE".to_string()))?;
    let prefix = &s[..open];
    let cols = &s[open + 1..close];

    let parts: Vec<&str> = prefix.split_whitespace().collect();
    let name = parse_identifier(parts.last().copied().unwrap_or(""));
    if name.is_empty() {
        return Err(Error::ParseError("missing table name".to_string()));
    }

    let columns = cols
        .split(',')
        .map(|col| {
            let tokens: Vec<&str> = col.split_whitespace().collect();
            let cname = parse_identifier(tokens.first().copied().unwrap_or(""));
            let dtype = parse_data_type(tokens.get(1).copied().unwrap_or("TEXT"));
            let col_upper = col.to_ascii_uppercase();
            ColumnDef {
                name: cname,
                data_type: dtype,
                nullable: !col_upper.contains("NOT NULL"),
                primary_key: col_upper.contains("PRIMARY KEY"),
                unique: col_upper.contains("UNIQUE"),
                default: None,
                references: None,
            }
        })
        .collect();

    Ok(Statement::CreateTable(CreateTable {
        name,
        columns,
        if_not_exists: prefix.to_ascii_uppercase().contains("IF NOT EXISTS"),
    }))
}

fn parse_drop_table(s: &str) -> Result<Statement> {
    let parts: Vec<&str> = s.split_whitespace().collect();
    let name = parse_identifier(parts.last().copied().unwrap_or(""));
    if name.is_empty() {
        return Err(Error::ParseError("missing table name".to_string()));
    }
    Ok(Statement::DropTable(DropTable {
        name,
        if_exists: s.to_ascii_uppercase().contains("IF EXISTS"),
    }))
}

fn parse_create_index(s: &str) -> Result<Statement> {
    let upper = s.to_ascii_uppercase();
    let on_pos = upper
        .find(" ON ")
        .ok_or_else(|| Error::ParseError("invalid CREATE INDEX".to_string()))?;
    let left = &s[..on_pos];
    let right = &s[on_pos + 4..];

    let idx_name = parse_identifier(left.split_whitespace().last().unwrap_or("idx"));
    let open = right
        .find('(')
        .ok_or_else(|| Error::ParseError("invalid CREATE INDEX".to_string()))?;
    let close = right
        .rfind(')')
        .ok_or_else(|| Error::ParseError("invalid CREATE INDEX".to_string()))?;
    let table = parse_identifier(right[..open].trim());
    let columns = right[open + 1..close]
        .split(',')
        .map(parse_identifier)
        .collect();

    Ok(Statement::CreateIndex(CreateIndex {
        name: idx_name,
        table,
        columns,
    }))
}

fn parse_insert(s: &str) -> Result<Statement> {
    let upper = s.to_ascii_uppercase();
    let values_idx = upper
        .find(" VALUES ")
        .ok_or_else(|| Error::ParseError("invalid INSERT".to_string()))?;

    let head = &s[..values_idx];
    let tail = &s[values_idx + 8..];

    let open = head
        .find('(')
        .ok_or_else(|| Error::ParseError("invalid INSERT".to_string()))?;
    let close = head
        .rfind(')')
        .ok_or_else(|| Error::ParseError("invalid INSERT".to_string()))?;

    let table = parse_identifier(head["INSERT INTO".len()..open].trim());
    let columns: Vec<String> = head[open + 1..close]
        .split(',')
        .map(parse_identifier)
        .collect();

    let tail_upper = tail.to_ascii_uppercase();
    let (values_part, conflict_part) = if let Some(idx) = tail_upper.find(" ON CONFLICT ") {
        (&tail[..idx], Some(&tail[idx + 13..]))
    } else {
        (tail, None)
    };

    let vopen = values_part
        .find('(')
        .ok_or_else(|| Error::ParseError("invalid INSERT values".to_string()))?;
    let vclose = values_part
        .rfind(')')
        .ok_or_else(|| Error::ParseError("invalid INSERT values".to_string()))?;
    let row: Vec<Expr> = values_part[vopen + 1..vclose]
        .split(',')
        .map(parse_literal)
        .collect();

    let on_conflict = conflict_part.and_then(|c| {
        let c_upper = c.to_ascii_uppercase();
        let copen = c.find('(')?;
        let cclose = c.find(')')?;
        let cols: Vec<String> = c[copen + 1..cclose].split(',').map(parse_identifier).collect();
        let set_pos = c_upper.find(" SET ")?;
        let assigns = c[set_pos + 5..]
            .split(',')
            .filter_map(|pair| {
                let mut it = pair.splitn(2, '=');
                let key = it.next()?.trim().to_string();
                let val = parse_literal(it.next()?.trim());
                Some((key, val))
            })
            .collect();
        Some(OnConflict {
            columns: cols,
            update_columns: assigns,
        })
    });

    Ok(Statement::Insert(Insert {
        table,
        columns,
        values: vec![row],
        on_conflict,
    }))
}

fn parse_delete(s: &str) -> Result<Statement> {
    let upper = s.to_ascii_uppercase();
    let where_idx = upper.find(" WHERE ");
    let table = if let Some(idx) = where_idx {
        parse_identifier(s["DELETE FROM".len()..idx].trim())
    } else {
        parse_identifier(s["DELETE FROM".len()..].trim())
    };

    let where_clause = where_idx.map(|idx| parse_literal(s[idx + 7..].trim()));

    Ok(Statement::Delete(Delete { table, where_clause }))
}

fn parse_update(s: &str) -> Result<Statement> {
    let upper = s.to_ascii_uppercase();
    let set_idx = upper
        .find(" SET ")
        .ok_or_else(|| Error::ParseError("invalid UPDATE".to_string()))?;
    let where_idx = upper.find(" WHERE ");

    let table = parse_identifier(s["UPDATE".len()..set_idx].trim());
    let assigns_str = if let Some(idx) = where_idx {
        &s[set_idx + 5..idx]
    } else {
        &s[set_idx + 5..]
    };

    let assignments = assigns_str
        .split(',')
        .filter_map(|pair| {
            let mut it = pair.splitn(2, '=');
            let key = it.next()?.trim().to_string();
            let value = parse_literal(it.next()?.trim());
            Some((key, value))
        })
        .collect();

    let where_clause = where_idx.map(|idx| parse_literal(s[idx + 7..].trim()));

    Ok(Statement::Update(Update {
        table,
        assignments,
        where_clause,
    }))
}

fn parse_select(s: &str) -> Result<Statement> {
    let upper = s.to_ascii_uppercase();
    let mut ctes = Vec::new();
    let mut select_source = s;

    if upper.starts_with("WITH ") {
        let as_pos = upper
            .find(" AS ")
            .ok_or_else(|| Error::ParseError("invalid WITH".to_string()))?;
        let cte_name = parse_identifier(s[5..as_pos].trim());
        let open = s[as_pos..]
            .find('(')
            .ok_or_else(|| Error::ParseError("invalid WITH".to_string()))?
            + as_pos;
        let close = s.rfind(')').ok_or_else(|| Error::ParseError("invalid WITH".to_string()))?;
        let cte_body = s[open + 1..close].trim();

        if cte_body.to_ascii_uppercase().starts_with("MATCH") {
            let edge_step = if cte_body.contains("*1..") {
                let idx = cte_body
                    .find("*1..")
                    .ok_or_else(|| Error::ParseError("invalid MATCH".to_string()))?;
                let rest = &cte_body[idx + 4..];
                let max = rest
                    .chars()
                    .take_while(|c| c.is_ascii_digit())
                    .collect::<String>()
                    .parse::<u32>()
                    .unwrap_or(1);
                EdgeStep {
                    direction: EdgeDirection::Outgoing,
                    edge_type: Some("EDGE".to_string()),
                    min_hops: 1,
                    max_hops: max,
                    alias: None,
                    target: NodePattern {
                        alias: "b".to_string(),
                        label: None,
                        properties: vec![],
                    },
                }
            } else {
                EdgeStep {
                    direction: EdgeDirection::Outgoing,
                    edge_type: Some("EDGE".to_string()),
                    min_hops: 1,
                    max_hops: 1,
                    alias: None,
                    target: NodePattern {
                        alias: "b".to_string(),
                        label: None,
                        properties: vec![],
                    },
                }
            };

            ctes.push(Cte::MatchCte {
                name: cte_name,
                match_clause: MatchClause {
                    pattern: GraphPattern {
                        start: NodePattern {
                            alias: "a".to_string(),
                            label: None,
                            properties: vec![],
                        },
                        edges: vec![edge_step],
                    },
                    where_clause: None,
                    return_cols: vec![],
                },
            });
        } else {
            ctes.push(Cte::SqlCte {
                name: cte_name,
                query: SelectBody {
                    columns: vec![],
                    from: None,
                    joins: vec![],
                    where_clause: None,
                    order_by: vec![],
                    limit: None,
                },
            });
        }

        let select_pos = upper
            .rfind(" SELECT ")
            .or_else(|| upper.find("SELECT"))
            .ok_or_else(|| Error::ParseError("missing SELECT".to_string()))?;
        select_source = &s[select_pos..];
    }

    if upper.contains(" IN (SELECT ") {
        let in_select_pos = upper
            .find(" IN (SELECT ")
            .ok_or_else(|| Error::ParseError("invalid IN subquery".to_string()))?;
        let subquery = &s[in_select_pos + " IN (".len()..];
        let sub_upper = subquery.to_ascii_uppercase();
        let from_pos = sub_upper
            .find(" FROM ")
            .ok_or_else(|| Error::ParseError("invalid IN subquery".to_string()))?;
        let after_from = subquery[from_pos + 6..].trim();
        let referenced = after_from
            .split(|c: char| c.is_whitespace() || c == ')')
            .next()
            .map(parse_identifier)
            .unwrap_or_default();

        let cte_names: Vec<String> = ctes
            .iter()
            .map(|c| match c {
                Cte::SqlCte { name, .. } | Cte::MatchCte { name, .. } => name.clone(),
            })
            .collect();

        if !cte_names.contains(&referenced) {
            return Err(Error::SubqueryNotSupported);
        }
    }

    let src_upper = select_source.to_ascii_uppercase();
    let from_pos = src_upper.find(" FROM ");
    let order_pos = src_upper.find(" ORDER BY ");
    let limit_pos = src_upper.find(" LIMIT ");
    let where_pos = src_upper.find(" WHERE ");

    let columns_part = if let Some(fp) = from_pos {
        &select_source["SELECT".len()..fp]
    } else {
        &select_source["SELECT".len()..]
    };

    let columns: Vec<SelectColumn> = columns_part
        .split(',')
        .map(|c| SelectColumn {
            expr: parse_literal(c.trim()),
            alias: None,
        })
        .collect();

    let from = from_pos.map(|fp| {
        let end = where_pos.or(order_pos).or(limit_pos).unwrap_or(select_source.len());
        FromClause {
            table: parse_identifier(select_source[fp + 6..end].trim()),
            alias: None,
        }
    });

    let where_clause = where_pos.map(|wp| {
        let end = order_pos.or(limit_pos).unwrap_or(select_source.len());
        parse_literal(select_source[wp + 7..end].trim())
    });

    let mut order_by = Vec::new();
    if let Some(op) = order_pos {
        let end = limit_pos.unwrap_or(select_source.len());
        let expr_text = select_source[op + 10..end].trim();
        if expr_text.contains("<= >") || expr_text.contains("<=>") {
            let parts: Vec<&str> = expr_text.split("<=>").collect();
            if parts.len() == 2 {
                order_by.push(OrderByItem {
                    expr: Expr::CosineDistance {
                        left: Box::new(parse_literal(parts[0].trim())),
                        right: Box::new(parse_literal(parts[1].trim())),
                    },
                    direction: SortDirection::CosineDistance,
                });
            }
        } else {
            order_by.push(OrderByItem {
                expr: parse_literal(expr_text),
                direction: if expr_text.to_ascii_uppercase().ends_with(" DESC") {
                    SortDirection::Desc
                } else {
                    SortDirection::Asc
                },
            });
        }
    }

    let limit = limit_pos.and_then(|lp| select_source[lp + 7..].trim().parse::<u64>().ok());

    Ok(Statement::Select(SelectStatement {
        ctes,
        body: SelectBody {
            columns,
            from,
            joins: vec![],
            where_clause,
            order_by,
            limit,
        },
    }))
}
