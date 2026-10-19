//! Physical retirement only. The engine supplies declarations and orders
//! preparation, durability, and publication under its existing gates.
use contextdb_core::read_memory::{ReadCredit, ReadMemoryBudget};
use contextdb_core::{Lsn, Result, RowId, TableMeta, VersionedRow};
use parking_lot::Mutex;
use std::sync::Arc;

type Key = (RowId, Lsn);
type Root = Option<Arc<Node>>;

struct Declaration {
    table: String,
    meta: TableMeta,
    _credit: ReadCredit,
}
struct Body {
    // The row is filled once, by moving its original buffer at publication.
    row: Mutex<Option<VersionedRow>>,
    declaration: Arc<Declaration>,
    _credit: ReadCredit,
}
struct Node {
    key: Key,
    body: Arc<Body>,
    left: Root,
    right: Root,
    height: u16,
    _credit: ReadCredit,
}

fn credit(owner: &Arc<dyn ReadMemoryBudget>, bytes: usize) -> Result<ReadCredit> {
    ReadCredit::try_new(
        owner.clone(),
        bytes,
        "writer_source",
        "schema_generation",
        "Raise MEMORY_LIMIT to prepare the schema generation.",
    )
}
fn height(root: &Root) -> u16 {
    root.as_ref().map_or(0, |node| node.height)
}
fn node(
    owner: &Arc<dyn ReadMemoryBudget>,
    key: Key,
    body: Arc<Body>,
    left: Root,
    right: Root,
) -> Result<Arc<Node>> {
    let paid = credit(
        owner,
        std::mem::size_of::<Node>() + 2 * std::mem::size_of::<usize>(),
    )?;
    Ok(Arc::new(Node {
        key,
        body,
        height: 1 + height(&left).max(height(&right)),
        left,
        right,
        _credit: paid,
    }))
}
fn balanced(
    owner: &Arc<dyn ReadMemoryBudget>,
    key: Key,
    body: Arc<Body>,
    left: Root,
    right: Root,
) -> Result<Arc<Node>> {
    if height(&left) > height(&right) + 1 {
        let l = left.as_ref().expect("left-heavy tree");
        if height(&l.left) >= height(&l.right) {
            let r = node(owner, key, body, l.right.clone(), right)?;
            return node(owner, l.key, l.body.clone(), l.left.clone(), Some(r));
        }
        let pivot = l.right.as_ref().expect("inner left child");
        let l2 = node(
            owner,
            l.key,
            l.body.clone(),
            l.left.clone(),
            pivot.left.clone(),
        )?;
        let r2 = node(owner, key, body, pivot.right.clone(), right)?;
        return node(owner, pivot.key, pivot.body.clone(), Some(l2), Some(r2));
    }
    if height(&right) > height(&left) + 1 {
        let r = right.as_ref().expect("right-heavy tree");
        if height(&r.right) >= height(&r.left) {
            let l = node(owner, key, body, left, r.left.clone())?;
            return node(owner, r.key, r.body.clone(), Some(l), r.right.clone());
        }
        let pivot = r.left.as_ref().expect("inner right child");
        let l2 = node(owner, key, body, left, pivot.left.clone())?;
        let r2 = node(
            owner,
            r.key,
            r.body.clone(),
            pivot.right.clone(),
            r.right.clone(),
        )?;
        return node(owner, pivot.key, pivot.body.clone(), Some(l2), Some(r2));
    }
    node(owner, key, body, left, right)
}
fn insert(
    root: &Root,
    key: Key,
    body: Arc<Body>,
    owner: &Arc<dyn ReadMemoryBudget>,
) -> Result<Root> {
    let Some(n) = root else {
        return node(owner, key, body, None, None).map(Some);
    };
    if key < n.key {
        balanced(
            owner,
            n.key,
            n.body.clone(),
            insert(&n.left, key, body, owner)?,
            n.right.clone(),
        )
        .map(Some)
    } else if key > n.key {
        balanced(
            owner,
            n.key,
            n.body.clone(),
            n.left.clone(),
            insert(&n.right, key, body, owner)?,
        )
        .map(Some)
    } else {
        Ok(root.clone())
    }
}
fn get(root: &Root, key: Key) -> Option<&Arc<Body>> {
    let mut next = root.as_ref();
    while let Some(n) = next {
        match key.cmp(&n.key) {
            std::cmp::Ordering::Less => next = n.left.as_ref(),
            std::cmp::Ordering::Greater => next = n.right.as_ref(),
            std::cmp::Ordering::Equal => return Some(&n.body),
        }
    }
    None
}
fn remove(root: &Root, key: Key, owner: &Arc<dyn ReadMemoryBudget>) -> Result<Root> {
    let Some(n) = root else {
        return Ok(None);
    };
    if key < n.key {
        return balanced(
            owner,
            n.key,
            n.body.clone(),
            remove(&n.left, key, owner)?,
            n.right.clone(),
        )
        .map(Some);
    }
    if key > n.key {
        return balanced(
            owner,
            n.key,
            n.body.clone(),
            n.left.clone(),
            remove(&n.right, key, owner)?,
        )
        .map(Some);
    }
    if n.left.is_none() {
        return Ok(n.right.clone());
    }
    if n.right.is_none() {
        return Ok(n.left.clone());
    }
    let mut successor = n.right.as_ref().expect("right child");
    while let Some(left) = successor.left.as_ref() {
        successor = left;
    }
    balanced(
        owner,
        successor.key,
        successor.body.clone(),
        n.left.clone(),
        remove(&n.right, successor.key, owner)?,
    )
    .map(Some)
}

#[derive(Default)]
pub(crate) struct History {
    root: Root,
}

/// Admitted directory replacement and empty destinations. No row bytes move
/// during fallible preparation, and no original body is cloned.
pub struct PreparedSchemaGeneration {
    root: Root,
    moves: Vec<(String, usize, Arc<Body>)>,
    _credit: ReadCredit,
}
/// A selected removal has no body destinations and cannot move live rows.
pub struct PreparedSchemaHistoryErasure(PreparedSchemaGeneration);
impl PreparedSchemaHistoryErasure {
    pub(crate) fn into_generation(self) -> PreparedSchemaGeneration {
        self.0
    }
}

impl History {
    pub(crate) fn prepare<'a>(
        &self,
        tables: impl IntoIterator<Item = (&'a str, &'a TableMeta, &'a [VersionedRow])>,
        owner: Arc<dyn ReadMemoryBudget>,
    ) -> Result<PreparedSchemaGeneration> {
        let mut root = self.root.clone();
        let mut moves = Vec::new();
        let mut control = credit(&owner, 0)?;
        for (table, meta, rows) in tables {
            let declaration_credit = credit(
                &owner,
                meta.estimated_bytes() + table.len() + std::mem::size_of::<Declaration>() + 16,
            )?;
            let declaration = Arc::new(Declaration {
                table: table.into(),
                meta: meta.clone(),
                _credit: declaration_credit,
            });
            for (position, row) in rows.iter().enumerate() {
                let key = (row.row_id, row.lsn);
                if get(&root, key).is_some() {
                    continue;
                }
                // Pre-admission covers the old/new overlap; it is not paid by
                // refunding the outgoing store image before physical retirement.
                let paid = credit(
                    &owner,
                    row.estimated_bytes() + std::mem::size_of::<Body>() + 16,
                )?;
                let body = Arc::new(Body {
                    row: Mutex::new(None),
                    declaration: declaration.clone(),
                    _credit: paid,
                });
                root = insert(&root, key, body.clone(), &owner)?;
                control.try_grow_for(
                    std::mem::size_of::<(String, usize, Arc<Body>)>() + table.len() + 32,
                    "writer_source",
                    "schema_generation",
                    "Raise MEMORY_LIMIT to prepare schema retirement.",
                )?;
                // Exact reservation prevents unadmitted geometric capacity.
                moves.reserve_exact(1);
                moves.push((table.into(), position, body));
            }
        }
        Ok(PreparedSchemaGeneration {
            root,
            moves,
            _credit: control,
        })
    }
    pub(crate) fn prepare_erasure(
        &self,
        keys: impl IntoIterator<Item = (RowId, Lsn)>,
        owner: Arc<dyn ReadMemoryBudget>,
    ) -> Result<PreparedSchemaHistoryErasure> {
        let mut root = self.root.clone();
        for key in keys {
            root = remove(&root, key, &owner)?;
        }
        Ok(PreparedSchemaHistoryErasure(PreparedSchemaGeneration {
            root,
            moves: Vec::new(),
            _credit: credit(&owner, 0)?,
        }))
    }
    pub(crate) fn publish_erasure(&mut self, prepared: PreparedSchemaHistoryErasure) {
        self.root = prepared.0.root;
    }

    pub(crate) fn publish(
        &mut self,
        prepared: PreparedSchemaGeneration,
        tables: &mut std::collections::HashMap<String, Vec<VersionedRow>>,
    ) {
        for (table, position, destination) in prepared.moves {
            let source = &mut tables.get_mut(&table).expect("prepared source table")[position];
            let empty = VersionedRow {
                row_id: source.row_id,
                values: Default::default(),
                created_tx: source.created_tx,
                deleted_tx: source.deleted_tx,
                lsn: source.lsn,
                created_at: source.created_at,
            };
            *destination.row.lock() = Some(std::mem::replace(source, empty));
        }
        self.root = prepared.root;
    }
    pub(crate) fn visit_all(
        &self,
        mut consume: impl FnMut(&str, &TableMeta, &VersionedRow) -> Result<()>,
    ) -> Result<()> {
        fn walk(
            root: &Root,
            consume: &mut dyn FnMut(&str, &TableMeta, &VersionedRow) -> Result<()>,
        ) -> Result<()> {
            let Some(node) = root else {
                return Ok(());
            };
            walk(&node.left, consume)?;
            if let Some(row) = node.body.row.lock().as_ref() {
                consume(
                    &node.body.declaration.table,
                    &node.body.declaration.meta,
                    row,
                )?;
            }
            walk(&node.right, consume)
        }
        walk(&self.root, &mut consume)
    }

    pub(crate) fn visit_row(
        &self,
        table: &str,
        row: RowId,
        mut consume: impl FnMut(&VersionedRow) -> Result<()>,
    ) -> Result<()> {
        fn walk(
            root: &Root,
            table: &str,
            row: RowId,
            consume: &mut dyn FnMut(&VersionedRow) -> Result<()>,
        ) -> Result<()> {
            let Some(node) = root else {
                return Ok(());
            };
            if node.key.0 >= row {
                walk(&node.left, table, row, consume)?;
            }
            if node.key.0 == row && node.body.declaration.table == table {
                if let Some(body) = node.body.row.lock().as_ref() {
                    consume(body)?;
                }
            }
            if node.key.0 <= row {
                walk(&node.right, table, row, consume)?;
            }
            Ok(())
        }
        walk(&self.root, table, row, &mut consume)
    }

    pub(crate) fn with_row<T>(
        &self,
        table: &str,
        row: RowId,
        lsn: Lsn,
        consume: impl FnOnce(Option<(&TableMeta, &VersionedRow)>) -> Result<T>,
    ) -> Result<T> {
        let Some(body) = get(&self.root, (row, lsn)) else {
            return consume(None);
        };
        if body.declaration.table != table {
            return consume(None);
        }
        let row = body.row.lock();
        consume(row.as_ref().map(|row| (&body.declaration.meta, row)))
    }
}

impl PreparedSchemaGeneration {
    #[doc(hidden)]
    pub fn retain_owned(&mut self, table: &str, meta: &TableMeta, row: &VersionedRow,
        owner: Arc<dyn ReadMemoryBudget>) -> Result<()> {
        let key = (row.row_id,row.lsn);
        if get(&self.root,key).is_some() { return Ok(()); }
        let declaration_credit = credit(&owner,meta.estimated_bytes()+table.len()+std::mem::size_of::<Declaration>()+16)?;
        let body_credit = credit(&owner,row.estimated_bytes()+std::mem::size_of::<Body>()+16)?;
        let declaration = Arc::new(Declaration { table: table.into(), meta: meta.clone(), _credit: declaration_credit });
        let body = Arc::new(Body { row: Mutex::new(Some(row.clone())), declaration, _credit: body_credit });
        self.root = insert(&self.root,key,body,&owner)?;
        Ok(())
    }
}
