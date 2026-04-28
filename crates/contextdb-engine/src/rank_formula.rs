use contextdb_core::{Result, VersionedRow};

#[derive(Debug, Clone)]
pub struct RankFormula {
    _formula: String,
}

impl RankFormula {
    pub fn compile(formula: &str) -> Result<Self> {
        Ok(Self {
            _formula: formula.to_string(),
        })
    }

    pub fn const_one() -> Self {
        Self {
            _formula: "1.0".to_string(),
        }
    }

    pub fn eval(&self, _anchor: &VersionedRow, _joined: Option<&VersionedRow>) -> Option<f32> {
        Some(1.0)
    }
}
