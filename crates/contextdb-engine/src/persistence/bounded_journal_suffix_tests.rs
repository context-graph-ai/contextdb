use super::*;
use std::cell::Cell;

thread_local! { static EXAMINED: Cell<usize> = const { Cell::new(0) }; }
