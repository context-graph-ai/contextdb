#![allow(clippy::duplicate_mod)] // Shared unchanged fixtures retain their original module paths.

#[path = "vector/public_row_apis_honor_quantized_columns.rs"]
mod public_row_apis_honor_quantized_columns;
#[path = "vector/quantized_vector_read_agreement.rs"]
mod quantized_vector_read_agreement;
#[path = "vector/vector_search_answers_quantized_columns_on_every_route.rs"]
mod vector_search_answers_quantized_columns_on_every_route;
