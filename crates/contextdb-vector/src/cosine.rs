pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm_a == 0.0 || norm_b == 0.0 {
        return 0.0;
    }
    dot / (norm_a * norm_b)
}

/// The one canonical cosine distance every route ranks by: each coordinate is
/// widened to f64 before it is accumulated, and a zero norm is distance 1.0.
/// The index route and the exact route both call this, so their orders agree.
pub fn canonical_cosine_distance_pairs(pairs: impl Iterator<Item = (f32, f32)>) -> f64 {
    let mut dot = 0.0;
    let mut aa = 0.0;
    let mut bb = 0.0;
    for (x, y) in pairs {
        let x = f64::from(x);
        let y = f64::from(y);
        dot += x * y;
        aa += x * x;
        bb += y * y;
    }
    if aa == 0.0 || bb == 0.0 {
        1.0
    } else {
        1.0 - dot / (aa.sqrt() * bb.sqrt())
    }
}

/// [`canonical_cosine_distance_pairs`] over two stored coordinate slices.
pub fn canonical_cosine_distance(a: &[f32], b: &[f32]) -> f64 {
    canonical_cosine_distance_pairs(a.iter().copied().zip(b.iter().copied()))
}

/// The exact route's ranking key: canonical similarity, higher is nearer.
/// Its displayed score is `key as f32`, the value the index route displays
/// as `(1.0 - distance) as f32`.
pub fn canonical_cosine_similarity(a: &[f32], b: &[f32]) -> f64 {
    1.0 - canonical_cosine_distance(a, b)
}
