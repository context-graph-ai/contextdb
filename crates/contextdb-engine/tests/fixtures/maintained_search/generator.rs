//! Deterministic port of the frozen Python topic-cluster fixture generator.
//!
//! Python's SHA-256 integer seeding, MT19937 state initialization, `random()`,
//! `randrange(24)`, and cached Box-Muller `gauss()` behavior are reproduced so
//! the generated little-endian `f32` bytes retain the published fingerprints.

#[cfg_attr(
    not(any(
        feature = "nightly-tests",
        feature = "production-smoke-driver",
        feature = "mdns"
    )),
    allow(
        dead_code,
        reason = "KEEP-FEATURE: live only under nightly-tests, production-smoke-driver or mdns"
    )
)]
const SHA256_INITIAL: [u32; 8] = [
    0x6a09_e667,
    0xbb67_ae85,
    0x3c6e_f372,
    0xa54f_f53a,
    0x510e_527f,
    0x9b05_688c,
    0x1f83_d9ab,
    0x5be0_cd19,
];

#[cfg_attr(
    not(any(
        feature = "nightly-tests",
        feature = "production-smoke-driver",
        feature = "mdns"
    )),
    allow(
        dead_code,
        reason = "KEEP-FEATURE: live only under nightly-tests, production-smoke-driver or mdns"
    )
)]
const SHA256_K: [u32; 64] = [
    0x428a_2f98,
    0x7137_4491,
    0xb5c0_fbcf,
    0xe9b5_dba5,
    0x3956_c25b,
    0x59f1_11f1,
    0x923f_82a4,
    0xab1c_5ed5,
    0xd807_aa98,
    0x1283_5b01,
    0x2431_85be,
    0x550c_7dc3,
    0x72be_5d74,
    0x80de_b1fe,
    0x9bdc_06a7,
    0xc19b_f174,
    0xe49b_69c1,
    0xefbe_4786,
    0x0fc1_9dc6,
    0x240c_a1cc,
    0x2de9_2c6f,
    0x4a74_84aa,
    0x5cb0_a9dc,
    0x76f9_88da,
    0x983e_5152,
    0xa831_c66d,
    0xb003_27c8,
    0xbf59_7fc7,
    0xc6e0_0bf3,
    0xd5a7_9147,
    0x06ca_6351,
    0x1429_2967,
    0x27b7_0a85,
    0x2e1b_2138,
    0x4d2c_6dfc,
    0x5338_0d13,
    0x650a_7354,
    0x766a_0abb,
    0x81c2_c92e,
    0x9272_2c85,
    0xa2bf_e8a1,
    0xa81a_664b,
    0xc24b_8b70,
    0xc76c_51a3,
    0xd192_e819,
    0xd699_0624,
    0xf40e_3585,
    0x106a_a070,
    0x19a4_c116,
    0x1e37_6c08,
    0x2748_774c,
    0x34b0_bcb5,
    0x391c_0cb3,
    0x4ed8_aa4a,
    0x5b9c_ca4f,
    0x682e_6ff3,
    0x748f_82ee,
    0x78a5_636f,
    0x84c8_7814,
    0x8cc7_0208,
    0x90be_fffa,
    0xa450_6ceb,
    0xbef9_a3f7,
    0xc671_78f2,
];

#[cfg_attr(
    not(any(
        feature = "nightly-tests",
        feature = "production-smoke-driver",
        feature = "mdns"
    )),
    allow(
        dead_code,
        reason = "KEEP-FEATURE: live only under nightly-tests, production-smoke-driver or mdns"
    )
)]
pub(super) fn sha256(input: &[u8]) -> [u8; 32] {
    let bit_len = (input.len() as u64).wrapping_mul(8);
    let mut padded = input.to_vec();
    padded.push(0x80);
    while padded.len() % 64 != 56 {
        padded.push(0);
    }
    padded.extend_from_slice(&bit_len.to_be_bytes());
    let mut state = SHA256_INITIAL;
    for block in padded.chunks_exact(64) {
        let mut words = [0_u32; 64];
        for (word, bytes) in words.iter_mut().zip(block.chunks_exact(4)) {
            *word = u32::from_be_bytes(bytes.try_into().expect("one SHA-256 word"));
        }
        for index in 16..64 {
            let s0 = words[index - 15].rotate_right(7)
                ^ words[index - 15].rotate_right(18)
                ^ (words[index - 15] >> 3);
            let s1 = words[index - 2].rotate_right(17)
                ^ words[index - 2].rotate_right(19)
                ^ (words[index - 2] >> 10);
            words[index] = words[index - 16]
                .wrapping_add(s0)
                .wrapping_add(words[index - 7])
                .wrapping_add(s1);
        }
        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut h] = state;
        for index in 0..64 {
            let sigma1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let choose = (e & f) ^ ((!e) & g);
            let first = h
                .wrapping_add(sigma1)
                .wrapping_add(choose)
                .wrapping_add(SHA256_K[index])
                .wrapping_add(words[index]);
            let sigma0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let majority = (a & b) ^ (a & c) ^ (b & c);
            let second = sigma0.wrapping_add(majority);
            h = g;
            g = f;
            f = e;
            e = d.wrapping_add(first);
            d = c;
            c = b;
            b = a;
            a = first.wrapping_add(second);
        }
        for (slot, value) in state.iter_mut().zip([a, b, c, d, e, f, g, h]) {
            *slot = slot.wrapping_add(value);
        }
    }
    let mut digest = [0_u8; 32];
    for (chunk, value) in digest.chunks_exact_mut(4).zip(state) {
        chunk.copy_from_slice(&value.to_be_bytes());
    }
    digest
}

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by vector, vector_a_filtered_scope_is_charged_for_its_candidates, vector_first_filt"
)]
pub(super) fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut out, "{byte:02x}").expect("writing into a String cannot fail");
    }
    out
}

#[cfg_attr(
    not(any(
        feature = "nightly-tests",
        feature = "production-smoke-driver",
        feature = "mdns"
    )),
    allow(
        dead_code,
        reason = "KEEP-FEATURE: live only under nightly-tests, production-smoke-driver or mdns"
    )
)]
struct PythonRandom {
    mt: [u32; 624],
    index: usize,
    gauss_next: Option<f64>,
}

impl PythonRandom {
    #[cfg_attr(
        not(any(
            feature = "nightly-tests",
            feature = "production-smoke-driver",
            feature = "mdns"
        )),
        allow(
            dead_code,
            reason = "KEEP-FEATURE: live only under nightly-tests, production-smoke-driver or mdns"
        )
    )]
    fn from_sha256(digest: [u8; 32]) -> Self {
        // Python seeds MT19937 from the absolute integer using least-significant
        // 32-bit words first. The fixture seed is a big-endian SHA-256 integer.
        let mut key = [0_u32; 8];
        for (slot, chunk) in key.iter_mut().zip(digest.chunks_exact(4).rev()) {
            *slot = u32::from_be_bytes(chunk.try_into().expect("one seed word"));
        }
        let mut source = Self {
            mt: [0; 624],
            index: 624,
            gauss_next: None,
        };
        source.init_by_array(&key);
        source
    }

    #[cfg_attr(
        not(any(
            feature = "nightly-tests",
            feature = "production-smoke-driver",
            feature = "mdns"
        )),
        allow(
            dead_code,
            reason = "KEEP-FEATURE: live only under nightly-tests, production-smoke-driver or mdns"
        )
    )]
    fn init_genrand(&mut self, seed: u32) {
        self.mt[0] = seed;
        for index in 1..624 {
            self.mt[index] = 1_812_433_253_u32
                .wrapping_mul(self.mt[index - 1] ^ (self.mt[index - 1] >> 30))
                .wrapping_add(index as u32);
        }
        self.index = 624;
    }

    #[cfg_attr(
        not(any(
            feature = "nightly-tests",
            feature = "production-smoke-driver",
            feature = "mdns"
        )),
        allow(
            dead_code,
            reason = "KEEP-FEATURE: live only under nightly-tests, production-smoke-driver or mdns"
        )
    )]
    fn init_by_array(&mut self, key: &[u32]) {
        self.init_genrand(19_650_218);
        let mut i = 1;
        let mut j = 0;
        for _ in 0..624.max(key.len()) {
            self.mt[i] = (self.mt[i]
                ^ (self.mt[i - 1] ^ (self.mt[i - 1] >> 30)).wrapping_mul(1_664_525))
            .wrapping_add(key[j])
            .wrapping_add(j as u32);
            i += 1;
            j += 1;
            if i >= 624 {
                self.mt[0] = self.mt[623];
                i = 1;
            }
            if j >= key.len() {
                j = 0;
            }
        }
        for _ in 0..623 {
            self.mt[i] = (self.mt[i]
                ^ (self.mt[i - 1] ^ (self.mt[i - 1] >> 30)).wrapping_mul(1_566_083_941))
            .wrapping_sub(i as u32);
            i += 1;
            if i >= 624 {
                self.mt[0] = self.mt[623];
                i = 1;
            }
        }
        self.mt[0] = 0x8000_0000;
    }

    #[cfg_attr(
        not(any(
            feature = "nightly-tests",
            feature = "production-smoke-driver",
            feature = "mdns"
        )),
        allow(
            dead_code,
            reason = "KEEP-FEATURE: live only under nightly-tests, production-smoke-driver or mdns"
        )
    )]
    fn gen_u32(&mut self) -> u32 {
        if self.index >= 624 {
            const MATRIX_A: u32 = 0x9908_b0df;
            for index in 0..624 {
                let joined =
                    (self.mt[index] & 0x8000_0000) | (self.mt[(index + 1) % 624] & 0x7fff_ffff);
                self.mt[index] = self.mt[(index + 397) % 624]
                    ^ (joined >> 1)
                    ^ if joined & 1 == 0 { 0 } else { MATRIX_A };
            }
            self.index = 0;
        }
        let mut value = self.mt[self.index];
        self.index += 1;
        value ^= value >> 11;
        value ^= (value << 7) & 0x9d2c_5680;
        value ^= (value << 15) & 0xefc6_0000;
        value ^= value >> 18;
        value
    }

    #[cfg_attr(
        not(any(
            feature = "nightly-tests",
            feature = "production-smoke-driver",
            feature = "mdns"
        )),
        allow(
            dead_code,
            reason = "KEEP-FEATURE: live only under nightly-tests, production-smoke-driver or mdns"
        )
    )]
    fn random(&mut self) -> f64 {
        let high = u64::from(self.gen_u32() >> 5);
        let low = u64::from(self.gen_u32() >> 6);
        ((high << 26) + low) as f64 / 9_007_199_254_740_992.0
    }

    #[cfg_attr(
        not(any(
            feature = "nightly-tests",
            feature = "production-smoke-driver",
            feature = "mdns"
        )),
        allow(
            dead_code,
            reason = "KEEP-FEATURE: live only under nightly-tests, production-smoke-driver or mdns"
        )
    )]
    fn gauss(&mut self) -> f64 {
        if let Some(value) = self.gauss_next.take() {
            return value;
        }
        let angle = self.random() * std::f64::consts::TAU;
        let radius = (-2.0 * (1.0 - self.random()).ln()).sqrt();
        let value = angle.cos() * radius;
        self.gauss_next = Some(angle.sin() * radius);
        value
    }

    #[cfg_attr(
        not(any(
            feature = "nightly-tests",
            feature = "production-smoke-driver",
            feature = "mdns"
        )),
        allow(
            dead_code,
            reason = "KEEP-FEATURE: live only under nightly-tests, production-smoke-driver or mdns"
        )
    )]
    fn randrange_24(&mut self) -> usize {
        loop {
            let value = (self.gen_u32() >> 27) as usize;
            if value < 24 {
                return value;
            }
        }
    }
}

#[cfg_attr(
    not(any(
        feature = "nightly-tests",
        feature = "production-smoke-driver",
        feature = "mdns"
    )),
    allow(
        dead_code,
        reason = "KEEP-FEATURE: live only under nightly-tests, production-smoke-driver or mdns"
    )
)]
fn fixture_digest(
    shape: &str,
    seed: usize,
    role: &str,
    ordinal: usize,
    dimensions: usize,
) -> [u8; 32] {
    sha256(format!(r#"["{shape}",{seed},"{role}",{ordinal},{dimensions}]"#).as_bytes())
}

#[cfg_attr(
    not(any(
        feature = "nightly-tests",
        feature = "production-smoke-driver",
        feature = "mdns"
    )),
    allow(
        dead_code,
        reason = "KEEP-FEATURE: live only under nightly-tests, production-smoke-driver or mdns"
    )
)]
fn center_digest(seed: usize, topic: usize, dimensions: usize) -> [u8; 32] {
    sha256(format!(r#"["center",{seed},{topic},{dimensions}]"#).as_bytes())
}

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by vector, vector_a_filtered_scope_is_charged_for_its_candidates, vector_first_filt"
)]
pub(super) fn vector(role: &str, ordinal: usize, dimensions: usize) -> Vec<f32> {
    vector_for("topic_clusters", 17, role, ordinal, dimensions)
}

#[cfg_attr(
    not(any(
        feature = "nightly-tests",
        feature = "production-smoke-driver",
        feature = "mdns"
    )),
    allow(
        dead_code,
        reason = "KEEP-FEATURE: live only under nightly-tests, production-smoke-driver or mdns"
    )
)]
pub(super) fn vector_for(
    shape: &str,
    seed: usize,
    role: &str,
    ordinal: usize,
    dimensions: usize,
) -> Vec<f32> {
    let mut source =
        PythonRandom::from_sha256(fixture_digest(shape, seed, role, ordinal, dimensions));
    let mut values = (0..dimensions).map(|_| source.gauss()).collect::<Vec<_>>();
    match shape {
        "topic_clusters" => {
            let topic = source.randrange_24();
            let mut center = PythonRandom::from_sha256(center_digest(seed, topic, dimensions));
            for value in &mut values {
                *value = center.gauss() + 0.35 * *value;
            }
        }
        "anisotropic" => {
            for (index, value) in values.iter_mut().enumerate() {
                *value = *value / ((index + 1) as f64).sqrt() + if index == 0 { 1.5 } else { 0.0 };
            }
        }
        "isotropic" => {}
        other => panic!("unknown frozen vector distribution {other}"),
    }
    let norm = values.iter().map(|value| value * value).sum::<f64>().sqrt();
    values
        .into_iter()
        .map(|value| (value / norm) as f32)
        .collect()
}
