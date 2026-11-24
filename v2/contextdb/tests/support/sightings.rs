//! Generated sightings for the find-at-scale tests and `examples/measure.rs`, one copy for both.
//! Nothing here reads a clock or a thread: a size gives the same vectors on every run.

/// The index pilot's generator of numbers: a xorshift, and a number near a normal one made of
/// six of its draws.
pub struct Random(pub u64);

impl Random {
    pub fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    pub fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    pub fn g(&mut self) -> f32 {
        let s: f32 = (0..6)
            .map(|_| (self.next() >> 40) as f32 / (1u64 << 24) as f32)
            .sum();
        (s - 3.0) * 1.414
    }
}

pub const DIMENSIONS: usize = 768;

/// Generated sightings, to the number as the second measurement of the index made them. A
/// subject is a direction of its own beside its class's; its appearances vary along eight
/// directions of its own; a track is ten sightings in a row whose appearance drifts; every
/// sighting has a little noise on every number.
pub struct Sightings {
    subjects: usize,
    classes: Vec<Vec<f32>>,
    bases: Vec<Vec<f32>>,
    axes: Vec<Vec<f32>>,
    /// The generator's state after the directions, from which the questions are made.
    after: u64,
}

impl Sightings {
    const AXES: usize = 8;
    pub const TRACK: usize = 10;

    fn unit(v: &mut [f32]) {
        let n = v.iter().map(|x| f64::from(*x) * f64::from(*x));
        let n = n.sum::<f64>().sqrt();
        if n > 0.0 {
            v.iter_mut().for_each(|x| *x = (f64::from(*x) / n) as f32);
        }
    }

    pub fn new(rows: usize) -> Sightings {
        let subjects = (rows / 2500).max(8);
        let mut random = Random(0x51A7_1265_0BAD_CAFE);
        let mut directions = |count: usize| -> Vec<Vec<f32>> {
            let direction = |_| {
                let mut c: Vec<f32> = (0..DIMENSIONS).map(|_| random.g()).collect();
                Sightings::unit(&mut c);
                c
            };
            (0..count).map(direction).collect()
        };
        Sightings {
            subjects,
            classes: directions(3),
            bases: directions(subjects + 20),
            axes: directions((subjects + 20) * Sightings::AXES),
            after: random.0,
        }
    }

    /// One sighting of a subject with this appearance.
    fn make(&self, subject: usize, appearance: &[f32; 8], random: &mut Random) -> Vec<f32> {
        let class = &self.classes[subject % 3];
        let base = &self.bases[subject];
        let mut out: Vec<f32> = (0..DIMENSIONS)
            .map(|d| {
                0.5 * class[d] + 0.6 * base[d] + 0.25 * random.g() / (DIMENSIONS as f32).sqrt()
            })
            .collect();
        for (j, a) in appearance.iter().enumerate() {
            let axis = &self.axes[subject * Sightings::AXES + j];
            let weight = 0.5 * a / (Sightings::AXES as f32).sqrt();
            for (x, along) in out.iter_mut().zip(axis) {
                *x += weight * along;
            }
        }
        Sightings::unit(&mut out);
        out
    }

    /// The first `count` sightings of one track.
    pub fn track(&self, track: usize, count: usize) -> Vec<Vec<f32>> {
        let seed = (track as u64 + 1).wrapping_mul(0x2545_F491_4F6C_DD1D);
        let mut random = Random(0x51A7_1265_0BAD_CAFE ^ seed);
        random.next();
        let subject = random.below(self.subjects);
        let mut appearance: [f32; 8] = std::array::from_fn(|_| random.g());
        let mut sighting = || {
            let made = self.make(subject, &appearance, &mut random);
            appearance = appearance.map(|a| 0.9 * a + 0.436 * random.g());
            made
        };
        (0..count).map(|_| sighting()).collect()
    }

    /// The first `rows` sightings, track after track.
    pub fn rows(&self, rows: usize) -> impl Iterator<Item = Vec<f32>> + '_ {
        let tracks = 0..rows.div_ceil(Sightings::TRACK);
        tracks.flat_map(move |track| {
            self.track(track, Sightings::TRACK.min(rows - track * Sightings::TRACK))
        })
    }

    /// The 100 questions: eighty new sightings of stored subjects, twenty of subjects never
    /// stored.
    pub fn questions(&self) -> Vec<Vec<f32>> {
        let mut random = Random(self.after);
        let mut questions = Vec::new();
        for j in 0..100 {
            let subject = if j % 5 == 4 {
                self.subjects + j / 5
            } else {
                random.below(self.subjects)
            };
            let appearance = std::array::from_fn(|_| random.g());
            questions.push(self.make(subject, &appearance, &mut random));
        }
        questions
    }
}
