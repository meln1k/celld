//! A seeded generator whose output is fixed by this file alone.
//!
//! A library generator can change its stream between releases, and a seed
//! that reproduced a failure yesterday would then replay a different
//! schedule. xoshiro256** seeded through SplitMix64 is small enough to own.

#[derive(Clone, Debug)]
pub struct Rng {
    s: [u64; 4],
}

impl Rng {
    pub fn new(seed: u64) -> Self {
        let mut x = seed;
        let mut next = || {
            x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut z = x;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            z ^ (z >> 31)
        };
        Self {
            s: [next(), next(), next(), next()],
        }
    }

    pub fn next_u64(&mut self) -> u64 {
        let result = self.s[1].wrapping_mul(5).rotate_left(7).wrapping_mul(9);
        let t = self.s[1] << 17;
        self.s[2] ^= self.s[0];
        self.s[3] ^= self.s[1];
        self.s[1] ^= self.s[2];
        self.s[0] ^= self.s[3];
        self.s[2] ^= t;
        self.s[3] = self.s[3].rotate_left(45);
        result
    }

    /// A value in `0..n`. `n` must be positive.
    pub fn below(&mut self, n: u64) -> u64 {
        assert!(n > 0, "empty range");
        // Rejection keeps the distribution uniform; the loop almost never
        // repeats for the small ranges the simulator draws from.
        let zone = u64::MAX - u64::MAX % n;
        loop {
            let value = self.next_u64();
            if value < zone {
                return value % n;
            }
        }
    }

    /// A value in `lo..=hi`.
    pub fn range(&mut self, lo: u64, hi: u64) -> u64 {
        assert!(lo <= hi, "inverted range {lo}..={hi}");
        lo + self.below(hi - lo + 1)
    }

    /// True with probability `p`.
    pub fn chance(&mut self, p: f64) -> bool {
        if p <= 0.0 {
            return false;
        }
        if p >= 1.0 {
            return true;
        }
        ((self.next_u64() >> 11) as f64) < p * (1u64 << 53) as f64
    }

    pub fn pick<'a, T>(&mut self, items: &'a [T]) -> Option<&'a T> {
        if items.is_empty() {
            None
        } else {
            Some(&items[self.below(items.len() as u64) as usize])
        }
    }

    /// An independent stream for one concern, so drawing more values for one
    /// purpose does not shift every other decision in the run.
    pub fn fork(&mut self) -> Self {
        Self::new(self.next_u64())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_is_fixed_by_the_seed() {
        // Pinned values, checked against an independent implementation of
        // SplitMix64 and xoshiro256**: if this changes, every recorded seed
        // replays a different schedule.
        let mut rng = Rng::new(0);
        let first: Vec<u64> = (0..3).map(|_| rng.next_u64()).collect();
        assert_eq!(
            first,
            [
                0x99ec_5f36_cb75_f2b4,
                0xbf6e_1f78_4956_452a,
                0x1a5f_849d_4933_e6e0
            ]
        );
    }

    #[test]
    fn below_stays_in_range() {
        let mut rng = Rng::new(7);
        for n in 1..50 {
            for _ in 0..100 {
                assert!(rng.below(n) < n);
            }
        }
    }
}
