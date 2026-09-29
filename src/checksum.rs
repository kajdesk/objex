//! Additional S3 checksums (x-amz-checksum-*). Values are base64 of the big-endian digest.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use serde::{Deserialize, Serialize};
use sha1::Digest as _;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChecksumAlgo {
    Crc32,
    Crc32c,
    Crc64nvme,
    Sha1,
    Sha256,
}

impl ChecksumAlgo {
    pub const ALL: [ChecksumAlgo; 5] =
        [ChecksumAlgo::Crc32, ChecksumAlgo::Crc32c, ChecksumAlgo::Crc64nvme, ChecksumAlgo::Sha1, ChecksumAlgo::Sha256];

    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_uppercase().as_str() {
            "CRC32" => Some(Self::Crc32),
            "CRC32C" => Some(Self::Crc32c),
            "CRC64NVME" => Some(Self::Crc64nvme),
            "SHA1" => Some(Self::Sha1),
            "SHA256" => Some(Self::Sha256),
            _ => None,
        }
    }

    /// Name used in XML and x-amz-checksum-algorithm headers.
    pub fn name(self) -> &'static str {
        match self {
            Self::Crc32 => "CRC32",
            Self::Crc32c => "CRC32C",
            Self::Crc64nvme => "CRC64NVME",
            Self::Sha1 => "SHA1",
            Self::Sha256 => "SHA256",
        }
    }

    /// Header carrying the value, e.g. x-amz-checksum-crc32.
    pub fn header(self) -> &'static str {
        match self {
            Self::Crc32 => "x-amz-checksum-crc32",
            Self::Crc32c => "x-amz-checksum-crc32c",
            Self::Crc64nvme => "x-amz-checksum-crc64nvme",
            Self::Sha1 => "x-amz-checksum-sha1",
            Self::Sha256 => "x-amz-checksum-sha256",
        }
    }

    pub fn from_header(h: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|a| a.header().eq_ignore_ascii_case(h))
    }

    /// XML element name in responses, e.g. ChecksumCRC32.
    pub fn xml_tag(self) -> &'static str {
        match self {
            Self::Crc32 => "ChecksumCRC32",
            Self::Crc32c => "ChecksumCRC32C",
            Self::Crc64nvme => "ChecksumCRC64NVME",
            Self::Sha1 => "ChecksumSHA1",
            Self::Sha256 => "ChecksumSHA256",
        }
    }
}

/// How a multipart object's checksum is formed: a checksum of the part checksums
/// ("value-N"), or a checksum of the whole object's bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChecksumType {
    Composite,
    FullObject,
}

impl ChecksumType {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_uppercase().as_str() {
            "COMPOSITE" => Some(Self::Composite),
            "FULL_OBJECT" => Some(Self::FullObject),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Composite => "COMPOSITE",
            Self::FullObject => "FULL_OBJECT",
        }
    }

    /// Type of a stored checksum value: composite values carry a "-N" suffix.
    pub fn of(c: &Checksum) -> Self {
        if c.value.contains('-') { Self::Composite } else { Self::FullObject }
    }
}

/// A computed or expected checksum.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checksum {
    pub algo: ChecksumAlgo,
    /// base64-encoded digest
    pub value: String,
}

pub enum ChecksumHasher {
    Crc32(crc32fast::Hasher),
    Crc32c(u32),
    Crc64nvme(crc64fast_nvme::Digest),
    Sha1(sha1::Sha1),
    Sha256(sha2::Sha256),
}

impl ChecksumHasher {
    pub fn new(algo: ChecksumAlgo) -> Self {
        match algo {
            ChecksumAlgo::Crc32 => Self::Crc32(crc32fast::Hasher::new()),
            ChecksumAlgo::Crc32c => Self::Crc32c(0),
            ChecksumAlgo::Crc64nvme => Self::Crc64nvme(crc64fast_nvme::Digest::new()),
            ChecksumAlgo::Sha1 => Self::Sha1(sha1::Sha1::new()),
            ChecksumAlgo::Sha256 => Self::Sha256(sha2::Sha256::new()),
        }
    }

    pub fn algo(&self) -> ChecksumAlgo {
        match self {
            Self::Crc32(_) => ChecksumAlgo::Crc32,
            Self::Crc32c(_) => ChecksumAlgo::Crc32c,
            Self::Crc64nvme(_) => ChecksumAlgo::Crc64nvme,
            Self::Sha1(_) => ChecksumAlgo::Sha1,
            Self::Sha256(_) => ChecksumAlgo::Sha256,
        }
    }

    pub fn update(&mut self, data: &[u8]) {
        match self {
            Self::Crc32(h) => h.update(data),
            Self::Crc32c(c) => *c = crc32c::crc32c_append(*c, data),
            Self::Crc64nvme(h) => h.write(data),
            Self::Sha1(h) => h.update(data),
            Self::Sha256(h) => h.update(data),
        }
    }

    pub fn finish(self) -> Checksum {
        let algo = self.algo();
        let value = match self {
            Self::Crc32(h) => B64.encode(h.finalize().to_be_bytes()),
            Self::Crc32c(c) => B64.encode(c.to_be_bytes()),
            Self::Crc64nvme(h) => B64.encode(h.sum64().to_be_bytes()),
            Self::Sha1(h) => B64.encode(h.finalize()),
            Self::Sha256(h) => B64.encode(h.finalize()),
        };
        Checksum { algo, value }
    }
}

impl ChecksumAlgo {
    pub fn is_crc(self) -> bool {
        matches!(self, Self::Crc32 | Self::Crc32c | Self::Crc64nvme)
    }

    /// Default multipart checksum type when the client does not choose one.
    pub fn default_type(self) -> ChecksumType {
        if self == Self::Crc64nvme { ChecksumType::FullObject } else { ChecksumType::Composite }
    }

    /// Reflected polynomial and width in bits, for CRC algorithms.
    fn crc_params(self) -> Option<(u64, u32)> {
        match self {
            Self::Crc32 => Some((0xEDB8_8320, 32)),
            Self::Crc32c => Some((0x82F6_3B78, 32)),
            Self::Crc64nvme => Some((0x9A6C_9329_AC4B_C9B5, 64)),
            _ => None,
        }
    }
}

impl Checksum {
    pub fn decode(&self) -> Option<Vec<u8>> {
        B64.decode(&self.value).ok()
    }
}

/// Checksum of the concatenated raw digests of the parts, suffixed with "-N".
pub fn composite(algo: ChecksumAlgo, parts: &[Checksum]) -> Option<Checksum> {
    let mut h = ChecksumHasher::new(algo);
    for p in parts {
        if p.algo != algo {
            return None;
        }
        h.update(&p.decode()?);
    }
    let mut c = h.finish();
    c.value = format!("{}-{}", c.value, parts.len());
    Some(c)
}

/// Full-object CRC of a multipart object, combined from the part CRCs and sizes.
pub fn combine_full(algo: ChecksumAlgo, parts: &[(Checksum, u64)]) -> Option<Checksum> {
    let (poly, bits) = algo.crc_params()?;
    let mut acc: Option<u64> = None;
    for (c, len) in parts {
        if c.algo != algo {
            return None;
        }
        let raw = c.decode()?;
        if raw.len() * 8 != bits as usize {
            return None;
        }
        let v = raw.iter().fold(0u64, |a, b| (a << 8) | *b as u64);
        acc = Some(match acc {
            None => v,
            Some(a) => crc_combine(poly, bits, a, v, *len),
        });
    }
    let v = acc?;
    let bytes = v.to_be_bytes();
    Some(Checksum { algo, value: B64.encode(&bytes[8 - bits as usize / 8..]) })
}

// zlib-style CRC combination over GF(2), valid for reflected CRCs with all-ones init
// and final xor (CRC32, CRC32C and CRC64/NVME all are).
fn gf2_times(mat: &[u64; 64], mut vec: u64) -> u64 {
    let mut sum = 0;
    let mut i = 0;
    while vec != 0 {
        if vec & 1 != 0 {
            sum ^= mat[i];
        }
        vec >>= 1;
        i += 1;
    }
    sum
}

fn gf2_square(sq: &mut [u64; 64], mat: &[u64; 64], bits: u32) {
    for n in 0..bits as usize {
        sq[n] = gf2_times(mat, mat[n]);
    }
}

fn crc_combine(poly: u64, bits: u32, crc1: u64, crc2: u64, mut len2: u64) -> u64 {
    if len2 == 0 {
        return crc1;
    }
    let mut even = [0u64; 64];
    let mut odd = [0u64; 64];
    // operator for one zero bit
    odd[0] = poly;
    let mut row = 1u64;
    for n in 1..bits as usize {
        odd[n] = row;
        row <<= 1;
    }
    gf2_square(&mut even, &odd, bits); // two zero bits
    gf2_square(&mut odd, &even, bits); // four zero bits
    let mut crc1 = crc1;
    loop {
        gf2_square(&mut even, &odd, bits);
        if len2 & 1 != 0 {
            crc1 = gf2_times(&even, crc1);
        }
        len2 >>= 1;
        if len2 == 0 {
            break;
        }
        gf2_square(&mut odd, &even, bits);
        if len2 & 1 != 0 {
            crc1 = gf2_times(&odd, crc1);
        }
        len2 >>= 1;
        if len2 == 0 {
            break;
        }
    }
    crc1 ^ crc2
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sum(algo: ChecksumAlgo, data: &[u8]) -> String {
        let mut h = ChecksumHasher::new(algo);
        h.update(&data[..3]);
        h.update(&data[3..]);
        h.finish().value
    }

    #[test]
    fn check_values() {
        let data = b"123456789";
        // standard CRC "check" values, big-endian, base64
        assert_eq!(sum(ChecksumAlgo::Crc32, data), B64.encode(0xCBF43926u32.to_be_bytes()));
        assert_eq!(sum(ChecksumAlgo::Crc32c, data), B64.encode(0xE3069283u32.to_be_bytes()));
        assert_eq!(sum(ChecksumAlgo::Crc64nvme, data), B64.encode(0xAE8B14860A799888u64.to_be_bytes()));
        assert_eq!(sum(ChecksumAlgo::Sha1, b"abc"), "qZk+NkcGgWq6PiVxeFDCbJzQ2J0=");
    }

    #[test]
    fn combine() {
        let data: Vec<u8> = (0..10_000u32).map(|i| (i * 31 % 251) as u8).collect();
        for algo in [ChecksumAlgo::Crc32, ChecksumAlgo::Crc32c, ChecksumAlgo::Crc64nvme] {
            let whole = sum(algo, &data);
            let parts: Vec<(Checksum, u64)> = [&data[..3000], &data[3000..3001], &data[3001..]]
                .iter()
                .map(|p| {
                    let mut h = ChecksumHasher::new(algo);
                    h.update(p);
                    (h.finish(), p.len() as u64)
                })
                .collect();
            assert_eq!(combine_full(algo, &parts).unwrap().value, whole, "{}", algo.name());
        }
    }
}
