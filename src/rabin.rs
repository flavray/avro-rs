use byteorder::{ByteOrder, LittleEndian};
use digest::consts::U8;
use digest::generic_array::GenericArray;
use digest::{FixedOutput, Reset, Update};
use lazy_static::lazy_static;

const EMPTY: i64 = -4513414715797952619;

lazy_static! {
    static ref FPTABLE: [i64; 256] = {
        let mut fp_table: [i64; 256] = [0; 256];
        for i in 0..256 {
            let mut fp = i;
            for _ in 0..8 {
                fp = (fp as u64 >> 1) as i64 ^ (EMPTY & -(fp & 1));
            }
            fp_table[i as usize] = fp
        }
        fp_table
    };
}

/// Implementation of the Rabin fingerprint algorithm
#[derive(Clone)]
pub struct Rabin {
    result: i64,
}

impl Default for Rabin {
    fn default() -> Self {
        Rabin { result: EMPTY }
    }
}

impl Update for Rabin {
    fn update(&mut self, input: impl AsRef<[u8]>) {
        for b in input.as_ref() {
            self.result = (self.result as u64 >> 8) as i64
                ^ FPTABLE[((self.result ^ *b as i64) & 0xff) as usize];
        }
    }
}

impl FixedOutput for Rabin {
    // 8-byte little-endian form of the i64
    // See: https://avro.apache.org/docs/current/spec.html#single_object_encoding
    type OutputSize = U8;

    fn finalize_into(self, out: &mut GenericArray<u8, Self::OutputSize>) {
        LittleEndian::write_i64(out, self.result);
    }

    fn finalize_into_reset(&mut self, out: &mut GenericArray<u8, Self::OutputSize>) {
        LittleEndian::write_i64(out, self.result);
        self.result = EMPTY;
    }
}

impl Reset for Rabin {
    fn reset(&mut self) {
        self.result = EMPTY;
    }
}

digest::impl_write!(Rabin);

#[cfg(test)]
mod tests {
    use super::Rabin;
    use byteorder::{ByteOrder, LittleEndian};
    use digest::Digest;

    // See: https://github.com/apache/avro/blob/master/share/test/data/schema-tests.txt
    #[test]
    fn test1() {
        let data: &[(&str, i64)] = &[
            (r#""null""#, 7195948357588979594),
            (r#""boolean""#, -6970731678124411036),
            (
                r#"{"name":"foo","type":"fixed","size":15}"#,
                1756455273707447556,
            ),
            (
                r#"{"name":"PigValue","type":"record","fields":[{"name":"value","type":["null","int","long","PigValue"]}]}"#,
                -1759257747318642341,
            ),
        ];

        let mut hasher = Rabin::new();

        for (s, fp) in data {
            hasher.update(s.as_bytes());
            let result = LittleEndian::read_i64(&hasher.finalize_reset().to_vec());
            assert_eq!(*fp, result);
        }
    }
}
