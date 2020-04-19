use num_bigint::BigInt;

#[derive(Debug, Clone)]
pub struct Decimal {
    value: BigInt,
    len: usize,
}

// We only care about value equality, not byte length. Can two equal BigInt's have two different
// byte lengths?
impl PartialEq for Decimal {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl Decimal {
    pub(crate) fn len(&self) -> usize {
        self.len
    }
}

impl From<&Decimal> for Vec<u8> {
    fn from(&Decimal { ref value, len }: &Decimal) -> Self {
        let sign_value = u8::from(value.sign() == num_bigint::Sign::Minus);
        let mut decimal_bytes = vec![sign_value; len];
        let raw_bytes = value.to_signed_bytes_be();
        decimal_bytes[(len - raw_bytes.len())..].copy_from_slice(&raw_bytes);
        decimal_bytes
    }
}

impl From<Vec<u8>> for Decimal {
    fn from(bytes: Vec<u8>) -> Self {
        Self {
            value: num_bigint::BigInt::from_signed_bytes_be(&bytes),
            len: bytes.len(),
        }
    }
}
