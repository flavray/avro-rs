#[derive(Debug, PartialEq, Clone)]
pub struct RawDecimal<T> {
    value: T,
    num_bytes: usize,
}

impl<T> RawDecimal<T> {
    pub fn into_inner(self) -> T {
        self.value
    }

    pub(crate) fn num_bytes(&self) -> usize {
        self.num_bytes
    }
}

#[cfg(feature = "safe-decimal")]
impl From<&Decimal> for Vec<u8> {
    fn from(decimal: &Decimal) -> Self {
        let sign_value = u8::from(decimal.value.sign() == num_bigint::Sign::Minus);
        let num_bytes = decimal.num_bytes;
        let mut result = vec![sign_value; num_bytes];
        let raw_bytes = decimal.value.to_signed_bytes_be();
        result[(num_bytes - raw_bytes.len())..].copy_from_slice(&raw_bytes);
        result
    }
}

#[cfg(feature = "safe-decimal")]
impl From<Vec<u8>> for Decimal {
    fn from(bytes: Vec<u8>) -> Self {
        let value = num_bigint::BigInt::from_signed_bytes_be(&bytes);
        let num_bytes = bytes.len();
        Self { value, num_bytes }
    }
}

#[cfg(not(feature = "safe-decimal"))]
impl From<&Decimal> for Vec<u8> {
    fn from(decimal: &Decimal) -> Self {
        decimal.value.clone()
    }
}

#[cfg(not(feature = "safe-decimal"))]
impl From<Vec<u8>> for Decimal {
    fn from(value: Vec<u8>) -> Self {
        let num_bytes = value.len();
        Self { value, num_bytes }
    }
}

#[cfg(feature = "safe-decimal")]
pub type Decimal = RawDecimal<num_bigint::BigInt>;

#[cfg(not(feature = "safe-decimal"))]
pub type Decimal = RawDecimal<Vec<u8>>;
