use crate::de;
use crate::ser;
use thiserror::Error;

pub(crate) type AvroResult<T> = Result<T, Error>;

#[non_exhaustive]
#[derive(Error, Debug)]
/// Error type returned from the library
pub enum Error {
    /// All cases of `std::io::Error`
    #[error(transparent)]
    IO(#[from] std::io::Error),

    /// Error due to unrecognized coded
    #[error("Unrecognized codec: {0:?}")]
    Codec(String),

    /// Errors happened while decoding Avro data (except for `std::io::Error`)
    #[error("Decoding error: {0}")]
    Decode(String),

    /// Errors happened while parsing Avro schemas
    #[error("Failed to parse schema: {0}")]
    Parse(String),

    /// Errors happened while performing schema resolution on Avro data
    #[error("Schema resolution error: {0}")]
    SchemaResolution(String),

    /// Errors happened while validating Avro data
    #[error("Validation error: {0}")]
    Validation(String),

    /// Errors that could be encountered while serializing data
    #[error(transparent)]
    Ser(#[from] ser::Error),

    /// Errors that could be encountered while serializing data
    #[error(transparent)]
    De(#[from] de::Error),

    // TODO: merge with SerError somehow?
    // /// All cases of `serde::ser::Error`
    //#[error(transparent)]
    //SerdeSerError(#[from] serde::ser::Error),
    /// Error happened trying to allocate too many bytes
    #[error("Unable to allocate {desired} bytes (maximum allowed: {maximum})")]
    MemoryAllocation { desired: usize, maximum: usize },

    /// All cases of `uuid::Error`
    #[error(transparent)]
    Uuid(#[from] uuid::Error),

    /// Error happening with decimal representation
    #[error("Number of bytes requested for decimal sign extension {requested} is less than the number of bytes needed to decode {needed}")]
    SignExtend { requested: usize, needed: usize },

    /// All cases of `std::num::TryFromIntError`
    #[error(transparent)]
    TryFromInt(#[from] std::num::TryFromIntError),

    /// All cases of `serde_json::Error`
    #[error(transparent)]
    JSON(#[from] serde_json::Error),

    /// All cases of `std::string::FromUtf8Error`
    #[error(transparent)]
    FromUtf8(#[from] std::string::FromUtf8Error),

    /// Error happening when there is a mismatch of the snappy CRC
    #[error("Bad Snappy CRC32; expected {0:x} but got {1:x}")]
    SnappyCrcError(u32, u32),

    /// Errors coming from Snappy encoding and decoding
    #[cfg(feature = "snappy")]
    #[error(transparent)]
    Snappy(#[from] snap::Error),
}
