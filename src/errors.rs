use crate::de;
use crate::ser;
use thiserror::Error;

pub(crate) type AvroResult<T> = Result<T, Error>;

#[non_exhaustive]
#[derive(Error, Debug)]
/// Error type returned from the library
pub enum Error {
    /// Represents all cases of `std::io::Error`
    #[error(transparent)]
    IO(#[from] std::io::Error),

    /// Describes errors happened while decoding Avro data (except for `std::io::Error`)
    #[error("Decoding error: {0}")]
    Decode(String),

    /// Describes errors happened while parsing Avro schemas
    #[error("Failed to parse schema: {0}")]
    Parse(String),

    /// Describes errors happened while performing schema resolution on Avro data.
    #[error("Schema resolution error: {0}")]
    SchemaResolution(String),

    /// Describes errors happened while validating Avro data.
    #[error("Validation error: {0}")]
    Validation(String),

    // TODO: figure out how to move the implementation of this error here
    /// Describes errors that could be encountered while serializing data
    #[error(transparent)]
    Ser(#[from] ser::Error),

    // TODO: figure out how to move the implementation of this error here
    /// Describes errors that could be encountered while serializing data
    #[error(transparent)]
    De(#[from] de::Error),

    // TODO: merge with SerError somehow?
    // /// Represents all cases of `serde::ser::Error`
    //#[error(transparent)]
    //SerdeSerError(#[from] serde::ser::Error),
    /// Describes errors happened trying to allocate too many bytes
    #[error("Unable to allocate {desired} bytes (maximum allowed: {maximum})")]
    MemoryAllocation { desired: usize, maximum: usize },

    /// Represents all cases of `uuid::Error`
    #[error(transparent)]
    Uuid(#[from] uuid::Error),

    /// Describe a specific error happening with decimal representation
    #[error("Number of bytes requested for decimal sign extension {requested} is less than the number of bytes needed to decode {needed}")]
    SignExtend { requested: usize, needed: usize },

    // TODO: Should this be wrapped by ParseSchemaError somehow?
    /// Represents all cases of `std::num::TryFromIntError`
    #[error(transparent)]
    TryFromInt(#[from] std::num::TryFromIntError),

    // TODO: Should this be wrapped by ParseSchemaError somehow?
    /// Represents all cases of `serde_json::Error`
    #[error(transparent)]
    JSON(#[from] serde_json::Error),

    // TODO: Should this be wrapped by SchemaResolutionError somehow?
    /// Represents all cases of `std::string::FromUtf8Error`
    #[error(transparent)]
    FromUtf8(#[from] std::string::FromUtf8Error),

    /// Represents errors coming from Snappy encoding and decoding
    #[cfg(feature = "snappy")]
    #[error(transparent)]
    Snappy(#[from] snap::Error),
}
