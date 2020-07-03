use crate::de;
use crate::ser;
use thiserror::Error;

#[derive(Error, Debug)]
/// Error type returned from the library
pub enum AvroError {
    /// Represents all cases of `std::io::Error`
    #[error(transparent)]
    IOError(#[from] std::io::Error),

    /// Describes errors happened while decoding Avro data (except for `std::io::Error`)
    #[error("Decoding error: {0}")]
    DecodeError(String),

    /// Describes errors happened while parsing Avro schemas
    #[error("Failed to parse schema: {0}")]
    ParseSchemaError(String),

    /// Describes errors happened while performing schema resolution on Avro data.
    #[error("Schema resolution error: {0}")]
    SchemaResolutionError(String),

    /// Describes errors happened while validating Avro data.
    #[error("Validation error: {0}")]
    ValidationError(String),

    // TODO: figure out how to move the implementation of this error here
    /// Describes errors that could be encountered while serializing data
    #[error(transparent)]
    SerError(#[from] ser::Error),

    // TODO: figure out how to move the implementation of this error here
    /// Describes errors that could be encountered while serializing data
    #[error(transparent)]
    DeError(#[from] de::Error),

    // TODO: merge with SerError somehow?
    // /// Represents all cases of `serde::ser::Error`
    //#[error(transparent)]
    //SerdeSerError(#[from] serde::ser::Error),
    /// Describes errors happened trying to allocate too many bytes
    #[error("Unable to allocate {desired} bytes (maximum allowed: {maximum})")]
    MemoryAllocationError { desired: usize, maximum: usize },

    /// Represents all cases of `uuid::Error`
    #[error(transparent)]
    UuidError(#[from] uuid::Error),

    /// Describe a specific error happening with decimal representation
    #[error("Number of bytes requested for decimal sign extension {requested} is less than the number of bytes needed to decode {needed}")]
    SignExtendError { requested: usize, needed: usize },

    // TODO: Should this be wrapped by ParseSchemaError somehow?
    /// Represents all cases of `std::num::TryFromIntError`
    #[error(transparent)]
    TryFromIntError(#[from] std::num::TryFromIntError),

    // TODO: Should this be wrapped by ParseSchemaError somehow?
    /// Represents all cases of `serde_json::Error`
    #[error(transparent)]
    JSONError(#[from] serde_json::Error),

    // TODO: Should this be wrapped by SchemaResolutionError somehow?
    /// Represents all cases of `std::string::FromUtf8Error`
    #[error(transparent)]
    FromUtf8Error(#[from] std::string::FromUtf8Error),

    /// Represents errors coming from Snappy encoding and decoding
    #[cfg(feature = "snappy")]
    #[error(transparent)]
    SnappyError(#[from] snap::Error),
}
