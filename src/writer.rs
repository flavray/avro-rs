//! Logic handling writing in Avro format at user level.
use std::collections::HashMap;
use std::io::Write;

use failure::{err_msg, Error};
use rand::random;
use serde::Serialize;
use serde_json;

use encode::{encode, encode_to_vec};
use schema::Schema;
use ser::Serializer;
use types::{ToAvro, Value};
use Codec;

const SYNC_SIZE: usize = 16;
const SYNC_INTERVAL: usize = 1000 * SYNC_SIZE; // TODO: parametrize in Writer

const AVRO_OBJECT_HEADER: &'static [u8] = &[b'O', b'b', b'j', 1u8];

/// Main interface for writing Avro formatted values.
pub struct Writer<'a, W> {
    schema: &'a Schema,
    serializer: Serializer,
    writer: W,
    buffer: Vec<u8>,
    num_values: usize,
    codec: Codec,
    marker: Vec<u8>,
    has_header: bool,
}

impl<'a, W: Write> Writer<'a, W> {
    /// Creates a `Writer` given a `Schema` and something implementing the `Write` trait to write
    /// to.
    /// No compression `Codec` will be used.
    pub fn new(schema: &'a Schema, writer: W) -> Writer<'a, W> {
        Self::with_codec(schema, writer, Codec::Null)
    }

    /// Creates a `Writer` with a specific `Codec` given a `Schema` and something implementing the
    /// `Write` trait to write to.
    pub fn with_codec(schema: &'a Schema, writer: W, codec: Codec) -> Writer<'a, W> {
        let mut marker = Vec::with_capacity(16);
        for _ in 0..16 {
            marker.push(random::<u8>());
        }

        Writer {
            schema,
            serializer: Serializer::new(),
            writer,
            buffer: Vec::with_capacity(SYNC_INTERVAL),
            num_values: 0,
            codec,
            marker,
            has_header: false,
        }
    }

    /// Get a reference to the `Schema` associated to a `Writer`.
    pub fn schema(&self) -> &'a Schema {
        self.schema
    }

    /// Append a compatible value (implementing the `ToAvro` trait) to a `Writer`, also performing
    /// schema validation.
    ///
    /// Return the number of bytes written (it might be 0, see below).
    ///
    /// **NOTE** This function is not guaranteed to perform any actual write, since it relies on
    /// internal buffering for performance reasons. If you want to be sure the value has been
    /// written, then call [`flush`](struct.Writer.html#method.flush).
    pub fn append<T: ToAvro>(&mut self, value: T) -> Result<usize, Error> {
        if !self.has_header {
            let header = header(self.schema, self.codec, self.marker.as_ref())?;
            self.append_bytes(header.as_ref())?;
            self.has_header = true;
        }

        write_avro_datum(self.schema, value, &mut self.buffer)?;

        self.num_values += 1;

        if self.buffer.len() >= SYNC_INTERVAL {
            return self.flush()
        }

        Ok(0) // Technically, no bytes have been written
    }

    /// Append anything implementing the `Serialize` to a `Writer` for
    /// [`serde`](https://docs.serde.rs/serde/index.html) compatibility, also performing schema
    /// validation.
    ///
    /// Return the number of bytes written.
    ///
    /// **NOTE** This function is not guaranteed to perform any actual write, since it relies on
    /// internal buffering for performance reasons. If you want to be sure the value has been
    /// written, then call [`flush`](struct.Writer.html#method.flush).
    pub fn append_ser<S: Serialize>(&mut self, value: S) -> Result<usize, Error> {
        let avro_value = value.serialize(&mut self.serializer)?;
        self.append(avro_value)
    }

    /// Generate and append synchronization marker to the payload.
    fn append_marker(&mut self) -> Result<usize, Error> {
        // using .writer.write directly to avoid mutable borrow of self
        // with ref borrowing of self.marker
        Ok(self.writer.write(&self.marker)?)
    }

    /// Append a raw Avro Value to the payload avoiding to encode it again.
    fn append_raw(&mut self, value: Value) -> Result<usize, Error> {
        self.append_bytes(encode_to_vec(value).as_ref())
    }

    /// Append pure bytes to the payload.
    fn append_bytes(&mut self, bytes: &[u8]) -> Result<usize, Error> {
        Ok(self.writer.write(bytes)?)
    }

    /// Extend a `Writer` with an `Iterator` of compatible values (implementing the `ToAvro`
    /// trait).
    ///
    /// Return the number of bytes written.
    ///
    /// **NOTE** This function is not guaranteed to perform any actual write, since it relies on
    /// internal buffering for performance reasons. If you want to be sure the value has been
    /// written, then call [`flush`](struct.Writer.html#method.flush).
    pub fn extend<I, T: ToAvro>(&mut self, values: I) -> Result<usize, Error>
    where
        I: Iterator<Item = T>,
    {
        /*
        https://github.com/rust-lang/rfcs/issues/811 :(
        let mut stream = values
            .filter_map(|value| value.serialize(&mut self.serializer).ok())
            .map(|value| value.encode(self.schema))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| err_msg("value does not match given schema"))?
            .into_iter()
            .fold(Vec::new(), |mut acc, stream| {
                num_values += 1;
                acc.extend(stream); acc
            });
        */

        let mut num_bytes = 0;
        for value in values {
            num_bytes += self.append(value)?;
        }
        self.flush()?;

        Ok(num_bytes)
    }

    /// Flush the content appended to a `Writer`. Call this function to make sure all the content
    /// has been written before releasing the `Writer`.
    ///
    /// Return the number of bytes written.
    pub fn flush(&mut self) -> Result<usize, Error> {
        if self.num_values == 0 {
            return Ok(0)
        }

        self.codec.compress(&mut self.buffer)?;

        let num_values = self.num_values;
        let stream_len = self.buffer.len();

        let num_bytes = self.append_raw(num_values.avro())? + self.append_raw(stream_len.avro())?
            + self.writer.write(self.buffer.as_ref())?
            + self.append_marker()?;

        self.buffer.clear();
        self.num_values = 0;

        Ok(num_bytes)
    }

    /// Return what the `Writer` is writing to, consuming the `Writer` itself.
    ///
    /// **NOTE** This function doesn't guarantee that everything gets written before consuming the
    /// buffer. Please call [`flush`](struct.Writer.html#method.flush) before.
    pub fn into_inner(self) -> W {
        self.writer
    }
}

/// Create an Avro header given a schema, a codec and a sync marker.
fn header(schema: &Schema, codec: Codec, marker: &[u8]) -> Result<Vec<u8>, Error> {
    let schema_bytes = serde_json::to_string(schema)?.into_bytes();

    let mut metadata = HashMap::with_capacity(2);
    metadata.insert("avro.schema", Value::Bytes(schema_bytes));
    metadata.insert("avro.codec", codec.avro());

    let mut header = Vec::new();
    header.extend_from_slice(AVRO_OBJECT_HEADER);
    encode(metadata.avro(), &mut header);
    header.extend_from_slice(marker);

    Ok(header)
}

/// Encode a compatible value (implementing the `ToAvro` trait) into Avro format, also performing
/// schema validation.
///
/// This is an internal function which gets the bytes buffer where to write as parameter instead of
/// creating a new one like `to_avro_datum`.
fn write_avro_datum<T: ToAvro>(
    schema: &Schema,
    value: T,
    buffer: &mut Vec<u8>,
) -> Result<(), Error> {
    let avro = value.avro();
    if !avro.validate(schema) {
        return Err(err_msg("value does not match schema"))
    }
    // TODO: this resolve call is super-expensive, try to pass the schema to avro() instead
    Ok(encode(avro.resolve(schema)?, buffer))
}

/// Encode a compatible value (implementing the `ToAvro` trait) into Avro format, also
/// performing schema validation.
///
/// **NOTE** This function has a quite small niche of usage and does NOT generate headers and sync
/// markers; use [`Writer`](struct.Writer.html) to be fully Avro-compatible if you don't know what
/// you are doing, instead.
pub fn to_avro_datum<T: ToAvro>(schema: &Schema, value: T) -> Result<Vec<u8>, Error> {
    let mut buffer = Vec::new();
    write_avro_datum(schema, value, &mut buffer)?;
    Ok(buffer)
}
