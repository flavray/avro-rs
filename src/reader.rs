//! Logic handling reading from Avro format at user level.
use std::collections::VecDeque;
use std::io::{ErrorKind, Read};
use std::rc::Rc;
use std::str::{from_utf8, FromStr};

use failure::{err_msg, Error};
use serde_json::from_slice;

use decode::decode;
use schema::Schema;
use types::Value;
use Codec;

/// Main interface for reading Avro formatted values.
///
/// To be used as in iterator:
///
/// ```no_run
/// # use avro::Reader;
/// # use std::io::Cursor;
/// # let input = Cursor::new(Vec::<u8>::new());
/// for value in Reader::new(input) {
///     match value {
///         Ok(v) => println!("{:?}", v),
///         Err(e) => println!("Error: {}", e),
///     };
/// }
/// ```
pub struct Reader<'a, R> {
    reader: R,
    reader_schema: Option<&'a Schema>,
    writer_schema: Schema,
    codec: Codec,
    marker: [u8; 16],
    items: VecDeque<Value>,
    already_read_header: bool,
}

impl<'a, R: Read> Reader<'a, R> {
    /// Creates a `Reader` given something implementing the `io::Read` trait to read from.
    /// No reader `Schema` will be set.
    pub fn new(reader: R) -> Reader<'a, R> {
        Reader {
            reader,
            reader_schema: None,
            writer_schema: Schema::Null,
            codec: Codec::Null,
            marker: [0u8; 16],
            items: VecDeque::new(),
            already_read_header: false,
        }
    }

    /// Creates a `Reader` given a reader `Schema` and something implementing the `io::Read` trait
    /// to read from.
    pub fn with_schema(schema: &'a Schema, reader: R) -> Reader<'a, R> {
        Reader {
            reader,
            reader_schema: Some(schema),
            writer_schema: Schema::Null,
            codec: Codec::Null,
            marker: [0u8; 16],
            items: VecDeque::new(),
            already_read_header: false,
        }
    }

    /// Get a reference to the writer `Schema`.
    pub fn writer_schema(&self) -> &Schema {
        &self.writer_schema
    }

    /// Get a reference to the optional reader `Schema`.
    pub fn reader_schema(&self) -> Option<&Schema> {
        self.reader_schema
    }

    /// Try to read the header and to set the writer `Schema`, the `Codec` and the marker based on
    /// its content.
    fn read_header(&mut self) -> Result<(), Error> {
        let meta_schema = Schema::Map(Rc::new(Schema::Bytes));

        let mut buf = [0u8; 4];
        self.reader.read_exact(&mut buf)?;

        if buf != ['O' as u8, 'b' as u8, 'j' as u8, 1u8] {
            return Err(err_msg("wrong magic in header"))
        }

        if let Value::Map(meta) = decode(&meta_schema, &mut self.reader)? {
            let schema = meta.get("avro.schema")
                .and_then(|bytes| {
                    if let &Value::Bytes(ref bytes) = bytes {
                        from_slice(bytes.as_ref()).ok()
                    } else {
                        None
                    }
                })
                .and_then(|json| Schema::parse(&json).ok());

            if let Some(schema) = schema {
                self.writer_schema = schema
            } else {
                return Err(err_msg("unable to parse schema"))
            }

            if let Some(codec) = meta.get("avro.codec")
                .and_then(|codec| {
                    if let &Value::Bytes(ref bytes) = codec {
                        from_utf8(bytes.as_ref()).ok()
                    } else {
                        None
                    }
                })
                .and_then(|codec| Codec::from_str(codec).ok())
            {
                self.codec = codec;
            }
        } else {
            return Err(err_msg("no metadata in header"))
        }

        let mut buf = [0u8; 16];
        self.reader.read_exact(&mut buf)?;

        self.marker = buf;

        Ok(())
    }

    /// Try to read a data block, also performing schema resolution for the objects contained in
    /// the block. The objects are stored in an internal buffer to the `Reader`.
    ///
    /// It will also read the header in case it hasn't been read yet.
    fn read_block(&mut self) -> Result<(), Error> {
        if !self.already_read_header {
            self.read_header()?;
            self.already_read_header = true;
        }

        match decode(&Schema::Long, &mut self.reader) {
            Ok(block) => {
                if let Value::Long(block_len) = block {
                    if let Value::Long(block_bytes) = decode(&Schema::Long, &mut self.reader)? {
                        let mut bytes = vec![0u8; block_bytes as usize];
                        self.reader.read_exact(&mut bytes)?;

                        let mut marker = [0u8; 16];
                        self.reader.read_exact(&mut marker)?;

                        if marker != self.marker {
                            return Err(err_msg("block marker does not match header marker"))
                        }

                        self.codec.decompress(&mut bytes)?;

                        self.items.clear();
                        self.items.reserve_exact(block_len as usize);

                        for _ in 0..block_len {
                            let item = decode(&self.writer_schema, &mut &bytes[..])?;

                            let item = match self.reader_schema {
                                Some(ref schema) => item.resolve(schema)?,
                                None => item,
                            };

                            self.items.push_back(item)
                        }

                        return Ok(())
                    }
                }
            },
            Err(e) => match e.downcast::<::std::io::Error>()?.kind() {
                ErrorKind::UnexpectedEof => return Ok(()),
                _ => (),
            },
        };
        Err(err_msg("unable to read block"))
    }
}

impl<'a, R: Read> Iterator for Reader<'a, R> {
    type Item = Result<Value, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.items.len() == 0 {
            if let Err(e) = self.read_block() {
                return Some(Err(err_msg(e)))
            }
        }

        self.items.pop_front().map(Ok)
    }
}
