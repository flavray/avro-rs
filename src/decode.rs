use std::collections::HashMap;
use std::io::Read;
use std::mem::transmute;
use std::rc::Rc;

use failure::Error;

use schema::{Schema, SchemaParseContext};
use types::Value;
use util::{safe_len, zag_i32, zag_i64, DecodeError};

#[inline]
fn decode_long<R: Read>(reader: &mut R) -> Result<Value, Error> {
    zag_i64(reader).map(Value::Long)
}

#[inline]
fn decode_int<R: Read>(reader: &mut R) -> Result<Value, Error> {
    zag_i32(reader).map(Value::Int)
}

#[inline]
fn decode_len<R: Read>(reader: &mut R) -> Result<usize, Error> {
    zag_i64(reader).and_then(|len| safe_len(len as usize))
}

/// Decode a `Value` from avro format given its `Schema`.
pub fn decode<R: Read>(schema: Rc<Schema>, reader: &mut R, context: &mut SchemaParseContext) -> Result<Value, Error> {
    match *schema {
        Schema::Null => Ok(Value::Null),
        Schema::Boolean => {
            let mut buf = [0u8; 1];
            reader.read_exact(&mut buf[..])?;

            match buf[0] {
                0u8 => Ok(Value::Boolean(false)),
                1u8 => Ok(Value::Boolean(true)),
                _ => Err(DecodeError::new("not a bool").into()),
            }
        },
        Schema::Int => decode_int(reader),
        Schema::Long => decode_long(reader),
        Schema::Float => {
            let mut buf = [0u8; 4];
            reader.read_exact(&mut buf[..])?;
            Ok(Value::Float(unsafe { transmute::<[u8; 4], f32>(buf) }))
        },
        Schema::Double => {
            let mut buf = [0u8; 8];
            reader.read_exact(&mut buf[..])?;
            Ok(Value::Double(unsafe { transmute::<[u8; 8], f64>(buf) }))
        },
        Schema::Bytes => {
            let len = decode_len(reader)?;
            let mut buf = Vec::with_capacity(len);
            unsafe {
                buf.set_len(len);
            }
            reader.read_exact(&mut buf)?;
            Ok(Value::Bytes(buf))
        },
        Schema::String => {
            let len = decode_len(reader)?;
            let mut buf = Vec::with_capacity(len);
            unsafe {
                buf.set_len(len);
            }
            reader.read_exact(&mut buf)?;

            String::from_utf8(buf)
                .map(Value::String)
                .map_err(|_| DecodeError::new("not a valid utf-8 string").into())
        },
        Schema::Fixed { size, .. } => {
            let mut buf = vec![0u8; size as usize];
            reader.read_exact(&mut buf)?;
            Ok(Value::Fixed(size, buf))
        },
        Schema::Array(ref inner) => {
            let mut items = Vec::new();

            loop {
                let len = decode_len(reader)?;
                // arrays are 0-terminated, 0i64 is also encoded as 0 in Avro
                // reading a length of 0 means the end of the array
                if len == 0 {
                    break
                }

                items.reserve(len as usize);
                for _ in 0..len {
                    items.push(decode(inner.clone(), reader, context)?);
                }
            }

            Ok(Value::Array(items))
        },
        Schema::Map(ref inner) => {
            let mut items = HashMap::new();

            loop {
                let len = decode_len(reader)?;
                // maps are 0-terminated, 0i64 is also encoded as 0 in Avro
                // reading a length of 0 means the end of the map
                if len == 0 {
                    break
                }

                items.reserve(len as usize);
                for _ in 0..len {
                    if let Value::String(key) = decode(Rc::new(Schema::String), reader, context)? {
                        let value = decode(inner.clone(), reader, context)?;
                        items.insert(key, value);
                    } else {
                        return Err(DecodeError::new("map key is not a string").into())
                    }
                }
            }

            Ok(Value::Map(items))
        },
        Schema::Union(ref inner) => {
            let index = zag_i64(reader)?;

            match index {
                0 => Ok(Value::Union(None)),
                1 => decode(inner.clone(), reader, context).map(|x| Value::Union(Some(Box::new(x)))),
                _ => Err(DecodeError::new("union index out of bounds").into()),
            }
        },
        Schema::Record { ref fields, ref name, .. } => {
            context.register_type(name.clone(), schema.clone());
            // Benchmarks indicate ~10% improvement using this method.
            let mut items = Vec::new();
            for field in fields {
                // This clone is also expensive. See if we can do away with it...
                items.push((field.name.clone(), decode(field.schema.clone(), reader, context)?));
            }
            Ok(Value::Record(items))
            // fields
            // .iter()
            // .map(|field| decode(&field.schema, reader).map(|value| (field.name.clone(), value)))
            // .collect::<Result<Vec<(String, Value)>, _>>()
            // .map(|items| Value::Record(items))
        },
        Schema::Enum { ref symbols, .. } => {
            if let Value::Int(index) = decode_int(reader)? {
                if index >= 0 && (index as usize) <= symbols.len() {
                    let symbol = symbols[index as usize].clone();
                    Ok(Value::Enum(index, symbol))
                } else {
                    Err(DecodeError::new("enum symbol index out of bounds").into())
                }
            } else {
                Err(DecodeError::new("enum symbol not found").into())
            }
        },
        Schema::TypeReference(ref name) => context.lookup_type(name, &context)
            .map_or_else(|| Err(DecodeError::new("enum symbol not found").into()),
                         |s| decode(s, reader, context)),
    }
}
