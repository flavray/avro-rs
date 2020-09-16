use crate::{
    decimal::Decimal,
    duration::Duration,
    schema::{Schema, SchemaType},
    types::Value,
    util::{safe_len, zag_i32, zag_i64},
    AvroResult, Error,
};
use std::{collections::HashMap, convert::TryFrom, io::Read, str::FromStr};
use uuid::Uuid;

#[inline]
fn decode_long<R: Read>(reader: &mut R) -> AvroResult<Value> {
    zag_i64(reader).map(Value::Long)
}

#[inline]
fn decode_int<R: Read>(reader: &mut R) -> AvroResult<Value> {
    zag_i32(reader).map(Value::Int)
}

#[inline]
fn decode_len<R: Read>(reader: &mut R) -> AvroResult<usize> {
    let len = zag_i64(reader)?;
    safe_len(usize::try_from(len).map_err(|e| Error::ConvertI64ToUsize(e, len))?)
}

/// Decode the length of a sequence.
///
/// Maps and arrays are 0-terminated, 0i64 is also encoded as 0 in Avro reading a length of 0 means
/// the end of the map or array.
fn decode_seq_len<R: Read>(reader: &mut R) -> AvroResult<usize> {
    let raw_len = zag_i64(reader)?;
    safe_len(
        usize::try_from(match raw_len.cmp(&0) {
            std::cmp::Ordering::Equal => return Ok(0),
            std::cmp::Ordering::Less => {
                let _size = zag_i64(reader)?;
                -raw_len
            }
            std::cmp::Ordering::Greater => raw_len,
        })
        .map_err(|e| Error::ConvertI64ToUsize(e, raw_len))?,
    )
}

/// Decode a `Value` from avro format given its `Schema`.
pub fn decode<R: Read>(schema: SchemaType, reader: &mut R) -> AvroResult<Value> {
    match schema {
        SchemaType::Null => Ok(Value::Null),
        SchemaType::Boolean => {
            let mut buf = [0u8; 1];
            reader
                .read_exact(&mut buf[..])
                .map_err(Error::ReadBoolean)?;

            match buf[0] {
                0u8 => Ok(Value::Boolean(false)),
                1u8 => Ok(Value::Boolean(true)),
                _ => Err(Error::BoolValue(buf[0])),
            }
        }
        SchemaType::Int => decode_int(reader),
        SchemaType::Long => decode_long(reader),
        SchemaType::Float => {
            let mut buf = [0u8; std::mem::size_of::<f32>()];
            reader.read_exact(&mut buf[..]).map_err(Error::ReadFloat)?;
            Ok(Value::Float(f32::from_le_bytes(buf)))
        }
        SchemaType::Decimal(d) => {
            let mut builder = Schema::builder();
            // TODO: name implementation
            let root = builder
                .decimal("name")
                .decimal(d.precision(), d.scale(), &mut builder)
                .unwrap();
            let expected = builder.build(root).unwrap();

            match expected.root() {
                SchemaType::Fixed { .. } => match decode(expected.root(), reader)? {
                    Value::Fixed(_, bytes) => Ok(Value::Decimal(Decimal::from(bytes))),
                    value => Err(Error::FixedValue(value.into())),
                },
                SchemaType::Bytes => match decode(expected.root(), reader)? {
                    Value::Bytes(bytes) => Ok(Value::Decimal(Decimal::from(bytes))),
                    value => Err(Error::BytesValue(value.into())),
                },
                _schema => Err(Error::ResolveDecimalSchema(
                    crate::schema::SchemaKind::Decimal,
                )),
            }
        }
        SchemaType::Uuid => Ok(Value::Uuid(
            Uuid::from_str(match decode(SchemaType::String, reader)? {
                Value::String(ref s) => s,
                value => return Err(Error::GetUuidFromStringValue(value.into())),
            })
            .map_err(Error::ConvertStrToUuid)?,
        )),
        SchemaType::Date => zag_i32(reader).map(Value::Date),
        SchemaType::TimeMillis => zag_i32(reader).map(Value::TimeMillis),
        SchemaType::TimeMicros => zag_i64(reader).map(Value::TimeMicros),
        SchemaType::TimestampMillis => zag_i64(reader).map(Value::TimestampMillis),
        SchemaType::TimestampMicros => zag_i64(reader).map(Value::TimestampMicros),
        SchemaType::Duration => {
            let mut buf = [0u8; 12];
            reader.read_exact(&mut buf).map_err(Error::ReadDuration)?;
            Ok(Value::Duration(Duration::from(buf)))
        }
        SchemaType::Double => {
            let mut buf = [0u8; std::mem::size_of::<f64>()];
            reader.read_exact(&mut buf[..]).map_err(Error::ReadDouble)?;
            Ok(Value::Double(f64::from_le_bytes(buf)))
        }
        SchemaType::Bytes => {
            let len = decode_len(reader)?;
            let mut buf = vec![0u8; len];
            reader.read_exact(&mut buf).map_err(Error::ReadBytes)?;
            Ok(Value::Bytes(buf))
        }
        SchemaType::String => {
            let len = decode_len(reader)?;
            let mut buf = vec![0u8; len];
            reader.read_exact(&mut buf).map_err(Error::ReadString)?;

            Ok(Value::String(
                String::from_utf8(buf).map_err(Error::ConvertToUtf8)?,
            ))
        }
        //TODO: what about name?
        SchemaType::Fixed(fixed) => {
            let mut buf = vec![0u8; fixed.size() as usize];
            reader.read_exact(&mut buf).map_err(Error::ReadBytes)?;
            Ok(Value::Fixed(fixed.size(), buf))
        }
        SchemaType::Array(array) => {
            let mut items = Vec::new();

            loop {
                let len = decode_seq_len(reader)?;
                if len == 0 {
                    break;
                }

                items.reserve(len);
                for _ in 0..len {
                    items.push(decode(array.items(), reader)?);
                }
            }

            Ok(Value::Array(items))
        }
        SchemaType::Map(map) => {
            let mut items = HashMap::new();

            loop {
                let len = decode_seq_len(reader)?;
                if len == 0 {
                    break;
                }

                items.reserve(len);
                for _ in 0..len {
                    match decode(SchemaType::String, reader)? {
                        Value::String(key) => {
                            let value = decode(map.items(), reader)?;
                            items.insert(key, value);
                        }
                        value => return Err(Error::MapKeyType(value.into())),
                    }
                }
            }

            Ok(Value::Map(items))
        }
        SchemaType::Union(union) => {
            let index = zag_i64(reader)?;
            let variants = union.variants();
            let variant = variants
                .get(usize::try_from(index).map_err(|e| Error::ConvertI64ToUsize(e, index))?)
                .ok_or_else(|| Error::GetUnionVariant {
                    index,
                    num_variants: variants.len(),
                })?;
            let value = decode(*variant, reader)?;
            Ok(Value::Union(Box::new(value)))
        }
        SchemaType::Record(record) => {
            // Benchmarks indicate ~10% improvement using this method.
            // let mut items = Vec::with_capacity(fields.len());
            let mut items = Vec::with_capacity(record.len());
            for field in record.fields() {
                // TODO: This clone is also expensive. See if we can do away with it...
                items.push((field.name().to_string(), decode(field.schema(), reader)?));
            }
            Ok(Value::Record(items))
        }
        SchemaType::Enum(enum_) => {
            let symbols = enum_.symbols();
            Ok(if let Value::Int(raw_index) = decode_int(reader)? {
                let index = usize::try_from(raw_index)
                    .map_err(|e| Error::ConvertI32ToUsize(e, raw_index))?;
                if (0..=symbols.len()).contains(&index) {
                    let symbol = &symbols[index];
                    // TODO: Can this alloc be removed?
                    Value::Enum(raw_index, symbol.to_string())
                } else {
                    return Err(Error::GetEnumValue {
                        index,
                        nsymbols: symbols.len(),
                    });
                }
            } else {
                return Err(Error::GetEnumSymbol);
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::schema::builder::SchemaBuilder;
    use crate::{
        decode::decode,
        types::Value::{Array, Int, Map},
    };
    use std::collections::HashMap;

    #[test]
    fn test_decode_array_without_size() {
        let mut input: &[u8] = &[6, 2, 4, 6, 0];
        let mut builder = SchemaBuilder::new();
        let root = builder.array().items(builder.int(), &mut builder).unwrap();
        let schema = builder.build(root).unwrap();
        let result = decode(schema.root(), &mut input);
        assert_eq!(Array(vec!(Int(1), Int(2), Int(3))), result.unwrap());
    }

    #[test]
    fn test_decode_array_with_size() {
        let mut input: &[u8] = &[5, 6, 2, 4, 6, 0];
        let mut builder = SchemaBuilder::new();
        let root = builder.array().items(builder.int(), &mut builder).unwrap();
        let schema = builder.build(root).unwrap();
        let result = decode(schema.root(), &mut input);
        assert_eq!(Array(vec!(Int(1), Int(2), Int(3))), result.unwrap());
    }

    #[test]
    fn test_decode_map_without_size() {
        let mut input: &[u8] = &[0x02, 0x08, 0x74, 0x65, 0x73, 0x74, 0x02, 0x00];
        let mut builder = SchemaBuilder::new();
        let root = builder.map().values(builder.int(), &mut builder).unwrap();
        let schema = builder.build(root).unwrap();
        let result = decode(schema.root(), &mut input);
        let mut expected = HashMap::new();
        expected.insert(String::from("test"), Int(1));
        assert_eq!(Map(expected), result.unwrap());
    }

    #[test]
    fn test_decode_map_with_size() {
        let mut input: &[u8] = &[0x01, 0x0C, 0x08, 0x74, 0x65, 0x73, 0x74, 0x02, 0x00];
        let mut builder = SchemaBuilder::new();
        let root = builder.map().values(builder.int(), &mut builder).unwrap();
        let schema = builder.build(root).unwrap();
        let result = decode(schema.root(), &mut input);

        let mut expected = HashMap::new();
        expected.insert(String::from("test"), Int(1));
        assert_eq!(Map(expected), result.unwrap());
    }

    // TODO: thread 'decode::tests::test_negative_decimal_value' has overflowed its stack
    // #[test]
    // fn test_negative_decimal_value() {
    //     use crate::encode::encode;
    //     use num_bigint::ToBigInt;
    //
    //     let schema = Schema::meta_schema();
    //     let bigint = -423.to_bigint().unwrap();
    //     let value = Value::Decimal(Decimal::from(bigint.to_signed_bytes_be()));
    //
    //     let mut buffer = Vec::new();
    //     encode(&value, schema.root(), &mut buffer);
    //
    //     let mut bytes = &buffer[..];
    //     let result = decode(schema.root(), &mut bytes).unwrap();
    //     assert_eq!(result, value);
    // }

    // TODO: thread 'decode::tests::test_decode_decimal_with_bigger_than_necessary_size' has overflowed its stack
    // #[test]
    // fn test_decode_decimal_with_bigger_than_necessary_size() {
    //     use crate::encode::encode;
    //     use num_bigint::ToBigInt;
    //     let schema = Schema::meta_schema();
    //     let value = Value::Decimal(Decimal::from(
    //         (-423.to_bigint().unwrap()).to_signed_bytes_be(),
    //     ));
    //     let mut buffer = Vec::<u8>::new();
    //
    //     encode(&value, schema.root(), &mut buffer);
    //     let mut bytes: &[u8] = &buffer[..];
    //     let result = decode(schema.root(), &mut bytes).unwrap();
    //     assert_eq!(result, value);
    // }
}
