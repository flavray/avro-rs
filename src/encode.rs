use crate::schema::SchemaType;
use crate::{
    types::Value,
    util::{zig_i32, zig_i64},
};

/// Encode a `Value` into avro format.
///
/// **NOTE** This will not perform schema validation. The value is assumed to
/// be valid with regards to the schema. Schema are needed only to guide the
/// encoding for complex type values.
pub fn encode(value: &Value, schema: SchemaType, buffer: &mut Vec<u8>) {
    encode_ref(&value, schema, buffer)
}

fn encode_bytes<B: AsRef<[u8]> + ?Sized>(s: &B, buffer: &mut Vec<u8>) {
    let bytes = s.as_ref();
    encode(&Value::Long(bytes.len() as i64), SchemaType::Long, buffer);
    buffer.extend_from_slice(bytes);
}

fn encode_long(i: i64, buffer: &mut Vec<u8>) {
    zig_i64(i, buffer)
}

fn encode_int(i: i32, buffer: &mut Vec<u8>) {
    zig_i32(i, buffer)
}

/// Encode a `Value` into avro format.
///
/// **NOTE** This will not perform schema validation. The value is assumed to
/// be valid with regards to the schema. Schema are needed only to guide the
/// encoding for complex type values.
pub fn encode_ref(value: &Value, schema: SchemaType, buffer: &mut Vec<u8>) {
    match value {
        Value::Null => (),
        Value::Boolean(b) => buffer.push(if *b { 1u8 } else { 0u8 }),
        // Pattern | Pattern here to signify that these _must_ have the same encoding.
        Value::Int(i) | Value::Date(i) | Value::TimeMillis(i) => encode_int(*i, buffer),
        Value::Long(i)
        | Value::TimestampMillis(i)
        | Value::TimestampMicros(i)
        | Value::TimeMicros(i) => encode_long(*i, buffer),
        Value::Float(x) => buffer.extend_from_slice(&x.to_le_bytes()),
        Value::Double(x) => buffer.extend_from_slice(&x.to_le_bytes()),
        Value::Decimal(decimal) => {
            let mut bytes = decimal.to_vec().unwrap();
            encode(value, schema, &mut bytes)
        }
        &Value::Duration(duration) => {
            let slice: [u8; 12] = duration.into();
            buffer.extend_from_slice(&slice);
        }
        Value::Uuid(uuid) => encode_bytes(&uuid.to_string(), buffer),
        Value::Bytes(bytes) => encode_bytes(bytes, buffer),
        Value::String(s) => match schema {
            SchemaType::String => encode_bytes(s, buffer),
            SchemaType::Enum(enum_) => {
                if let Some(index) = enum_.symbols().iter().position(|item| item == s) {
                    encode_int(index as i32, buffer);
                }
            }
            _ => (),
        },
        Value::Fixed(_, bytes) => buffer.extend(bytes),
        Value::Enum(i, _) => encode_int(*i, buffer),
        Value::Union(item) => {
            if let SchemaType::Union(union) = schema {
                // Find the schema that is matched here. Due to validation, this should always
                // return a value.
                let (idx, inner_schema) = union
                    .resolve_union_schema(item)
                    .expect("Invalid Union validation occurred");
                encode_long(idx as i64, buffer);
                encode_ref(&*item, inner_schema, buffer);
            }
        }
        Value::Array(items) => {
            if let SchemaType::Array(array) = schema {
                if !items.is_empty() {
                    encode_long(items.len() as i64, buffer);
                    for item in items.iter() {
                        encode_ref(item, array.items(), buffer);
                    }
                }
                buffer.push(0u8);
            }
        }
        Value::Map(items) => {
            if let SchemaType::Map(map) = schema {
                if !items.is_empty() {
                    encode_long(items.len() as i64, buffer);
                    for (key, value) in items {
                        encode_bytes(key, buffer);
                        encode_ref(value, map.items(), buffer);
                    }
                }
                buffer.push(0u8);
            }
        }
        Value::Record(fields) => {
            if let SchemaType::Record(record) = schema {
                for (i, &(_, ref value)) in fields.iter().enumerate() {
                    encode_ref(value, record.fields()[i].schema(), buffer);
                }
            }
        }
    }
}

pub fn encode_to_vec(value: &Value, schema: SchemaType) -> Vec<u8> {
    let mut buffer = Vec::new();
    encode(&value, schema, &mut buffer);
    buffer
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Schema;
    use std::collections::HashMap;

    #[test]
    fn test_encode_empty_array() {
        let mut buf = Vec::new();
        let empty: Vec<Value> = Vec::new();
        let mut builder = Schema::builder();
        let root = builder.array().items(builder.int(), &mut builder).unwrap();
        let schema = builder.build(root).unwrap();

        encode(&Value::Array(empty), schema.root(), &mut buf);
        assert_eq!(vec![0u8], buf);
    }

    #[test]
    fn test_encode_empty_map() {
        let mut buf = Vec::new();
        let empty: HashMap<String, Value> = HashMap::new();
        let mut builder = Schema::builder();
        let root = builder.map().values(builder.int(), &mut builder).unwrap();
        let schema = builder.build(root).unwrap();

        encode(&Value::Map(empty), schema.root(), &mut buf);
        assert_eq!(vec![0u8], buf);
    }
}
