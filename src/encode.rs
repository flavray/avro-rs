use std::mem::transmute;

use schema::Schema;
use types::{ToAvro, Value};
use util::{zig_i32, zig_i64};

/// Encode a `Value` into avro format.
///
/// **NOTE** This will not perform schema validation. The value is assumed to
/// be valid with regards to the schema. Schema are needed only to guide the
/// encoding for complex type values.
pub fn encode(value: Value, schema: &Schema, buffer: &mut Vec<u8>) {
    match value {
        Value::Null => (),
        Value::Boolean(b) => buffer.push(if b { 1u8 } else { 0u8 }),
        Value::Int(i) => zig_i32(i, buffer),
        Value::Long(i) => zig_i64(i, buffer),
        Value::Float(x) => buffer.extend_from_slice(&unsafe { transmute::<f32, [u8; 4]>(x) }),
        Value::Double(x) => buffer.extend_from_slice(&unsafe { transmute::<f64, [u8; 8]>(x) }),
        Value::Bytes(bytes) => {
            encode(Value::Long(bytes.len() as i64), &Schema::Long, buffer);
            buffer.extend(bytes);
        },
        Value::String(s) => match schema {
            &Schema::String => {
                encode(Value::Long(s.len() as i64), &Schema::Long, buffer);
                buffer.extend(s.into_bytes());
            },
            &Schema::Enum { ref symbols, .. } => {
                if let Some(index) = symbols.iter().position(|ref item| item == &&s) {
                    encode(Value::Enum(index as i32, s), schema, buffer);
                }
            },
            _ => (),
        },
        Value::Fixed(_, bytes) => buffer.extend(bytes),
        Value::Enum(i, _) => encode(Value::Int(i), &Schema::Int, buffer),
        Value::Union(None) => buffer.push(0u8),
        Value::Union(Some(item)) => {
            if let &Schema::Union(ref inner) = schema {
                encode(Value::Long(1), &Schema::Long, buffer);
                encode(*item, inner, buffer);
            }
        },
        Value::Array(items) => {
            if let &Schema::Array(ref inner) = schema {
                encode(items.len().avro(), &Schema::Long, buffer);
                for item in items.into_iter() {
                    encode(item, inner, buffer);
                }
                buffer.push(0u8);
            }
        },
        Value::Map(items) => {
            if let &Schema::Map(ref inner) = schema {
                encode(items.len().avro(), &Schema::Long, buffer);
                for (key, value) in items.into_iter() {
                    encode(Value::String(key), &Schema::String, buffer);
                    encode(value, inner, buffer);
                }
                buffer.push(0u8);
            }
        },
        Value::Record(fields) => {
            if let &Schema::Record {
                fields: ref schema_fields,
                ..
            } = schema
            {
                for (i, (_, value)) in fields.into_iter().enumerate() {
                    encode(value, &schema_fields[i].schema, buffer);
                }
            }
        },
    }
}

fn encode_bytes<B: AsRef<[u8]> + ?Sized>(s: &B, buffer: &mut Vec<u8>) {
    let bytes = s.as_ref();
    encode(Value::Long(bytes.len() as i64), &Schema::Long, buffer);
    buffer.extend_from_slice(bytes);
}

fn encode_long(i: i64, buffer: &mut Vec<u8>) {
    zig_i64(i, buffer)
}

fn encode_int(i: i32, buffer: &mut Vec<u8>) {
    zig_i32(i, buffer)
}

pub fn encode_ref(value: &Value, schema: &Schema, buffer: &mut Vec<u8>) {
    match value {
        Value::Null => (),
        Value::Boolean(b) => buffer.push(if *b { 1u8 } else { 0u8 }),
        Value::Int(i) => encode_int(*i, buffer),
        Value::Long(i) => encode_long(*i, buffer),
        Value::Float(x) => buffer.extend_from_slice(&unsafe { transmute::<f32, [u8; 4]>(*x) }),
        Value::Double(x) => buffer.extend_from_slice(&unsafe { transmute::<f64, [u8; 8]>(*x) }),
        Value::Bytes(bytes) => encode_bytes(bytes, buffer), 
        Value::String(s) => match schema {
            &Schema::String => {
                encode_bytes(s, buffer);
            },
            &Schema::Enum { ref symbols, .. } => {
                if let Some(index) = symbols.iter().position(|item| item == s) {
                    encode_int(index as i32, buffer);
                }
            },
            _ => (),
        },
        Value::Fixed(_, bytes) => buffer.extend(bytes),
        Value::Enum(i, _) => encode_int(*i, buffer),
        Value::Union(None) => buffer.push(0u8),
        Value::Union(Some(item)) => {
            if let &Schema::Union(ref inner) = schema {
                encode_long(1, buffer);
                encode_ref(&*item, inner, buffer);
            }
        },
        Value::Array(items) => {
            if let &Schema::Array(ref inner) = schema {
                encode_long(items.len() as i64, buffer);
                for item in items.into_iter() {
                    encode_ref(item, inner, buffer);
                }
                buffer.push(0u8);
            }
        },
        Value::Map(items) => {
            if let &Schema::Map(ref inner) = schema {
                encode_long(items.len() as i64, buffer);
                for (key, value) in items.into_iter() {
                    encode_bytes(key, buffer);
                    encode_ref(value, inner, buffer);
                }
                buffer.push(0u8);
            }
        },
        Value::Record(fields) => {
            if let &Schema::Record {
                fields: ref schema_fields,
                ..
            } = schema
            {
                for (i, &(_, ref value)) in fields.iter().enumerate() {
                    encode_ref(value, &schema_fields[i].schema, buffer);
                }
            }
        },
    }

}

pub fn encode_to_vec(value: Value, schema: &Schema) -> Vec<u8> {
    let mut buffer = Vec::new();
    encode(value, schema, &mut buffer);
    buffer
}

pub fn encode_ref_to_vec(value: &Value, schema: &Schema) -> Vec<u8> {
    let mut buffer = Vec::new();
    encode_ref(value, schema, &mut buffer);
    buffer
}
