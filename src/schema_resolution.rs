use std::collections::HashMap;

use crate::schema::Name as RecordName;
use crate::schema::RecordField;
use crate::types::ToAvro;
use crate::types::Value;
use crate::Schema;
use serde_json::Value as JsonValue;

#[derive(Fail, Debug, Clone, PartialEq)]
pub enum ResolutionError {
    #[fail(display = "ResolutionError::BytesToStringUtf8Error")]
    BytesToStringUtf8Error,

    #[fail(
        display = "ResolutionError::IncompatibleSchema: writer={:?}, reader={:?}",
        _0, _1
    )]
    IncompatibleSchema(Schema, Schema),

    #[fail(display = "ResolutionError::IncompatibleData: data={:?}", _0)]
    IncompatibleData(Value),

    #[fail(display = "ResolutionError::UnexpectedVariant: variant={}", _0)]
    UnexpectedVariant(usize),

    #[fail(
        display = "ResolutionError::RecordFieldMissing: record={:?}, field={}",
        _0, _1
    )]
    RecordFieldMissing(RecordName, usize),

    #[fail(display = "ResolutionError::RecordFieldValueMissing: {}", _0)]
    RecordFieldValueMissing(String),

    #[fail(display = "ResolutionError::InvalidDefaultValue: {:?}", _0)]
    InvalidDefaultValue(JsonValue),

    #[fail(display = "ResolutionError::Generic: {}", _0)]
    Generic(String),
}

#[derive(Debug, Clone)]
pub enum RecordFieldResolution {
    Present {
        w_pos: usize,
        r_pos: usize,
        name: String,
        resolution: Resolution,
    },

    Absent {
        r_pos: usize,
        name: String,
        value: Value,
    },
}
// pub struct RecordFieldResolution {
//     from_pos: usize,
//     to_pos: usize,
//     value: Resolution,
// }

#[derive(Debug, Clone)]
pub enum Resolution {
    Identity,
    IntToLong,
    IntToFloat,
    IntToDouble,
    LongToFloat,
    LongToDouble,
    FloatToDouble,
    StringToBytes,
    BytesToString,
    Array(Box<Resolution>),
    Map(Box<Resolution>),
    Record(Vec<RecordFieldResolution>),
    UnionVariant(usize, Box<Resolution>),
    UnionChoice(Vec<Resolution>),
    EnumChoice(HashMap<i32, (i32, String)>),
    Failure(ResolutionError),
}

impl From<std::string::FromUtf8Error> for ResolutionError {
    fn from(_: std::string::FromUtf8Error) -> ResolutionError {
        ResolutionError::BytesToStringUtf8Error
    }
}

impl Resolution {
    // pub fn id() -> Resolution {
    //     Resolution::Identity
    // }

    pub fn new(
        writer_schema: &Schema,
        reader_schema: &Schema,
    ) -> Result<Resolution, ResolutionError> {
        if writer_schema == reader_schema {
            Ok(Resolution::Identity)
        } else {
            match (writer_schema, reader_schema) {
                // To match, one of the following must hold:
                // * both schemas have same primitive type ...

                // (Schema::Null, Schema::Null) => Ok(Resolution::Identity),
                // (Schema::Boolean, Schema::Boolean) => Ok(Resolution::Identity),
                // (Schema::Int, Schema::Int) => Ok(Resolution::Identity),
                // (Schema::Long, Schema::Long) => Ok(Resolution::Identity),
                // (Schema::Float, Schema::Float) => Ok(Resolution::Identity),
                // (Schema::Double, Schema::Double) => Ok(Resolution::Identity),
                // (Schema::Bytes, Schema::Bytes) => Ok(Resolution::Identity),
                // (Schema::String, Schema::String) => Ok(Resolution::Identity),

                // * both schemas are arrays whose item types match
                (Schema::Array(ref writer_inner), Schema::Array(ref reader_inner)) => {
                    Self::new(writer_inner, reader_inner).map(|inner_resolution| {
                        match inner_resolution {
                            Resolution::Identity => Resolution::Identity,
                            promotion => Resolution::Array(Box::new(promotion)),
                        }
                    })
                }
                // * both schemas are maps whose value types match
                (Schema::Map(ref writer_inner), Schema::Map(ref reader_inner)) => {
                    Self::new(writer_inner, reader_inner).map(|inner_resolution| {
                        match inner_resolution {
                            Resolution::Identity => Resolution::Identity,
                            promotion => Resolution::Map(Box::new(promotion)),
                        }
                    })
                }

                // ... the writer's schema may be promoted to the reader's as follows:
                // * bytes is promotable to string
                (Schema::Bytes, Schema::String) => Ok(Resolution::BytesToString),
                // * string is promotable to bytes
                (Schema::String, Schema::Bytes) => Ok(Resolution::StringToBytes),
                // * float is promotable to double
                (Schema::Float, Schema::Double) => Ok(Resolution::FloatToDouble),
                // * long is promotable to float or double
                (Schema::Long, Schema::Float) => Ok(Resolution::LongToFloat),
                (Schema::Long, Schema::Double) => Ok(Resolution::LongToDouble),
                // * int is promotable to long, float, or double
                (Schema::Int, Schema::Long) => Ok(Resolution::IntToLong),
                (Schema::Int, Schema::Float) => Ok(Resolution::IntToFloat),
                (Schema::Int, Schema::Double) => Ok(Resolution::IntToDouble),

                /*
                 * if both are enums:
                 *  if the writer's symbol is not present in the reader's enum, then an error is signalled.
                 */
                (
                    Schema::Enum {
                        name: w_name,
                        symbols: w_symbols,
                        ..
                    },
                    Schema::Enum {
                        name: r_name,
                        symbols: r_symbols,
                        ..
                    },
                ) if w_name == r_name => {
                    let r_symbol_map = r_symbols
                        .iter()
                        .enumerate()
                        .map(|(idx, symbol)| (symbol, idx as i32))
                        .collect::<HashMap<_, _>>();

                    let translation_map = w_symbols
                        .iter()
                        .enumerate()
                        .filter_map(|(w_idx, symbol)| {
                            r_symbol_map
                                .get(&symbol)
                                .map(|r_idx| (w_idx as i32, (*r_idx, symbol.clone())))
                        })
                        .collect::<HashMap<i32, (i32, String)>>();

                    Ok(Resolution::EnumChoice(translation_map))
                }

                // * if both are unions:
                //   The first schema in the reader's union that matches the selected writer's union schema is recursively resolved against it. if none match, an error is signalled.
                (Schema::Union(w_union_schema), r_schema @ Schema::Union(_)) => {
                    Ok(Resolution::UnionChoice(
                        w_union_schema
                            .variants()
                            .iter()
                            .map(|w_variant| match Self::new(w_variant, r_schema) {
                                Ok(resolution) => resolution,
                                Err(reason) => Resolution::Failure(reason),
                            })
                            .collect::<Vec<_>>(),
                    ))
                }

                // * if reader's is a union, but writer's is not
                //   The first schema in the reader's union that matches the writer's schema is recursively resolved against it. If none match, an error is signalled.
                (w_non_union, Schema::Union(r_union_schema)) => {
                    let r_variants = r_union_schema.variants();
                    r_variants
                        .iter()
                        .enumerate()
                        .find_map(|(r_idx, r_variant)| {
                            Self::new(w_non_union, r_variant).ok().map(|resolution| {
                                Resolution::UnionVariant(r_idx, Box::new(resolution))
                            })
                        })
                        .map(|resolution| Ok(resolution))
                        .unwrap_or_else(|| {
                            Err(ResolutionError::IncompatibleSchema(
                                w_non_union.clone(),
                                Schema::Union(r_union_schema.clone()),
                            ))
                        })
                }

                // * if writer's is a union, but reader's is not
                //   If the reader's schema matches the selected writer's schema, it is recursively resolved against it. If they do not match, an error is signalled.
                (Schema::Union(w_union_schema), r_non_union) => Ok(Resolution::UnionChoice(
                    w_union_schema
                        .variants()
                        .iter()
                        .map(|w_variant| match Self::new(w_variant, r_non_union) {
                            Ok(resolution) => resolution,
                            Err(reason) => Resolution::Failure(reason),
                        })
                        .collect::<Vec<_>>(),
                )),

                (
                    Schema::Record {
                        name: w_name,
                        fields: w_fields,
                        lookup: w_lookup,
                        ..
                    },
                    Schema::Record {
                        name: r_name,
                        fields: r_fields,
                        ..
                    },
                ) if r_name == w_name => {
                    /*
                     * ... if both are records:
                     *   - the ordering of fields may be different: fields are matched by name.
                     *   - schemas for fields with the same name in both records are resolved recursively.
                     *   - if the writer's record contains a field with a name not present in the reader's record, the writer's value for that field is ignored.
                     *   - if the reader's record schema has a field that contains a default value, and writer's schema does not have a field with the same name, then the reader should use the default value from its field.
                     *   - if the reader's record schema has a field with no default value, and writer's schema does not have a field with the same name, an error is signalled.
                     */
                    r_fields
                        .into_iter()
                        .enumerate()
                        .map(|(r_pos, r_field)| {
                            assert_eq!(r_pos, r_field.position);

                            let w_pos_opt: Option<&usize> = w_lookup.get(&r_field.name);

                            let w_pos_and_field_opt: Option<(usize, RecordField)> =
                                w_pos_opt.map(|w_pos| {
                                    assert_eq!(*w_pos, w_fields[*w_pos].position);

                                    (*w_pos, w_fields[*w_pos].clone())
                                });

                            let field_present_result_opt: Option<
                                Result<RecordFieldResolution, ResolutionError>,
                            > = w_pos_and_field_opt.map(|(w_pos, w_field)| {
                                Self::new(&w_field.schema, &r_field.schema).map(|resolution| {
                                    RecordFieldResolution::Present {
                                        w_pos,
                                        r_pos,
                                        name: r_field.name.clone(),
                                        resolution,
                                    }
                                })
                            });

                            let field_result: Result<RecordFieldResolution, ResolutionError> =
                                field_present_result_opt.unwrap_or_else(|| match r_field.default {
                                    None => Err(ResolutionError::RecordFieldMissing(
                                        r_name.clone(),
                                        r_pos,
                                    )),
                                    Some(ref default_value_json) => {
                                        Self::value_from_json(&r_field.schema, default_value_json)
                                            .map(|default_value| RecordFieldResolution::Absent {
                                                r_pos,
                                                name: r_field.name.clone(),
                                                value: default_value,
                                            })
                                    }
                                });

                            field_result
                        })
                        .collect::<Result<_, _>>()
                        .map(|field_resolutions| Resolution::Record(field_resolutions))
                }

                (writer_schema, reader_schema) => Err(ResolutionError::IncompatibleSchema(
                    writer_schema.clone(),
                    reader_schema.clone(),
                )),
            }
        }
    }

    pub fn promote_value(&self, value: Value) -> Result<Value, ResolutionError> {
        match (self.clone(), value) {
            (Resolution::Failure(reason), _) => Err(reason),
            (Resolution::Identity, as_is) => Ok(as_is),

            (Resolution::IntToLong, Value::Int(v)) => Ok(Value::Long(v.into())),
            // XXX: I feel really bad for those poor folks who will experience such "promotion" of their values.
            (Resolution::IntToFloat, Value::Int(v)) => Ok(Value::Float(v as f32)), // should we check if |v| < 2^24?
            (Resolution::IntToDouble, Value::Int(v)) => Ok(Value::Double(v.into())),

            // XXX: See above
            (Resolution::LongToFloat, Value::Long(v)) => Ok(Value::Float(v as f32)), // should we check if |v| < 2^24?
            // XXX: See above
            (Resolution::LongToDouble, Value::Long(v)) => Ok(Value::Double(v as f64)), // should we check if |v| < 2^53?

            (Resolution::FloatToDouble, Value::Float(v)) => Ok(Value::Double(v.into())),

            (Resolution::StringToBytes, Value::String(v)) => Ok(Value::Bytes(v.into_bytes())),
            (Resolution::BytesToString, Value::Bytes(v)) => {
                Ok(Value::String(String::from_utf8(v)?))
            }

            (Resolution::Array(inner), Value::Array(vs)) => vs
                .into_iter()
                .map(|v| inner.promote_value(v))
                .collect::<Result<_, _>>()
                .map(|vs| Value::Array(vs)),

            (Resolution::Map(inner), Value::Map(vs_map)) => vs_map
                .into_iter()
                .map(|(k, v)| inner.promote_value(v).map(|v| (k, v)))
                .collect::<Result<Vec<_>, _>>()
                .map(|pairs| pairs.into_iter().collect::<HashMap<_, _>>())
                .map(|vs_map| Value::Map(vs_map)),

            (Resolution::Record(resolutions), Value::Record(w_fields)) => {
                let mut w_field_map = w_fields.into_iter().collect::<HashMap<_, _>>();

                resolutions
                    .into_iter()
                    .map(|field_resolution| match field_resolution {
                        RecordFieldResolution::Absent { name, value, .. } => Ok((name, value)),
                        RecordFieldResolution::Present {
                            name, resolution, ..
                        } => w_field_map
                            .remove(&name)
                            .ok_or(ResolutionError::RecordFieldValueMissing(name.clone()))
                            .and_then(|w_value| resolution.promote_value(w_value))
                            .map(|promoted_value| (name, promoted_value)),
                    })
                    .collect::<Result<Vec<(String, Value)>, ResolutionError>>()
                    .map(|fields| Value::Record(fields))
            }

            (Resolution::EnumChoice(symbols), Value::Enum(w_idx, w_symbol)) => symbols
                .get(&w_idx)
                .map(|(r_idx, r_symbol)| Ok(Value::Enum(*r_idx, r_symbol.clone())))
                .unwrap_or_else(|| {
                    Err(ResolutionError::IncompatibleData(Value::Enum(
                        w_idx, w_symbol,
                    )))
                }),

            (Resolution::UnionChoice(variants), Value::Union(idx, value)) => variants
                .get(idx)
                .map(|resolution| resolution.promote_value(*value))
                .unwrap_or_else(|| Err(ResolutionError::UnexpectedVariant(idx))),

            (Resolution::UnionVariant(idx, inner), value) => inner
                .promote_value(value)
                .map(|promoted_inner| Value::Union(idx, Box::new(promoted_inner))),

            (_resolution, data) => Err(ResolutionError::IncompatibleData(data)),
        }
    }

    fn value_from_json(schema: &Schema, value: &JsonValue) -> Result<Value, ResolutionError> {
        // Calling .avro() here does not work terribly well for 'int' — it is conservatively cast into Value::Long(_)
        // Target field schema should be regarded to choose the exact Avro-type.
        // Illustrated in `schema_resolution::test_resolve_and_promote_record_with_default_int`
        match (schema, value) {
            (Schema::Null, JsonValue::Null) => Ok(Value::Null),
            (Schema::Boolean, JsonValue::Bool(v)) => Ok(Value::Boolean(*v)),
            (Schema::Int, JsonValue::Number(v)) if v.is_i64() => {
                Ok(Value::Int(v.as_i64().unwrap() as i32))
            }
            (Schema::Long, JsonValue::Number(v)) if v.is_i64() => {
                Ok(Value::Long(v.as_i64().unwrap()))
            }
            (Schema::Float, JsonValue::Number(v)) if v.is_f64() => {
                Ok(Value::Float(v.as_f64().unwrap() as f32))
            }
            (Schema::Double, JsonValue::Number(v)) if v.is_f64() => {
                Ok(Value::Double(v.as_f64().unwrap()))
            }
            (Schema::String, JsonValue::String(v)) => Ok(Value::String(v.clone())),
            (Schema::Bytes, JsonValue::String(v)) => Ok(Value::Bytes(v.clone().into_bytes())),
            (Schema::Array(inner), JsonValue::Array(vs)) => vs
                .iter()
                .map(|v| Self::value_from_json(inner, v))
                .collect::<Result<_, _>>()
                .map(|vs| Value::Array(vs)),
            (Schema::Enum { symbols, .. }, JsonValue::String(v)) => symbols
                .iter()
                .enumerate()
                .find_map(|(idx, symbol)| {
                    if symbol == v {
                        Some(Ok(Value::Enum(idx as i32, symbol.clone())))
                    } else {
                        None
                    }
                })
                .unwrap_or_else(|| {
                    Err(ResolutionError::InvalidDefaultValue(JsonValue::String(
                        v.to_owned(),
                    )))
                }),
            (Schema::Record { name, doc, fields, lookup }, JsonValue::Object(vs)) =>
                vs.iter().map(|(k, v)| {
                    (k, lookup.get(k).map(|p| Self::value_from_json(&fields[*p].schema, v)).unwrap_or(Err(ResolutionError::RecordFieldValueMissing(k.to_string()))))
                })
                .fold(Ok(vec![]), |mut v, r| match &r.1 {
                    Ok(x) => { v.as_mut().map(|v| v.push((r.0.clone(), x.to_owned()))); v },
                    Err(e) => Err(e.to_owned()),
                })
                .map(|f| Value::Record(f)),
            (Schema::Map(inner), JsonValue::Object(vs)) =>
                vs.iter().map(|(k, v)| {
                    (k, Self::value_from_json(inner, v))
                })
                .fold(Ok(HashMap::new()), |mut v, r| match &r.1 {
                    Ok(x) => { v.as_mut().map(|v| v.insert(r.0.clone(), x.to_owned())); v },
                    Err(e) => Err(e.to_owned()),
                })
                .map(|m| Value::Map(m)),
            (_, _) =>
                {
                    Err(ResolutionError::InvalidDefaultValue(value.clone()))
                },
        }
    }
}

fn resolve_and_promote_single(
    writer_schema: &Schema,
    reader_schema: &Schema,
    original_value: Value,
    expected_adjusted_value: Value,
) -> Result<(), ResolutionError> {
    let resolution = Resolution::new(writer_schema, reader_schema)?;
    let adjusted_value = resolution.promote_value(original_value)?;
    if adjusted_value != expected_adjusted_value {
        Err(ResolutionError::Generic(
            format!("{:?} != {:?}", adjusted_value, expected_adjusted_value).into(),
        ))
    } else {
        Ok(())
    }
}

#[test]
fn test_resolve_and_promote_primitives() {
    resolve_and_promote_single(&Schema::Null, &Schema::Null, Value::Null, Value::Null).unwrap();

    resolve_and_promote_single(
        &Schema::Boolean,
        &Schema::Boolean,
        Value::Boolean(false),
        Value::Boolean(false),
    )
    .unwrap();
    resolve_and_promote_single(
        &Schema::Boolean,
        &Schema::Boolean,
        Value::Boolean(true),
        Value::Boolean(true),
    )
    .unwrap();

    resolve_and_promote_single(&Schema::Int, &Schema::Int, Value::Int(42), Value::Int(42)).unwrap();
    resolve_and_promote_single(&Schema::Int, &Schema::Long, Value::Int(42), Value::Long(42))
        .unwrap();
    resolve_and_promote_single(
        &Schema::Int,
        &Schema::Float,
        Value::Int(42),
        Value::Float(42.0),
    )
    .unwrap();
    resolve_and_promote_single(
        &Schema::Int,
        &Schema::Double,
        Value::Int(42),
        Value::Double(42.0),
    )
    .unwrap();

    resolve_and_promote_single(
        &Schema::Long,
        &Schema::Long,
        Value::Long(42),
        Value::Long(42),
    )
    .unwrap();
    resolve_and_promote_single(
        &Schema::Long,
        &Schema::Float,
        Value::Long(42),
        Value::Float(42.0),
    )
    .unwrap();
    resolve_and_promote_single(
        &Schema::Long,
        &Schema::Double,
        Value::Long(42),
        Value::Double(42.0),
    )
    .unwrap();

    resolve_and_promote_single(
        &Schema::Float,
        &Schema::Float,
        Value::Float(42.0),
        Value::Float(42.0),
    )
    .unwrap();
    resolve_and_promote_single(
        &Schema::Float,
        &Schema::Double,
        Value::Float(42.0),
        Value::Double(42.0),
    )
    .unwrap();

    resolve_and_promote_single(
        &Schema::Double,
        &Schema::Double,
        Value::Double(42.0),
        Value::Double(42.0),
    )
    .unwrap();

    resolve_and_promote_single(
        &Schema::Bytes,
        &Schema::Bytes,
        Value::Bytes(vec![1, 2, 3]),
        Value::Bytes(vec![1, 2, 3]),
    )
    .unwrap();
    resolve_and_promote_single(
        &Schema::Bytes,
        &Schema::String,
        Value::Bytes("abc".to_owned().into_bytes()),
        Value::String("abc".to_owned()),
    )
    .unwrap();

    resolve_and_promote_single(
        &Schema::String,
        &Schema::String,
        Value::String("abc".to_owned()),
        Value::String("abc".to_owned()),
    )
    .unwrap();
    resolve_and_promote_single(
        &Schema::String,
        &Schema::Bytes,
        Value::String("abc".to_owned()),
        Value::Bytes("abc".to_owned().into_bytes()),
    )
    .unwrap();
}

#[test]
fn test_resolve_and_promote_arrays() {
    resolve_and_promote_single(
        &Schema::Array(Box::new(Schema::Int)),
        &Schema::Array(Box::new(Schema::Int)),
        Value::Array(vec![Value::Int(1), Value::Int(2)]),
        Value::Array(vec![Value::Int(1), Value::Int(2)]),
    )
    .unwrap();
    resolve_and_promote_single(
        &Schema::Array(Box::new(Schema::Int)),
        &Schema::Array(Box::new(Schema::Long)),
        Value::Array(vec![Value::Int(1), Value::Int(2)]),
        Value::Array(vec![Value::Long(1), Value::Long(2)]),
    )
    .unwrap();
}

#[test]
fn test_resolve_and_promote_maps() {
    resolve_and_promote_single(
        &Schema::Map(Box::new(Schema::Int)),
        &Schema::Map(Box::new(Schema::Int)),
        Value::Map(
            vec![
                ("one".to_owned(), Value::Int(1)),
                ("two".to_owned(), Value::Int(2)),
            ]
            .into_iter()
            .collect(),
        ),
        Value::Map(
            vec![
                ("one".to_owned(), Value::Int(1)),
                ("two".to_owned(), Value::Int(2)),
            ]
            .into_iter()
            .collect(),
        ),
    )
    .unwrap();
    resolve_and_promote_single(
        &Schema::Map(Box::new(Schema::Int)),
        &Schema::Map(Box::new(Schema::Long)),
        Value::Map(
            vec![
                ("one".to_owned(), Value::Int(1)),
                ("two".to_owned(), Value::Int(2)),
            ]
            .into_iter()
            .collect(),
        ),
        Value::Map(
            vec![
                ("one".to_owned(), Value::Long(1)),
                ("two".to_owned(), Value::Long(2)),
            ]
            .into_iter()
            .collect(),
        ),
    )
    .unwrap();
}

#[test]
fn test_resolve_and_promote_records() {
    let w_schema = Schema::parse_str(
        r#"
            {
                "type": "record",
                "name": "r1",
                "fields": [
                    {"name": "a_number_no_default", "type": "int"},
                    {"name": "a_number_obsolete", "type": "int"}
                ]
            }
        "#,
    )
    .unwrap();
    let r_schema = Schema::parse_str(
        r#"
            {
                "type": "record",
                "name": "r1",
                "fields": [
                    {"name": "a_number_no_default", "type": "long"},
                    {"name": "a_number_with_default", "type": "long", "default": 42}
                ]
            }
        "#,
    )
    .unwrap();
    let w_record = Value::Record(vec![
        ("a_number_no_default".to_owned(), Value::Int(13)),
        ("a_number_obsolete".to_owned(), Value::Int(-1)),
    ]);
    let r_record = Value::Record(vec![
        ("a_number_no_default".to_owned(), Value::Long(13)),
        ("a_number_with_default".to_owned(), Value::Long(42)),
    ]);
    resolve_and_promote_single(&w_schema, &w_schema, w_record.clone(), w_record.clone()).unwrap();
    resolve_and_promote_single(&w_schema, &r_schema, w_record.clone(), r_record.clone()).unwrap();
}

#[test]
fn test_resolve_and_promote_record_with_default_int() {
    let w_schema = Schema::parse_str(
        r#"
            {
                "type": "record",
                "name": "r2",
                "fields": []
            }
        "#,
    )
    .unwrap();
    let r_schema = Schema::parse_str(
        r#"
            {
                "type": "record",
                "name": "r2",
                "fields": [
                    {"name": "a_field", "type": "int", "default": 42}
                ]
            }
        "#,
    )
    .unwrap();
    let w_data = Value::Record(vec![]);
    let r_data = Value::Record(vec![("a_field".to_owned(), Value::Int(42))]);
    resolve_and_promote_single(&w_schema, &w_schema, w_data.clone(), w_data.clone()).unwrap();
    resolve_and_promote_single(&w_schema, &r_schema, w_data.clone(), r_data.clone()).unwrap();
}

#[test]
fn test_resolve_and_promote_into_union() {
    let w_schema = Schema::parse_str(
        r#"
            {
                "type": "record",
                "name": "r2",
                "fields": [
                    {"name": "a_field", "type": "long"}
                ]
            }
        "#,
    )
    .unwrap();
    let r_schema = Schema::parse_str(
        r#"
            [
                {"type": "long"},
                {
                    "type": "record",
                    "name": "r1",
                    "fields": [
                        {"name": "an_optional_field", "type": "long", "default": 42}
                    ]
                },
                {
                    "type": "record",
                    "name": "r2",
                    "fields": [
                        {"name": "a_field", "type": "long"},
                        {"name": "an_optional_field", "type": "long", "default": 42}
                    ]
                }
            ]
        "#,
    )
    .unwrap();
    let w_data = Value::Record(vec![("a_field".to_owned(), Value::Long(13))]);
    let r_data = Value::Union(
        2,
        Box::new(Value::Record(vec![
            ("a_field".to_owned(), Value::Long(13)),
            ("an_optional_field".to_owned(), Value::Long(42)),
        ])),
    );

    resolve_and_promote_single(&w_schema, &w_schema, w_data.clone(), w_data.clone()).unwrap();
    resolve_and_promote_single(&w_schema, &r_schema, w_data.clone(), r_data.clone()).unwrap();
}

#[test]
fn test_resolve_and_promote_from_union() {
    let w_schema = Schema::parse_str(
        r#"
            [
                {"type": "long"},
                {
                    "type": "record",
                    "name": "r1",
                    "fields": [
                        {"name": "an_optional_field", "type": "long", "default": 42}
                    ]
                },
                {
                    "type": "record",
                    "name": "r2",
                    "fields": [
                        {"name": "a_field", "type": "long"}
                    ]
                }
            ]
        "#,
    )
    .unwrap();
    let r_schema = Schema::parse_str(
        r#"
            {
                "type": "record",
                "name": "r2",
                "fields": [
                    {"name": "a_field", "type": "long"}
                ]
            }
        "#,
    )
    .unwrap();

    let w_data_ok = Value::Union(
        2,
        Box::new(Value::Record(vec![("a_field".to_owned(), Value::Long(42))])),
    );

    let w_data_err = Value::Union(
        1,
        Box::new(Value::Record(vec![(
            "an_optional_field".to_owned(),
            Value::Long(1),
        )])),
    );

    let r_data_ok = Value::Record(vec![("a_field".to_owned(), Value::Long(42))]);

    resolve_and_promote_single(&w_schema, &w_schema, w_data_ok.clone(), w_data_ok.clone()).unwrap();

    resolve_and_promote_single(&w_schema, &r_schema, w_data_ok.clone(), r_data_ok.clone()).unwrap();

    match resolve_and_promote_single(&w_schema, &r_schema, w_data_err.clone(), r_data_ok.clone())
        .err()
        .unwrap()
    {
        ResolutionError::IncompatibleSchema(_, _) => (),
        _ => assert!(false),
    }
}

#[test]
fn test_resolve_and_promote_unions() {
    let w_schema = Schema::parse_str(
        r#"
        [
            {"type": "null"},
            {"type": "long"},
            {"type": "int"}
        ]
    "#,
    )
    .unwrap();
    let r_schema = Schema::parse_str(
        r#"
        [
            {"type": "null"},
            {"type": "int"},
            {"type": "boolean"}
        ]
    "#,
    )
    .unwrap();

    let w_null = Value::Union(0, Box::new(Value::Null));
    let r_null = Value::Union(0, Box::new(Value::Null));
    let w_long = Value::Union(1, Box::new(Value::Long(42)));
    let w_int = Value::Union(2, Box::new(Value::Int(42)));
    let r_int = Value::Union(1, Box::new(Value::Int(42)));

    resolve_and_promote_single(&w_schema, &r_schema, w_null.clone(), r_null.clone()).unwrap();

    match resolve_and_promote_single(&w_schema, &r_schema, w_long.clone(), w_long.clone())
        .err()
        .unwrap()
    {
        ResolutionError::IncompatibleSchema(_, _) => (),
        what => assert!(false),
    };

    resolve_and_promote_single(&w_schema, &r_schema, w_int.clone(), r_int.clone()).unwrap();
}

#[test]
fn test_resolve_and_promote_enums() {
    let w_schema = Schema::parse_str(
        r#"
        {"type": "enum", "name": "e", "symbols": ["a", "b", "c"]}
    "#,
    )
    .unwrap();

    let r_schema = Schema::parse_str(
        r#"
        {"type": "enum", "name": "e", "symbols": ["c", "b", "d"]}
    "#,
    )
    .unwrap();

    let w_a = Value::Enum(0, "a".to_owned());
    let w_b = Value::Enum(1, "b".to_owned());
    let r_b = Value::Enum(1, "b".to_owned());
    let w_c = Value::Enum(2, "c".to_owned());
    let r_c = Value::Enum(0, "c".to_owned());

    match resolve_and_promote_single(&w_schema, &r_schema, w_a.clone(), w_a.clone())
        .err()
        .unwrap()
    {
        ResolutionError::IncompatibleData(_) => (),
        what => panic!("{:?}", what),
    };

    resolve_and_promote_single(&w_schema, &r_schema, w_b.clone(), r_b.clone()).unwrap();

    resolve_and_promote_single(&w_schema, &r_schema, w_c.clone(), r_c.clone()).unwrap();
}
