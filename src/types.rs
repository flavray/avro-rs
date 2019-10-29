//! Logic handling the intermediate representation of Avro values.
use std::collections::HashMap;
use std::hash::BuildHasher;
use std::u8;

use failure::{Error, Fail};
use serde_json::Value as JsonValue;

use crate::schema::{Aggregate, RecordSchema, Schema, SchemaKind, SchemaType, UnionSchema};

/// Describes errors happened while performing schema resolution on Avro data.
#[derive(Fail, Debug)]
#[fail(display = "Schema resoulution error: {}", _0)]
pub struct SchemaResolutionError(pub String);

impl SchemaResolutionError {
    pub fn new<S>(msg: S) -> SchemaResolutionError
    where
        S: Into<String>,
    {
        SchemaResolutionError(msg.into())
    }
}

/// Represents any valid Avro value
/// More information about Avro values can be found in the
/// [Avro Specification](https://avro.apache.org/docs/current/spec.html#schemas)
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    /// A `null` Avro value.
    Null,
    /// A `boolean` Avro value.
    Boolean(bool),
    /// A `int` Avro value.
    Int(i32),
    /// A `long` Avro value.
    Long(i64),
    /// A `float` Avro value.
    Float(f32),
    /// A `double` Avro value.
    Double(f64),
    /// A `bytes` Avro value.
    Bytes(Vec<u8>),
    /// A `string` Avro value.
    String(String),
    /// A `fixed` Avro value.
    /// The size of the fixed value is represented as a `usize`.
    Fixed(usize, Vec<u8>),
    /// An `enum` Avro value.
    ///
    /// An Enum is represented by a symbol and its position in the symbols list
    /// of its corresponding schema.
    /// This allows schema-less encoding, as well as schema resolution while
    /// reading values.
    Enum(i32, String),
    /// An `union` Avro value.
    Union(Box<Value>),
    /// An `array` Avro value.
    Array(Vec<Value>),
    /// A `map` Avro value.
    Map(HashMap<String, Value>),
    /// A `record` Avro value.
    ///
    /// A Record is represented by a vector of (`<record name>`, `value`).
    /// This allows schema-less encoding.
    ///
    /// See [Record](types.Record) for a more user-friendly support.
    Record(Vec<(String, Value)>),
}

/// Any structure implementing the [ToAvro](trait.ToAvro.html) trait will be usable
/// from a [Writer](../writer/struct.Writer.html).
pub trait ToAvro {
    /// Transforms this value into an Avro-compatible [Value](enum.Value.html).
    fn avro(self) -> Value;
}

macro_rules! to_avro(
    ($t:ty, $v:expr) => (
        impl ToAvro for $t {
            fn avro(self) -> Value {
                $v(self)
            }
        }
    );
);

to_avro!(bool, Value::Boolean);
to_avro!(i32, Value::Int);
to_avro!(i64, Value::Long);
to_avro!(f32, Value::Float);
to_avro!(f64, Value::Double);
to_avro!(String, Value::String);

impl ToAvro for () {
    fn avro(self) -> Value {
        Value::Null
    }
}

impl ToAvro for usize {
    fn avro(self) -> Value {
        (self as i64).avro()
    }
}

impl<'a> ToAvro for &'a str {
    fn avro(self) -> Value {
        Value::String(self.to_owned())
    }
}

impl<'a> ToAvro for &'a [u8] {
    fn avro(self) -> Value {
        Value::Bytes(self.to_owned())
    }
}

impl<T> ToAvro for Option<T>
where
    T: ToAvro,
{
    fn avro(self) -> Value {
        let v = match self {
            Some(v) => T::avro(v),
            None => Value::Null,
        };
        Value::Union(Box::new(v))
    }
}

impl<T, S: BuildHasher> ToAvro for HashMap<String, T, S>
where
    T: ToAvro,
{
    fn avro(self) -> Value {
        Value::Map(
            self.into_iter()
                .map(|(key, value)| (key, value.avro()))
                .collect::<_>(),
        )
    }
}

impl<'a, T, S: BuildHasher> ToAvro for HashMap<&'a str, T, S>
where
    T: ToAvro,
{
    fn avro(self) -> Value {
        Value::Map(
            self.into_iter()
                .map(|(key, value)| (key.to_owned(), value.avro()))
                .collect::<_>(),
        )
    }
}

impl ToAvro for Value {
    fn avro(self) -> Value {
        self
    }
}

/*
impl<S: Serialize> ToAvro for S {
    fn avro(self) -> Value {
        use ser::Serializer;

        self.serialize(&mut Serializer::new()).unwrap()
    }
}
*/

/// Utility interface to build `Value::Record` objects.
#[derive(Debug, Clone)]
pub struct Record<'a> {
    /// List of fields contained in the record.
    /// Ordered according to the fields in the schema given to create this
    /// `Record` object. Any unset field defaults to `Value::Null`.
    pub fields: Vec<(String, Value)>,
    schema_type: SchemaType<'a>,
}

impl<'a> Record<'a> {
    /// Create a `Record` given a `Schema`.
    ///
    /// If the `Schema` is not a `Schema::Record` variant, `None` will be returned.
    pub fn new<'s>(schema: &'s Schema) -> Option<Record<'s>> {
        let schema_type = schema.root();
        match schema_type {
            SchemaType::Record(record) => {
                let schema_fields = record.fields();
                let mut fields = Vec::with_capacity(schema_fields.len());
                for schema_field in schema_fields.iter() {
                    fields.push((schema_field.name().to_string(), Value::Null));
                }

                Some(Record {
                    fields,
                    schema_type,
                })
            }
            _ => None,
        }
    }

    /// Put a compatible value (implementing the `ToAvro` trait) in the
    /// `Record` for a given `field` name.
    ///
    /// **NOTE** Only ensure that the field name is present in the `Schema` given when creating
    /// this `Record`. Does not perform any schema validation.
    pub fn put<V>(&mut self, field: &str, value: V)
    where
        V: ToAvro,
    {
        if let SchemaType::Record(record) = self.schema_type {
            if let Some(schema_field) = record.field(field) {
                self.fields[schema_field.position()].1 = value.avro()
            }
        }
    }
}

impl<'a> ToAvro for Record<'a> {
    fn avro(self) -> Value {
        Value::Record(self.fields)
    }
}

impl ToAvro for JsonValue {
    fn avro(self) -> Value {
        match self {
            JsonValue::Null => Value::Null,
            JsonValue::Bool(b) => Value::Boolean(b),
            JsonValue::Number(ref n) if n.is_i64() => Value::Long(n.as_i64().unwrap()),
            JsonValue::Number(ref n) if n.is_f64() => Value::Double(n.as_f64().unwrap()),
            JsonValue::Number(n) => Value::Long(n.as_u64().unwrap() as i64), // TODO: Not so great
            JsonValue::String(s) => Value::String(s),
            JsonValue::Array(items) => {
                Value::Array(items.into_iter().map(|item| item.avro()).collect::<_>())
            }
            JsonValue::Object(items) => Value::Map(
                items
                    .into_iter()
                    .map(|(key, value)| (key, value.avro()))
                    .collect::<_>(),
            ),
        }
    }
}

impl Value {
    /// Validate the value against the given [Schema](../schema/enum.Schema.html).
    ///
    /// See the [Avro specification](https://avro.apache.org/docs/current/spec.html)
    /// for the full set of rules of schema validation.
    pub fn validate(&self, schema: SchemaType) -> bool {
        match (self, schema) {
            (&Value::Null, SchemaType::Null) => true,
            (&Value::Boolean(_), SchemaType::Boolean) => true,
            (&Value::Int(_), SchemaType::Int) => true,
            (&Value::Long(_), SchemaType::Long) => true,
            (&Value::Float(_), SchemaType::Float) => true,
            (&Value::Double(_), SchemaType::Double) => true,
            (&Value::Bytes(_), SchemaType::Bytes) => true,
            (&Value::String(_), SchemaType::String) => true,
            (&Value::Fixed(n, _), SchemaType::Fixed(fixed)) => n == fixed.size(),
            (&Value::String(ref s), SchemaType::Enum(enum_)) => {
                enum_.symbols().contains(&s.as_ref())
            }
            (&Value::Enum(i, ref s), SchemaType::Enum(enum_)) => enum_
                .symbols()
                .get(i as usize)
                .map(|ref symbol| symbol == &s)
                .unwrap_or(false),
            (&Value::Union(ref value), SchemaType::Union(union)) => {
                union.find_schema(value).is_some()
            }
            (&Value::Array(ref items), SchemaType::Array(array)) => {
                items.iter().all(|item| item.validate(array.items()))
            }
            (&Value::Map(ref items), SchemaType::Map(map)) => {
                items.iter().all(|(_, value)| value.validate(map.items()))
            }
            (&Value::Record(ref record_fields), SchemaType::Record(record)) => {
                let fields = record.fields();
                fields.len() == record_fields.len()
                    && fields.iter().zip(record_fields.iter()).all(
                        |(field, &(ref name, ref value))| {
                            field.name() == *name && value.validate(field.schema())
                        },
                    )
            }
            _ => false,
        }
    }

    /// Attempt to perform schema resolution on the value, with the given
    /// [Schema](../schema/enum.Schema.html).
    ///
    /// See [Schema Resolution](https://avro.apache.org/docs/current/spec.html#Schema+Resolution)
    /// in the Avro specification for the full set of rules of schema
    /// resolution.
    pub fn resolve(mut self, schema: SchemaType) -> Result<Self, Error> {
        // Check if this schema is a union, and if the reader schema is not.
        if SchemaKind::from(&self) == SchemaKind::Union
            && SchemaKind::from(schema) != SchemaKind::Union
        {
            // Pull out the Union, and attempt to resolve against it.
            let v = match self {
                Value::Union(b) => *b,
                _ => unreachable!(),
            };
            self = v;
        }
        match schema {
            SchemaType::Null => self.resolve_null(),
            SchemaType::Boolean => self.resolve_boolean(),
            SchemaType::Int => self.resolve_int(),
            SchemaType::Long => self.resolve_long(),
            SchemaType::Float => self.resolve_float(),
            SchemaType::Double => self.resolve_double(),
            SchemaType::Bytes => self.resolve_bytes(),
            SchemaType::String => self.resolve_string(),
            SchemaType::Fixed(fixed) => self.resolve_fixed(fixed.size()),
            SchemaType::Union(union_) => self.resolve_union(&union_),
            SchemaType::Enum(enum_) => self.resolve_enum(enum_.symbols().as_slice()),
            SchemaType::Array(array) => self.resolve_array(&array),
            SchemaType::Map(map) => self.resolve_map(&map),
            SchemaType::Record(record) => self.resolve_record(&record),
        }
    }

    fn resolve_null(self) -> Result<Self, Error> {
        match self {
            Value::Null => Ok(Value::Null),
            other => {
                Err(SchemaResolutionError::new(format!("Null expected, got {:?}", other)).into())
            }
        }
    }

    fn resolve_boolean(self) -> Result<Self, Error> {
        match self {
            Value::Boolean(b) => Ok(Value::Boolean(b)),
            other => {
                Err(SchemaResolutionError::new(format!("Boolean expected, got {:?}", other)).into())
            }
        }
    }

    fn resolve_int(self) -> Result<Self, Error> {
        match self {
            Value::Int(n) => Ok(Value::Int(n)),
            Value::Long(n) => Ok(Value::Int(n as i32)),
            other => {
                Err(SchemaResolutionError::new(format!("Int expected, got {:?}", other)).into())
            }
        }
    }

    fn resolve_long(self) -> Result<Self, Error> {
        match self {
            Value::Int(n) => Ok(Value::Long(i64::from(n))),
            Value::Long(n) => Ok(Value::Long(n)),
            other => {
                Err(SchemaResolutionError::new(format!("Long expected, got {:?}", other)).into())
            }
        }
    }

    fn resolve_float(self) -> Result<Self, Error> {
        match self {
            Value::Int(n) => Ok(Value::Float(n as f32)),
            Value::Long(n) => Ok(Value::Float(n as f32)),
            Value::Float(x) => Ok(Value::Float(x)),
            Value::Double(x) => Ok(Value::Float(x as f32)),
            other => {
                Err(SchemaResolutionError::new(format!("Float expected, got {:?}", other)).into())
            }
        }
    }

    fn resolve_double(self) -> Result<Self, Error> {
        match self {
            Value::Int(n) => Ok(Value::Double(f64::from(n))),
            Value::Long(n) => Ok(Value::Double(n as f64)),
            Value::Float(x) => Ok(Value::Double(f64::from(x))),
            Value::Double(x) => Ok(Value::Double(x)),
            other => {
                Err(SchemaResolutionError::new(format!("Double expected, got {:?}", other)).into())
            }
        }
    }

    fn resolve_bytes(self) -> Result<Self, Error> {
        match self {
            Value::Bytes(bytes) => Ok(Value::Bytes(bytes)),
            Value::String(s) => Ok(Value::Bytes(s.into_bytes())),
            Value::Array(items) => Ok(Value::Bytes(
                items
                    .into_iter()
                    .map(Value::try_u8)
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            other => {
                Err(SchemaResolutionError::new(format!("Bytes expected, got {:?}", other)).into())
            }
        }
    }

    fn resolve_string(self) -> Result<Self, Error> {
        match self {
            Value::String(s) => Ok(Value::String(s)),
            Value::Bytes(bytes) => Ok(Value::String(String::from_utf8(bytes)?)),
            other => {
                Err(SchemaResolutionError::new(format!("String expected, got {:?}", other)).into())
            }
        }
    }

    fn resolve_fixed(self, size: usize) -> Result<Self, Error> {
        match self {
            Value::Fixed(n, bytes) => {
                if n == size {
                    Ok(Value::Fixed(n, bytes))
                } else {
                    Err(SchemaResolutionError::new(format!(
                        "Fixed size mismatch, {} expected, got {}",
                        size, n
                    ))
                    .into())
                }
            }
            other => {
                Err(SchemaResolutionError::new(format!("String expected, got {:?}", other)).into())
            }
        }
    }

    fn resolve_enum(self, symbols: &[&str]) -> Result<Self, Error> {
        let validate_symbol = |symbol: String, symbols: &[&str]| {
            if let Some(index) = symbols.iter().position(|ref item| item == &&symbol) {
                Ok(Value::Enum(index as i32, symbol))
            } else {
                Err(SchemaResolutionError::new(format!(
                    "Enum default {} is not among allowed symbols {:?}",
                    symbol, symbols,
                ))
                .into())
            }
        };

        match self {
            Value::Enum(i, s) => {
                if i >= 0 && i < symbols.len() as i32 {
                    validate_symbol(s, symbols)
                } else {
                    Err(SchemaResolutionError::new(format!(
                        "Enum value {} is out of bound {}",
                        i,
                        symbols.len() as i32
                    ))
                    .into())
                }
            }
            Value::String(s) => validate_symbol(s, symbols),
            other => Err(SchemaResolutionError::new(format!(
                "Enum({:?}) expected, got {:?}",
                symbols, other
            ))
            .into()),
        }
    }

    fn resolve_union(self, schema: &UnionSchema) -> Result<Self, Error> {
        let v = match self {
            // Both are unions case.
            Value::Union(v) => *v,
            // Reader is a union, but writer is not.
            v => v,
        };
        // Find the first match in the reader schema.
        let (_, inner) = schema
            .find_schema(&v)
            .ok_or_else(|| SchemaResolutionError::new("Could not find matching type in union"))?;
        v.resolve(inner)
    }

    fn resolve_array(self, array_schema: &Aggregate) -> Result<Self, Error> {
        let elem_type = array_schema.items();
        match self {
            Value::Array(items) => Ok(Value::Array(
                items
                    .into_iter()
                    .map(|item| item.resolve(elem_type))
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            other => Err(SchemaResolutionError::new(format!(
                "Array({:?}) expected, got {:?}",
                elem_type, other
            ))
            .into()),
        }
    }

    fn resolve_map(self, map_schema: &Aggregate) -> Result<Self, Error> {
        let elem_type = map_schema.items();
        match self {
            Value::Map(items) => Ok(Value::Map(
                items
                    .into_iter()
                    .map(|(key, value)| value.resolve(elem_type).map(|value| (key, value)))
                    .collect::<Result<HashMap<_, _>, _>>()?,
            )),
            other => Err(SchemaResolutionError::new(format!(
                "Map({:?}) expected, got {:?}",
                elem_type, other
            ))
            .into()),
        }
    }

    fn resolve_record(self, record: &RecordSchema) -> Result<Self, Error> {
        let fields = record.fields();
        let mut items = match self {
            Value::Map(items) => Ok(items),
            Value::Record(fields) => Ok(fields.into_iter().collect::<HashMap<_, _>>()),
            other => Err(Error::from(SchemaResolutionError::new(format!(
                "Record({:?}) expected, got {:?}",
                fields, other
            )))),
        }?;

        let new_fields = fields
            .iter()
            .map(|field| {
                let value = match items.remove(field.name()) {
                    Some(value) => value,
                    None => match field.default() {
                        Some(value) => {
                            let value = value.clone().avro();
                            match field.schema() {
                                SchemaType::Enum(enum_) => value.resolve_enum(&enum_.symbols())?,
                                _ => value,
                            }
                        }
                        _ => {
                            return Err(SchemaResolutionError::new(format!(
                                "missing field {} in record",
                                field.name()
                            ))
                            .into())
                        }
                    },
                };
                value
                    .resolve(field.schema())
                    .map(|value| (field.name().to_string(), value))
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Value::Record(new_fields))
    }

    fn try_u8(self) -> Result<u8, Error> {
        let int = self.resolve(SchemaType::Int)?;
        if let Value::Int(n) = int {
            if n >= 0 && n <= i32::from(u8::MAX) {
                return Ok(n as u8);
            }
        }

        Err(SchemaResolutionError::new(format!("Unable to convert to u8, got {:?}", int)).into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{Name, RecordField, RecordFieldOrder, UnionSchema};

    #[test]
    fn validate() {
        let value_schema_valid = vec![
            (
                Value::Int(42),
                {
                    let builder = Schema::builder();
                    let root = builder.int();
                    builder.build(root).unwrap()
                },
                true,
            ),
            (
                Value::Int(42),
                {
                    let builder = Schema::builder();
                    let root = builder.boolean();
                    builder.build(root).unwrap()
                },
                false,
            ),
            (
                Value::Union(Box::new(Value::Null)),
                {
                    let mut builder = Schema::builder();
                    let mut union = builder.union();
                    union.variant(builder.null());
                    union.variant(builder.int());
                    let root = union.build(&mut builder).unwrap();
                    builder.build(root).unwrap()
                },
                true,
            ),
            (
                Value::Union(Box::new(Value::Int(42))),
                {
                    let mut builder = Schema::builder();
                    let mut union = builder.union();
                    union.variant(builder.null());
                    union.variant(builder.int());
                    let root = union.build(&mut builder).unwrap();
                    builder.build(root).unwrap()
                },
                true,
            ),
            (
                Value::Union(Box::new(Value::Null)),
                {
                    let mut builder = Schema::builder();
                    let mut union = builder.union();
                    union.variant(builder.double());
                    union.variant(builder.int());
                    let root = union.build(&mut builder).unwrap();
                    builder.build(root).unwrap()
                },
                false,
            ),
            (
                Value::Union(Box::new(Value::Int(42))),
                {
                    let mut builder = Schema::builder();
                    let mut union = builder.union();
                    union.variant(builder.null());
                    union.variant(builder.double());
                    union.variant(builder.string());
                    union.variant(builder.int());
                    let root = union.build(&mut builder).unwrap();
                    builder.build(root).unwrap()
                },
                true,
            ),
            (
                Value::Array(vec![Value::Long(42i64)]),
                {
                    let mut builder = Schema::builder();
                    let root = builder.array().items(builder.long(), &mut builder).unwrap();
                    builder.build(root).unwrap()
                },
                true,
            ),
            (
                Value::Array(vec![Value::Boolean(true)]),
                {
                    let mut builder = Schema::builder();
                    let root = builder.array().items(builder.long(), &mut builder).unwrap();
                    builder.build(root).unwrap()
                },
                false,
            ),
            (
                Value::Record(vec![]),
                {
                    let builder = Schema::builder();
                    let root = builder.null();
                    builder.build(root).unwrap()
                },
                false,
            ),
        ];

        for (value, schema, valid) in value_schema_valid.into_iter() {
            assert_eq!(valid, value.validate(schema.root()));
        }
    }

    #[test]
    fn validate_fixed() {
        let schema = {
            let mut builder = Schema::builder();
            let root = builder.fixed("some_fixed").size(4, &mut builder).unwrap();
            builder.build(root).unwrap()
        };

        assert!(Value::Fixed(4, vec![0, 0, 0, 0]).validate(schema.root()));
        assert!(!Value::Fixed(5, vec![0, 0, 0, 0, 0]).validate(schema.root()));
    }

    #[test]
    fn validate_enum() {
        let schema = {
            let mut builder = Schema::builder();
            let root = builder
                .enumeration("some_enum")
                .symbols(vec!["spades", "hearts", "diamonds", "clubs"], &mut builder)
                .unwrap();
            builder.build(root).unwrap()
        };

        assert!(Value::Enum(0, "spades".to_string()).validate(schema.root()));
        assert!(Value::String("spades".to_string()).validate(schema.root()));

        assert!(!Value::Enum(1, "spades".to_string()).validate(schema.root()));
        assert!(!Value::String("lorem".to_string()).validate(schema.root()));

        let other_schema = {
            let mut builder = Schema::builder();
            let root = builder
                .enumeration("some_other_enum")
                .symbols(vec!["hearts", "diamonds", "clubs", "spades"], &mut builder)
                .unwrap();
            builder.build(root).unwrap()
        };

        assert!(!Value::Enum(0, "spades".to_string()).validate(other_schema.root()));
    }

    #[test]
    fn validate_record() {
        // {
        //    "type": "record",
        //    "fields": [
        //      {"type": "long", "name": "a"},
        //      {"type": "string", "name": "b"}
        //    ]
        // }
        let schema = {
            let mut builder = Schema::builder();
            let mut some_record = builder.record("some_record");
            some_record.field("a", builder.long());
            some_record.field("b", builder.string());
            let root = some_record.build(&mut builder).unwrap();
            builder.build(root).unwrap()
        };

        assert!(Value::Record(vec![
            ("a".to_string(), Value::Long(42i64)),
            ("b".to_string(), Value::String("foo".to_string())),
        ])
        .validate(schema.root()));

        assert!(!Value::Record(vec![
            ("b".to_string(), Value::String("foo".to_string())),
            ("a".to_string(), Value::Long(42i64)),
        ])
        .validate(schema.root()));

        assert!(!Value::Record(vec![
            ("a".to_string(), Value::Boolean(false)),
            ("b".to_string(), Value::String("foo".to_string())),
        ])
        .validate(schema.root()));

        assert!(!Value::Record(vec![
            ("a".to_string(), Value::Long(42i64)),
            ("c".to_string(), Value::String("foo".to_string())),
        ])
        .validate(schema.root()));

        assert!(!Value::Record(vec![
            ("a".to_string(), Value::Long(42i64)),
            ("b".to_string(), Value::String("foo".to_string())),
            ("c".to_string(), Value::Null),
        ])
        .validate(schema.root()));
    }

    #[test]
    fn resolve_bytes_ok() {
        let value = Value::Array(vec![Value::Int(0), Value::Int(42)]);
        assert_eq!(
            value.resolve(SchemaType::Bytes).unwrap(),
            Value::Bytes(vec![0u8, 42u8])
        );
    }

    #[test]
    fn resolve_bytes_failure() {
        let value = Value::Array(vec![Value::Int(2000), Value::Int(-42)]);
        assert!(value.resolve(SchemaType::Bytes).is_err());
    }
}
