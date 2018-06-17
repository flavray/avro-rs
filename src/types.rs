//! Logic handling the intermediate representation of Avro values.
use std::collections::HashMap;
use std::hash::BuildHasher;
use std::rc::Rc;

use failure::Error;
use serde_json::Value as JsonValue;

use schema::{RecordField, Schema};

/// Describes errors happened while performing schema resolution on Avro data.
#[derive(Fail, Debug)]
#[fail(display = "Decoding error: {}", _0)]
pub struct SchemaResolutionError(String);

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
    ///
    /// The current implementation limits union support to \["null", "< type >"\].
    ///
    /// `None` represents a `null` value, while `Some(<X>)` represents a `type`
    /// value.
    Union(Option<Box<Value>>),
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
        Value::Union(self.map(|v| Box::new(v.avro())))
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
pub struct Record {
    /// List of fields contained in the record.
    /// Ordered according to the fields in the schema given to create this
    /// `Record` object. Any unset field defaults to `Value::Null`.
    pub fields: Vec<(String, Value)>,
    schema_lookup: Rc<HashMap<String, usize>>,
}

impl Record {
    /// Create a `Record` given a `Schema`.
    ///
    /// If the `Schema` is not a `Schema::Record` variant, `None` will be returned.
    pub fn new(schema: &Schema) -> Option<Record> {
        match *schema {
            Schema::Record {
                fields: ref schema_fields,
                lookup: ref schema_lookup,
                ..
            } => {
                let mut fields = Vec::with_capacity(schema_fields.len());
                for schema_field in schema_fields.iter() {
                    fields.push((schema_field.name.clone(), Value::Null));
                }

                Some(Record {
                    fields,
                    schema_lookup: schema_lookup.clone(),
                })
            },
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
        if let Some(&position) = self.schema_lookup.get(field) {
            self.fields[position].1 = value.avro()
        }
    }
}

impl ToAvro for Record {
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
            },
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
    pub fn validate(&self, schema: &Schema) -> bool {
        match (self, schema) {
            (&Value::Null, &Schema::Null) => true,
            (&Value::Boolean(_), &Schema::Boolean) => true,
            (&Value::Int(_), &Schema::Int) => true,
            (&Value::Long(_), &Schema::Long) => true,
            (&Value::Float(_), &Schema::Float) => true,
            (&Value::Double(_), &Schema::Double) => true,
            (&Value::Bytes(_), &Schema::Bytes) => true,
            (&Value::String(_), &Schema::String) => true,
            (&Value::Fixed(n, _), &Schema::Fixed { size, .. }) => n == size,
            (&Value::String(ref s), &Schema::Enum { ref symbols, .. }) => symbols.contains(s),
            (&Value::Enum(i, ref s), &Schema::Enum { ref symbols, .. }) => symbols
                .get(i as usize)
                .map(|ref symbol| symbol == &s)
                .unwrap_or(false),
            (&Value::Union(None), &Schema::Union(_)) => true,
            (&Value::Union(Some(ref value)), &Schema::Union(ref inner)) => value.validate(inner),
            (&Value::Array(ref items), &Schema::Array(ref inner)) => {
                items.iter().all(|item| item.validate(inner))
            },
            (&Value::Map(ref items), &Schema::Map(ref inner)) => {
                items.iter().all(|(_, value)| value.validate(inner))
            },
            (&Value::Record(ref record_fields), &Schema::Record { ref fields, .. }) => {
                fields.len() == record_fields.len()
                    && fields.iter().zip(record_fields.iter()).all(
                        |(field, &(ref name, ref value))| {
                            field.name == *name && value.validate(&field.schema)
                        },
                    )
            },
            _ => false,
        }
    }

    /// Attempt to perform schema resolution on the value, with the given
    /// [Schema](../schema/enum.Schema.html).
    ///
    /// See [Schema Resolution](https://avro.apache.org/docs/current/spec.html#Schema+Resolution)
    /// in the Avro specification for the full set of rules of schema
    /// resolution.
    pub fn resolve(self, schema: &Schema) -> Result<Self, Error> {
        match *schema {
            Schema::Null => self.resolve_null(),
            Schema::Boolean => self.resolve_boolean(),
            Schema::Int => self.resolve_int(),
            Schema::Long => self.resolve_long(),
            Schema::Float => self.resolve_float(),
            Schema::Double => self.resolve_double(),
            Schema::Bytes => self.resolve_bytes(),
            Schema::String => self.resolve_string(),
            Schema::Fixed { size, .. } => self.resolve_fixed(size),
            Schema::Union(ref inner) => self.resolve_union(inner),
            Schema::Enum { ref symbols, .. } => self.resolve_enum(symbols),
            Schema::Array(ref inner) => self.resolve_array(inner),
            Schema::Map(ref inner) => self.resolve_map(inner),
            Schema::Record { ref fields, .. } => self.resolve_record(fields),
        }
    }

    fn resolve_null(self) -> Result<Self, Error> {
        match self {
            Value::Null => Ok(Value::Null),
            other => {
                Err(SchemaResolutionError::new(format!("Null expected, got {:?}", other)).into())
            },
        }
    }

    fn resolve_boolean(self) -> Result<Self, Error> {
        match self {
            Value::Boolean(b) => Ok(Value::Boolean(b)),
            other => {
                Err(SchemaResolutionError::new(format!("Boolean expected, got {:?}", other)).into())
            },
        }
    }

    fn resolve_int(self) -> Result<Self, Error> {
        match self {
            Value::Int(n) => Ok(Value::Int(n)),
            Value::Long(n) => Ok(Value::Int(n as i32)),
            other => {
                Err(SchemaResolutionError::new(format!("Int expected, got {:?}", other)).into())
            },
        }
    }

    fn resolve_long(self) -> Result<Self, Error> {
        match self {
            Value::Int(n) => Ok(Value::Long(i64::from(n))),
            Value::Long(n) => Ok(Value::Long(n)),
            other => {
                Err(SchemaResolutionError::new(format!("Long expected, got {:?}", other)).into())
            },
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
            },
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
            },
        }
    }

    fn resolve_bytes(self) -> Result<Self, Error> {
        match self {
            Value::Bytes(bytes) => Ok(Value::Bytes(bytes)),
            Value::String(s) => Ok(Value::Bytes(s.into_bytes())),
            other => {
                Err(SchemaResolutionError::new(format!("Bytes expected, got {:?}", other)).into())
            },
        }
    }

    fn resolve_string(self) -> Result<Self, Error> {
        match self {
            Value::String(s) => Ok(Value::String(s)),
            Value::Bytes(bytes) => Ok(Value::String(String::from_utf8(bytes)?)),
            other => {
                Err(SchemaResolutionError::new(format!("String expected, got {:?}", other)).into())
            },
        }
    }

    fn resolve_fixed(self, size: usize) -> Result<Self, Error> {
        match self {
            Value::Fixed(n, bytes) => if n == size {
                Ok(Value::Fixed(n, bytes))
            } else {
                Err(SchemaResolutionError::new(format!(
                    "Fixed size mismatch, {} expected, got {}",
                    size, n
                )).into())
            },
            other => {
                Err(SchemaResolutionError::new(format!("String expected, got {:?}", other)).into())
            },
        }
    }

    fn resolve_enum(self, symbols: &[String]) -> Result<Self, Error> {
        let validate_symbol = |symbol: String, symbols: &[String]| {
            if let Some(index) = symbols.iter().position(|ref item| item == &&symbol) {
                Ok(Value::Enum(index as i32, symbol))
            } else {
                Err(SchemaResolutionError::new(format!(
                    "Enum default {} is not among allowed symbols {:?}",
                    symbol, symbols,
                )).into())
            }
        };

        match self {
            Value::Enum(i, s) => if i > 0 && i < symbols.len() as i32 {
                validate_symbol(s, symbols)
            } else {
                Err(SchemaResolutionError::new(format!(
                    "Enum value {} is out of bound {}",
                    i,
                    symbols.len() as i32
                )).into())
            },
            Value::String(s) => validate_symbol(s, symbols),
            other => Err(SchemaResolutionError::new(format!(
                "Enum({:?}) expected, got {:?}",
                symbols, other
            )).into()),
        }
    }

    fn resolve_union(self, schema: &Schema) -> Result<Self, Error> {
        match self {
            Value::Union(None) => Ok(Value::Union(None)),
            Value::Union(Some(inner)) => Ok(Value::Union(Some(Box::new(inner.resolve(schema)?)))),
            other => Err(SchemaResolutionError::new(format!(
                "Union({:?}) expected, got {:?}",
                schema, other
            )).into()),
        }
    }

    fn resolve_array(self, schema: &Schema) -> Result<Self, Error> {
        match self {
            Value::Array(items) => Ok(Value::Array(
                items
                    .into_iter()
                    .map(|item| item.resolve(schema))
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            other => Err(SchemaResolutionError::new(format!(
                "Array({:?}) expected, got {:?}",
                schema, other
            )).into()),
        }
    }

    fn resolve_map(self, schema: &Schema) -> Result<Self, Error> {
        match self {
            Value::Map(items) => Ok(Value::Map(
                items
                    .into_iter()
                    .map(|(key, value)| value.resolve(schema).map(|value| (key, value)))
                    .collect::<Result<HashMap<_, _>, _>>()?,
            )),
            other => Err(SchemaResolutionError::new(format!(
                "Map({:?}) expected, got {:?}",
                schema, other
            )).into()),
        }
    }

    fn resolve_record(self, fields: &[RecordField]) -> Result<Self, Error> {
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
                let value = match items.remove(&field.name) {
                    Some(value) => value,
                    None => match field.default {
                        Some(ref value) => match field.schema {
                            Schema::Enum { ref symbols, .. } => {
                                value.clone().avro().resolve_enum(symbols)?
                            },
                            _ => value.clone().avro(),
                        },
                        _ => {
                            return Err(SchemaResolutionError::new(format!(
                                "missing field {} in record",
                                field.name
                            )).into())
                        },
                    },
                };
                value
                    .resolve(&field.schema)
                    .map(|value| (field.name.clone(), value))
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Value::Record(new_fields))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::rc::Rc;

    use schema::{Name, RecordField, RecordFieldOrder};

    #[test]
    fn validate() {
        let value_schema_valid = vec![
            (Value::Int(42), Schema::Int, true),
            (Value::Int(42), Schema::Boolean, false),
            (
                Value::Union(None),
                Schema::Union(Rc::new(Schema::Int)),
                true,
            ),
            (
                Value::Union(Some(Box::new(Value::Int(42)))),
                Schema::Union(Rc::new(Schema::Int)),
                true,
            ),
            (
                Value::Union(Some(Box::new(Value::Null))),
                Schema::Union(Rc::new(Schema::Int)),
                false,
            ),
            (
                Value::Array(vec![Value::Long(42i64)]),
                Schema::Array(Rc::new(Schema::Long)),
                true,
            ),
            (
                Value::Array(vec![Value::Boolean(true)]),
                Schema::Array(Rc::new(Schema::Long)),
                false,
            ),
            (Value::Record(vec![]), Schema::Null, false),
        ];

        for (value, schema, valid) in value_schema_valid.into_iter() {
            assert_eq!(valid, value.validate(&schema));
        }
    }

    #[test]
    fn validate_fixed() {
        let schema = Schema::Fixed {
            size: 4,
            name: Name::new("some_fixed"),
        };

        assert!(Value::Fixed(4, vec![0, 0, 0, 0]).validate(&schema));
        assert!(!Value::Fixed(5, vec![0, 0, 0, 0, 0]).validate(&schema));
    }

    #[test]
    fn validate_enum() {
        let schema = Schema::Enum {
            name: Name::new("some_enum"),
            doc: None,
            symbols: vec![
                "spades".to_string(),
                "hearts".to_string(),
                "diamonds".to_string(),
                "clubs".to_string(),
            ],
        };

        assert!(Value::Enum(0, "spades".to_string()).validate(&schema));
        assert!(Value::String("spades".to_string()).validate(&schema));

        assert!(!Value::Enum(1, "spades".to_string()).validate(&schema));
        assert!(!Value::String("lorem".to_string()).validate(&schema));

        let other_schema = Schema::Enum {
            name: Name::new("some_other_enum"),
            doc: None,
            symbols: vec![
                "hearts".to_string(),
                "diamonds".to_string(),
                "clubs".to_string(),
                "spades".to_string(),
            ],
        };

        assert!(!Value::Enum(0, "spades".to_string()).validate(&other_schema));
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
        let schema = Schema::Record {
            name: Name::new("some_record"),
            doc: None,
            fields: vec![
                RecordField {
                    name: "a".to_string(),
                    doc: None,
                    default: None,
                    schema: Schema::Long,
                    order: RecordFieldOrder::Ascending,
                    position: 0,
                },
                RecordField {
                    name: "b".to_string(),
                    doc: None,
                    default: None,
                    schema: Schema::String,
                    order: RecordFieldOrder::Ascending,
                    position: 1,
                },
            ],
            lookup: Rc::new(HashMap::new()),
        };

        assert!(
            Value::Record(vec![
                ("a".to_string(), Value::Long(42i64)),
                ("b".to_string(), Value::String("foo".to_string())),
            ]).validate(&schema)
        );

        assert!(
            !Value::Record(vec![
                ("b".to_string(), Value::String("foo".to_string())),
                ("a".to_string(), Value::Long(42i64)),
            ]).validate(&schema)
        );

        assert!(
            !Value::Record(vec![
                ("a".to_string(), Value::Boolean(false)),
                ("b".to_string(), Value::String("foo".to_string())),
            ]).validate(&schema)
        );

        assert!(
            !Value::Record(vec![
                ("a".to_string(), Value::Long(42i64)),
                ("c".to_string(), Value::String("foo".to_string())),
            ]).validate(&schema)
        );

        assert!(
            !Value::Record(vec![
                ("a".to_string(), Value::Long(42i64)),
                ("b".to_string(), Value::String("foo".to_string())),
                ("c".to_string(), Value::Null),
            ]).validate(&schema)
        );
    }
}
