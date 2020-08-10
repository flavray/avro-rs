//! Logic handling the intermediate representation of Avro values.
use crate::{
    decimal::Decimal,
    duration::Duration,
    errors::{AvroResult, Error},
    schema::{Precision, RecordField, Scale, Schema, SchemaKind, UnionSchema},
};
use serde_json::{Number, Value as JsonValue};
use std::{collections::HashMap, convert::TryFrom, hash::BuildHasher, str::FromStr, u8};
use uuid::Uuid;

/// Compute the maximum decimal value precision of a byte array of length `len` could hold.
fn max_prec_for_len(len: usize) -> Result<usize, std::num::TryFromIntError> {
    Ok((2.0_f64.powi(i32::try_from(8 * len - 1)?) - 1.0 as f64)
        .log10()
        .floor() as usize)
}

/// A valid Avro value.
///
/// More information about Avro values can be found in the [Avro
/// Specification](https://avro.apache.org/docs/current/spec.html#schemas)
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
    /// A date value.
    ///
    /// Serialized and deserialized as `i32` directly. Can only be deserialized properly with a
    /// schema.
    Date(i32),
    /// An Avro Decimal value. Bytes are in big-endian order, per the Avro spec.
    Decimal(Decimal),
    /// Time in milliseconds.
    TimeMillis(i32),
    /// Time in microseconds.
    TimeMicros(i64),
    /// Timestamp in milliseconds.
    TimestampMillis(i64),
    /// Timestamp in microseconds.
    TimestampMicros(i64),
    /// Avro Duration. An amount of time defined by months, days and milliseconds.
    Duration(Duration),
    /// Universally unique identifier.
    /// Universally unique identifier.
    Uuid(Uuid),
}
/// Any structure implementing the [ToAvro](trait.ToAvro.html) trait will be usable
/// from a [Writer](../writer/struct.Writer.html).
#[deprecated(
    since = "0.11.0",
    note = "Please use Value::from, Into::into or value.into() instead"
)]
pub trait ToAvro {
    /// Transforms this value into an Avro-compatible [Value](enum.Value.html).
    fn avro(self) -> Value;
}

#[allow(deprecated)]
impl<T: Into<Value>> ToAvro for T {
    fn avro(self) -> Value {
        self.into()
    }
}

macro_rules! to_value(
    ($type:ty, $variant_constructor:expr) => (
        impl From<$type> for Value {
            fn from(value: $type) -> Self {
                $variant_constructor(value)
            }
        }
    );
);

to_value!(bool, Value::Boolean);
to_value!(i32, Value::Int);
to_value!(i64, Value::Long);
to_value!(f32, Value::Float);
to_value!(f64, Value::Double);
to_value!(String, Value::String);
to_value!(Vec<u8>, Value::Bytes);
to_value!(uuid::Uuid, Value::Uuid);
to_value!(Decimal, Value::Decimal);
to_value!(Duration, Value::Duration);

impl From<()> for Value {
    fn from(_: ()) -> Self {
        Self::Null
    }
}

impl From<usize> for Value {
    fn from(value: usize) -> Self {
        i64::try_from(value)
            .expect("cannot convert usize to i64")
            .into()
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

impl From<&[u8]> for Value {
    fn from(value: &[u8]) -> Self {
        Self::Bytes(value.to_owned())
    }
}

impl<T> From<Option<T>> for Value
where
    T: Into<Self>,
{
    fn from(value: Option<T>) -> Self {
        Self::Union(Box::new(value.map_or_else(|| Self::Null, Into::into)))
    }
}

impl<K, V, S> From<HashMap<K, V, S>> for Value
where
    K: Into<String>,
    V: Into<Self>,
    S: BuildHasher,
{
    fn from(value: HashMap<K, V, S>) -> Self {
        Self::Map(
            value
                .into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
        )
    }
}

/// Utility interface to build `Value::Record` objects.
#[derive(Debug, Clone)]
pub struct Record<'a> {
    /// List of fields contained in the record.
    /// Ordered according to the fields in the schema given to create this
    /// `Record` object. Any unset field defaults to `Value::Null`.
    pub fields: Vec<(String, Value)>,
    schema_lookup: &'a HashMap<String, usize>,
}

impl<'a> Record<'a> {
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
                    schema_lookup,
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
        V: Into<Value>,
    {
        if let Some(&position) = self.schema_lookup.get(field) {
            self.fields[position].1 = value.into()
        }
    }
}

impl<'a> From<Record<'a>> for Value {
    fn from(value: Record<'a>) -> Self {
        Self::Record(value.fields)
    }
}

impl From<JsonValue> for Value {
    fn from(value: JsonValue) -> Self {
        match value {
            JsonValue::Null => Self::Null,
            JsonValue::Bool(b) => b.into(),
            JsonValue::Number(ref n) if n.is_i64() => Value::Long(n.as_i64().unwrap()),
            JsonValue::Number(ref n) if n.is_f64() => Value::Double(n.as_f64().unwrap()),
            JsonValue::Number(n) => Value::Long(n.as_u64().unwrap() as i64), // TODO: Not so great
            JsonValue::String(s) => s.into(),
            JsonValue::Array(items) => Value::Array(items.into_iter().map(Value::from).collect()),
            JsonValue::Object(items) => Value::Map(
                items
                    .into_iter()
                    .map(|(key, value)| (key, value.into()))
                    .collect(),
            ),
        }
    }
}

/// Convert Avro values to Json values
impl std::convert::TryFrom<Value> for JsonValue {
    type Error = crate::errors::Error;
    fn try_from(value: Value) -> AvroResult<Self> {
        match value {
            Value::Null => Ok(JsonValue::Null),
            Value::Boolean(b) => Ok(JsonValue::Bool(b)),
            Value::Int(i) => Ok(JsonValue::Number(i.into())),
            Value::Long(l) => Ok(JsonValue::Number(l.into())),
            Value::Float(f) => Number::from_f64(f.into())
                .map(|n| JsonValue::Number(n))
                .ok_or(Error::JsonTryFrom(
                    "failed to convert f64 to json".to_string(),
                )),
            Value::Double(d) => {
                Number::from_f64(d)
                    .map(|n| JsonValue::Number(n))
                    .ok_or(Error::JsonTryFrom(
                        "failed to convert f64 to json".to_string(),
                    ))
            }
            Value::Bytes(bytes) => Ok(JsonValue::Array(
                bytes.into_iter().map(|b| b.into()).collect(),
            )),
            Value::String(s) => Ok(JsonValue::String(s)),
            Value::Fixed(_size, items) => Ok(JsonValue::Array(
                items.into_iter().map(|v| v.into()).collect(),
            )),
            Value::Enum(_i, s) => Ok(JsonValue::String(s)),
            Value::Union(b) => JsonValue::try_from(*b),
            Value::Array(items) => {
                let results: Result<Vec<JsonValue>, Error> =
                    items.into_iter().map(|v| JsonValue::try_from(v)).collect();
                results.map(JsonValue::Array)
            }
            Value::Map(items) => {
                let results: Result<Vec<(String, JsonValue)>, Error> = items
                    .into_iter()
                    .map(|(key, value)| JsonValue::try_from(value).map(|v| (key, v)))
                    .collect();
                results.map(|v| JsonValue::Object(v.into_iter().collect()))
            }
            Value::Record(items) => {
                let results: Result<Vec<(String, JsonValue)>, Error> = items
                    .into_iter()
                    .map(|(key, value)| JsonValue::try_from(value).map(|v| (key, v)))
                    .collect();
                results.map(|v| JsonValue::Object(v.into_iter().collect()))
            }
            Value::Date(d) => Ok(JsonValue::Number(d.into())),
            Value::Decimal(d) => d
                .to_vec()
                .map(|vec| JsonValue::Array(vec.into_iter().map(|v| v.into()).collect())),
            Value::TimeMillis(t) => Ok(JsonValue::Number(t.into())),
            Value::TimeMicros(t) => Ok(JsonValue::Number(t.into())),
            Value::TimestampMillis(t) => Ok(JsonValue::Number(t.into())),
            Value::TimestampMicros(t) => Ok(JsonValue::Number(t.into())),
            Value::Duration(d) => {
                let vector: [u8; 12] = d.into();
                Ok(JsonValue::Array(vector.iter().map(|&v| v.into()).collect()))
            }
            Value::Uuid(uuid) => Ok(JsonValue::String(uuid.to_hyphenated().to_string())),
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
            (&Value::Int(_), &Schema::Date) => true,
            (&Value::Int(_), &Schema::TimeMillis) => true,
            (&Value::Long(_), &Schema::Long) => true,
            (&Value::Long(_), &Schema::TimeMicros) => true,
            (&Value::Long(_), &Schema::TimestampMillis) => true,
            (&Value::Long(_), &Schema::TimestampMicros) => true,
            (&Value::TimestampMicros(_), &Schema::TimestampMicros) => true,
            (&Value::TimestampMillis(_), &Schema::TimestampMillis) => true,
            (&Value::TimeMicros(_), &Schema::TimeMicros) => true,
            (&Value::TimeMillis(_), &Schema::TimeMillis) => true,
            (&Value::Date(_), &Schema::Date) => true,
            (&Value::Decimal(_), &Schema::Decimal { .. }) => true,
            (&Value::Duration(_), &Schema::Duration) => true,
            (&Value::Uuid(_), &Schema::Uuid) => true,
            (&Value::Float(_), &Schema::Float) => true,
            (&Value::Double(_), &Schema::Double) => true,
            (&Value::Bytes(_), &Schema::Bytes) => true,
            (&Value::Bytes(_), &Schema::Decimal { .. }) => true,
            (&Value::String(_), &Schema::String) => true,
            (&Value::String(_), &Schema::Uuid) => true,
            (&Value::Fixed(n, _), &Schema::Fixed { size, .. }) => n == size,
            (&Value::Fixed(n, _), &Schema::Duration) => n == 12,
            // TODO: check precision against n
            (&Value::Fixed(_n, _), &Schema::Decimal { .. }) => true,
            (&Value::String(ref s), &Schema::Enum { ref symbols, .. }) => symbols.contains(s),
            (&Value::Enum(i, ref s), &Schema::Enum { ref symbols, .. }) => symbols
                .get(i as usize)
                .map(|ref symbol| symbol == &s)
                .unwrap_or(false),
            // (&Value::Union(None), &Schema::Union(_)) => true,
            (&Value::Union(ref value), &Schema::Union(ref inner)) => {
                inner.find_schema(value).is_some()
            }
            (&Value::Array(ref items), &Schema::Array(ref inner)) => {
                items.iter().all(|item| item.validate(inner))
            }
            (&Value::Map(ref items), &Schema::Map(ref inner)) => {
                items.iter().all(|(_, value)| value.validate(inner))
            }
            (&Value::Record(ref record_fields), &Schema::Record { ref fields, .. }) => {
                fields.len() == record_fields.len()
                    && fields.iter().zip(record_fields.iter()).all(
                        |(field, &(ref name, ref value))| {
                            field.name == *name && value.validate(&field.schema)
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
    pub fn resolve(mut self, schema: &Schema) -> AvroResult<Self> {
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
            Schema::Decimal {
                scale,
                precision,
                ref inner,
            } => self.resolve_decimal(precision, scale, inner),
            Schema::Date => self.resolve_date(),
            Schema::TimeMillis => self.resolve_time_millis(),
            Schema::TimeMicros => self.resolve_time_micros(),
            Schema::TimestampMillis => self.resolve_timestamp_millis(),
            Schema::TimestampMicros => self.resolve_timestamp_micros(),
            Schema::Duration => self.resolve_duration(),
            Schema::Uuid => self.resolve_uuid(),
        }
    }

    fn resolve_uuid(self) -> AvroResult<Self> {
        match self {
            uuid @ Value::Uuid(_) => Ok(uuid),
            Value::String(ref string) => Ok(Value::Uuid(Uuid::from_str(string)?)),
            other => Err(Error::SchemaResolution(format!(
                "UUID expected, got {:?}",
                other
            ))),
        }
    }

    fn resolve_duration(self) -> AvroResult<Self> {
        match self {
            duration @ Value::Duration { .. } => Ok(duration),
            Value::Fixed(size, bytes) => {
                if size != 12 {
                    return Err(Error::SchemaResolution(format!(
                        "Fixed bytes of size 12 expected, got Fixed of size {}",
                        size
                    )));
                }
                Ok(Value::Duration(Duration::from([
                    bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
                    bytes[8], bytes[9], bytes[10], bytes[11],
                ])))
            }
            other => Err(Error::SchemaResolution(format!(
                "Duration expected, got {:?}",
                other
            ))),
        }
    }

    fn resolve_decimal(
        self,
        precision: Precision,
        scale: Scale,
        inner: &Schema,
    ) -> AvroResult<Self> {
        if scale > precision {
            return Err(Error::SchemaResolution(format!(
                "Scale {} is greater than precision {}",
                scale, precision
            )));
        }
        match inner {
            &Schema::Fixed { size, .. } => {
                if max_prec_for_len(size)? < precision {
                    return Err(Error::SchemaResolution(format!(
                        "Fixed size {} is not large enough to hold decimal values of precision {}",
                        size, precision,
                    )));
                }
            }
            Schema::Bytes => (),
            _ => {
                return Err(Error::SchemaResolution(format!(
                    "Underlying decimal type must be fixed or bytes, got {:?}",
                    inner
                )))
            }
        };
        match self {
            Value::Decimal(num) => {
                let num_bytes = num.len();
                if max_prec_for_len(num_bytes)? > precision {
                    Err(Error::SchemaResolution(format!(
                        "Precision {} too small to hold decimal values with {} bytes",
                        precision, num_bytes,
                    )))
                } else {
                    Ok(Value::Decimal(num))
                }
                // check num.bits() here
            }
            Value::Fixed(_, bytes) | Value::Bytes(bytes) => {
                if max_prec_for_len(bytes.len())? > precision {
                    Err(Error::SchemaResolution(format!(
                        "Precision {} too small to hold decimal values with {} bytes",
                        precision,
                        bytes.len(),
                    )))
                } else {
                    // precision and scale match, can we assume the underlying type can hold the data?
                    Ok(Value::Decimal(Decimal::from(bytes)))
                }
            }
            other => Err(Error::SchemaResolution(format!(
                "Decimal expected, got {:?}",
                other
            ))),
        }
    }

    fn resolve_date(self) -> AvroResult<Self> {
        match self {
            Value::Date(d) | Value::Int(d) => Ok(Value::Date(d)),
            other => Err(Error::SchemaResolution(format!(
                "Date expected, got {:?}",
                other
            ))),
        }
    }

    fn resolve_time_millis(self) -> AvroResult<Self> {
        match self {
            Value::TimeMillis(t) | Value::Int(t) => Ok(Value::TimeMillis(t)),
            other => Err(Error::SchemaResolution(format!(
                "TimeMillis expected, got {:?}",
                other
            ))),
        }
    }

    fn resolve_time_micros(self) -> AvroResult<Self> {
        match self {
            Value::TimeMicros(t) | Value::Long(t) => Ok(Value::TimeMicros(t)),
            Value::Int(t) => Ok(Value::TimeMicros(i64::from(t))),
            other => Err(Error::SchemaResolution(format!(
                "TimeMicros expected, got {:?}",
                other
            ))),
        }
    }

    fn resolve_timestamp_millis(self) -> AvroResult<Self> {
        match self {
            Value::TimestampMillis(ts) | Value::Long(ts) => Ok(Value::TimestampMillis(ts)),
            Value::Int(ts) => Ok(Value::TimestampMillis(i64::from(ts))),
            other => Err(Error::SchemaResolution(format!(
                "TimestampMillis expected, got {:?}",
                other
            ))),
        }
    }

    fn resolve_timestamp_micros(self) -> AvroResult<Self> {
        match self {
            Value::TimestampMicros(ts) | Value::Long(ts) => Ok(Value::TimestampMicros(ts)),
            Value::Int(ts) => Ok(Value::TimestampMicros(i64::from(ts))),
            other => Err(Error::SchemaResolution(format!(
                "TimestampMicros expected, got {:?}",
                other
            ))),
        }
    }

    fn resolve_null(self) -> AvroResult<Self> {
        match self {
            Value::Null => Ok(Value::Null),
            other => Err(Error::SchemaResolution(format!(
                "Null expected, got {:?}",
                other
            ))),
        }
    }

    fn resolve_boolean(self) -> AvroResult<Self> {
        match self {
            Value::Boolean(b) => Ok(Value::Boolean(b)),
            other => Err(Error::SchemaResolution(format!(
                "Boolean expected, got {:?}",
                other
            ))),
        }
    }

    fn resolve_int(self) -> AvroResult<Self> {
        match self {
            Value::Int(n) => Ok(Value::Int(n)),
            Value::Long(n) => Ok(Value::Int(n as i32)),
            other => Err(Error::SchemaResolution(format!(
                "Int expected, got {:?}",
                other
            ))),
        }
    }

    fn resolve_long(self) -> AvroResult<Self> {
        match self {
            Value::Int(n) => Ok(Value::Long(i64::from(n))),
            Value::Long(n) => Ok(Value::Long(n)),
            other => Err(Error::SchemaResolution(format!(
                "Long expected, got {:?}",
                other
            ))),
        }
    }

    fn resolve_float(self) -> AvroResult<Self> {
        match self {
            Value::Int(n) => Ok(Value::Float(n as f32)),
            Value::Long(n) => Ok(Value::Float(n as f32)),
            Value::Float(x) => Ok(Value::Float(x)),
            Value::Double(x) => Ok(Value::Float(x as f32)),
            other => Err(Error::SchemaResolution(format!(
                "Float expected, got {:?}",
                other
            ))),
        }
    }

    fn resolve_double(self) -> AvroResult<Self> {
        match self {
            Value::Int(n) => Ok(Value::Double(f64::from(n))),
            Value::Long(n) => Ok(Value::Double(n as f64)),
            Value::Float(x) => Ok(Value::Double(f64::from(x))),
            Value::Double(x) => Ok(Value::Double(x)),
            other => Err(Error::SchemaResolution(format!(
                "Double expected, got {:?}",
                other
            ))),
        }
    }

    fn resolve_bytes(self) -> AvroResult<Self> {
        match self {
            Value::Bytes(bytes) => Ok(Value::Bytes(bytes)),
            Value::String(s) => Ok(Value::Bytes(s.into_bytes())),
            Value::Array(items) => Ok(Value::Bytes(
                items
                    .into_iter()
                    .map(Value::try_u8)
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            other => Err(Error::SchemaResolution(format!(
                "Bytes expected, got {:?}",
                other
            ))),
        }
    }

    fn resolve_string(self) -> AvroResult<Self> {
        match self {
            Value::String(s) => Ok(Value::String(s)),
            Value::Bytes(bytes) => Ok(Value::String(String::from_utf8(bytes)?)),
            other => Err(Error::SchemaResolution(format!(
                "String expected, got {:?}",
                other
            ))),
        }
    }

    fn resolve_fixed(self, size: usize) -> AvroResult<Self> {
        match self {
            Value::Fixed(n, bytes) => {
                if n == size {
                    Ok(Value::Fixed(n, bytes))
                } else {
                    Err(Error::SchemaResolution(format!(
                        "Fixed size mismatch, {} expected, got {}",
                        size, n
                    )))
                }
            }
            other => Err(Error::SchemaResolution(format!(
                "String expected, got {:?}",
                other
            ))),
        }
    }

    fn resolve_enum(self, symbols: &[String]) -> AvroResult<Self> {
        let validate_symbol = |symbol: String, symbols: &[String]| {
            if let Some(index) = symbols.iter().position(|ref item| item == &&symbol) {
                Ok(Value::Enum(index as i32, symbol))
            } else {
                Err(Error::SchemaResolution(format!(
                    "Enum default {} is not among allowed symbols {:?}",
                    symbol, symbols,
                )))
            }
        };

        match self {
            Value::Enum(i, s) => {
                if i >= 0 && i < symbols.len() as i32 {
                    validate_symbol(s, symbols)
                } else {
                    Err(Error::SchemaResolution(format!(
                        "Enum value {} is out of bound {}",
                        i,
                        symbols.len() as i32
                    )))
                }
            }
            Value::String(s) => validate_symbol(s, symbols),
            other => Err(Error::SchemaResolution(format!(
                "Enum({:?}) expected, got {:?}",
                symbols, other
            ))),
        }
    }

    fn resolve_union(self, schema: &UnionSchema) -> AvroResult<Self> {
        let v = match self {
            // Both are unions case.
            Value::Union(v) => *v,
            // Reader is a union, but writer is not.
            v => v,
        };
        // Find the first match in the reader schema.
        let (_, inner) = schema.find_schema(&v).ok_or_else(|| {
            Error::SchemaResolution("Could not find matching type in union".to_string())
        })?;
        Ok(Value::Union(Box::new(v.resolve(inner)?)))
    }

    fn resolve_array(self, schema: &Schema) -> AvroResult<Self> {
        match self {
            Value::Array(items) => Ok(Value::Array(
                items
                    .into_iter()
                    .map(|item| item.resolve(schema))
                    .collect::<Result<_, _>>()?,
            )),
            other => Err(Error::SchemaResolution(format!(
                "Array({:?}) expected, got {:?}",
                schema, other
            ))),
        }
    }

    fn resolve_map(self, schema: &Schema) -> AvroResult<Self> {
        match self {
            Value::Map(items) => Ok(Value::Map(
                items
                    .into_iter()
                    .map(|(key, value)| value.resolve(schema).map(|value| (key, value)))
                    .collect::<Result<_, _>>()?,
            )),
            other => Err(Error::SchemaResolution(format!(
                "Map({:?}) expected, got {:?}",
                schema, other
            ))),
        }
    }

    fn resolve_record(self, fields: &[RecordField]) -> AvroResult<Self> {
        let mut items = match self {
            Value::Map(items) => Ok(items),
            Value::Record(fields) => Ok(fields.into_iter().collect::<HashMap<_, _>>()),
            other => Err(Error::SchemaResolution(format!(
                "Record({:?}) expected, got {:?}",
                fields, other
            ))),
        }?;

        let new_fields = fields
            .iter()
            .map(|field| {
                let value = match items.remove(&field.name) {
                    Some(value) => value,
                    None => match field.default {
                        Some(ref value) => match field.schema {
                            Schema::Enum { ref symbols, .. } => {
                                Value::from(value.clone()).resolve_enum(symbols)?
                            }
                            Schema::Union(ref union_schema) => {
                                let first = &union_schema.variants()[0];
                                // NOTE: this match exists only to optimize null defaults for large
                                // backward-compatible schemas with many nullable fields
                                match first {
                                    Schema::Null => Value::Union(Box::new(Value::Null)),
                                    _ => Value::Union(Box::new(
                                        Value::from(value.clone()).resolve(first)?,
                                    )),
                                }
                            }
                            _ => Value::from(value.clone()),
                        },
                        None => {
                            return Err(Error::SchemaResolution(format!(
                                "missing field {} in record",
                                field.name
                            )));
                        }
                    },
                };
                value
                    .resolve(&field.schema)
                    .map(|value| (field.name.clone(), value))
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Value::Record(new_fields))
    }

    fn try_u8(self) -> AvroResult<u8> {
        let int = self.resolve(&Schema::Int)?;
        if let Value::Int(n) = int {
            if n >= 0 && n <= i32::from(u8::MAX) {
                return Ok(n as u8);
            }
        }

        Err(Error::SchemaResolution(format!(
            "Unable to convert to u8, got {:?}",
            int
        )))
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        decimal::Decimal,
        duration::{Days, Duration, Millis, Months},
        schema::{Name, RecordField, RecordFieldOrder, Schema, UnionSchema},
        types::Value,
    };
    use uuid::Uuid;

    #[test]
    fn validate() {
        let value_schema_valid = vec![
            (Value::Int(42), Schema::Int, true),
            (Value::Int(42), Schema::Boolean, false),
            (
                Value::Union(Box::new(Value::Null)),
                Schema::Union(UnionSchema::new(vec![Schema::Null, Schema::Int]).unwrap()),
                true,
            ),
            (
                Value::Union(Box::new(Value::Int(42))),
                Schema::Union(UnionSchema::new(vec![Schema::Null, Schema::Int]).unwrap()),
                true,
            ),
            (
                Value::Union(Box::new(Value::Null)),
                Schema::Union(UnionSchema::new(vec![Schema::Double, Schema::Int]).unwrap()),
                false,
            ),
            (
                Value::Union(Box::new(Value::Int(42))),
                Schema::Union(
                    UnionSchema::new(vec![
                        Schema::Null,
                        Schema::Double,
                        Schema::String,
                        Schema::Int,
                    ])
                    .unwrap(),
                ),
                true,
            ),
            (
                Value::Array(vec![Value::Long(42i64)]),
                Schema::Array(Box::new(Schema::Long)),
                true,
            ),
            (
                Value::Array(vec![Value::Boolean(true)]),
                Schema::Array(Box::new(Schema::Long)),
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
        use std::collections::HashMap;
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
            lookup: HashMap::new(),
        };

        assert!(Value::Record(vec![
            ("a".to_string(), Value::Long(42i64)),
            ("b".to_string(), Value::String("foo".to_string())),
        ])
        .validate(&schema));

        assert!(!Value::Record(vec![
            ("b".to_string(), Value::String("foo".to_string())),
            ("a".to_string(), Value::Long(42i64)),
        ])
        .validate(&schema));

        assert!(!Value::Record(vec![
            ("a".to_string(), Value::Boolean(false)),
            ("b".to_string(), Value::String("foo".to_string())),
        ])
        .validate(&schema));

        assert!(!Value::Record(vec![
            ("a".to_string(), Value::Long(42i64)),
            ("c".to_string(), Value::String("foo".to_string())),
        ])
        .validate(&schema));

        assert!(!Value::Record(vec![
            ("a".to_string(), Value::Long(42i64)),
            ("b".to_string(), Value::String("foo".to_string())),
            ("c".to_string(), Value::Null),
        ])
        .validate(&schema));
    }

    #[test]
    fn resolve_bytes_ok() {
        let value = Value::Array(vec![Value::Int(0), Value::Int(42)]);
        assert_eq!(
            value.resolve(&Schema::Bytes).unwrap(),
            Value::Bytes(vec![0u8, 42u8])
        );
    }

    #[test]
    fn resolve_bytes_failure() {
        let value = Value::Array(vec![Value::Int(2000), Value::Int(-42)]);
        assert!(value.resolve(&Schema::Bytes).is_err());
    }

    #[test]
    fn resolve_decimal_bytes() {
        let value = Value::Decimal(Decimal::from(vec![1, 2]));
        value
            .clone()
            .resolve(&Schema::Decimal {
                precision: 10,
                scale: 4,
                inner: Box::new(Schema::Bytes),
            })
            .unwrap();
        assert!(value.resolve(&Schema::String).is_err());
    }

    #[test]
    fn resolve_decimal_invalid_scale() {
        let value = Value::Decimal(Decimal::from(vec![1]));
        assert!(value
            .resolve(&Schema::Decimal {
                precision: 2,
                scale: 3,
                inner: Box::new(Schema::Bytes),
            })
            .is_err());
    }

    #[test]
    fn resolve_decimal_invalid_precision_for_length() {
        let value = Value::Decimal(Decimal::from((1u8..=8u8).rev().collect::<Vec<_>>()));
        assert!(value
            .resolve(&Schema::Decimal {
                precision: 1,
                scale: 0,
                inner: Box::new(Schema::Bytes),
            })
            .is_err());
    }

    #[test]
    fn resolve_decimal_fixed() {
        let value = Value::Decimal(Decimal::from(vec![1, 2]));
        assert!(value
            .clone()
            .resolve(&Schema::Decimal {
                precision: 10,
                scale: 1,
                inner: Box::new(Schema::Fixed {
                    name: Name::new("decimal"),
                    size: 20
                })
            })
            .is_ok());
        assert!(value.resolve(&Schema::String).is_err());
    }

    #[test]
    fn resolve_date() {
        let value = Value::Date(2345);
        assert!(value.clone().resolve(&Schema::Date).is_ok());
        assert!(value.resolve(&Schema::String).is_err());
    }

    #[test]
    fn resolve_time_millis() {
        let value = Value::TimeMillis(10);
        assert!(value.clone().resolve(&Schema::TimeMillis).is_ok());
        assert!(value.resolve(&Schema::TimeMicros).is_err());
    }

    #[test]
    fn resolve_time_micros() {
        let value = Value::TimeMicros(10);
        assert!(value.clone().resolve(&Schema::TimeMicros).is_ok());
        assert!(value.resolve(&Schema::TimeMillis).is_err());
    }

    #[test]
    fn resolve_timestamp_millis() {
        let value = Value::TimestampMillis(10);
        assert!(value.clone().resolve(&Schema::TimestampMillis).is_ok());
        assert!(value.resolve(&Schema::Float).is_err());

        let value = Value::Float(10.0f32);
        assert!(value.resolve(&Schema::TimestampMillis).is_err());
    }

    #[test]
    fn resolve_timestamp_micros() {
        let value = Value::TimestampMicros(10);
        assert!(value.clone().resolve(&Schema::TimestampMicros).is_ok());
        assert!(value.resolve(&Schema::Int).is_err());

        let value = Value::Double(10.0);
        assert!(value.resolve(&Schema::TimestampMicros).is_err());
    }

    #[test]
    fn resolve_duration() {
        let value = Value::Duration(Duration::new(
            Months::new(10),
            Days::new(5),
            Millis::new(3000),
        ));
        assert!(value.clone().resolve(&Schema::Duration).is_ok());
        assert!(value.resolve(&Schema::TimestampMicros).is_err());
        assert!(Value::Long(1i64).resolve(&Schema::Duration).is_err());
    }

    #[test]
    fn resolve_uuid() {
        let value = Value::Uuid(Uuid::parse_str("1481531d-ccc9-46d9-a56f-5b67459c0537").unwrap());
        assert!(value.clone().resolve(&Schema::Uuid).is_ok());
        assert!(value.resolve(&Schema::TimestampMicros).is_err());
    }
}
