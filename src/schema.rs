//! Logic for parsing and interacting with schemas in Avro format.
use std::borrow::Cow;
use std::collections::HashMap;
use std::convert::TryInto;
use std::fmt;

use digest::Digest;
use failure::{Error, Fail};
use serde::{
    ser::{SerializeMap, SerializeSeq},
    Deserialize, Serialize, Serializer,
};
use serde_json::{Map, Value};

use crate::types;
use crate::util::MapHelper;

/// Describes errors happened while parsing Avro schemas.
#[derive(Fail, Debug)]
#[fail(display = "Failed to parse schema: {}", _0)]
pub struct ParseSchemaError(String);

impl ParseSchemaError {
    pub fn new<S>(msg: S) -> ParseSchemaError
    where
        S: Into<String>,
    {
        ParseSchemaError(msg.into())
    }
}

/// Represents an Avro schema fingerprint
/// More information about Avro schema fingerprints can be found in the
/// [Avro Schema Fingerprint documentation](https://avro.apache.org/docs/current/spec.html#schema_fingerprints)
pub struct SchemaFingerprint {
    pub bytes: Vec<u8>,
}

impl fmt::Display for SchemaFingerprint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            self.bytes
                .iter()
                .map(|byte| format!("{:02x}", byte))
                .collect::<Vec<String>>()
                .join("")
        )
    }
}

/// Represents any valid Avro schema
/// More information about Avro schemas can be found in the
/// [Avro Specification](https://avro.apache.org/docs/current/spec.html#schemas)
#[derive(Clone, Debug, strum_macros::EnumDiscriminants)]
#[strum_discriminants(name(SchemaKind), derive(Hash))]
pub enum Schema {
    /// A `null` Avro schema.
    Null,
    /// A `boolean` Avro schema.
    Boolean,
    /// An `int` Avro schema.
    Int,
    /// A `long` Avro schema.
    Long,
    /// A `float` Avro schema.
    Float,
    /// A `double` Avro schema.
    Double,
    /// A `bytes` Avro schema.
    /// `Bytes` represents a sequence of 8-bit unsigned bytes.
    Bytes,
    /// A `string` Avro schema.
    /// `String` represents a unicode character sequence.
    String,
    /// A `array` Avro schema. Avro arrays are required to have the same type for each element.
    /// This variant holds the `Schema` for the array element type.
    Array(Box<Schema>),
    /// A `map` Avro schema.
    /// `Map` holds a pointer to the `Schema` of its values, which must all be the same schema.
    /// `Map` keys are assumed to be `string`.
    Map(Box<Schema>),
    /// A `union` Avro schema.
    Union(UnionSchema),
    /// A `record` Avro schema.
    ///
    /// The `lookup` table maps field names to their position in the `Vec`
    /// of `fields`.
    Record {
        name: Name,
        doc: Documentation,
        fields: Vec<RecordField>,
        lookup: HashMap<String, usize>,
    },
    /// An `enum` Avro schema.
    Enum {
        name: Name,
        doc: Documentation,
        symbols: Vec<String>,
    },
    /// A `fixed` Avro schema.
    Fixed { name: Name, size: usize },
    /// Logical type which represents `Decimal` values. The underlying type is serialized and
    /// deserialized as `Schema::Bytes` or `Schema::Fixed`.
    ///
    /// `scale` defaults to 0 and is an integer greater than or equal to 0 and `precision` is an
    /// integer greater than 0.
    Decimal {
        precision: DecimalMetadata,
        scale: DecimalMetadata,
        inner: Box<Schema>,
    },
    /// A universally unique identifier, annotating a string.
    Uuid,
    /// Logical type which represents the number of days since the unix epoch.
    /// Serialization format is `Schema::Int`.
    Date,
    /// The time of day in number of milliseconds after midnight with no reference any calendar,
    /// time zone or date in particular.
    TimeMillis,
    /// The time of day in number of microseconds after midnight with no reference any calendar,
    /// time zone or date in particular.
    TimeMicros,
    /// An instant in time represented as the number of milliseconds after the UNIX epoch.
    TimestampMillis,
    /// An instant in time represented as the number of microseconds after the UNIX epoch.
    TimestampMicros,
    /// An amount of time defined by a number of months, days and milliseconds.
    Duration,
}

impl PartialEq for Schema {
    /// Assess equality of two `Schema` based on [Parsing Canonical Form].
    ///
    /// [Parsing Canonical Form]:
    /// https://avro.apache.org/docs/1.8.2/spec.html#Parsing+Canonical+Form+for+Schemas
    fn eq(&self, other: &Self) -> bool {
        self.canonical_form() == other.canonical_form()
    }
}

impl<'a> From<&'a types::Value> for SchemaKind {
    fn from(value: &'a types::Value) -> Self {
        use crate::types::Value;
        match value {
            Value::Null => Self::Null,
            Value::Boolean(_) => Self::Boolean,
            Value::Int(_) => Self::Int,
            Value::Long(_) => Self::Long,
            Value::Float(_) => Self::Float,
            Value::Double(_) => Self::Double,
            Value::Bytes(_) => Self::Bytes,
            Value::String(_) => Self::String,
            Value::Array(_) => Self::Array,
            Value::Map(_) => Self::Map,
            Value::Union(_) => Self::Union,
            Value::Record(_) => Self::Record,
            Value::Enum(_, _) => Self::Enum,
            Value::Fixed(_, _) => Self::Fixed,
            Value::Decimal { .. } => Self::Decimal,
            Value::Uuid(_) => Self::Uuid,
            Value::Date(_) => Self::Date,
            Value::TimeMillis(_) => Self::TimeMillis,
            Value::TimeMicros(_) => Self::TimeMicros,
            Value::TimestampMillis(_) => Self::TimestampMillis,
            Value::TimestampMicros(_) => Self::TimestampMicros,
            Value::Duration { .. } => Self::Duration,
        }
    }
}

/// Represents names for `record`, `enum` and `fixed` Avro schemas.
///
/// Each of these `Schema`s have a `fullname` composed of two parts:
///   * a name
///   * a namespace
///
/// `aliases` can also be defined, to facilitate schema evolution.
///
/// More information about schema names can be found in the
/// [Avro specification](https://avro.apache.org/docs/current/spec.html#names)
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct Name {
    pub name: String,
    pub namespace: Option<String>,
    pub aliases: Option<Vec<String>>,
}

/// Represents documentation for complex Avro schemas.
pub type Documentation = Option<String>;

impl Name {
    /// Create a new `Name`.
    /// No `namespace` nor `aliases` will be defined.
    pub fn new(name: &str) -> Name {
        Name {
            name: name.to_owned(),
            namespace: None,
            aliases: None,
        }
    }

    /// Parse a `serde_json::Value` into a `Name`.
    fn parse(complex: &Map<String, Value>) -> Result<Self, Error> {
        let name = complex
            .name()
            .ok_or_else(|| ParseSchemaError::new("No `name` field"))?;

        let namespace = complex.string("namespace");

        let aliases: Option<Vec<String>> = complex
            .get("aliases")
            .and_then(|aliases| aliases.as_array())
            .and_then(|aliases| {
                aliases
                    .iter()
                    .map(|alias| alias.as_str())
                    .map(|alias| alias.map(|a| a.to_string()))
                    .collect::<Option<_>>()
            });

        Ok(Name {
            name,
            namespace,
            aliases,
        })
    }

    /// Return the `fullname` of this `Name`
    ///
    /// More information about fullnames can be found in the
    /// [Avro specification](https://avro.apache.org/docs/current/spec.html#names)
    pub fn fullname(&self, default_namespace: Option<&str>) -> String {
        if self.name.contains('.') {
            self.name.clone()
        } else {
            let namespace = self
                .namespace
                .as_ref()
                .map(|s| s.as_ref())
                .or(default_namespace);

            match namespace {
                Some(ref namespace) => format!("{}.{}", namespace, self.name),
                None => self.name.clone(),
            }
        }
    }
}

/// Represents a `field` in a `record` Avro schema.
#[derive(Clone, Debug, PartialEq)]
pub struct RecordField {
    /// Name of the field.
    pub name: String,
    /// Documentation of the field.
    pub doc: Documentation,
    /// Default value of the field.
    /// This value will be used when reading Avro datum if schema resolution
    /// is enabled.
    pub default: Option<Value>,
    /// Schema of the field.
    pub schema: Schema,
    /// Order of the field.
    ///
    /// **NOTE** This currently has no effect.
    pub order: RecordFieldOrder,
    /// Position of the field in the list of `field` of its parent `Schema`
    pub position: usize,
}

/// Represents any valid order for a `field` in a `record` Avro schema.
#[derive(Clone, Debug, PartialEq)]
pub enum RecordFieldOrder {
    Ascending,
    Descending,
    Ignore,
}

impl RecordField {
    /// Parse a `serde_json::Value` into a `RecordField`.
    fn parse(field: &Map<String, Value>, position: usize) -> Result<Self, Error> {
        let name = field
            .name()
            .ok_or_else(|| ParseSchemaError::new("No `name` in record field"))?;

        // TODO: "type" = "<record name>"
        let schema = Schema::parse_complex(field)?;

        let default = field.get("default").cloned();

        let order = field
            .get("order")
            .and_then(|order| order.as_str())
            .and_then(|order| match order {
                "ascending" => Some(RecordFieldOrder::Ascending),
                "descending" => Some(RecordFieldOrder::Descending),
                "ignore" => Some(RecordFieldOrder::Ignore),
                _ => None,
            })
            .unwrap_or_else(|| RecordFieldOrder::Ascending);

        Ok(RecordField {
            name,
            doc: field.doc(),
            default,
            schema,
            order,
            position,
        })
    }
}

#[derive(Debug, Clone)]
pub struct UnionSchema {
    schemas: Vec<Schema>,
    // Used to ensure uniqueness of schema inputs, and provide constant time finding of the
    // schema index given a value.
    // **NOTE** that this approach does not work for named types, and will have to be modified
    // to support that. A simple solution is to also keep a mapping of the names used.
    variant_index: HashMap<SchemaKind, usize>,
}

impl UnionSchema {
    pub(crate) fn new(schemas: Vec<Schema>) -> Result<Self, Error> {
        let mut vindex = HashMap::new();
        for (i, schema) in schemas.iter().enumerate() {
            if let Schema::Union(_) = schema {
                Err(ParseSchemaError::new(
                    "Unions may not directly contain a union",
                ))?;
            }
            let kind = SchemaKind::from(schema);
            if vindex.insert(kind, i).is_some() {
                Err(ParseSchemaError::new(
                    "Unions cannot contain duplicate types",
                ))?;
            }
        }
        Ok(UnionSchema {
            schemas,
            variant_index: vindex,
        })
    }

    /// Returns a slice to all variants of this schema.
    pub fn variants(&self) -> &[Schema] {
        &self.schemas
    }

    /// Returns true if the first variant of this `UnionSchema` is `Null`.
    pub fn is_nullable(&self) -> bool {
        !self.schemas.is_empty() && self.schemas[0] == Schema::Null
    }

    /// Optionally returns a reference to the schema matched by this value, as well as its position
    /// within this union.
    pub fn find_schema(&self, value: &types::Value) -> Option<(usize, &Schema)> {
        self.variant_index
            .get(&SchemaKind::from(value))
            .cloned()
            .map(|i| (i, &self.schemas[i]))
    }
}

// No need to compare variant_index, it is derivative of schemas.
impl PartialEq for UnionSchema {
    fn eq(&self, other: &UnionSchema) -> bool {
        self.schemas.eq(&other.schemas)
    }
}

type DecimalMetadata = usize;
pub(crate) type Precision = DecimalMetadata;
pub(crate) type Scale = DecimalMetadata;

fn parse_json_integer_for_decimal(value: &serde_json::Number) -> Result<DecimalMetadata, Error> {
    if value.is_u64() {
        return Ok(value
            .as_u64()
            .ok_or_else(|| {
                ParseSchemaError::new(format!(
                    "JSON value {} claims to be u64 but cannot be converted",
                    value
                ))
            })?
            .try_into()?);
    }

    if value.is_i64() {
        return Ok(value
            .as_i64()
            .ok_or_else(|| {
                ParseSchemaError::new(format!(
                    "JSON value {} claims to be i64 but cannot be converted",
                    value
                ))
            })?
            .try_into()?);
    }
    return Err(ParseSchemaError::new(format!(
        "Invalid JSON value for decimal precision/scale integer: {}",
        value
    )))?;
}

impl Schema {
    /// Create a `Schema` from a string representing a JSON Avro schema.
    pub fn parse_str(input: &str) -> Result<Self, Error> {
        // TODO: (#82) this should be a ParseSchemaError wrapping the JSON error
        let value = serde_json::from_str(input)?;
        Self::parse(&value)
    }

    /// Create a `Schema` from a `serde_json::Value` representing a JSON Avro
    /// schema.
    pub fn parse(value: &Value) -> Result<Self, Error> {
        match *value {
            Value::String(ref t) => Schema::parse_primitive(t.as_str()),
            Value::Object(ref data) => Schema::parse_complex(data),
            Value::Array(ref data) => Schema::parse_union(data),
            _ => Err(ParseSchemaError::new("Must be a JSON string, object or array").into()),
        }
    }

    /// Converts `self` into its [Parsing Canonical Form].
    ///
    /// [Parsing Canonical Form]:
    /// https://avro.apache.org/docs/1.8.2/spec.html#Parsing+Canonical+Form+for+Schemas
    pub fn canonical_form(&self) -> String {
        let json = serde_json::to_value(self).unwrap();
        parsing_canonical_form(&json)
    }

    /// Generate [fingerprint] of Schema's [Parsing Canonical Form].
    ///
    /// [Parsing Canonical Form]:
    /// https://avro.apache.org/docs/1.8.2/spec.html#Parsing+Canonical+Form+for+Schemas
    /// [fingerprint]:
    /// https://avro.apache.org/docs/current/spec.html#schema_fingerprints
    pub fn fingerprint<D: Digest>(&self) -> SchemaFingerprint {
        let mut d = D::new();
        d.input(self.canonical_form());
        SchemaFingerprint {
            bytes: d.result().to_vec(),
        }
    }

    /// Parse a `serde_json::Value` representing a primitive Avro type into a
    /// `Schema`.
    fn parse_primitive(primitive: &str) -> Result<Self, Error> {
        match primitive {
            "null" => Ok(Schema::Null),
            "boolean" => Ok(Schema::Boolean),
            "int" => Ok(Schema::Int),
            "long" => Ok(Schema::Long),
            "double" => Ok(Schema::Double),
            "float" => Ok(Schema::Float),
            "bytes" => Ok(Schema::Bytes),
            "string" => Ok(Schema::String),
            other => Err(ParseSchemaError::new(format!("Unknown type: {}", other)).into()),
        }
    }

    fn parse_precision_and_scale(
        complex: &Map<String, Value>,
    ) -> Result<(Precision, Scale), Error> {
        fn get_decimal_integer(
            complex: &Map<String, Value>,
            key: &str,
        ) -> Result<DecimalMetadata, Error> {
            match complex.get(key) {
                Some(&Value::Number(ref value)) => parse_json_integer_for_decimal(value),
                None => {
                    return Err(ParseSchemaError::new(format!(
                        "{} missing for decimal type",
                        key
                    )))?
                }
                precision => {
                    return Err(ParseSchemaError::new(format!(
                        "invalid JSON for {}: {:?}",
                        key, precision,
                    )))?
                }
            }
        }
        let precision = get_decimal_integer(complex, "precision")?;
        let scale = get_decimal_integer(complex, "scale")?;
        Ok((precision, scale))
    }

    /// Parse a `serde_json::Value` representing a complex Avro type into a
    /// `Schema`.
    ///
    /// Avro supports "recursive" definition of types.
    /// e.g: {"type": {"type": "string"}}
    fn parse_complex(complex: &Map<String, Value>) -> Result<Self, Error> {
        fn logical_verify_type(
            complex: &Map<String, Value>,
            kinds: &[SchemaKind],
        ) -> Result<Schema, Error> {
            match complex.get("type") {
                Some(value) => {
                    let ty = Schema::parse(value)?;
                    if kinds
                        .iter()
                        .any(|&kind| SchemaKind::from(ty.clone()) == kind)
                    {
                        Ok(ty)
                    } else {
                        Err(ParseSchemaError::new(format!(
                            "Unexpected `type` ({}) variant for `logicalType`",
                            value
                        )))?
                    }
                }
                None => Err(ParseSchemaError::new(
                    "No `type` field found for `logicalType`",
                ))?,
            }
        }
        match complex.get("logicalType") {
            Some(&Value::String(ref t)) => match t.as_str() {
                "decimal" => {
                    let inner = Box::new(logical_verify_type(
                        complex,
                        &[SchemaKind::Fixed, SchemaKind::Bytes],
                    )?);

                    let (precision, scale) = Self::parse_precision_and_scale(complex)?;

                    return Ok(Schema::Decimal {
                        precision,
                        scale,
                        inner,
                    });
                }
                "uuid" => {
                    logical_verify_type(complex, &[SchemaKind::String])?;
                    return Ok(Schema::Uuid);
                }
                "date" => {
                    logical_verify_type(complex, &[SchemaKind::Int])?;
                    return Ok(Schema::Date);
                }
                "time-millis" => {
                    logical_verify_type(complex, &[SchemaKind::Int])?;
                    return Ok(Schema::TimeMillis);
                }
                "time-micros" => {
                    logical_verify_type(complex, &[SchemaKind::Long])?;
                    return Ok(Schema::TimeMicros);
                }
                "timestamp-millis" => {
                    logical_verify_type(complex, &[SchemaKind::Long])?;
                    return Ok(Schema::TimestampMillis);
                }
                "timestamp-micros" => {
                    logical_verify_type(complex, &[SchemaKind::Long])?;
                    return Ok(Schema::TimestampMicros);
                }
                "duration" => {
                    logical_verify_type(complex, &[SchemaKind::Fixed])?;
                    return Ok(Schema::Duration);
                }
                // In this case, of an unknown logical type, we just pass through to the underlying
                // type.
                _ => {}
            },
            // The spec says to ignore invalid logical types and just continue through to the
            // underlying type - It is unclear whether that applies to this case or not, where the
            // `logicalType` is not a string.
            Some(_) => Err(ParseSchemaError::new("logicalType must be a string"))?,
            _ => {}
        }
        match complex.get("type") {
            Some(&Value::String(ref t)) => match t.as_str() {
                "record" => Schema::parse_record(complex),
                "enum" => Schema::parse_enum(complex),
                "array" => Schema::parse_array(complex),
                "map" => Schema::parse_map(complex),
                "fixed" => Schema::parse_fixed(complex),
                other => Schema::parse_primitive(other),
            },
            Some(&Value::Object(ref data)) => Schema::parse_complex(data),
            Some(&Value::Array(ref variants)) => Schema::parse_union(variants),
            Some(unknown) => {
                Err(ParseSchemaError::new(format!("Unknown complex type: {0:?}", unknown)).into())
            }
            None => Err(ParseSchemaError::new("No `type` in complex type").into()),
        }
    }

    /// Parse a `serde_json::Value` representing a Avro record type into a
    /// `Schema`.
    fn parse_record(complex: &Map<String, Value>) -> Result<Self, Error> {
        let name = Name::parse(complex)?;

        let mut lookup = HashMap::new();

        let fields: Vec<RecordField> = complex
            .get("fields")
            .and_then(|fields| fields.as_array())
            .ok_or_else(|| ParseSchemaError::new("No `fields` in record").into())
            .and_then(|fields| {
                fields
                    .iter()
                    .filter_map(|field| field.as_object())
                    .enumerate()
                    .map(|(position, field)| RecordField::parse(field, position))
                    .collect::<Result<_, _>>()
            })?;

        for field in &fields {
            lookup.insert(field.name.clone(), field.position);
        }

        Ok(Schema::Record {
            name,
            doc: complex.doc(),
            fields,
            lookup,
        })
    }

    /// Parse a `serde_json::Value` representing a Avro enum type into a
    /// `Schema`.
    fn parse_enum(complex: &Map<String, Value>) -> Result<Self, Error> {
        let name = Name::parse(complex)?;

        let symbols = complex
            .get("symbols")
            .and_then(|v| v.as_array())
            .ok_or_else(|| ParseSchemaError::new("No `symbols` field in enum"))
            .and_then(|symbols| {
                symbols
                    .iter()
                    .map(|symbol| symbol.as_str().map(|s| s.to_string()))
                    .collect::<Option<_>>()
                    .ok_or_else(|| ParseSchemaError::new("Unable to parse `symbols` in enum"))
            })?;

        Ok(Schema::Enum {
            name,
            doc: complex.doc(),
            symbols,
        })
    }

    /// Parse a `serde_json::Value` representing a Avro array type into a
    /// `Schema`.
    fn parse_array(complex: &Map<String, Value>) -> Result<Self, Error> {
        complex
            .get("items")
            .ok_or_else(|| ParseSchemaError::new("No `items` in array").into())
            .and_then(|items| Schema::parse(items))
            .map(|schema| Schema::Array(Box::new(schema)))
    }

    /// Parse a `serde_json::Value` representing a Avro map type into a
    /// `Schema`.
    fn parse_map(complex: &Map<String, Value>) -> Result<Self, Error> {
        complex
            .get("values")
            .ok_or_else(|| ParseSchemaError::new("No `values` in map").into())
            .and_then(|items| Schema::parse(items))
            .map(|schema| Schema::Map(Box::new(schema)))
    }

    /// Parse a `serde_json::Value` representing a Avro union type into a
    /// `Schema`.
    fn parse_union(items: &[Value]) -> Result<Self, Error> {
        items
            .iter()
            .map(Schema::parse)
            .collect::<Result<Vec<_>, _>>()
            .and_then(|schemas| Ok(Schema::Union(UnionSchema::new(schemas)?)))
    }

    /// Parse a `serde_json::Value` representing a Avro fixed type into a
    /// `Schema`.
    fn parse_fixed(complex: &Map<String, Value>) -> Result<Self, Error> {
        let name = Name::parse(complex)?;

        let size = complex
            .get("size")
            .and_then(|v| v.as_i64())
            .ok_or_else(|| ParseSchemaError::new("No `size` in fixed"))?;

        Ok(Schema::Fixed {
            name,
            size: size as usize,
        })
    }
}

impl Serialize for Schema {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match *self {
            Schema::Null => serializer.serialize_str("null"),
            Schema::Boolean => serializer.serialize_str("boolean"),
            Schema::Int => serializer.serialize_str("int"),
            Schema::Long => serializer.serialize_str("long"),
            Schema::Float => serializer.serialize_str("float"),
            Schema::Double => serializer.serialize_str("double"),
            Schema::Bytes => serializer.serialize_str("bytes"),
            Schema::String => serializer.serialize_str("string"),
            Schema::Array(ref inner) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("type", "array")?;
                map.serialize_entry("items", &*inner.clone())?;
                map.end()
            }
            Schema::Map(ref inner) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("type", "map")?;
                map.serialize_entry("values", &*inner.clone())?;
                map.end()
            }
            Schema::Union(ref inner) => {
                let variants = inner.variants();
                let mut seq = serializer.serialize_seq(Some(variants.len()))?;
                for v in variants {
                    seq.serialize_element(v)?;
                }
                seq.end()
            }
            Schema::Record {
                ref name,
                ref doc,
                ref fields,
                ..
            } => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "record")?;
                if let Some(ref n) = name.namespace {
                    map.serialize_entry("namespace", n)?;
                }
                map.serialize_entry("name", &name.name)?;
                if let Some(ref docstr) = doc {
                    map.serialize_entry("doc", docstr)?;
                }
                if let Some(ref aliases) = name.aliases {
                    map.serialize_entry("aliases", aliases)?;
                }
                map.serialize_entry("fields", fields)?;
                map.end()
            }
            Schema::Enum {
                ref name,
                ref symbols,
                ..
            } => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "enum")?;
                map.serialize_entry("name", &name.name)?;
                map.serialize_entry("symbols", symbols)?;
                map.end()
            }
            Schema::Fixed { ref name, ref size } => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "fixed")?;
                map.serialize_entry("name", &name.name)?;
                map.serialize_entry("size", size)?;
                map.end()
            }
            Schema::Decimal {
                ref scale,
                ref precision,
                ref inner,
            } => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", &*inner.clone())?;
                map.serialize_entry("logicalType", "decimal")?;
                map.serialize_entry("scale", scale)?;
                map.serialize_entry("precision", precision)?;
                map.end()
            }
            Schema::Uuid => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "string")?;
                map.serialize_entry("logicalType", "uuid")?;
                map.end()
            }
            Schema::Date => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "int")?;
                map.serialize_entry("logicalType", "date")?;
                map.end()
            }
            Schema::TimeMillis => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "int")?;
                map.serialize_entry("logicalType", "time-millis")?;
                map.end()
            }
            Schema::TimeMicros => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "long")?;
                map.serialize_entry("logicalType", "time-micros")?;
                map.end()
            }
            Schema::TimestampMillis => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "long")?;
                map.serialize_entry("logicalType", "timestamp-millis")?;
                map.end()
            }
            Schema::TimestampMicros => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "long")?;
                map.serialize_entry("logicalType", "timestamp-micros")?;
                map.end()
            }
            Schema::Duration => {
                let mut map = serializer.serialize_map(None)?;

                // the Avro doesn't indicate what the name of the underlying fixed type of a
                // duration should be or typically is.
                let inner = Schema::Fixed {
                    name: Name::new("duration"),
                    size: 12,
                };
                map.serialize_entry("type", &inner)?;
                map.serialize_entry("logicalType", "duration")?;
                map.end()
            }
        }
    }
}

impl Serialize for RecordField {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("name", &self.name)?;
        map.serialize_entry("type", &self.schema)?;

        if let Some(ref default) = self.default {
            map.serialize_entry("default", default)?;
        }

        map.end()
    }
}

/// Parses a **valid** avro schema into the Parsing Canonical Form.
/// https://avro.apache.org/docs/1.8.2/spec.html#Parsing+Canonical+Form+for+Schemas
fn parsing_canonical_form(schema: &serde_json::Value) -> String {
    match schema {
        serde_json::Value::Object(map) => pcf_map(map),
        serde_json::Value::String(s) => pcf_string(s),
        serde_json::Value::Array(v) => pcf_array(v),
        _ => unreachable!(),
    }
}

fn pcf_map(schema: &Map<String, serde_json::Value>) -> String {
    // Look for the namespace variant up front.
    let ns = schema.get("namespace").and_then(|v| v.as_str());
    let mut fields = Vec::new();
    for (k, v) in schema {
        // Reduce primitive types to their simple form. ([PRIMITIVE] rule)
        if schema.len() == 1 && k == "type" {
            // Invariant: function is only callable from a valid schema, so this is acceptable.
            if let serde_json::Value::String(s) = v {
                return pcf_string(s);
            }
        }

        // Strip out unused fields ([STRIP] rule)
        if field_ordering_position(k).is_none() {
            continue;
        }

        // Fully qualify the name, if it isn't already ([FULLNAMES] rule).
        if k == "name" {
            // Invariant: Only valid schemas. Must be a string.
            let name = v.as_str().unwrap();
            let n = match ns {
                Some(namespace) if !name.contains('.') => {
                    Cow::Owned(format!("{}.{}", namespace, name))
                }
                _ => Cow::Borrowed(name),
            };

            fields.push((k, format!("{}:{}", pcf_string(k), pcf_string(&*n))));
            continue;
        }

        // Strip off quotes surrounding "size" type, if they exist ([INTEGERS] rule).
        if k == "size" {
            let i = match v.as_str() {
                Some(s) => s.parse::<i64>().expect("Only valid schemas are accepted!"),
                None => v.as_i64().unwrap(),
            };
            fields.push((k, format!("{}:{}", pcf_string(k), i)));
            continue;
        }

        // For anything else, recursively process the result.
        fields.push((
            k,
            format!("{}:{}", pcf_string(k), parsing_canonical_form(v)),
        ));
    }

    // Sort the fields by their canonical ordering ([ORDER] rule).
    fields.sort_unstable_by_key(|(k, _)| field_ordering_position(k).unwrap());
    let inter = fields
        .into_iter()
        .map(|(_, v)| v)
        .collect::<Vec<_>>()
        .join(",");
    format!("{{{}}}", inter)
}

fn pcf_array(arr: &[serde_json::Value]) -> String {
    let inter = arr
        .iter()
        .map(parsing_canonical_form)
        .collect::<Vec<String>>()
        .join(",");
    format!("[{}]", inter)
}

fn pcf_string(s: &str) -> String {
    format!("\"{}\"", s)
}

// Used to define the ordering and inclusion of fields.
fn field_ordering_position(field: &str) -> Option<usize> {
    let v = match field {
        "name" => 1,
        "type" => 2,
        "fields" => 3,
        "symbols" => 4,
        "items" => 5,
        "values" => 6,
        "size" => 7,
        _ => return None,
    };

    Some(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_invalid_schema() {
        assert!(Schema::parse_str("invalid").is_err());
    }

    #[test]
    fn test_primitive_schema() {
        assert_eq!(Schema::Null, Schema::parse_str("\"null\"").unwrap());
        assert_eq!(Schema::Int, Schema::parse_str("\"int\"").unwrap());
        assert_eq!(Schema::Double, Schema::parse_str("\"double\"").unwrap());
    }

    #[test]
    fn test_array_schema() {
        let schema = Schema::parse_str(r#"{"type": "array", "items": "string"}"#).unwrap();
        assert_eq!(Schema::Array(Box::new(Schema::String)), schema);
    }

    #[test]
    fn test_map_schema() {
        let schema = Schema::parse_str(r#"{"type": "map", "values": "double"}"#).unwrap();
        assert_eq!(Schema::Map(Box::new(Schema::Double)), schema);
    }

    #[test]
    fn test_union_schema() {
        let schema = Schema::parse_str(r#"["null", "int"]"#).unwrap();
        assert_eq!(
            Schema::Union(UnionSchema::new(vec![Schema::Null, Schema::Int]).unwrap()),
            schema
        );
    }

    #[test]
    fn test_union_unsupported_schema() {
        let schema = Schema::parse_str(r#"["null", ["null", "int"], "string"]"#);
        assert!(schema.is_err());
    }

    #[test]
    fn test_multi_union_schema() {
        let schema = Schema::parse_str(r#"["null", "int", "float", "string", "bytes"]"#);
        assert!(schema.is_ok());
        let schema = schema.unwrap();
        assert_eq!(SchemaKind::from(&schema), SchemaKind::Union);
        let union_schema = match schema {
            Schema::Union(u) => u,
            _ => unreachable!(),
        };
        assert_eq!(union_schema.variants().len(), 5);
        let mut variants = union_schema.variants().iter();
        assert_eq!(SchemaKind::from(variants.next().unwrap()), SchemaKind::Null);
        assert_eq!(SchemaKind::from(variants.next().unwrap()), SchemaKind::Int);
        assert_eq!(
            SchemaKind::from(variants.next().unwrap()),
            SchemaKind::Float
        );
        assert_eq!(
            SchemaKind::from(variants.next().unwrap()),
            SchemaKind::String
        );
        assert_eq!(
            SchemaKind::from(variants.next().unwrap()),
            SchemaKind::Bytes
        );
        assert_eq!(variants.next(), None);
    }

    #[test]
    fn test_record_schema() {
        let schema = Schema::parse_str(
            r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "a", "type": "long", "default": 42},
                    {"name": "b", "type": "string"}
                ]
            }
        "#,
        )
        .unwrap();

        let mut lookup = HashMap::new();
        lookup.insert("a".to_owned(), 0);
        lookup.insert("b".to_owned(), 1);

        let expected = Schema::Record {
            name: Name::new("test"),
            doc: None,
            fields: vec![
                RecordField {
                    name: "a".to_string(),
                    doc: None,
                    default: Some(Value::Number(42i64.into())),
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
            lookup,
        };

        assert_eq!(expected, schema);
    }

    #[test]
    fn test_enum_schema() {
        let schema = Schema::parse_str(
            r#"{"type": "enum", "name": "Suit", "symbols": ["diamonds", "spades", "clubs", "hearts"]}"#,
        ).unwrap();

        let expected = Schema::Enum {
            name: Name::new("Suit"),
            doc: None,
            symbols: vec![
                "diamonds".to_owned(),
                "spades".to_owned(),
                "clubs".to_owned(),
                "hearts".to_owned(),
            ],
        };

        assert_eq!(expected, schema);
    }

    #[test]
    fn test_fixed_schema() {
        let schema = Schema::parse_str(r#"{"type": "fixed", "name": "test", "size": 16}"#).unwrap();

        let expected = Schema::Fixed {
            name: Name::new("test"),
            size: 16usize,
        };

        assert_eq!(expected, schema);
    }

    #[test]
    fn test_no_documentation() {
        let schema =
            Schema::parse_str(r#"{"type": "enum", "name": "Coin", "symbols": ["heads", "tails"]}"#)
                .unwrap();

        let doc = match schema {
            Schema::Enum { doc, .. } => doc,
            _ => return assert!(false),
        };

        assert!(doc.is_none());
    }

    #[test]
    fn test_documentation() {
        let schema = Schema::parse_str(
            r#"{"type": "enum", "name": "Coin", "doc": "Some documentation", "symbols": ["heads", "tails"]}"#
        ).unwrap();

        let doc = match schema {
            Schema::Enum { doc, .. } => doc,
            _ => None,
        };

        assert_eq!("Some documentation".to_owned(), doc.unwrap());
    }

    // Tests to ensure Schema is Send + Sync. These tests don't need to _do_ anything, if they can
    // compile, they pass.
    #[test]
    fn test_schema_is_send() {
        fn send<S: Send>(_s: S) {}

        let schema = Schema::Null;
        send(schema);
    }

    #[test]
    fn test_schema_is_sync() {
        fn sync<S: Sync>(_s: S) {}

        let schema = Schema::Null;
        sync(&schema);
        sync(schema);
    }

    #[test]
    fn test_schema_fingerprint() {
        use md5::Md5;
        use sha2::Sha256;

        let raw_schema = r#"
    {
        "type": "record",
        "name": "test",
        "fields": [
            {"name": "a", "type": "long", "default": 42},
            {"name": "b", "type": "string"},
            {"name": "c", "type": "long", "logicalType": "timestamp-micros"}
        ]
    }
"#;

        let schema = Schema::parse_str(raw_schema).unwrap();
        assert_eq!(
            "7eb3b28d73dfc99bdd9af1848298b40804a2f8ad5d2642be2ecc2ad34842b987",
            format!("{}", schema.fingerprint::<Sha256>())
        );

        assert_eq!(
            "cb11615e412ee5d872620d8df78ff6ae",
            format!("{}", schema.fingerprint::<Md5>())
        );
    }

    #[test]
    fn test_logical_types() {
        let schema = Schema::parse_str(r#"{"type": "int", "logicalType": "date"}"#).unwrap();
        assert_eq!(schema, Schema::Date);

        let schema =
            Schema::parse_str(r#"{"type": "long", "logicalType": "timestamp-micros"}"#).unwrap();
        assert_eq!(schema, Schema::TimestampMicros);
    }
}
