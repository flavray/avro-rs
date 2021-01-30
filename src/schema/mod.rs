//! Logic for parsing and interacting with schemas in Avro format.
pub mod builder;
mod data;
mod parse;
mod pcf;
mod serialize;

use crate::{
    error::Error,
    schema::{
        builder::SchemaBuilder,
        data::{RecordFieldData, SchemaData},
        parse::SchemaParser,
    },
    types,
    util::MapHelper,
};
use digest::Digest;
use once_cell::sync::OnceCell;
use serde_json::{self, Map, Value};
use std::{
    borrow::Cow,
    cell::RefCell,
    collections::{HashMap, HashSet},
    convert::TryInto,
    fmt,
    rc::Rc,
};
use string_interner::{DefaultStringInterner, Sym as RawNameSym};
use wyhash::{self, WyHash};

/// Trait to provide hash implementations for HashMaps
#[derive(Clone, Default)]
pub(crate) struct WyHashBuilder;

impl std::hash::BuildHasher for WyHashBuilder {
    type Hasher = WyHash;

    fn build_hasher(&self) -> Self::Hasher {
        WyHash::default()
    }
}

/// Simple type to reduce noise in definitions
type JsonMap = Map<String, Value>;

/// Representation of an avro schema
#[derive(Clone)]
pub struct Schema {
    namespace_names: DefaultStringInterner,
    type_names: DefaultStringInterner,
    aliases: HashMap<NameRef, NameRef, WyHashBuilder>,
    reverse_aliases: HashMap<NameRef, Vec<NameRef>, WyHashBuilder>,
    types: HashMap<NameRef, SchemaData, WyHashBuilder>,
    root: NameRef,
}

impl Schema {
    /// Parse the schema from a serde json value
    pub fn parse(value: &Value) -> Result<Schema, Error> {
        SchemaParser::parse(value).map_err(|e| e.into())
    }

    /// Parse the schema from a string
    pub fn parse_str(raw: &str) -> Result<Schema, Error> {
        Self::parse_slice(raw.as_bytes())
    }

    /// Parse the schema from a slice
    pub fn parse_slice<'a>(raw: &'a [u8]) -> Result<Schema, Error> {
        let value = serde_json::from_slice(raw).map_err(|e| Error::ParseSchemaJson(e))?;
        Self::parse(&value).map_err(|e| e.into())
    }

    /// Create a builder for the construction of a schema
    pub fn builder() -> SchemaBuilder {
        SchemaBuilder::new()
    }

    /// Get the root element for the schema, this is the element that defines the current schema
    pub fn root(&self) -> SchemaType<'_> {
        self.lookup(self.root).bind(self, self.root)
    }

    /// Converts `self` into its [Parsing Canonical Form].
    ///
    /// [Parsing Canonical Form]:
    /// https://avro.apache.org/docs/1.8.2/spec.html#Parsing+Canonical+Form+for+Schemas
    pub fn canonical_form(&self) -> String {
        let json = serde_json::to_value(self).unwrap();
        pcf::parsing_canonical_form(&json)
    }

    /// Generate [fingerprint] of Schema's [Parsing Canonical Form].
    ///
    /// [Parsing Canonical Form]:
    /// https://avro.apache.org/docs/1.8.2/spec.html#Parsing+Canonical+Form+for+Schemas
    /// [fingerprint]:
    /// https://avro.apache.org/docs/current/spec.html#schema_fingerprints
    pub fn fingerprint<D: Digest>(&self) -> SchemaFingerprint {
        let mut d = D::new();
        d.update(self.canonical_form());
        SchemaFingerprint {
            bytes: d.finalize().to_vec(),
        }
    }

    /// Get the metadata schema used in parsing
    pub(crate) fn meta_schema() -> Schema {
        static META_SCHEMA: OnceCell<Schema> = OnceCell::new();
        META_SCHEMA
            .get_or_init(|| {
                let mut builder = Schema::builder();
                let root = builder
                    .map()
                    .values(builder.bytes(), &mut builder)
                    .expect("Unable to create meta schema");
                builder.build(root).expect("Unable to create meta schema")
            })
            .clone()
    }

    #[inline]
    fn lookup(&self, name: NameRef) -> &SchemaData {
        let name = self.resolve_nameref(name);
        self.types.get(&name).unwrap()
    }

    #[inline]
    fn name(&self, name: NameRef) -> &str {
        self.type_names.resolve(name.name).unwrap()
    }

    #[inline]
    fn namespace(&self, name: NameRef) -> Option<&str> {
        name.namespace
            .map(|ns_ref| self.namespace_names.resolve(ns_ref).unwrap())
    }

    #[inline]
    fn aliases(&self, name: NameRef) -> Option<impl Iterator<Item = Name<'_>>> {
        self.reverse_aliases
            .get(&name)
            .map(|aliases| aliases.iter().map(move |x| Name(self, *x)))
    }

    #[inline]
    fn resolve_nameref(&self, mut name: NameRef) -> NameRef {
        while let Some(aliased) = self.aliases.get(&name) {
            name = *aliased;
        }
        name
    }

    #[inline]
    fn canonical_name(&self, name: NameRef) -> Name<'_> {
        Name(self, self.resolve_nameref(name))
    }

    /// Return true if the name is anonymous, that is starts with `$`
    ///
    /// *NOTE* Anonymous names are illegal in actual Avro, and are only used internally to
    /// implement the schema with fairly generic parsing semnatics, as such this is private to
    /// schema, and should not be exposed externally from this module
    fn is_anonymous(&self, name: NameRef) -> bool {
        self.name(name).starts_with("$")
    }
}

impl PartialEq for Schema {
    fn eq(&self, other: &Schema) -> bool {
        serde_json::to_value(self).unwrap() == serde_json::to_value(other).unwrap()
    }
}

impl fmt::Display for Schema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let repr = serde_json::to_string_pretty(self).map_err(|_e| std::fmt::Error)?;
        f.write_str(&repr)
    }
}

impl fmt::Debug for Schema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("schema")
            .field("root", &self.root())
            .finish()
    }
}

/// A token referencing a type in an avro schema, this type is opaque
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
pub struct NameRef {
    name: RawNameSym,
    namespace: Option<RawNameSym>,
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
#[derive(Copy, Clone)]
pub enum SchemaType<'schema> {
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
    Array(Aggregate<'schema>),
    /// A `map` Avro schema.
    /// `Map` holds a pointer to the `Schema` of its values, which must all be the same schema.
    /// `Map` keys are assumed to be `string`.
    Map(Aggregate<'schema>),
    /// A `union` Avro schema.
    Union(UnionSchema<'schema>),
    /// A `record` Avro schema.
    ///
    /// The `lookup` table maps field names to their position in the `Vec`
    /// of `fields`.
    Record(RecordSchema<'schema>),
    /// An `enum` Avro schema.
    Enum(EnumSchema<'schema>),
    /// A `fixed` Avro schema.
    // Fixed { name: Name<'schema>, size: usize },
    Fixed(FixedSchema<'schema>),
    /// Logical type which represents `Decimal` values. The underlying type is serialized and
    /// deserialized as `Schema::Bytes` or `Schema::Fixed`.
    ///
    /// `scale` defaults to 0 and is an integer greater than or equal to 0 and `precision` is an
    /// integer greater than 0.
    Decimal(DecimalSchema<'schema>),
    // A universally unique identifier, annotating a string.
    Uuid,
    // /// Logical type which represents the number of days since the unix epoch.
    // /// Serialization format is `Schema::Int`.
    Date,
    // /// The time of day in number of milliseconds after midnight with no reference any calendar,
    // /// time zone or date in particular.
    TimeMillis,
    // /// The time of day in number of microseconds after midnight with no reference any calendar,
    // /// time zone or date in particular.
    TimeMicros,
    // /// An instant in time represented as the number of milliseconds after the UNIX epoch.
    TimestampMillis,
    // /// An instant in time represented as the number of microseconds after the UNIX epoch.
    TimestampMicros,
    // An amount of time defined by a number of months, days and milliseconds.
    Duration,
}

impl<'schema> PartialEq<SchemaType<'_>> for SchemaType<'schema> {
    fn eq(&self, other: &SchemaType<'_>) -> bool {
        let us = serde_json::to_value(OnceSchemaCell::new(*self)).unwrap();
        let other = serde_json::to_value(OnceSchemaCell::new(*other)).unwrap();

        us == other
    }
}

impl fmt::Display for SchemaType<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let cell = OnceSchemaCell::new(*self);
        let repr = serde_json::to_string_pretty(&cell).map_err(|_e| std::fmt::Error)?;
        f.write_str(&repr)
    }
}

type DecimalMetadata = usize;
pub(crate) type Precision = DecimalMetadata;
pub(crate) type Scale = DecimalMetadata;

// TODO: Remove this function?
#[allow(dead_code)]
fn parse_json_integer_for_decimal(value: &serde_json::Number) -> Result<DecimalMetadata, Error> {
    Ok(if value.is_u64() {
        let num = value
            .as_u64()
            .ok_or_else(|| Error::GetU64FromJson(value.clone()))?;
        num.try_into()
            .map_err(|e| Error::ConvertU64ToUsize(e, num))?
    } else if value.is_i64() {
        let num = value
            .as_i64()
            .ok_or_else(|| Error::GetI64FromJson(value.clone()))?;
        num.try_into()
            .map_err(|e| Error::ConvertI64ToUsize(e, num))?
    } else {
        return Err(Error::GetPrecisionOrScaleFromJson(value.clone()));
    })
}

// replaced with derive
impl fmt::Debug for SchemaType<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SchemaType::Null => write!(f, "null"),
            SchemaType::Boolean => write!(f, "boolean"),
            SchemaType::Int => write!(f, "int"),
            SchemaType::Long => write!(f, "long"),
            SchemaType::Float => write!(f, "float"),
            SchemaType::Double => write!(f, "double"),
            SchemaType::Bytes => write!(f, "bytes"),
            SchemaType::String => write!(f, "string"),
            SchemaType::Array(agg) => f
                .debug_struct("Array")
                .field("name", &agg.name())
                .field("value", &agg.items())
                .finish(),
            SchemaType::Map(agg) => f
                .debug_struct("Map")
                .field("name", &agg.name())
                // .field("value", &agg.items())
                .finish(),
            SchemaType::Union(_union) => f
                .debug_struct("Union")
                // .field("variants", &&union_.variants())
                .finish(),
            SchemaType::Record(record) => f
                .debug_struct("Record")
                .field("name", &record.name())
                .field("doc", &record.doc())
                .field("fields", &record.fields())
                .finish(),
            SchemaType::Enum(enum_) => f
                .debug_struct("Enum")
                .field("name", &enum_.name())
                .field("symbols", &enum_.symbols())
                .finish(),
            //TODO: extend to support name field
            SchemaType::Fixed(fixed) => f
                .debug_struct("Fixed")
                .field("name", &fixed.name())
                .field("size", &fixed.size())
                .finish(),
            SchemaType::Decimal(decimal) => f
                .debug_struct("Decimal")
                .field("name", &decimal.name())
                .field("precision", &decimal.precision())
                .field("scale", &decimal.scale())
                .finish(),
            SchemaType::Uuid => write!(f, "uuid"),
            SchemaType::Date => write!(f, "date"),
            SchemaType::TimeMillis => write!(f, "time millis"),
            SchemaType::TimeMicros => write!(f, "time micros"),
            SchemaType::TimestampMillis => write!(f, "timestamp millis"),
            SchemaType::TimestampMicros => write!(f, "tiumestamp micros"),
            SchemaType::Duration => write!(f, "duration"),
        }
    }
}

/// Unwrap the flyweight to its data member for easier and less verbose implementation of getters
macro_rules! match_lookup {
    ($self: expr, $match: pat => $extraction: expr) => (
        match $self.0.lookup($self.1) {
            $match => $extraction,
            _ => panic!("Incorrect data shape"),
        }
    );

    ($self: expr, $($match: pat)|+ => $extraction: expr) => (
        match $self.0.lookup($self.1) {
            $($match => $extraction,)+
            _ => panic!("Incorrect data shape"),
        }
    )
}

/// Represents `fixed` types in Avro schemas.
///
/// More information about `fixed` can be found in the
/// [Avro specification](https://avro.apache.org/docs/current/spec.html#schema_fixed)
#[derive(Copy, Clone, Debug)]
pub struct FixedSchema<'s>(&'s Schema, NameRef);

impl<'s> FixedSchema<'s> {
    pub fn name(&self) -> Name<'_> {
        Name(self.0, self.1)
    }

    pub fn size(&self) -> usize {
        match_lookup!(self, SchemaData::Fixed(size) => *size)
    }
}

/// Represents `decimal` types in Avro schemas.
///
/// More information about `decimal` can be found in the
/// [Avro specification](https://avro.apache.org/docs/current/spec.html#schema_decimal)
#[derive(Copy, Clone, Debug)]
pub struct DecimalSchema<'s>(&'s Schema, NameRef);

impl<'s> DecimalSchema<'s> {
    pub fn name(&self) -> Name<'_> {
        Name(self.0, self.1)
    }

    pub fn precision(&self) -> usize {
        match_lookup!(self, SchemaData::Decimal(p, _s) => *p)
    }

    pub fn scale(&self) -> usize {
        match_lookup!(self, SchemaData::Decimal(_p, s) => *s)
    }
}

/// Represents `enum` types in Avro schemas.
///
/// More information about `enum` can be found in the
/// [Avro specification](https://avro.apache.org/docs/current/spec.html#schema_enum)
#[derive(Copy, Clone, Debug)]
pub struct EnumSchema<'s>(&'s Schema, NameRef);

impl<'s> EnumSchema<'s> {
    pub fn name(&self) -> Name<'_> {
        Name(self.0, self.1)
    }

    pub fn doc(&self) -> Option<&str> {
        match_lookup!(self, SchemaData::Enum(doc, _) => doc.as_ref().map(|x| x.as_ref()))
    }

    pub fn iter_symbols(&self) -> impl Iterator<Item = &str> {
        match_lookup!(self, SchemaData::Enum(_, syms) => syms.iter().map(|x| x.as_ref()))
    }

    pub fn symbols(&self) -> Vec<&str> {
        self.iter_symbols().collect()
    }
}

/// Represents `record` types in Avro schemas.
///
/// More information about `records` can be found in the
/// [Avro specification](https://avro.apache.org/docs/current/spec.html#schema_record)
#[derive(Copy, Clone, Debug)]
pub struct RecordSchema<'s>(&'s Schema, NameRef);

impl<'s> RecordSchema<'s> {
    /// The name of the record
    pub fn name(&self) -> Name<'_> {
        Name(self.0, self.1)
    }

    /// Documentation of this record type
    pub fn doc(&self) -> Option<&str> {
        match_lookup!(self, SchemaData::Record(doc, _) => doc.as_ref().map(|x| x.as_ref()))
    }

    /// The fields of this record as an iterator
    pub fn iter_fields(&'s self) -> impl Iterator<Item = RecordField<'s>> + 's {
        match_lookup!(self, SchemaData::Record(_, flds) => {
            let schema = &*self.0;
            flds.iter().map(move |x| RecordField(schema, x))
        })
    }

    /// The fields provided by this record
    pub fn fields(&self) -> Vec<RecordField<'_>> {
        self.iter_fields().collect()
    }

    /// A convenience for looking up a _specific_ field in this record
    pub fn field(&self, field: &str) -> Option<RecordField<'_>> {
        self.iter_fields().find(|fld| fld.name() == field)
    }

    pub fn len(&self) -> usize {
        match_lookup!(self, SchemaData::Record(_, flds) => {
        // let schema = &*self.0;
        flds.len()
        })
    }
}

/// Represents `array` and `map` types in Avro schemas.
///
/// More information about `vixed` can be found in the
/// [Avro specification (arrays)](https://avro.apache.org/docs/current/spec.html#schema_arrays)
/// [Avro specification (maps)](https://avro.apache.org/docs/current/spec.html#schema_maps)
#[derive(Copy, Clone, Debug)]
pub struct Aggregate<'s>(&'s Schema, NameRef);

impl<'s> Aggregate<'s> {
    /// Return the name of the array if it was named, otherwise None
    pub fn name(&self) -> Option<Name<'_>> {
        Name::from_ref(self.0, self.1)
    }

    /// The type of value that is represented by this aggregated type
    pub fn items(&self) -> SchemaType<'_> {
        let elem_type = match_lookup!(self, SchemaData::Array(x) | SchemaData::Map(x) => x);
        self.0.lookup(*elem_type).bind(self.0, *elem_type)
    }
}

/// Represents `union` types in Avro schemas.
///
/// More information about `unions` can be found in the
/// [Avro specification](https://avro.apache.org/docs/current/spec.html#schema_union)
#[derive(Copy, Clone, Debug)]
pub struct UnionSchema<'s>(&'s Schema, NameRef);

impl<'s> UnionSchema<'s> {
    /// Returns a slice to all variants of this schema.
    pub fn iter_variants(&self) -> impl Iterator<Item = SchemaType<'_>> {
        match_lookup!(self, SchemaData::Union(variants) => {
            let schema = &*self.0;
            variants.iter().map(move |name| schema.lookup(*name).bind(schema, *name))
        })
    }

    pub fn variants(&self) -> Vec<SchemaType> {
        self.iter_variants().collect()
    }

    /// Returns true if the first variant of this `UnionSchema` is `Null`.
    pub fn is_nullable(&self) -> bool {
        self.iter_variants()
            .next()
            .map(|x| x == SchemaType::Null)
            .unwrap_or(true)
    }

    /// Optionally returns a reference to the schema matched by this value, as well as its position
    /// within this enum.
    pub fn find_schema(&self, value: &crate::types::Value) -> Option<(usize, SchemaType)> {
        let kind = SchemaKind::from(value);
        self.iter_variants()
            .enumerate()
            // TODO shouldn't we also check for name and namespace?
            .find(|(_pos, schema_type)| SchemaKind::from(*schema_type) == kind)
    }

    /// Optionally returns a reference to the schema matched by this value, as well as its position
    /// within this enum. For `Record` schemas it checks number of fields and their names to make
    /// sure to return the correct one.
    pub fn resolve_union_schema(&self, value: &crate::types::Value) -> Option<(usize, SchemaType)> {
        let mut counter = 0;
        match value {
            crate::types::Value::Record(fields) => {
                self.iter_variants()
                    .enumerate()
                    .find(|(_pos, schema_type)| {
                        if let SchemaType::Record(record_schema) = schema_type {
                            if record_schema.fields().len() == fields.len() {
                                for (name, _value) in fields.iter() {
                                    record_schema
                                        .iter_fields()
                                        .find(|field| field.name() == name)
                                        .and_then(|found| {
                                            counter += 1;
                                            Some(found)
                                        });
                                }
                            }
                        }
                        counter == fields.len()
                    })
            }
            _ => self.find_schema(value),
        }
    }
}

/// This type is used to simplify enum variant comparison between `Schema` and `types::Value`.
/// It may have utility as part of the public API, but defining as `pub(crate)` for now.
///
/// **NOTE** This type was introduced due to a limitation of `mem::discriminant` requiring a _value_
/// be constructed in order to get the discriminant, which makes it difficult to implement a
/// function that maps from `Discriminant<Schema> -> Discriminant<Value>`. Conversion into this
/// intermediate type should be especially fast, as the number of enum variants is small, which
/// _should_ compile into a jump-table for the conversion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SchemaKind {
    Null,
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    String,
    Array,
    Map,
    Union,
    Record,
    Enum,
    Fixed,
    Decimal,
    Uuid,
    Date,
    TimeMillis,
    TimeMicros,
    TimestampMillis,
    TimestampMicros,
    Duration,
}

impl SchemaKind {
    pub fn is_primitive(self) -> bool {
        match self {
            SchemaKind::Null
            | SchemaKind::Boolean
            | SchemaKind::Int
            | SchemaKind::Long
            | SchemaKind::Double
            | SchemaKind::Float
            | SchemaKind::Bytes
            | SchemaKind::String => true,
            _ => false,
        }
    }
}

impl From<SchemaType<'_>> for SchemaKind {
    #[inline(always)]
    fn from(schema: SchemaType) -> SchemaKind {
        // NOTE: I _believe_ this will always be fast as it should convert into a jump table.
        match schema {
            SchemaType::Null => SchemaKind::Null,
            SchemaType::Boolean => SchemaKind::Boolean,
            SchemaType::Int => SchemaKind::Int,
            SchemaType::Long => SchemaKind::Long,
            SchemaType::Float => SchemaKind::Float,
            SchemaType::Double => SchemaKind::Double,
            SchemaType::Bytes => SchemaKind::Bytes,
            SchemaType::String => SchemaKind::String,
            SchemaType::Array(_) => SchemaKind::Array,
            SchemaType::Map(_) => SchemaKind::Map,
            SchemaType::Union(_) => SchemaKind::Union,
            SchemaType::Record(_) => SchemaKind::Record,
            SchemaType::Enum(_) => SchemaKind::Enum,
            SchemaType::Fixed(_) => SchemaKind::Fixed,
            SchemaType::Decimal { .. } => SchemaKind::Decimal,
            SchemaType::Uuid => SchemaKind::Uuid,
            SchemaType::Date => SchemaKind::Date,
            SchemaType::TimeMillis => SchemaKind::TimeMillis,
            SchemaType::TimeMicros => SchemaKind::TimeMicros,
            SchemaType::TimestampMillis => SchemaKind::TimestampMillis,
            SchemaType::TimestampMicros => SchemaKind::TimestampMicros,
            SchemaType::Duration => SchemaKind::Duration,
        }
    }
}

impl<'a> From<&'a SchemaType<'_>> for SchemaKind {
    #[inline(always)]
    fn from(schema: &SchemaType) -> SchemaKind {
        // NOTE: I _believe_ this will always be fast as it should convert into a jump table.
        match schema {
            SchemaType::Null => SchemaKind::Null,
            SchemaType::Boolean => SchemaKind::Boolean,
            SchemaType::Int => SchemaKind::Int,
            SchemaType::Long => SchemaKind::Long,
            SchemaType::Float => SchemaKind::Float,
            SchemaType::Double => SchemaKind::Double,
            SchemaType::Bytes => SchemaKind::Bytes,
            SchemaType::String => SchemaKind::String,
            SchemaType::Array(_) => SchemaKind::Array,
            SchemaType::Map(_) => SchemaKind::Map,
            SchemaType::Union(_) => SchemaKind::Union,
            SchemaType::Record(_) => SchemaKind::Record,
            SchemaType::Enum(_) => SchemaKind::Enum,
            SchemaType::Fixed(_) => SchemaKind::Fixed,
            SchemaType::Decimal { .. } => SchemaKind::Decimal,
            SchemaType::Uuid => SchemaKind::Uuid,
            SchemaType::Date => SchemaKind::Date,
            SchemaType::TimeMillis => SchemaKind::TimeMillis,
            SchemaType::TimeMicros => SchemaKind::TimeMicros,
            SchemaType::TimestampMillis => SchemaKind::TimestampMillis,
            SchemaType::TimestampMicros => SchemaKind::TimestampMicros,
            SchemaType::Duration => SchemaKind::Duration,
        }
    }
}

impl<'a> From<&'a types::Value> for SchemaKind {
    #[inline(always)]
    fn from(value: &'a types::Value) -> SchemaKind {
        match value {
            types::Value::Null => SchemaKind::Null,
            types::Value::Boolean(_) => SchemaKind::Boolean,
            types::Value::Int(_) => SchemaKind::Int,
            types::Value::Long(_) => SchemaKind::Long,
            types::Value::Float(_) => SchemaKind::Float,
            types::Value::Double(_) => SchemaKind::Double,
            types::Value::Decimal(_) => SchemaKind::Decimal,
            types::Value::Bytes(_) => SchemaKind::Bytes,
            types::Value::String(_) => SchemaKind::String,
            types::Value::Array(_) => SchemaKind::Array,
            types::Value::Map(_) => SchemaKind::Map,
            types::Value::Union(_) => SchemaKind::Union,
            types::Value::Record(_) => SchemaKind::Record,
            types::Value::Enum(_, _) => SchemaKind::Enum,
            types::Value::Fixed(_, _) => SchemaKind::Fixed,
            types::Value::Uuid(_) => SchemaKind::Uuid,
            types::Value::Duration(_) => SchemaKind::Duration,
            types::Value::TimestampMicros(_) => SchemaKind::TimestampMicros,
            types::Value::TimestampMillis(_) => SchemaKind::TimestampMillis,
            types::Value::TimeMicros(_) => SchemaKind::TimeMicros,
            types::Value::TimeMillis(_) => SchemaKind::TimeMillis,
            types::Value::Date(_) => SchemaKind::Date,
        }
    }
}

/// Represents documentation for complex Avro schemas.
pub type Documentation = Option<String>;

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
#[derive(Copy, Clone)]
pub struct Name<'s>(&'s Schema, NameRef);

impl<'s> Name<'s> {
    /// Decompose a name ref to the name of the schema element if it is not anonymous
    fn from_ref(schema: &Schema, name_ref: NameRef) -> Option<Name<'_>> {
        match schema.is_anonymous(name_ref) {
            true => None,
            false => Some(Name(schema, name_ref)),
        }
    }

    /// Return the name of this item
    pub fn name(&self) -> &str {
        self.0.name(self.1)
    }

    /// Return the namespace if present for this item
    pub fn namespace(&self) -> Option<&str> {
        self.0.namespace(self.1)
    }

    /// Return an iterator of any known aliases of this item
    pub fn iter_aliases(&self) -> Option<impl Iterator<Item = Name<'s>>> {
        self.0.aliases(self.1)
    }

    /// Return the aliases for this item
    pub fn aliases(&self) -> Option<Vec<Name<'s>>> {
        self.iter_aliases().map(|aliases| aliases.collect())
    }

    /// Resolve the name given to its actual type, discarding aliases
    pub fn canonical_name(&self) -> Name<'s> {
        self.0.canonical_name(self.1)
    }

    /// Return the `fullname` of the `Name`
    ///
    /// More information about fullnames can be found in the
    /// [Avro specification](https://avro.apache.org/docs/current/spec.html#names)
    pub fn fullname(&self, default_namespace: Option<&str>) -> Cow<'_, str> {
        match self.namespace().or(default_namespace) {
            Some(ns) => Cow::Owned(format!("{}.{}", ns, self.name())),
            None => Cow::Borrowed(self.name()),
        }
    }
}

impl PartialEq<Name<'_>> for Name<'_> {
    fn eq(&self, other: &Name) -> bool {
        self.canonical_name().1 == other.canonical_name().1
    }
}

impl<'s> fmt::Debug for Name<'s> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("name")
            .field("name", &self.name())
            .field("name_sym", &self.1.name)
            .field("namespace", &self.namespace())
            .field("namespace_sym", &self.1.namespace)
            .finish()
    }
}

#[derive(Copy, Clone)]
pub struct RecordField<'s>(&'s Schema, &'s RecordFieldData);

impl<'s> RecordField<'s> {
    /// Name of the field.
    pub fn name(&self) -> &str {
        &self.1.name
    }

    /// Schema of the field.
    pub fn schema(&self) -> SchemaType<'_> {
        self.0.lookup(self.1.schema).bind(self.0, self.1.schema)
    }

    /// Documentation of the field.
    pub fn doc(&self) -> Option<&str> {
        self.1.doc.as_ref().map(|x| x.as_ref())
    }

    /// Default value of the field.
    /// This value will be used when reading Avro datum if schema resolution
    /// is enabled.
    pub fn default(&self) -> Option<&Value> {
        (&self.1.default).as_ref()
    }

    /// Order of the field.
    ///
    /// **NOTE** This currently has no effect.
    pub fn order(&self) -> Option<RecordFieldOrder> {
        self.1.order
    }

    /// Position of the field in the list of `field` of its parent `Schema`
    pub fn position(&self) -> usize {
        self.1.position
    }
}

impl<'s> fmt::Debug for RecordField<'s> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecordField")
            .field("name", &self.name())
            // Unfortunatly letting this be .schema() makes a hard to solve infinite recursion
            .field("schema", &Name(self.0, self.1.schema))
            .field("doc", &self.doc())
            .field("default", &self.default())
            .field("order", &self.order())
            .field("position", &self.position())
            .finish()
    }
}

/// Represents any valid order for a `field` in a `record` Avro schema.
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum RecordFieldOrder {
    Ascending,
    Descending,
    Ignore,
}

/// State wrapping type that allows serialisation to avoid infinite recursion
///
/// Avro schemas are often highly recursive in nature and naturally can form cyclic graphs.
/// Naturally these are a challenge to serialise correctly, if done in the naive way visitation
/// will not be halting.
///
/// To solve this we wrap anything that, in the main schema implementation, could produce NameRefs
/// with this object. This object also hides a small amount of interior mutability that identifies
/// objects that have already been seen once in full form.
///
/// Once an object is written the first time as its full definition, subsequent writes of that
/// datum are simply emitted as the fullname. This mirrors how most observed avro schema appears to
/// be constructed.
#[derive(Clone)]
struct OnceSchemaCell<T: Copy> {
    actual: T,
    prev_ns: Rc<RefCell<Option<RawNameSym>>>,
    current_ns: Rc<RefCell<Option<RawNameSym>>>,
    seen: Rc<RefCell<HashSet<NameRef>>>,
}

impl<T: Copy> OnceSchemaCell<T> {
    fn new(actual: T) -> Self {
        OnceSchemaCell {
            actual,
            prev_ns: Rc::new(RefCell::new(None)),
            current_ns: Rc::new(RefCell::new(None)),
            seen: Rc::new(RefCell::new(HashSet::new())),
        }
    }

    /// Allow a carrier cell to mutate inplace, carrying a new datum but retaining the state
    ///
    /// This is used to simplify the serialiser aspects that carry the data through with the
    /// existing serialiser state used to identify what fields have already been seen as full
    /// objects
    fn map<U: Copy>(&self, new: U) -> OnceSchemaCell<U> {
        OnceSchemaCell {
            actual: new,
            prev_ns: self.prev_ns.clone(),
            current_ns: self.current_ns.clone(),
            seen: self.seen.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    extern crate md5;
    extern crate sha2;

    use super::*;

    #[test]
    fn test_invalid_schema() {
        assert!(Schema::parse_str("invalid").is_err());
    }

    #[test]
    fn test_primitive_schema() -> Result<(), Error> {
        assert_eq!(SchemaType::Null, Schema::parse_str("\"null\"")?.root());
        assert_eq!(SchemaType::Int, Schema::parse_str("\"int\"")?.root());
        assert_eq!(SchemaType::Double, Schema::parse_str("\"double\"")?.root());
        Ok(())
    }

    #[test]
    fn test_array_schema() -> Result<(), Error> {
        match Schema::parse_str(r#"{"type": "array", "items": "string"}"#)?.root() {
            SchemaType::Array(array) => assert_eq!(array.items(), SchemaType::String),
            invalid => panic!("Invalid schema type `{:?}`", invalid),
        }
        Ok(())
    }

    #[test]
    fn test_map_schema() -> Result<(), Error> {
        match Schema::parse_str(r#"{"type": "map", "values": "double"}"#)?.root() {
            SchemaType::Map(map) => assert_eq!(map.items(), SchemaType::Double),
            invalid => panic!("Invalid schema type `{:?}`", invalid),
        }
        Ok(())
    }

    #[test]
    fn test_union_schema() -> Result<(), Error> {
        match Schema::parse_str(r#"["null", "int"]"#)?.root() {
            SchemaType::Union(union) => {
                assert_eq!(union.variants(), vec![SchemaType::Null, SchemaType::Int])
            }
            invalid => panic!("Invalid schema type `{:?}`", invalid),
        }
        Ok(())
    }

    #[test]
    fn test_union_unsupported_schema() {
        let schema = Schema::parse_str(r#"["null", ["null", "int"], "string"]"#);
        assert!(schema.is_err());
    }

    #[test]
    fn test_multi_union_schema() -> Result<(), Error> {
        let schema = Schema::parse_str(r#"["null", "int", "float", "string", "bytes"]"#)?;
        assert_eq!(SchemaKind::from(schema.root()), SchemaKind::Union);

        let union_schema = match schema.root() {
            SchemaType::Union(u) => u,
            _ => unreachable!(),
        };
        assert_eq!(union_schema.variants().len(), 5);

        let mut variants = union_schema.iter_variants().map(SchemaKind::from);
        assert_eq!(variants.next().unwrap(), SchemaKind::Null);
        assert_eq!(variants.next().unwrap(), SchemaKind::Int);
        assert_eq!(variants.next().unwrap(), SchemaKind::Float);
        assert_eq!(variants.next().unwrap(), SchemaKind::String);
        assert_eq!(variants.next().unwrap(), SchemaKind::Bytes);
        assert_eq!(variants.next(), None);
        Ok(())
    }

    #[test]
    fn test_record_schema() -> Result<(), Error> {
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
        )?;

        let mut builder = Schema::builder();
        let mut root_builder = builder.record("test");
        root_builder
            .field("a", builder.long())
            .default(Some(Value::Number(42i64.into())));
        root_builder.field("b", builder.string());
        let root = root_builder.build(&mut builder)?;
        let expected = builder.build(root)?;

        assert_eq!(expected, schema);
        Ok(())
    }

    #[test]
    fn test_enum_schema() -> Result<(), Error> {
        let schema = Schema::parse_str(
            r#"{"type": "enum", "name": "Suit", "symbols": ["diamonds", "spades", "clubs", "hearts"]}"#,
        )?;

        let mut builder = Schema::builder();
        let root = builder
            .enumeration("Suit")
            .symbols(vec!["diamonds", "spades", "clubs", "hearts"], &mut builder)?;
        let expected = builder.build(root)?;

        assert_eq!(expected, schema);
        Ok(())
    }

    #[test]
    fn test_fixed_schema() -> Result<(), Error> {
        let schema = Schema::parse_str(r#"{"type": "fixed", "name": "test", "size": 16}"#).unwrap();

        let mut builder = Schema::builder();
        let root = builder.fixed("test").size(16usize, &mut builder)?;
        let expected = builder.build(root)?;

        assert_eq!(expected, schema);
        Ok(())
    }

    #[test]
    fn test_no_documentation() -> Result<(), Error> {
        let schema = Schema::parse_str(
            r#"{"type": "enum", "name": "Coin", "symbols": ["heads", "tails"]}"#,
        )?;

        match schema.root() {
            SchemaType::Enum(enum_) => assert!(enum_.doc().is_none()),
            _ => assert!(false),
        };

        Ok(())
    }

    #[test]
    fn test_documentation() -> Result<(), Error> {
        let schema = Schema::parse_str(
            r#"{"type": "enum", "name": "Coin", "doc": "Some documentation", "symbols": ["heads", "tails"]}"#,
        )?;

        match schema.root() {
            SchemaType::Enum(enum_) => assert_eq!(Some("Some documentation"), enum_.doc()),
            _ => assert!(false),
        };

        Ok(())
    }

    // Tests to ensure Schema is Send + Sync. These tests don't need to _do_ anything, if they can
    // compile, they pass.
    #[test]
    fn test_schema_is_send() {
        fn send<S: Send>(_s: S) {}

        let schema = SchemaType::Null;
        send(schema);
    }

    #[test]
    fn test_schema_is_sync() {
        fn sync<S: Sync>(_s: S) {}

        let schema = SchemaType::Null;
        sync(&schema);
        sync(schema);
    }

    #[test]
    fn test_schema_fingerprint() {
        use self::{md5::Md5, sha2::Sha256};

        let raw_schema = r#"
    {
        "type": "record",
        "name": "test",
        "fields": [
            {"name": "a", "type": "long", "default": 42},
            {"name": "b", "type": "string"}
        ]
    }
"#;

        let schema = Schema::parse_str(raw_schema).unwrap();
        assert_eq!(
            "c4d97949770866dec733ae7afa3046757e901d0cfea32eb92a8faeadcc4de153",
            format!("{}", schema.fingerprint::<Sha256>())
        );

        assert_eq!(
            "7bce8188f28e66480a45ffbdc3615b7d",
            format!("{}", schema.fingerprint::<Md5>())
        );
    }
}
