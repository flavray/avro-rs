use super::{
    Aggregate, Documentation, EnumSchema, FixedSchema, NameRef, RecordFieldOrder, RecordSchema,
    Schema, SchemaType, UnionSchema,
};
use serde_json::Value;
use std::fmt;

/// The actual data inside the schema
#[derive(Clone, PartialEq)]
pub(super) enum SchemaData {
    Null,
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    String,
    Array(NameRef),
    Map(NameRef),
    Union(Vec<NameRef>),
    Record(Documentation, Vec<RecordFieldData>),
    Enum(Documentation, Vec<String>),
    Fixed(usize),
}

impl fmt::Debug for SchemaData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SchemaData::Null => f.write_str("null"),
            SchemaData::Boolean => f.write_str("boolean"),
            SchemaData::Int => f.write_str("int"),
            SchemaData::Long => f.write_str("long"),
            SchemaData::Float => f.write_str("float"),
            SchemaData::Double => f.write_str("double"),
            SchemaData::Bytes => f.write_str("bytes"),
            SchemaData::String => f.write_str("string"),
            SchemaData::Array(_) => f.write_str("array(...)"),
            SchemaData::Map(_) => f.write_str("map(...)"),
            SchemaData::Union(_) => f.write_str("union(...)"),
            SchemaData::Record(_, _) => f.write_str("record(...)"),
            SchemaData::Enum(_, syms) => {
                f.write_str("enum ")?;
                f.debug_set().entries(syms).finish()
            }
            SchemaData::Fixed(size) => f.debug_tuple("fixed").field(size).finish(),
        }
    }
}

impl SchemaData {
    pub(super) fn bind<'s>(&self, schema: &'s Schema, name: NameRef) -> SchemaType<'s> {
        match self {
            SchemaData::Null => SchemaType::Null,
            SchemaData::Boolean => SchemaType::Boolean,
            SchemaData::Int => SchemaType::Int,
            SchemaData::Long => SchemaType::Long,
            SchemaData::Float => SchemaType::Float,
            SchemaData::Double => SchemaType::Double,
            SchemaData::Bytes => SchemaType::Bytes,
            SchemaData::String => SchemaType::String,
            SchemaData::Array(_) => SchemaType::Array(Aggregate(schema, name)),
            SchemaData::Map(_) => SchemaType::Map(Aggregate(schema, name)),
            SchemaData::Union(_) => SchemaType::Union(UnionSchema(schema, name)),
            SchemaData::Record(_, _) => SchemaType::Record(RecordSchema(schema, name)),
            SchemaData::Enum(_, _) => SchemaType::Enum(EnumSchema(schema, name)),
            SchemaData::Fixed(_) => SchemaType::Fixed(FixedSchema(schema, name)),
        }
    }
}

/// Represents a `field` in a `record` Avro schema.
#[derive(Clone, PartialEq)]
pub(crate) struct RecordFieldData {
    pub(crate) name: String,
    pub(crate) doc: Documentation,
    pub(crate) default: Option<Value>,
    pub(crate) schema: NameRef,
    pub(crate) order: Option<RecordFieldOrder>,
    pub(crate) position: usize,
}
