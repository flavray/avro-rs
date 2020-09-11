//! Logic for serializing schemas to Avro schema json format.
//!
//! This handles serialization of schemas into Json, care is taken in this serialization
//! to ensure that recursive schemas do not create infinite serialization.
//!
//! As such the rule we follow is similar to how avro json is typically constructed, when we first
//! encounter a `NameRef` we serialize it directly into the position we are at in the tree, after
//! this all _subsequent_ serialisations of the aliased type are presented as a fullname.
use super::*;

use serde::ser::{Serialize, SerializeMap, Serializer};

macro_rules! serialize_formal_name {
    ($name: expr, $current_ns: expr, $map: expr) => {
        $map.serialize_entry("name", &$name.name())?;
        if let Some(ref n) = $name.namespace() {
            if $name.1.namespace != *$current_ns {
                $map.serialize_entry("namespace", n)?;
            }
        }

        if let Some(aliases) = $name.iter_aliases() {
            let aliases = aliases.map(|x| x.name().to_string()).collect::<Vec<_>>();
            if !aliases.is_empty() {
                $map.serialize_entry("aliases", &aliases)?;
            }
        }
    };
}

macro_rules! serialize_doc {
    ($doc: expr, $map: expr) => {
        if let Some(ref doc) = $doc {
            $map.serialize_entry("doc", doc)?;
        }
    };
}

macro_rules! serialize_aggregate {
    ($wrapper: expr, $agg: expr, $elem_id: expr, $type: expr, $serializer: expr) => {{
        let mut map = $serializer.serialize_map(None)?;
        map.serialize_entry("type", $type)?;
        if let Some(name) = $agg.name() {
            serialize_formal_name!(name, $wrapper.prev_ns.borrow(), map);
        }
        map.serialize_entry($elem_id, &$wrapper.map($agg.items()))?;
        map.end()
    }};
}

impl Serialize for RecordFieldOrder {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            RecordFieldOrder::Ascending => serializer.serialize_str("ascending"),
            RecordFieldOrder::Descending => serializer.serialize_str("descending"),
            RecordFieldOrder::Ignore => serializer.serialize_str("ignore"),
        }
    }
}

impl Serialize for OnceSchemaCell<RecordField<'_>> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("name", &self.actual.name())?;
        map.serialize_entry("type", &self.map(self.actual.schema()))?;
        if let Some(order) = self.actual.order() {
            map.serialize_entry("order", &order)?;
        }
        serialize_doc!(self.actual.doc(), map);

        if let Some(ref default) = self.actual.default() {
            map.serialize_entry("default", default)?;
        }

        map.end()
    }
}

impl Serialize for OnceSchemaCell<UnionSchema<'_>> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.actual.iter_variants().map(|x| self.map(x)))
    }
}

impl Serialize for OnceSchemaCell<FixedSchema<'_>> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("type", "fixed")?;
        serialize_formal_name!(self.actual.name(), self.prev_ns.borrow(), map);
        map.serialize_entry("size", &self.actual.size())?;
        map.end()
    }
}

impl Serialize for OnceSchemaCell<EnumSchema<'_>> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("type", "enum")?;
        serialize_formal_name!(self.actual.name(), self.prev_ns.borrow(), map);
        serialize_doc!(self.actual.doc(), map);
        map.serialize_entry("symbols", &self.actual.symbols())?;
        map.end()
    }
}

/// Wrapper type to handle serialising fields in the precense of state
struct OnceRecordCell<'s>(OnceSchemaCell<RecordSchema<'s>>);

impl Serialize for OnceRecordCell<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.0.actual.iter_fields().map(|x| self.0.map(x)))
    }
}

impl Serialize for OnceSchemaCell<SchemaType<'_>> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.actual {
            SchemaType::Null => serializer.serialize_str("null"),
            SchemaType::Boolean => serializer.serialize_str("boolean"),
            SchemaType::Int => serializer.serialize_str("int"),
            SchemaType::Long => serializer.serialize_str("long"),
            SchemaType::Float => serializer.serialize_str("float"),
            SchemaType::Double => serializer.serialize_str("double"),
            SchemaType::Bytes => serializer.serialize_str("bytes"),
            SchemaType::String => serializer.serialize_str("string"),
            SchemaType::Array(array) => {
                serialize_aggregate!(self, array, "items", "array", serializer)
            }
            SchemaType::Map(map) => serialize_aggregate!(self, map, "values", "map", serializer),
            SchemaType::Union(union_) => self.map(union_).serialize(serializer),
            SchemaType::Record(record) => {
                let canonic_name = record.0.resolve_nameref(record.1);
                let already_written = self.seen.borrow().contains(&canonic_name);
                match already_written {
                    true => {
                        let same_ns = *self.current_ns.borrow() == record.1.namespace;
                        match same_ns {
                            true => serializer.serialize_str(&record.name().name()),
                            false => serializer.serialize_str(&record.name().fullname(None)),
                        }
                    }
                    false => {
                        self.seen.borrow_mut().insert(canonic_name);
                        *self.prev_ns.borrow_mut() = *self.current_ns.borrow();
                        *self.current_ns.borrow_mut() = record.1.namespace;
                        let mut map = serializer.serialize_map(None)?;
                        map.serialize_entry("type", "record")?;
                        serialize_formal_name!(record.name(), self.prev_ns.borrow(), map);
                        serialize_doc!(record.doc(), map);
                        map.serialize_entry("fields", &OnceRecordCell(self.map(record)))?;
                        *self.current_ns.borrow_mut() = *self.prev_ns.borrow();
                        *self.prev_ns.borrow_mut() = None;
                        map.end()
                    }
                }
            }
            SchemaType::Enum(enum_) => self.map(enum_).serialize(serializer),
            SchemaType::Fixed(fixed) => self.map(fixed).serialize(serializer),
            //TODO: I have no frickking idea what to do here
            SchemaType::Decimal{  precision, scale, inner} => (),
            SchemaType::Uuid => (),
            SchemaType::Date => (),
            SchemaType::TimeMillis => (),
            SchemaType::TimeMicros => (),
            SchemaType::TimestampMillis => (),
            SchemaType::TimestampMicros => (),
            SchemaType::Duration => (),
            _ => (),
        }
    }
}

impl Serialize for Schema {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let state = OnceSchemaCell::new(self.root());
        state.serialize(serializer)
    }
}
