//! Logic for parsing and interacting with schemas in Avro format.
use std::borrow::Cow;
use std::sync::Arc;
use std::collections::{HashMap, HashSet};

use failure::Error;
use serde::ser::{Serialize, SerializeMap, SerializeSeq, Serializer};
use serde_json::{self, Map, Value};

use types;
use util::MapHelper;

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

/// Represents any valid Avro schema
/// More information about Avro schemas can be found in the
/// [Avro Specification](https://avro.apache.org/docs/current/spec.html#schemas)
#[derive(Clone, Debug, PartialEq)]
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
    /// A reference to a type defined in this schema
    TypeReference(NameRef),
    /// A `array` Avro schema.
    /// `Array` holds a counted reference (`Rc`) to the `Schema` of its items.
    Array(Arc<Schema>),
    /// A `map` Avro schema.
    /// `Map` holds a pointer to the `Schema` of its values, which must all be the same schema.
    /// `Map` keys are assumed to be `string`.
    Map(Arc<Schema>),
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
}

/// This type is used to simplify enum variant comparison between `Schema` and `types::Value`.
/// It may have utility as part of the public API, but defining as `pub(crate)` for now.
///
/// **NOTE** This type was introduced due to a limitation of `mem::discriminant` requiring a _value_
/// be constructed in order to get the discriminant, which makes it difficult to implement a
/// function that maps from `Discriminant<Schema> -> Discriminant<Value>`. Conversion into this
/// intermediate type should be especially fast, as the number of enum variants is small, which
/// _should_ compile into a jump-table for the conversion.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum SchemaKind {
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
    Named,
}

impl<'a> From<&'a Schema> for SchemaKind {
    #[inline(always)]
    fn from(schema: &'a Schema) -> SchemaKind {
        // NOTE: I _believe_ this will always be fast as it should convert into a jump table.
        match schema {
            Schema::Null => SchemaKind::Null,
            Schema::Boolean => SchemaKind::Boolean,
            Schema::Int => SchemaKind::Int,
            Schema::Long => SchemaKind::Long,
            Schema::Float => SchemaKind::Float,
            Schema::Double => SchemaKind::Double,
            Schema::Bytes => SchemaKind::Bytes,
            Schema::String => SchemaKind::String,
            Schema::Array(_) => SchemaKind::Array,
            Schema::Map(_) => SchemaKind::Map,
            Schema::Union(_) => SchemaKind::Union,
            Schema::Record{ .. } => SchemaKind::Named,
            Schema::TypeReference{ .. } => SchemaKind::Named,
            Schema::Fixed{ .. } => SchemaKind::Named,
            Schema::Enum{ .. } => SchemaKind::Named,
            
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
            types::Value::Bytes(_) => SchemaKind::Bytes,
            types::Value::String(_) => SchemaKind::String,
            types::Value::Array(_) => SchemaKind::Array,
            types::Value::Map(_) => SchemaKind::Map,
            types::Value::Union(_) => SchemaKind::Union,
            types::Value::Record(_) => SchemaKind::Named,
            types::Value::Enum(_, _) => SchemaKind::Named,
            types::Value::Fixed(_, _) => SchemaKind::Named,
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
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct NameRef {
    pub name: String,
    pub namespace: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Name {
    pub name: NameRef,
    pub aliases: Option<Vec<NameRef>>,
}


/// Represents documentation for complex Avro schemas.
pub type Documentation = Option<String>;

impl NameRef {
    /// parse a string into a Name, using the given namespace if the
    /// parsed string wasn't fully qualified.
    fn parse_str(name: &str, namespace: &Option<String>) -> Result<Self, Error> {
        let mut names: Vec<&str> = name.split('.').collect();
        if names.len() > 1 {
            Ok(NameRef {
                name: names.pop().unwrap().to_string(),
                namespace: Some(names.join(".")),
            })
        } else {
            Ok(NameRef {
                name: name.to_string(),
                namespace: namespace.clone(),
            })
        }
    }

    pub fn fullname(&self, default_namespace: &Option<String>) -> String {
        if self.name.contains('.') {
            self.name.to_string()
        } else {
            let ns: &Option<String> = match self.namespace {
                None => default_namespace,
                _ => &self.namespace,
            };

            match ns {
                Some(ref namespace) => format!("{}.{}", namespace, self.name),
                None => self.name.clone(),
            }
        }
    }
}

impl Name {
    /// Create a new `Name`.
    /// No `namespace` nor `aliases` will be defined.
    pub fn new(name: &str) -> Name {
        Name {
            name: NameRef {
                name:name.to_owned(),
                namespace: None,
            },
            aliases: None,
        }
    }


    /// Parse a `serde_json::Value` into a `Name`.
    fn parse(complex: &Map<String, Value>, current_namespace: &Option<String>) -> Result<Self, Error> {
        let name_str = complex
            .name()
            .ok_or_else(|| ParseSchemaError::new("No `name` field"))?;

        let mut name = NameRef::parse_str(&name_str, &complex.string("namespace"))?;

        if name.namespace.is_none() {
            name.namespace = current_namespace.clone();
        }

        let aliases: Option<Vec<NameRef>> = if let Some(aliases) = complex.get("aliases") {
            let v = aliases.as_array().ok_or_else(|| ParseSchemaError::new("aliases must be an array"))
            .and_then(|aliases| {
                aliases
                    .iter()
                    .map(|alias| alias.as_str())
                    .map(|alias| alias
                         .ok_or_else(|| ParseSchemaError::new("couldn't parse type alias"))
                         .and_then(|a| NameRef::parse_str(a, &name.namespace)
                                   .map_err(|e| ParseSchemaError::new(format!("couldn't parse name: {:?}: {}", name, e))))
                    ).collect::<Result<_,_>>()
            })?;
            Some(v)
        } else { None };

        Ok(Name {
            name,
            aliases,
        })
    }


    /// Return a vector of all the `fullname`s of this `Name`,
    /// The first item on the result Vec will be the fully qualified name.
    /// Subsequent items will be fully qualified aliases.
    ///
    /// More information about fullnames can be found in the
    /// [Avro specification](https://avro.apache.org/docs/current/spec.html#names)
    pub fn fullnames(&self, default_namespace: &Option<String>) -> Vec<String> {
        let num = self.aliases.as_ref().map_or(0, |a| a.len()) + 1;
        let mut v = Vec::with_capacity(num);

        v.push(self.name.fullname(&default_namespace));
        if let Some(ref aliases) = self.aliases {
            for a in aliases {
                v.push(a.fullname(&default_namespace));
            }
        }

        v
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
    pub schema: Arc<Schema>,
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
    fn parse(field: &Map<String, Value>, position: usize, context: &mut SchemaParseContext) -> Result<Self, Error> {
        let name = field
            .name()
            .ok_or_else(|| ParseSchemaError::new("No `name` in record field"))?;

        let schema = field
            .get("type")
            .ok_or_else(|| ParseSchemaError::new("No `type` in record field").into())
            .and_then(|type_| Schema::parse_with_context(type_, context))?;

        let default = field.get("default").cloned();

        let order = field
            .get("order")
            .and_then(|order| order.as_str())
            .and_then(|order| match order {
                "ascending" => Some(RecordFieldOrder::Ascending),
                "descending" => Some(RecordFieldOrder::Descending),
                "ignore" => Some(RecordFieldOrder::Ignore),
                _ => None,
            }).unwrap_or_else(|| RecordFieldOrder::Ascending);

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

#[derive(Clone, Debug, Default)]
pub struct SchemaParseContext {
    pub(crate) current_namespace: Option<String>,
    pub(crate) type_registry: HashMap<String, Arc<Schema>>,
}

impl SchemaParseContext {
    pub fn new() -> Self {
        Self {
            current_namespace: None,
            type_registry: HashMap::new(),
        }
    }

    pub fn register_type(&mut self, name: &Name, schema: &Arc<Schema>) {
        for key in name.fullnames(&self.current_namespace) {
            self.type_registry.insert(key, schema.clone());
        }
    }
    pub fn lookup_type(&self, name: &NameRef, context: &SchemaParseContext) -> Option<Arc<Schema>> {
        self.type_registry.get(&name.fullname(&context.current_namespace)).cloned()
    }
}

#[derive(Debug, Clone)]
pub struct UnionSchema {
    schemas: Vec<Arc<Schema>>,

    // this will hold the irrefutable schemata such ans Int or Null
    variant_index: HashMap<SchemaKind, usize>,

    // this will hold the indices of the named schemata which must be
    // tested one by one if the irrefutable schemata don't match
    named_index: Vec<usize>,
}

impl UnionSchema {
    pub(crate) fn new(schemas: Vec<Arc<Schema>>, context: &SchemaParseContext) -> Result<Self, Error> {
        let mut variant_index = HashMap::new();
        let mut named_index = Vec::new();
        let mut seen_names = HashSet::new();
        
        for (i, schema) in schemas.iter().enumerate() {
            if let Schema::Union(_) = **schema {
                Err(ParseSchemaError::new(
                    "Unions may not directly contain a union",
                ))?;
            }

            let kind = SchemaKind::from(&**schema);

            if let SchemaKind::Named = kind {
                let names = match **schema {
                    Schema::Record { ref name, .. } =>  name.fullnames(&context.current_namespace),
                    Schema::Enum { ref name, .. } => name.fullnames(&context.current_namespace),
                    Schema::Fixed { ref name, .. } => name.fullnames(&context.current_namespace),
                    Schema::TypeReference(ref name) => vec!(name.fullname(&context.current_namespace)),
                    _ => unreachable!(),
                };
                
                for n in names {
                    if seen_names.contains(&n) {
                        Err(ParseSchemaError::new(
                            "Unions cannot contain duplicate named record/enum/fixed members",
                        ))?;
                    }
                    seen_names.insert(n);
                    named_index.push(i);
                }
            } else if variant_index.insert(kind, i).is_some() {
                Err(ParseSchemaError::new(
                    "Unions cannot contain duplicate types",
                ))?;

            }
        }

        Ok(UnionSchema {
            schemas,
            variant_index,
            named_index,
        })
    }


    /// Returns a slice to all variants of this schema.
    pub fn variants(&self) -> &[Arc<Schema>] {
        &self.schemas
    }

    /// Returns true if the first variant of this `UnionSchema` is `Null`.
    pub fn is_nullable(&self) -> bool {
        self.variant_index.contains_key(&SchemaKind::Null)
    }

    /// Optionally returns a reference to the schema matched by this value, as well as its position
    /// within this enum.
    pub fn find_schema(&self, value: &::types::Value, context: &mut SchemaParseContext) -> Option<(usize, Arc<Schema>)> {
        let kind = SchemaKind::from(value);

        if let SchemaKind::Named = kind {
            for i in &self.named_index {
                if value.validate_inner(&self.schemas[*i], &mut context.clone()) {
                    return Some((*i, self.schemas[*i].clone()));
                }
            }
            None
        } else {
            self.variant_index
                .get(&kind)
                .cloned()
                .map(|i| (i, self.schemas[i].clone()))
        }
    }
}

// No need to compare variant_index, it is derivative of schemas.
impl PartialEq for UnionSchema {
    fn eq(&self, other: &UnionSchema) -> bool {
        self.schemas.eq(&other.schemas)

    }
}

impl Schema {
    /// Create a `Schema` from a string representing a JSON Avro schema.
    pub fn parse_str(input: &str) -> Result<Self, Error> {
        let value = serde_json::from_str(input)?;
        Self::parse(&value)
    }

    /// Create a `Schema` from a `serde_json::Value` representing a JSON Avro
    /// schema.
    pub fn parse(value: &Value) -> Result<Self, Error> {
        let res = {
            let mut context = SchemaParseContext::new();
            Self::parse_with_context(value, &mut context)
        };

        res.map(|b| (*b).clone())
    }

    fn parse_with_context<'a>(value: &'a Value , context: &mut SchemaParseContext) -> Result<Arc<Self>, Error> {
        match *value {
            Value::String(ref t) => Schema::parse_type(t.as_str(), context),
            Value::Object(ref data) => Schema::parse_complex(data, context),
            Value::Array(ref data) => Schema::parse_union(data, context),
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

    /// Parse a `serde_json::Value` representing a primitive Avro type into a
    /// `Schema`.
    fn parse_type(primitive: &str, context: &mut SchemaParseContext) -> Result<Arc<Self>, Error> {
        match primitive {
            "null" => Ok(Arc::new(Schema::Null)),
            "boolean" => Ok(Arc::new(Schema::Boolean)),
            "int" => Ok(Arc::new(Schema::Int)),
            "long" => Ok(Arc::new(Schema::Long)),
            "double" => Ok(Arc::new(Schema::Double)),
            "float" => Ok(Arc::new(Schema::Float)),
            "bytes" => Ok(Arc::new(Schema::Bytes)),
            "string" => Ok(Arc::new(Schema::String)),
            other => Schema::parse_reference(other, context),
        }
    }

    fn parse_reference(reference: &str, context: &mut SchemaParseContext) -> Result<Arc<Self>, Error> {
        NameRef::parse_str(reference, &context.current_namespace).map(Schema::TypeReference).map(Arc::new)
    }

    /// Parse a `serde_json::Value` representing a complex Avro type into a
    /// `Schema`.
    ///
    /// Avro supports "recursive" definition of types.
    /// e.g: {"type": {"type": "string"}}
    fn parse_complex(complex: &Map<String, Value>, context: &mut SchemaParseContext) -> Result<Arc<Self>, Error> {
        match complex.get("type") {
            Some(&Value::String(ref t)) => match t.as_str() {
                "array" => Schema::parse_array(complex, context),
                "map" => Schema::parse_map(complex, context),
                "record" | "enum" | "fixed" => Schema::parse_named(complex, context),
                other => Schema::parse_type(other, context),
            },
            Some(&Value::Object(ref data)) => match data.get("type") {
                Some(ref value) => Schema::parse_with_context(value, context),
                None => Err(
                    ParseSchemaError::new(format!("Unknown complex type: {:?}", complex)).into(),
                ),
            },
            _ => Err(ParseSchemaError::new("No `type` in complex type").into()),
        }
    }

    fn parse_named(complex: &Map<String, Value>, context: &mut SchemaParseContext) -> Result<Arc<Self>, Error> {
        let name = Name::parse(complex, &context.current_namespace)?;
        context.current_namespace = name.name.namespace.clone();

        let schema = match complex.get("type") {
            Some(&Value::String(ref t)) => match t.as_str() {
                "record" => Schema::parse_record(complex, context),
                "enum" => Schema::parse_enum(complex, context),
                "fixed" => Schema::parse_fixed(complex, context),
                _ => panic!("parse_named got wrong type"),
            },
            _ => panic!("parse_named got wrong type"),
        };

        if let Ok(ref schema) = schema {
            context.register_type(&name, schema)
        }

        schema
    }

    /// Parse a `serde_json::Value` representing a Avro record type into a
    /// `Schema`.
    fn parse_record(complex: &Map<String, Value>, context: &mut SchemaParseContext) -> Result<Arc<Self>, Error> {
        let name = Name::parse(complex, &context.current_namespace)?;

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
                    .map(|(position, field)| RecordField::parse(field, position, context))
                    .collect::<Result<_, _>>()
            })?;

        for field in &fields {
            lookup.insert(field.name.clone(), field.position);
        }

        Ok(Arc::new(Schema::Record {
            name,
            doc: complex.doc(),
            fields,
            lookup,
        }))
    }

    /// Parse a `serde_json::Value` representing a Avro enum type into a
    /// `Schema`.
    fn parse_enum(complex: &Map<String, Value>, context: &SchemaParseContext) -> Result<Arc<Self>, Error> {
        let name = Name::parse(complex, &context.current_namespace)?;

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

        Ok(Arc::new(Schema::Enum {
            name,
            doc: complex.doc(),
            symbols,
        }))
    }

    /// Parse a `serde_json::Value` representing a Avro array type into a
    /// `Schema`.
    fn parse_array(complex: &Map<String, Value>, context: &mut SchemaParseContext) -> Result<Arc<Self>, Error> {
        complex
            .get("items")
            .ok_or_else(|| ParseSchemaError::new("No `items` in array").into())
            .and_then(|items| Schema::parse_with_context(items, context))
            .map(|schema| Arc::new(Schema::Array(schema)))
    }

    /// Parse a `serde_json::Value` representing a Avro map type into a
    /// `Schema`.
    fn parse_map(complex: &Map<String, Value>, context: &mut SchemaParseContext) -> Result<Arc<Self>, Error> {
        complex
            .get("values")
            .ok_or_else(|| ParseSchemaError::new("No `values` in map").into())
            .and_then(|items| Schema::parse_with_context(items, context))
            .map(|schema| Arc::new(Schema::Map(schema)))
    }

    /// Parse a `serde_json::Value` representing a Avro union type into a
    /// `Schema`.
    fn parse_union(items: &[Value], context: &SchemaParseContext) -> Result<Arc<Self>, Error> {
        items
            .iter()
            .map(|s| Schema::parse(s).map(Arc::new))
            .collect::<Result<Vec<_>, _>>()
            .and_then(|schemas| Ok(Arc::new(Schema::Union(UnionSchema::new(schemas, context)?))))
    }

    /// Parse a `serde_json::Value` representing a Avro fixed type into a
    /// `Schema`.
    fn parse_fixed(complex: &Map<String, Value>, context: &mut SchemaParseContext) -> Result<Arc<Self>, Error> {
        let name = Name::parse(complex, &context.current_namespace)?;

        let size = complex
            .get("size")
            .and_then(|v| v.as_i64())
            .ok_or_else(|| ParseSchemaError::new("No `size` in fixed"))?;

        Ok(Arc::new(Schema::Fixed {
            name,
            size: size as usize,
        }))
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
            Schema::TypeReference(ref name) => serializer.serialize_str(&name.fullname(&None)),
            Schema::Bytes => serializer.serialize_str("bytes"),
            Schema::String => serializer.serialize_str("string"),
            Schema::Array(ref inner) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("type", "array")?;
                map.serialize_entry("items", &*inner.clone())?;
                map.end()
            },
            Schema::Map(ref inner) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("type", "map")?;
                map.serialize_entry("values", &*inner.clone())?;
                map.end()
            },
            Schema::Union(ref inner) => {
                let variants = inner.variants();
                let mut seq = serializer.serialize_seq(Some(variants.len()))?;
                for v in variants {
                    seq.serialize_element(&**v)?;
                }
                seq.end()
            },
            Schema::Record {
                ref name,
                ref doc,
                ref fields,
                ..
            } => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "record")?;
                if let Some(ref n) = name.name.namespace {
                    map.serialize_entry("namespace", n)?;
                }
                map.serialize_entry("name", &name.name.name)?;
                if let Some(ref docstr) = doc {
                    map.serialize_entry("doc", docstr)?;
                }
                if let Some(ref aliases) = name.aliases {
                    map.serialize_entry("aliases", &aliases.iter().map(|e| e.fullname(&None)).collect::<Vec<_>>())?;
                }
                map.serialize_entry("fields", fields)?;
                map.end()
            },
            Schema::Enum {
                ref name,
                ref symbols,
                ..
            } => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "enum")?;
                map.serialize_entry("name", &name.name.name)?;
                map.serialize_entry("symbols", symbols)?;
                map.end()
            },
            Schema::Fixed { ref name, ref size } => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "fixed")?;
                map.serialize_entry("name", &name.name.name)?;
                map.serialize_entry("size", size)?;
                map.end()
            },
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
        map.serialize_entry("type", &*self.schema)?;

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
                return pcf_string(s)
            }
        }

        // Strip out unused fields ([STRIP] rule)
        if field_ordering_position(k).is_none() {
            continue
        }

        // Fully qualify the name, if it isn't already ([FULLNAMES] rule).
        if k == "name" {
            // Invariant: Only valid schemas. Must be a string.
            let name = v.as_str().unwrap();
            let n = match ns {
                Some(namespace) if !name.contains('.') => {
                    Cow::Owned(format!("{}.{}", namespace, name))
                },
                _ => Cow::Borrowed(name),
            };

            fields.push((k, format!("{}:{}", pcf_string(k), pcf_string(&*n))));
            continue
        }

        // Strip off quotes surrounding "size" type, if they exist ([INTEGERS] rule).
        if k == "size" {
            let i = match v.as_str() {
                Some(s) => s.parse::<i64>().expect("Only valid schemas are accepted!"),
                None => v.as_i64().unwrap(),
            };
            fields.push((k, format!("{}:{}", pcf_string(k), i)));
            continue
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
        assert_eq!(Schema::Array(Arc::new(Schema::String)), schema);
    }

    #[test]
    fn test_map_schema() {
        let schema = Schema::parse_str(r#"{"type": "map", "values": "double"}"#).unwrap();
        assert_eq!(Schema::Map(Arc::new(Schema::Double)), schema);
    }

    #[test]
    fn test_union_schema() {
        let schema = Schema::parse_str(r#"["null", "int"]"#).unwrap();
        assert_eq!(
            Schema::Union(UnionSchema::new(vec![Arc::new(Schema::Null), Arc::new(Schema::Int)], &SchemaParseContext::new()).unwrap()),
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
        assert_eq!(SchemaKind::from(&**variants.next().unwrap()), SchemaKind::Null);
        assert_eq!(SchemaKind::from(&**variants.next().unwrap()), SchemaKind::Int);
        assert_eq!(
            SchemaKind::from(&**variants.next().unwrap()),
            SchemaKind::Float
        );
        assert_eq!(
            SchemaKind::from(&**variants.next().unwrap()),
            SchemaKind::String
        );
        assert_eq!(
            SchemaKind::from(&**variants.next().unwrap()),
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
        ).unwrap();

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
                    schema: Arc::new(Schema::Long),
                    order: RecordFieldOrder::Ascending,
                    position: 0,
                },
                RecordField {
                    name: "b".to_string(),
                    doc: None,
                    default: None,
                    schema: Arc::new(Schema::String),
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
    fn test_parse_recursive_schema() {
        let _schema = Schema::parse_str(r#"
                     {
                       "type": "record",
                       "name": "test",
                       "fields": [
                         {
                           "name": "recurse",
                           "type": ["null", "test"]
                         }
                       ]
                     }"#).unwrap();

    }

    #[test]
    fn test_nested_named_fixed_schema() {
        let schema = Schema::parse_str(
            r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {
                        "name": "a",
                        "type": {
                           "name": "fixed_test",
                           "namespace": "com.test",
                           "type": "fixed",
                           "size": 2
                         }
                    },
                    {
                        "name": "b",
                        "type": "com.test.fixed_test"
                    }
                ]
            }
        "#,
        ).unwrap();

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
                    default: None,
                    schema: Arc::new(Schema::Fixed {
                        name: Name {
                            name: NameRef::parse_str("fixed_test", &Some("com.test".to_string())).unwrap(),
                            aliases: None,
                        },
                        size: 2usize,
                    }),
                    order: RecordFieldOrder::Ascending,
                    position: 0,
                },
                RecordField {
                    name: "b".to_string(),
                    doc: None,
                    default: None,
                    schema: Arc::new(Schema::TypeReference(NameRef::parse_str("fixed_test", &Some("com.test".to_string())).unwrap())),
                    order: RecordFieldOrder::Ascending,
                    position: 1,
                },
            ],
            lookup: lookup,
        };

        assert_eq!(expected, schema);
    }

    #[test]
    fn test_namespaces() {
        let mut lookup = HashMap::new();
        lookup.insert("one".to_owned(), 0);
        lookup.insert("two".to_owned(), 1);
        lookup.insert("three".to_owned(), 2);
        lookup.insert("four".to_owned(), 3);

        let schema = Schema::parse_str(
            r#"{ "type": "record",
                 "name": "top",
                 "namespace": "com.example",
                 "fields": [
                   { "name": "one",
                     "type": { "name": "one",
                               "type": "record",
                               "fields": []
                             }
                   },
                   { "name": "two",
                     "type": { "type": "record",
                               "name": "two",
                               "namespace": "biz.good",
                               "fields": []
                             }
                   },
                   { "name": "three",
                     "type": { "type": "record",
                               "name": "three",
                               "namespace": "com.example.sub",
                               "fields": []
                             }
                   },
                   { "name": "four",
                     "type": { "type": "record",
                               "name": "four.five",
                               "namespace": "should.be.ignored",
                               "fields": []
                             }
                   }
                 ]
               }"#).unwrap();

        let expected = Schema::Record {
            name: Name { name: NameRef::parse_str("com.example.top", &None).unwrap(), aliases: None },
            doc: None,
            fields: vec![
                RecordField {
                    name: "one".to_string(),
                    doc: None,
                    default: None,
                    schema: Arc::new(Schema::Record{
                        name: Name { name: NameRef::parse_str("com.example.one", &None).unwrap(), aliases: None },
                        doc: None,
                        fields: Vec::new(),
                        lookup: HashMap::new()
                    }),
                    order: RecordFieldOrder::Ascending,
                    position: 0,
                },
                RecordField {
                    name: "two".to_string(),
                    doc: None,
                    default: None,
                    schema: Arc::new(Schema::Record{
                        name: Name { name: NameRef::parse_str("biz.good.two", &None).unwrap(), aliases: None },
                        doc: None,
                        fields: Vec::new(),
                        lookup: HashMap::new()
                    }),
                    order: RecordFieldOrder::Ascending,
                    position: 1,
                },
                RecordField {
                    name: "three".to_string(),
                    doc: None,
                    default: None,
                    schema: Arc::new(Schema::Record{
                        name: Name { name: NameRef::parse_str("com.example.sub.three", &None).unwrap(), aliases: None },
                        doc: None,
                        fields: Vec::new(),
                        lookup: HashMap::new()
                    }),
                    order: RecordFieldOrder::Ascending,
                    position: 2,
                },
                RecordField {
                    name: "four".to_string(),
                    doc: None,
                    default: None,
                    schema: Arc::new(Schema::Record{
                        name: Name { name: NameRef::parse_str("four.five", &None).unwrap(), aliases: None },
                        doc: None,
                        fields: Vec::new(),
                        lookup: HashMap::new()
                    }),
                    order: RecordFieldOrder::Ascending,
                    position: 3,
                },
            ],
            lookup: lookup,
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
}
