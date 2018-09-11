//! Logic for parsing and interacting with schemas in Avro format.
use std::collections::HashMap;
use std::rc::Rc;

use failure::Error;
use serde::ser::{Serialize, SerializeMap, SerializeSeq, Serializer};
use serde_json::{self, Map, Value};

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
    Array(Rc<Schema>),
    /// A `map` Avro schema.
    /// `Map` holds a counted reference (`Rc`) to the `Schema` of its values.
    /// `Map` keys are assumed to be `string`.
    Map(Rc<Schema>),
    /// A `union` Avro schema.
    ///
    /// `Union` holds a counted reference (`Rc`) to its non-`null` `Schema`.
    ///
    /// **NOTE** Only `["null", "< type >"]` unions are currently supported.
    /// Any other combination of `Schema`s contained in a `union` will be
    /// considered invalid and errors will be reported when trying to parse
    /// such a schema.
    Union(Rc<Schema>),
    /// A `record` Avro schema.
    ///
    /// The `lookup` table maps field names to their position in the `Vec`
    /// of `fields`.
    Record {
        name: Name,
        doc: Documentation,
        fields: Vec<RecordField>,

        // TODO: this does not look great.
        // This is just a trick to avoid borrows of Schema into types::Record.
        lookup: Rc<HashMap<String, usize>>,
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
#[derive(Clone, Debug, PartialEq)]
pub struct NameRef {
    pub name: String,
    pub namespace: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
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
        let mut names: Vec<&str> = name.split(".").collect();
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

    fn fullname(&self, default_namespace: &Option<String>) -> String {
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
            let v = aliases.as_array().ok_or(ParseSchemaError::new("aliases must be an array").into())
            .and_then(|aliases| {
                aliases
                    .iter()
                    .map(|alias| alias.as_str())
                    .map(|alias| alias
                         .ok_or(ParseSchemaError::new("couldn't parse type alias").into())
                         .and_then(|a| NameRef::parse_str(a, &name.namespace))
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
            for ref a in aliases {
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
    pub schema: Rc<Schema>,
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

pub struct SchemaParseContext {
    pub current_namespace: Option<String>,
    type_registry: HashMap<String, Rc<Schema>>,
}

impl SchemaParseContext {
    pub fn new() -> Self {
        Self {
            current_namespace: None,
            type_registry: HashMap::new(),
        }
    }

    pub fn register_type(&mut self, name: Name, schema: Rc<Schema>) {
        for key in name.fullnames(&self.current_namespace) {
            self.type_registry.insert(key, schema.clone());
        }
    }

    pub fn lookup_type(&self, name: &NameRef, context: &SchemaParseContext) -> Option<Rc<Schema>> {
        self.type_registry.get(&name.fullname(&context.current_namespace)).cloned()
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

    fn parse_with_context<'a>(value: &'a Value , context: &mut SchemaParseContext) -> Result<Rc<Self>, Error> {
        match *value {
            Value::String(ref t) => Schema::parse_type(t.as_str(), context),
            Value::Object(ref data) => Schema::parse_complex(data, context),
            Value::Array(ref data) => Schema::parse_union(data, context),
            _ => Err(ParseSchemaError::new("Must be a JSON string, object or array").into()),
        }
    }

    /// Parse a `serde_json::Value` representing a primitive Avro type into a
    /// `Schema`.
    fn parse_type(primitive: &str, context: &mut SchemaParseContext) -> Result<Rc<Self>, Error> {
        match primitive {
            "null" => Ok(Rc::new(Schema::Null)),
            "boolean" => Ok(Rc::new(Schema::Boolean)),
            "int" => Ok(Rc::new(Schema::Int)),
            "long" => Ok(Rc::new(Schema::Long)),
            "double" => Ok(Rc::new(Schema::Double)),
            "float" => Ok(Rc::new(Schema::Float)),
            "bytes" => Ok(Rc::new(Schema::Bytes)),
            "string" => Ok(Rc::new(Schema::String)),
            other => Schema::parse_reference(other, context),
        }
    }

    fn parse_reference(reference: &str, context: &mut SchemaParseContext) -> Result<Rc<Self>, Error> {
        NameRef::parse_str(reference, &context.current_namespace).map(Schema::TypeReference).map(Rc::new)
    }

    /// Parse a `serde_json::Value` representing a complex Avro type into a
    /// `Schema`.
    ///
    /// Avro supports "recursive" definition of types.
    /// e.g: {"type": {"type": "string"}}
    fn parse_complex(complex: &Map<String, Value>, context: &mut SchemaParseContext) -> Result<Rc<Self>, Error> {
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

    fn parse_named(complex: &Map<String, Value>, context: &mut SchemaParseContext) -> Result<Rc<Self>, Error> {
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
            context.register_type(name, schema.clone())
        }

        schema
    }

    /// Parse a `serde_json::Value` representing a Avro record type into a
    /// `Schema`.
    fn parse_record(complex: &Map<String, Value>, context: &mut SchemaParseContext) -> Result<Rc<Self>, Error> {
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

        Ok(Rc::new(Schema::Record {
            name,
            doc: complex.doc(),
            fields,
            lookup: Rc::new(lookup),
        }))
    }

    /// Parse a `serde_json::Value` representing a Avro enum type into a
    /// `Schema`.
    fn parse_enum(complex: &Map<String, Value>, context: &SchemaParseContext) -> Result<Rc<Self>, Error> {
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

        Ok(Rc::new(Schema::Enum {
            name,
            doc: complex.doc(),
            symbols,
        }))
    }

    /// Parse a `serde_json::Value` representing a Avro array type into a
    /// `Schema`.
    fn parse_array(complex: &Map<String, Value>, context: &mut SchemaParseContext) -> Result<Rc<Self>, Error> {
        complex
            .get("items")
            .ok_or_else(|| ParseSchemaError::new("No `items` in array").into())
            .and_then(|items| Schema::parse_with_context(items, context))
            .map(|schema| Rc::new(Schema::Array(schema)))
    }

    /// Parse a `serde_json::Value` representing a Avro map type into a
    /// `Schema`.
    fn parse_map(complex: &Map<String, Value>, context: &mut SchemaParseContext) -> Result<Rc<Self>, Error> {
        complex
            .get("values")
            .ok_or_else(|| ParseSchemaError::new("No `values` in map").into())
            .and_then(|items| Schema::parse_with_context(items, context))
            .map(|schema| Rc::new(Schema::Map(schema)))
    }

    /// Parse a `serde_json::Value` representing a Avro union type into a
    /// `Schema`.
    fn parse_union(items: &[Value], context: &mut SchemaParseContext) -> Result<Rc<Self>, Error> {
        /*
        items.iter()
            .map(|item| Schema::parse(item))
            .collect::<Result<_, _>>()
            .map(|schemas| Schema::Union(schemas))
        */

        if items.len() == 2 && items[0] == Value::String("null".to_owned()) {
            Schema::parse_with_context(&items[1], context).map(|s| Rc::new(Schema::Union(s)))
        } else {
            Err(ParseSchemaError::new("Unions only support null and type").into())
        }

        /*
        match items.as_slice() {
            // &[Value::String(ref null), ref x] | &[ref x, Value::String(ref null)] if null == "null" => {
            &[Value::String(ref null), ref x] if null == "null" => {
                Schema::parse(&x).map(|s| Schema::Union(Rc::new(s)))
            },
            _ => Err(ParseSchemaError::new("Unions only support null and type")),
        }
        */
    }

    /// Parse a `serde_json::Value` representing a Avro fixed type into a
    /// `Schema`.
    fn parse_fixed(complex: &Map<String, Value>, context: &mut SchemaParseContext) -> Result<Rc<Self>, Error> {
        let name = Name::parse(complex, &context.current_namespace)?;

        let size = complex
            .get("size")
            .and_then(|v| v.as_i64())
            .ok_or_else(|| ParseSchemaError::new("No `size` in fixed"))?;

        Ok(Rc::new(Schema::Fixed {
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
                let mut seq = serializer.serialize_seq(Some(2))?;
                seq.serialize_element("null")?;
                seq.serialize_element(&*inner.clone())?;
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
        assert_eq!(Schema::Array(Rc::new(Schema::String)), schema);
    }

    #[test]
    fn test_map_schema() {
        let schema = Schema::parse_str(r#"{"type": "map", "values": "double"}"#).unwrap();
        assert_eq!(Schema::Map(Rc::new(Schema::Double)), schema);
    }

    #[test]
    fn test_union_schema() {
        let schema = Schema::parse_str(r#"["null", "int"]"#).unwrap();
        assert_eq!(Schema::Union(Rc::new(Schema::Int)), schema);
    }

    #[test]
    fn test_union_unsupported_schema() {
        let schema = Schema::parse_str(r#"["null", "int", "string"]"#);
        assert!(schema.is_err());
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
                    schema: Rc::new(Schema::Long),
                    order: RecordFieldOrder::Ascending,
                    position: 0,
                },
                RecordField {
                    name: "b".to_string(),
                    doc: None,
                    default: None,
                    schema: Rc::new(Schema::String),
                    order: RecordFieldOrder::Ascending,
                    position: 1,
                },
            ],
            lookup: Rc::new(lookup),
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
    fn test_recursive_schema() {
        let schema = Schema::parse_str(r#"
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
                    schema: Rc::new(Schema::Fixed {
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
                    schema: Rc::new(Schema::TypeReference(NameRef::parse_str("fixed_test", &Some("com.test".to_string())).unwrap())),
                    order: RecordFieldOrder::Ascending,
                    position: 1,
                },
            ],
            lookup: Rc::new(lookup),
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
                    schema: Rc::new(Schema::Record{
                        name: Name { name: NameRef::parse_str("com.example.one", &None).unwrap(), aliases: None },
                        doc: None,
                        fields: Vec::new(),
                        lookup: Rc::new(HashMap::new())
                    }),
                    order: RecordFieldOrder::Ascending,
                    position: 0,
                },
                RecordField {
                    name: "two".to_string(),
                    doc: None,
                    default: None,
                    schema: Rc::new(Schema::Record{
                        name: Name { name: NameRef::parse_str("biz.good.two", &None).unwrap(), aliases: None },
                        doc: None,
                        fields: Vec::new(),
                        lookup: Rc::new(HashMap::new())
                    }),
                    order: RecordFieldOrder::Ascending,
                    position: 1,
                },
                RecordField {
                    name: "three".to_string(),
                    doc: None,
                    default: None,
                    schema: Rc::new(Schema::Record{
                        name: Name { name: NameRef::parse_str("com.example.sub.three", &None).unwrap(), aliases: None },
                        doc: None,
                        fields: Vec::new(),
                        lookup: Rc::new(HashMap::new())
                    }),
                    order: RecordFieldOrder::Ascending,
                    position: 2,
                },
                RecordField {
                    name: "four".to_string(),
                    doc: None,
                    default: None,
                    schema: Rc::new(Schema::Record{
                        name: Name { name: NameRef::parse_str("four.five", &None).unwrap(), aliases: None },
                        doc: None,
                        fields: Vec::new(),
                        lookup: Rc::new(HashMap::new())
                    }),
                    order: RecordFieldOrder::Ascending,
                    position: 3,
                },
            ],
            lookup: Rc::new(lookup),
        };

        assert_eq!(expected, schema);
    }

    #[test]
    fn test_no_documentation() {
        let schema = Schema::parse_str(
            r#"{"type": "enum", "name": "Coin", "symbols": ["heads", "tails"]}"#,
        ).unwrap();

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
}
