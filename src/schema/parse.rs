use crate::AvroResult;
use {
    super::*,
    crate::{
        error::Error,
        schema::builder::{
            ArrayBuilder, EnumBuilder, FixedBuilder, MapBuilder, NamedBuilder, RecordBuilder,
            SchemaBuilder, UnionBuilder,
        },
    },
};

// /// Describes errors happened while parsing Avro schemas.
// #[derive(Fail, Debug)]
// #[fail(display = "Failed to parse schema: {}", _0)]
// pub struct ParseSchemaError(pub(crate) String);

// impl ParseSchemaError {
//     pub fn new<S: Into<String>>(msg: S) -> ParseSchemaError {
//         ParseSchemaError(msg.into())
//     }
// }
//
// impl From<BuilderError> for ParseSchemaError {
//     fn from(err: BuilderError) -> Self {
//         Self(err.0)
//     }
// }

pub(super) struct SchemaParser<'s> {
    current_ns: Option<&'s str>,
    builder: SchemaBuilder,
}

impl<'s> SchemaParser<'s> {
    pub(super) fn parse(value: &Value) -> Result<Schema, Error> {
        let mut parser = SchemaParser {
            current_ns: None,
            builder: SchemaBuilder::new(),
        };
        let root = parser.parse_schema(value)?;
        parser
            .builder
            .build(root)
            .map_err(|e| Error::SchemaResolution(e.to_string()))
    }

    /// Create a `AvroSchema` from a `serde_json::Value` representing a JSON Avro schema.
    pub(super) fn parse_schema(&mut self, value: &'s Value) -> AvroResult<NameRef> {
        let curr_ns = self.current_ns.clone();

        let name_ref = match *value {
            Value::String(ref t) => self.parse_typeref(t.as_str()),
            Value::Object(ref data) => self.parse_complex(data),
            Value::Array(ref data) => self.parse_union(data),
            _ => Err(Error::ParseSchemaFromValidJson),
        };

        if self.current_ns != curr_ns {
            self.current_ns = curr_ns;
        }

        name_ref
    }

    fn parse_typeref(&mut self, name: &str) -> Result<NameRef, Error> {
        self.builder
            .primitive_or_forward_declaration(name, self.current_ns.as_ref().map(|x| x.as_ref()))
    }

    /// Parse a bare named item and add to the string index if needed
    fn parse_name<'b, B>(&mut self, complex: &'s JsonMap) -> AvroResult<B>
    where
        B: NamedBuilder<'s>,
    {
        let mut builder = B::name(complex.name().ok_or_else(|| Error::GetNameField)?);

        if let namespace @ Some(_) = complex.string("namespace") {
            if namespace != self.current_ns {
                self.current_ns = namespace;
            }
        }

        builder.namespace(self.current_ns);

        if let Some(aliases) = complex
            .get("aliases")
            .and_then(|aliases| aliases.as_array())
        {
            for alias in aliases {
                if let Some(alias) = alias.as_str() {
                    builder.alias(alias);
                }
            }
        }

        Ok(builder)
    }

    /// Parse a `serde_json::Value` representing a complex Avro type into a `Schema`.
    ///
    /// Avro supports "recursive" definition of types.
    /// e.g: {"type": {"type": "string"}}
    fn parse_complex(&mut self, complex: &'s JsonMap) -> AvroResult<NameRef> {
        match complex.get("type") {
            Some(&Value::String(ref t)) => match t.as_str() {
                "record" => self.parse_record(complex),
                "enum" => self.parse_enum(complex),
                "array" => self.parse_array(complex),
                "map" => self.parse_map(complex),
                "fixed" => self.parse_fixed(complex),
                other => self.parse_typeref(other),
            },
            Some(&Value::Object(ref data)) => match data.get("type") {
                Some(ref value) => self.parse_schema(value),
                None => Err(
                    // Error::new(format!("Unknown complex type: {:?}", complex)).into(),
                    Error::GetComplexTypeField,
                ),
            },
            _ => Err(
                //ParseSchemaError::new("No `type` in complex type").into()),
                Error::GetComplexTypeField,
            ),
        }
    }

    /// Parse a `serde_json::Value` representing a Avro record type into a `Schema`.
    fn parse_record(&mut self, complex: &'s JsonMap) -> AvroResult<NameRef> {
        let mut record = self.parse_name::<RecordBuilder>(complex)?;
        record.doc(complex.doc());

        let items = complex
            .get("fields")
            .and_then(|fields| fields.as_array())
            .ok_or_else::<Error, _>(|| Error::GetRecordFieldsJson)?
            .iter()
            .filter_map(|field| field.as_object());

        for item in items {
            let name = item.name().ok_or_else(|| Error::GetNameFieldFromRecord)?;

            let schema = item
                .get("type")
                .ok_or_else(|| Error::GetTypeFieldFromRecord)
                .and_then(|type_| self.parse_schema(type_))?;

            let order = item
                .get("order")
                .and_then(|order| order.as_str())
                .and_then(|order| match order {
                    "ascending" => Some(RecordFieldOrder::Ascending),
                    "descending" => Some(RecordFieldOrder::Descending),
                    "ignore" => Some(RecordFieldOrder::Ignore),
                    _ => None,
                });

            record
                .field(name, schema)
                .order(order)
                .default(item.get("default").cloned())
                .doc(item.doc());
        }

        record.build(&mut self.builder).map_err(|e| e.into())
    }

    /// Parse a `serde_json::Value` representing a Avro enum type into a `Schema`.
    fn parse_enum(&mut self, complex: &'s JsonMap) -> AvroResult<NameRef> {
        let mut enum_builder = self.parse_name::<EnumBuilder>(complex)?;
        enum_builder.doc(complex.doc());

        let mut symbols = complex
            .get("symbols")
            .and_then(|v| v.as_array())
            .ok_or_else(|| Error::GetEnumSymbolsField)
            .map(|syms| syms.iter().filter_map(|sym| sym.as_str()).peekable())?;
        symbols.peek().ok_or_else(|| Error::GetEnumSymbols)?;

        enum_builder.symbols(symbols, &mut self.builder)
    }

    /// Parse a `serde_json::Value` representing a Avro array type into a `Schema`.
    fn parse_array(&mut self, complex: &'s JsonMap) -> AvroResult<NameRef> {
        let array_builder = self
            .parse_name::<ArrayBuilder>(complex)
            .unwrap_or_else(|_| ArrayBuilder::new());

        let items = complex
            .get("items")
            .ok_or_else(|| Error::GetArrayItemsField)
            .and_then(|items| self.parse_schema(items))?;

        array_builder.items(items, &mut self.builder)
    }

    /// Parse a `serde_json::Value` representing a Avro map type into a `Schema`.
    fn parse_map(&mut self, complex: &'s JsonMap) -> AvroResult<NameRef> {
        let map_builder = self
            .parse_name::<MapBuilder>(complex)
            .unwrap_or_else(|_| MapBuilder::new());

        let values = complex
            .get("values")
            .ok_or_else(|| Error::GetMapValuesField)
            .and_then(|items| self.parse_schema(items))?;

        map_builder.values(values, &mut self.builder)
    }

    /// Parse a `serde_json::Value` representing a Avro union type into a `Schema`.
    fn parse_union(&mut self, items: &'s [Value]) -> AvroResult<NameRef> {
        let mut union_builder = UnionBuilder::new();
        for item in items {
            union_builder.variant(self.parse_schema(item)?);
        }
        union_builder.build(&mut self.builder)
    }

    /// Parse a `serde_json::Value` representing a Avro fixed type into a `Schema`.
    fn parse_fixed(&mut self, complex: &'s JsonMap) -> AvroResult<NameRef> {
        let fixed_builder = self.parse_name::<FixedBuilder>(complex)?;

        let size = complex
            .get("size")
            .and_then(|v| v.as_i64().map(|x| x as usize))
            .ok_or_else(|| Error::GetFixedSizeField)?;

        fixed_builder.size(size, &mut self.builder)
    }
}
