use {
    super::*,
    crate::{error::Error, schema::data::*, AvroResult},
    std::collections::HashSet,
};

// #[derive(Debug)]
// #[fail(display = "Invalid schema construction: {}", _0)]
// pub struct BuilderError(pub String);
//
// #[derive(Fail, Debug)]
// #[fail(display = "Schema validation failed: [{:?}]", _0)]
// pub struct BuilderErrors(pub Vec<BuilderError>);
//
// impl BuilderError {
//     pub fn new<S: ToString>(msg: S) -> Self {
//         Self(msg.to_string())
//     }
// }

/// Builder that allows the creation of new schemas in a programmatic fashion
#[derive(Default)]
pub struct SchemaBuilder {
    anon_type_ctr: usize,
    primitive_types: HashMap<&'static str, NameRef, WyHashBuilder>,

    namespace_names: DefaultStringInterner,
    type_names: DefaultStringInterner,
    aliases: HashMap<NameRef, NameRef, WyHashBuilder>,
    reverse_aliases: HashMap<NameRef, Vec<NameRef>, WyHashBuilder>,
    types: HashMap<NameRef, SchemaData, WyHashBuilder>,
}

/// Abstraction on schema types that need to be named as part of construction
pub trait NamedBuilder<'s> {
    /// Sets the name of the schema element
    fn name(name: &'s str) -> Self;

    /// Sets the namespace the schema element is within
    fn namespace(&mut self, namespace: Option<&'s str>) -> &mut Self;

    /// Adds an alias for this schema element
    fn alias(&mut self, alias: &'s str) -> &mut Self;
}

/// Simple type to reduce the noise in implementing builders that must be named
#[derive(Default)]
struct Naming<'s> {
    raw_name: &'s str,
    namespace: Option<&'s str>,
    aliases: Vec<&'s str>,
}

impl<'s> Naming<'s> {
    fn new(raw_name: &'s str) -> Self {
        Naming {
            raw_name,
            ..Default::default()
        }
    }

    fn into_ref(self, builder: &mut SchemaBuilder) -> AvroResult<NameRef> {
        builder.name_ref_alised(self.raw_name, self.namespace, self.aliases)
    }
}

/// Macro to reduce the noise in implementing names for schema elements that are named
macro_rules! impl_namebuilder {
    ($builder: ident, $ctor: expr) => {
        impl<'s> NamedBuilder<'s> for $builder<'s> {
            fn name(name: &'s str) -> Self {
                $ctor(Naming::new(name))
            }

            fn namespace(&mut self, namespace: Option<&'s str>) -> &mut Self {
                self.name.namespace = namespace;
                self
            }

            fn alias(&mut self, alias: &'s str) -> &mut Self {
                self.name.aliases.push(alias);
                self
            }
        }
    };
}

impl<'s> RecordBuilder<'s> {
    fn named(name: Naming<'s>) -> Self {
        RecordBuilder {
            pos: 0,
            documentation: None,
            fields: vec![],
            name,
        }
    }

    pub fn doc(&mut self, documentation: Documentation) -> &mut Self {
        self.documentation = documentation;
        self
    }

    pub fn field(&mut self, name: &str, type_ref: NameRef) -> RecordFieldBuilder {
        let data = RecordFieldData {
            name: name.to_string(),
            doc: None,
            default: None,
            schema: type_ref,
            order: None,
            position: self.pos,
        };
        self.pos += 1;
        self.fields.push(data);
        RecordFieldBuilder(self.fields.last_mut().unwrap())
    }

    pub fn build(self, builder: &mut SchemaBuilder) -> Result<NameRef, Error> {
        let name_ref = self.name.into_ref(builder)?;
        builder.add_type(
            name_ref,
            SchemaData::Record(self.documentation, self.fields),
        )
    }
}

impl_namebuilder!(RecordBuilder, RecordBuilder::named);

/// Builder to add additional details to a record field if necessary
pub struct RecordFieldBuilder<'r>(&'r mut RecordFieldData);

impl<'r> RecordFieldBuilder<'r> {
    /// Associate any documentation with this field
    pub fn doc(&mut self, documentation: Documentation) -> &mut Self {
        self.0.doc = documentation;
        self
    }

    /// Associate a default value with this field
    pub fn default(&mut self, default: Option<Value>) -> &mut Self {
        self.0.default = default;
        self
    }

    /// How, if needed, is this field ordered in Avro
    pub fn order(&mut self, order: Option<RecordFieldOrder>) -> &mut Self {
        self.0.order = order;
        self
    }
}

/// Builder that creates a record in a schema
pub struct RecordBuilder<'s> {
    name: Naming<'s>,
    pos: usize,
    documentation: Documentation,
    fields: Vec<RecordFieldData>,
}

/// Builder that creates a fixed buffer in a schema
pub struct FixedBuilder<'s> {
    name: Naming<'s>,
}

impl_namebuilder!(FixedBuilder, FixedBuilder::named);

impl<'s> FixedBuilder<'s> {
    fn named(name: Naming<'s>) -> Self {
        FixedBuilder { name }
    }

    pub fn size(self, size: usize, builder: &mut SchemaBuilder) -> AvroResult<NameRef> {
        let name_ref = self.name.into_ref(builder)?;
        builder.add_type(name_ref, SchemaData::Fixed(size))
    }
}

pub struct EnumBuilder<'s> {
    name: Naming<'s>,
    doc: Documentation,
}

impl_namebuilder!(EnumBuilder, EnumBuilder::named);

impl<'s> EnumBuilder<'s> {
    fn named(name: Naming<'s>) -> Self {
        EnumBuilder { name, doc: None }
    }

    pub fn doc(&mut self, doc: Documentation) -> &mut Self {
        self.doc = doc;
        self
    }

    pub fn symbols<I, S>(self, syms: I, builder: &mut SchemaBuilder) -> AvroResult<NameRef>
    where
        I: IntoIterator<Item = S>,
        S: ToString,
    {
        let name_ref = self.name.into_ref(builder)?;
        let syms = syms.into_iter().map(|s| s.to_string()).collect();
        builder.add_type(name_ref, SchemaData::Enum(self.doc, syms))
    }
}

macro_rules! impl_aggregated_builder {
    ($name: ident, $setter: ident, $data: expr) => {
        pub struct $name<'s> {
            name: Option<Naming<'s>>,
        }

        impl<'s> $name<'s> {
            pub fn new() -> Self {
                Self { name: None }
            }

            pub fn $setter(
                self,
                $setter: NameRef,
                builder: &mut SchemaBuilder,
            ) -> AvroResult<NameRef> {
                match self.name {
                    Some(name) => {
                        let name_ref = name.into_ref(builder)?;
                        builder.add_type(name_ref, $data($setter))
                    }
                    None => builder.add_anon_type($data($setter)),
                }
            }
        }

        impl<'s> NamedBuilder<'s> for $name<'s> {
            fn name(name: &'s str) -> Self {
                Self {
                    name: Some(Naming::new(name)),
                }
            }

            fn namespace(&mut self, namespace: Option<&'s str>) -> &mut Self {
                if let Some(ref mut name) = self.name {
                    name.namespace = namespace;
                }

                self
            }

            fn alias(&mut self, alias: &'s str) -> &mut Self {
                if let Some(ref mut name) = self.name {
                    name.aliases.push(alias);
                }

                self
            }
        }
    };
}

impl_aggregated_builder!(ArrayBuilder, items, SchemaData::Array);
impl_aggregated_builder!(MapBuilder, values, SchemaData::Map);

pub struct UnionBuilder(Vec<NameRef>);

impl UnionBuilder {
    pub fn new() -> Self {
        UnionBuilder(vec![])
    }

    pub fn build(self, builder: &mut SchemaBuilder) -> AvroResult<NameRef> {
        builder.add_anon_type(SchemaData::Union(self.0))
    }

    pub fn variant(&mut self, variant: NameRef) {
        self.0.push(variant)
    }
}

macro_rules! primitive_type_lookup {
    ($name: ident) => {
        pub fn $name(&self) -> NameRef {
            *self.primitive_types.get(stringify!($name)).unwrap()
        }
    };
}

impl SchemaBuilder {
    pub fn new() -> SchemaBuilder {
        let mut builder = SchemaBuilder::default();

        // Pre-register all the primitive types upfront
        let mut add_type = |name, kind| {
            let name_ref = builder.name_ref(name, None).unwrap();
            builder.primitive_types.insert(name, name_ref);
            builder.types.insert(name_ref, kind);
        };

        add_type("null", SchemaData::Null);
        add_type("boolean", SchemaData::Boolean);
        add_type("int", SchemaData::Int);
        add_type("long", SchemaData::Long);
        add_type("double", SchemaData::Double);
        add_type("float", SchemaData::Float);
        add_type("bytes", SchemaData::Bytes);
        add_type("string", SchemaData::String);

        builder
    }

    primitive_type_lookup!(null);
    primitive_type_lookup!(boolean);
    primitive_type_lookup!(int);
    primitive_type_lookup!(long);
    primitive_type_lookup!(double);
    primitive_type_lookup!(float);
    primitive_type_lookup!(bytes);
    primitive_type_lookup!(string);

    /// Allow to register a name that will be defined later while resolving primitives
    pub fn primitive_or_forward_declaration(
        &mut self,
        name: &str,
        namespace: Option<&str>,
    ) -> Result<NameRef, Error> {
        self.primitive_types
            .get(name)
            .map(|name_ref| Ok(*name_ref))
            .unwrap_or_else(|| self.name_ref(name, namespace))
    }

    pub fn record<'s>(&self, name: &'s str) -> RecordBuilder<'s> {
        RecordBuilder::name(name)
    }

    pub fn fixed<'s>(&self, name: &'s str) -> FixedBuilder<'s> {
        FixedBuilder::name(name)
    }

    pub fn enumeration<'s>(&self, name: &'s str) -> EnumBuilder<'s> {
        EnumBuilder::name(name)
    }

    pub fn union(&self) -> UnionBuilder {
        UnionBuilder::new()
    }

    pub fn array<'s>(&self) -> ArrayBuilder<'s> {
        ArrayBuilder::new()
    }

    pub fn named_array<'s>(&self, name: &'s str) -> ArrayBuilder<'s> {
        ArrayBuilder::name(name)
    }

    pub fn map<'s>(&self) -> MapBuilder<'s> {
        MapBuilder::new()
    }

    pub fn named_map<'s>(&self, name: &'s str) -> MapBuilder<'s> {
        MapBuilder::name(name)
    }

    pub fn build(self, root: NameRef) -> Result<Schema, Error> {
        self.validate(&root)?;
        Ok(Schema {
            namespace_names: self.namespace_names,
            type_names: self.type_names,
            aliases: self.aliases,
            reverse_aliases: self.reverse_aliases,
            types: self.types,
            root,
        })
    }
}

/// Validations performed during schema construction
impl SchemaBuilder {
    /// Validate the type was actually defined in the end
    fn validate_exists(&self, name: &NameRef) -> AvroResult<NameRef> {
        let mut cname = name.clone();
        while let Some(aliased) = self.aliases.get(&cname) {
            cname = *aliased;
        }

        match self.types.contains_key(&cname) {
            true => Ok(cname),
            false => Err(Error::UndefinedReference(self.basic_name(name))),
        }
    }

    fn validate_union(&self, name: &NameRef, variants: &Vec<NameRef>) -> Vec<Error> {
        let mut errors = vec![];
        let mut uniq = HashSet::new();

        for variant in variants {
            match self.validate_exists(variant) {
                Err(err) => errors.push(err),
                Ok(ref cname) => {
                    let variant_type = self.types.get(cname).unwrap();

                    if let SchemaData::Union(_) = variant_type {
                        errors.push(Error::GetNestedUnion);
                    }

                    if !uniq.insert(variant) {
                        errors.push(Error::GetUnionDuplicate(
                            self.basic_name(name),
                            self.basic_name(variant),
                        ));
                    }
                }
            }
        }

        errors
    }

    /// Validate the schema, reporting errors in construction
    pub fn validate(&self, root: &NameRef) -> Result<(), Error> {
        let mut errors = vec![];

        if !self.types.contains_key(root) {
            let msg = format!(
                "Given root `{:?}` for schema does not resolve to a type",
                self.basic_name(root)
            );
            errors.push(Error::SchemaBuilderInvalidSchema(msg));
        }

        for (name, schema_type) in self.types.iter() {
            match schema_type {
                SchemaData::Array(elem_name) | SchemaData::Map(elem_name) => {
                    if let Err(err) = self.validate_exists(elem_name) {
                        errors.push(Error::SchemaBuilderInvalidSchema(err.to_string()))
                    }
                }
                SchemaData::Record(_, fields) => {
                    fields
                        .iter()
                        .map(|field| self.validate_exists(&field.schema))
                        .filter(|validation| validation.is_err())
                        .for_each(|err| {
                            errors.push(Error::SchemaBuilderInvalidSchema(
                                err.unwrap_err().to_string(),
                            ))
                        });
                }
                SchemaData::Union(vars) => errors.append(&mut self.validate_union(name, vars)),
                _ => (),
            }
        }

        match errors.is_empty() {
            true => Ok(()),
            false => Err(Error::SchemaBuilderInvalidationFail(errors)),
        }
    }
}

impl SchemaBuilder {
    fn add_anon_type(&mut self, data: SchemaData) -> AvroResult<NameRef> {
        let anon_name = self.next_anon_id();
        let name = self.name_ref(&anon_name, None)?;
        self.add_type(name, data)
    }

    /// Makes an id that allows for naming anon types
    fn next_anon_id(&mut self) -> String {
        self.anon_type_ctr += 1;
        format!("${}", self.anon_type_ctr)
    }

    /// Register a type with the schema, returning an error if it already is defined
    fn add_type(&mut self, name: NameRef, value: SchemaData) -> Result<NameRef, Error> {
        if let Some(other) = self.types.insert(name, value) {
            // Check if we are seeing the same type in parsing in case its canonical form
            let value = self.types.get(&name).unwrap();
            if &other != value {
                let msg = format!(
                    "Type `{}` already defined as `{:?}` but being redefined as `{:?}`",
                    self.basic_name(&name),
                    other,
                    value
                );
                return Err(Error::SchemaBuilderInvalidSchema(msg));
            }
        }
        Ok(name)
    }

    fn name_ref(&mut self, name: &str, namespace: Option<&str>) -> AvroResult<NameRef> {
        let (name, namespace) = match name.rfind(".") {
            Some(pos) => {
                if name.len() == pos {
                    return Err(Error::SchemaBuilderInvalidSchema(format!(
                        "Invalid name `{}`",
                        name
                    )));
                }
                (&name[pos + 1..name.len()], Some(&name[0..pos]))
            }
            None => (name, namespace),
        };

        Ok(NameRef {
            name: self.type_names.get_or_intern(name),
            namespace: namespace.map(|ns| self.namespace_names.get_or_intern(ns)),
        })
    }

    fn name_ref_alised(
        &mut self,
        name: &str,
        namespace: Option<&str>,
        aliases: Vec<&str>,
    ) -> AvroResult<NameRef> {
        let name_ref = self.name_ref(name, namespace)?;

        for alias in aliases {
            let alias_name = self.name_ref(alias, namespace)?;

            // Deal with the all important forward mapping
            if let Some(existing) = self.aliases.insert(alias_name, name_ref) {
                let msg = format!(
                    "Multiple names found for alias `{}` known: `{}` vs given: `{}`",
                    self.basic_name(&alias_name),
                    self.basic_name(&existing),
                    self.basic_name(&alias_name)
                );
                return Err(Error::SchemaBuilderInvalidSchema(msg));
            }

            // Now deal with the reverse mapping
            self.reverse_aliases
                .entry(name_ref)
                .or_default()
                .push(alias_name);
        }

        Ok(name_ref)
    }

    fn basic_name(&self, nameref: &NameRef) -> String {
        let name = self.type_names.resolve(nameref.name).unwrap();
        match nameref
            .namespace
            .and_then(|x| self.namespace_names.resolve(x))
        {
            Some(ns) => format!("{}.{}", ns, name),
            None => name.to_string(),
        }
    }
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn test_basic_builder() -> Result<(), Error> {
        let mut builder = SchemaBuilder::new();
        let mut record_builder = builder.record("test");
        record_builder.field("field1", builder.int());
        record_builder.field("field2", builder.long());
        let record = record_builder.build(&mut builder)?;

        let schema = builder.build(record)?;

        let root = schema.root();
        match root {
            SchemaType::Record(record) => {
                assert_eq!(record.name().name(), "test");
                assert_eq!(record.fields()[0].name(), "field1");
                assert_eq!(record.fields()[1].name(), "field2");
                Ok(())
            }
            _ => panic!("Incorrect root element"),
        }
    }

    #[test]
    fn test_broken_fwd_decl() -> Result<(), Error> {
        let mut builder = SchemaBuilder::new();
        let decl_ref = builder.primitive_or_forward_declaration("true", None)?;
        let schema = builder.build(decl_ref);
        assert!(schema.is_err());

        Ok(())
    }
}
