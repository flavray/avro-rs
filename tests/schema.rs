//! Port of https://github.com/apache/avro/blob/release-1.9.1/lang/py/test/test_schema.py
use avro_rs::{Schema, SchemaType};
use lazy_static::lazy_static;

lazy_static! {
    static ref PRIMITIVE_EXAMPLES: Vec<(&'static str, bool)> = vec![
        (r#""null""#, true),
        (r#"{"type": "null"}"#, true),
        (r#""boolean""#, true),
        (r#"{"type": "boolean"}"#, true),
        (r#""string""#, true),
        (r#"{"type": "string"}"#, true),
        (r#""bytes""#, true),
        (r#"{"type": "bytes"}"#, true),
        (r#""int""#, true),
        (r#"{"type": "int"}"#, true),
        (r#""long""#, true),
        (r#"{"type": "long"}"#, true),
        (r#""float""#, true),
        (r#"{"type": "float"}"#, true),
        (r#""double""#, true),
        (r#"{"type": "double"}"#, true),
        (r#""true""#, false),
        (r#"true"#, false),
        (r#"{"no_type": "test"}"#, false),
        (r#"{"type": "panther"}"#, false),
    ];
    static ref FIXED_EXAMPLES: Vec<(&'static str, bool)> = vec![
        (r#"{"type": "fixed", "name": "Test", "size": 1}"#, true),
        (
            r#"{
                "type": "fixed",
                "name": "MyFixed",
                "namespace": "org.apache.hadoop.avro",
                "size": 1
            }"#,
            true
        ),
        (r#"{"type": "fixed", "name": "Missing size"}"#, false),
        (r#"{"type": "fixed", "size": 314}"#, false),
    ];
    static ref ENUM_EXAMPLES: Vec<(&'static str, bool)> = vec![
        (r#"{"type": "enum", "name": "Test", "symbols": ["A", "B"]}"#, true),
        (
            r#"{
                "type": "enum",
                "name": "Status",
                "symbols": "Normal Caution Critical"
            }"#,
            false
        ),
        (
            r#"{
                "type": "enum",
                "name": [ 0, 1, 1, 2, 3, 5, 8 ],
                "symbols": ["Golden", "Mean"]
            }"#,
            false
        ),
        (
            r#"{
                "type": "enum",
                "symbols" : ["I", "will", "fail", "no", "name"]
            }"#,
            false
        ),
        (
            r#"{
                "type": "enum",
                 "name": "Test"
                 "symbols" : ["AA", "AA"]
            }"#,
            false
        ),
    ];
    static ref ARRAY_EXAMPLES: Vec<(&'static str, bool)> = vec![
        (r#"{"type": "array", "items": "long"}"#, true),
        (
            r#"{
                "type": "array",
                 "items": {"type": "enum", "name": "Test", "symbols": ["A", "B"]}
            }"#,
            true
        ),
    ];
    static ref MAP_EXAMPLES: Vec<(&'static str, bool)> = vec![
        (r#"{"type": "map", "values": "long"}"#, true),
        (
            r#"{
                "type": "map",
                "values": {"type": "enum", "name": "Test", "symbols": ["A", "B"]}
            }"#,
            true
        ),
    ];
    static ref UNION_EXAMPLES: Vec<(&'static str, bool)> = vec![
        (r#"["string", "null", "long"]"#, true),
        (r#"["null", "null"]"#, false),
        (r#"["long", "long"]"#, false),
        (
            r#"[
                {"type": "array", "items": "long"}
                {"type": "array", "items": "string"}
            ]"#,
            false
        ),
    ];
    static ref RECORD_EXAMPLES: Vec<(&'static str, bool)> = vec![
        (
            r#"{
                "type": "record",
                "name": "Test",
                "fields": [{"name": "f", "type": "long"}]
            }"#,
            true
        ),
        /*
        // TODO: (#91) figure out why "type": "error" seems to be valid (search in spec) and uncomment
        (
            r#"{
                "type": "error",
                "name": "Test",
                "fields": [{"name": "f", "type": "long"}]
            }"#,
            true
        ),
        */
        (
            r#"{
                "type": "record",
                "name": "Node",
                "fields": [
                    {"name": "label", "type": "string"},
                    {"name": "children", "type": {"type": "array", "items": "Node"}}
                ]
            }"#,
            true
        ),
        (
            r#"{
                "type": "record",
                "name": "Lisp",
                "fields": [
                    {
                        "name": "value",
                        "type": [
                            "null", "string",
                            {
                                "type": "record",
                                "name": "Cons",
                                "fields": [
                                    {"name": "car", "type": "Lisp"},
                                    {"name": "cdr", "type": "Lisp"}
                                ]
                            }
                        ]
                    }
                ]
            }"#,
            true
        ),
        (
            r#"{
                "type": "record",
                "name": "HandshakeRequest",
                "namespace": "org.apache.avro.ipc",
                "fields": [
                    {"name": "clientHash", "type": {"type": "fixed", "name": "MD5", "size": 16}},
                    {"name": "clientProtocol", "type": ["null", "string"]},
                    {"name": "serverHash", "type": "MD5"},
                    {"name": "meta", "type": ["null", {"type": "map", "values": "bytes"}]}
                ]
            }"#, true
        ),
        (
            r#"{
                "type":"record",
                "name":"HandshakeResponse",
                "namespace":"org.apache.avro.ipc",
                "fields":[
                    {
                        "name":"match",
                        "type":{
                           "type":"enum",
                           "name":"HandshakeMatch",
                           "symbols":["BOTH", "CLIENT", "NONE"]
                        }
                    },
                    {"name":"serverProtocol", "type":["null", "string"]},
                    {
                        "name":"serverHash",
                        "type":["null", {"name":"MD5", "size":16, "type":"fixed"}]
                    },
                    {
                        "name":"meta",
                        "type":["null", {"type":"map", "values":"bytes"}]
                    }
                ]
            }"#,
            true
        ),
        (
            r#"{
                "type":"record",
                "name":"HandshakeResponse",
                "namespace":"org.apache.avro.ipc",
                "fields":[
                    {
                        "name":"match",
                        "type":{
                            "type":"enum",
                            "name":"HandshakeMatch",
                            "symbols":["BOTH", "CLIENT", "NONE"]
                        }
                    },
                    {"name":"serverProtocol", "type":["null", "string"]},
                    {
                        "name":"serverHash",
                        "type":["null", { "name":"MD5", "size":16, "type":"fixed"}]
                    },
                    {"name":"meta", "type":["null", { "type":"map", "values":"bytes"}]}
                ]
            }"#,
            true
        ),
        // Unions may not contain more than one schema with the same type, except for the named
        // types record, fixed and enum. For example, unions containing two array types or two map
        // types are not permitted, but two types with different names are permitted.
        // (Names permit efficient resolution when reading and writing unions.)
        (
            r#"{
                "type": "record",
                "name": "ipAddr",
                "fields": [
                    {
                        "name": "addr",
                        "type": [
                            {"name": "IPv6", "type": "fixed", "size": 16},
                            {"name": "IPv4", "type": "fixed", "size": 4}
                        ]
                    }
                ]
            }"#,
            true
        ),
        (
            r#"{
                "type": "record",
                "name": "Address",
                "fields": [
                    {"type": "string"},
                    {"type": "string", "name": "City"}
                ]
            }"#,
            false
        ),
        (
            r#"{
                "type": "record",
                "name": "Event",
                "fields": [{"name": "Sponsor"}, {"name": "City", "type": "string"}]
            }"#,
            false
        ),
        (
            r#"{
                "type": "record",
                "fields": "His vision, from the constantly passing bars,"
                "name",
                "Rainer"
            }"#,
            false
        ),
        (
            r#"{
                "name": ["Tom", "Jerry"],
                "type": "record",
                "fields": [{"name": "name", "type": "string"}]
            }"#,
            false
        ),
    ];
    static ref DOC_EXAMPLES: Vec<(&'static str, bool)> = vec![
        (
            r#"{
                "type": "record",
                "name": "TestDoc",
                "doc":  "Record Doc string",
                "fields": [{"name": "name", "type": "string", "doc" : "Field Doc String"}]
            }"#,
            true
        ),
        (
            r#"{"type": "enum", "name": "Test", "symbols": ["A", "B"], "doc": "Doc String"}"#,
            true
        ),
    ];
    static ref OTHER_ATTRIBUTES_EXAMPLES: Vec<(&'static str, bool)> = vec![
        (
            r#"{
                "type": "record",
                "name": "TestRecord",
                "cp_string": "string",
                "cp_int": 1,
                "cp_array": [ 1, 2, 3, 4],
                "fields": [
                    {"name": "f1", "type": "string", "cp_object": {"a":1,"b":2}},
                    {"name": "f2", "type": "long", "cp_null": null}
                ]
            }"#,
            true
        ),
        (
            r#"{"type": "map", "values": "long", "cp_boolean": true}"#,
            true
        ),
        (
            r#"{
                "type": "enum",
                 "name": "TestEnum",
                 "symbols": [ "one", "two", "three" ],
                 "cp_float" : 1.0
            }"#,
            true
        ),
        (r#"{"type": "long", "date": "true"}"#, true),
    ];
    static ref DECIMAL_LOGICAL_TYPE: Vec<(&'static str, bool)> = vec![
        /*
        // TODO: (#93) support logical types and uncomment
        (
            r#"{
                "type": "fixed",
                "logicalType": "decimal",
                "name": "TestDecimal",
                "precision": 4,
                "size": 10,
                "scale": 2
            }"#,
            true
        ),
        (
            r#"{
                "type": "bytes",
                "logicalType": "decimal",
                "precision": 4,
                "scale": 2
            }"#,
            true
        ),
        (
            r#"{
                "type": "bytes",
                "logicalType": "decimal",
                "precision": 2,
                "scale": -2
            }"#,
            false
        ),
        (
            r#"{
                "type": "bytes",
                "logicalType": "decimal",
                "precision": -2,
                "scale": 2
            }"#,
            false
        ),
        (
            r#"{
                "type": "bytes",
                "logicalType": "decimal",
                "precision": 2,
                "scale": 3
            }"#,
            false
        ),
        (
            r#"{
                "type": "fixed",
                "logicalType": "decimal",
                "name": "TestDecimal",
                "precision": -10,
                "scale": 2,
                "size": 5
            }"#,
            false
        ),
        (
            r#"{
                "type": "fixed",
                "logicalType": "decimal",
                "name": "TestDecimal",
                "precision": 2,
                "scale": 3,
                "size": 2
            }"#,
            false
        ),
        (
            r#"{
                "type": "fixed",
                "logicalType": "decimal",
                "name": "TestDecimal",
                "precision": 2,
                "scale": 2,
                "size": -2
            }"#,
            false
        ),
        */
    ];
    static ref DECIMAL_LOGICAL_TYPE_ATTRIBUTES: Vec<(&'static str, bool)> = vec![
        /*
        // TODO: (#93) support logical types and attributes and uncomment
        (
            r#"{
                "type": "fixed",
                "logicalType": "decimal",
                "name": "TestDecimal",
                "precision": 4,
                "scale": 2,
                "size": 2
            }"#,
            true
        ),
        (
            r#"{
                "type": "bytes",
                "logicalType": "decimal",
                "precision": 4
            }"#,
            true
        ),
        */
    ];
    static ref DATE_LOGICAL_TYPE: Vec<(&'static str, bool)> = vec![
    /*
        // TODO: (#93) support logical types and uncomment
        (r#"{"type": "int", "logicalType": "date"}"#, true),
        (r#"{"type": "int", "logicalType": "date1"}"#, false),
        (r#"{"type": "long", "logicalType": "date"}"#, false),
    */
    ];
    static ref TIMEMILLIS_LOGICAL_TYPE: Vec<(&'static str, bool)> = vec![
    /*
        // TODO: (#93) support logical types and uncomment
        (r#"{"type": "int", "logicalType": "time-millis"}"#, true),
        (r#"{"type": "int", "logicalType": "time-milis"}"#, false),
        (r#"{"type": "long", "logicalType": "time-millis"}"#, false),
    */
    ];
    static ref TIMEMICROS_LOGICAL_TYPE: Vec<(&'static str, bool)> = vec![
        /*
        // TODO: (#93) support logical types and uncomment
        (r#"{"type": "long", "logicalType": "time-micros"}"#, true),
        (r#"{"type": "long", "logicalType": "time-micro"}"#, false),
        (r#"{"type": "int", "logicalType": "time-micros"}"#, false),
        */
    ];
    static ref TIMESTAMPMILLIS_LOGICAL_TYPE: Vec<(&'static str, bool)> = vec![
        /*
        // TODO: (#93) support logical types and uncomment
        (r#"{"type": "long", "logicalType": "timestamp-millis"}"#, true),
        (r#"{"type": "long", "logicalType": "timestamp-milis"}"#, false),
        (r#"{"type": "int", "logicalType": "timestamp-millis"}"#, false),
        */
    ];
    static ref TIMESTAMPMICROS_LOGICAL_TYPE: Vec<(&'static str, bool)> = vec![
        /*
        // TODO: (#93) support logical types and uncomment
        (r#"{"type": "long", "logicalType": "timestamp-micros"}"#, true),
        (r#"{"type": "long", "logicalType": "timestamp-micro"}"#, false),
        (r#"{"type": "int", "logicalType": "timestamp-micros"}"#, false),
        */
    ];
    static ref EXAMPLES: Vec<(&'static str, bool)> = Vec::new()
        .iter()
        .cloned()
        .chain(PRIMITIVE_EXAMPLES.iter().cloned())
        .chain(FIXED_EXAMPLES.iter().cloned())
        .chain(ENUM_EXAMPLES.iter().cloned())
        .chain(ARRAY_EXAMPLES.iter().cloned())
        .chain(MAP_EXAMPLES.iter().cloned())
        .chain(UNION_EXAMPLES.iter().cloned())
        .chain(RECORD_EXAMPLES.iter().cloned())
        .chain(DOC_EXAMPLES.iter().cloned())
        .chain(OTHER_ATTRIBUTES_EXAMPLES.iter().cloned())
        .chain(DECIMAL_LOGICAL_TYPE.iter().cloned())
        .chain(DECIMAL_LOGICAL_TYPE_ATTRIBUTES.iter().cloned())
        .chain(DATE_LOGICAL_TYPE.iter().cloned())
        .chain(TIMEMILLIS_LOGICAL_TYPE.iter().cloned())
        .chain(TIMEMICROS_LOGICAL_TYPE.iter().cloned())
        .chain(TIMESTAMPMILLIS_LOGICAL_TYPE.iter().cloned())
        .chain(TIMESTAMPMICROS_LOGICAL_TYPE.iter().cloned())
        .collect();
    static ref VALID_EXAMPLES: Vec<(&'static str, bool)> =
        EXAMPLES.iter().cloned().filter(|s| s.1).collect();
}

#[test]
fn test_correct_recursive_extraction() {
    let raw_outer_schema = r#"{
        "type": "record",
        "name": "X",
        "fields": [
            {
                "name": "y",
                "type": {
                    "type": "record",
                    "name": "Y",
                    "fields": [
                        {
                            "name": "Z",
                            "type": "X"
                        }
                    ]
                }
            }
        ]
    }"#;
    let outer_schema = Schema::parse_str(raw_outer_schema).unwrap();
    if let SchemaType::Record(x_record) = outer_schema.root() {
        let fields = x_record.fields();
        let inner_schema = fields[0].schema();
        if let SchemaType::Record(y_record) = inner_schema {
            if let SchemaType::Record(recur_record) = y_record.fields()[0].schema() {
                assert_eq!("X", recur_record.name().name());
            }
        } else {
            panic!("inner schema {} should have been a record", inner_schema)
        }
    } else {
        panic!("outer schema {} should have been a record", outer_schema)
    }
}

#[test]
fn test_parse() {
    for (raw_schema, valid) in EXAMPLES.iter() {
        let schema = Schema::parse_str(raw_schema);
        if *valid {
            assert!(
                schema.is_ok(),
                "schema {} was supposed to be valid; error: {:?}",
                raw_schema,
                schema,
            )
        } else {
            assert!(
                schema.is_err(),
                "schema {} was supposed to be invalid",
                raw_schema
            )
        }
    }
}

#[test]
/// Test that the string generated by an Avro Schema object is, in fact, a valid Avro schema.
fn test_valid_cast_to_string_after_parse() {
    for (raw_schema, _) in VALID_EXAMPLES.iter() {
        let schema = Schema::parse_str(raw_schema).unwrap();
        Schema::parse_str(schema.canonical_form().as_str()).unwrap();
    }
}

#[test]
/// 1. Given a string, parse it to get Avro schema "original".
/// 2. Serialize "original" to a string and parse that string to generate Avro schema "round trip".
/// 3. Ensure "original" and "round trip" schemas are equivalent.
fn test_equivalence_after_round_trip() {
    for (raw_schema, _) in VALID_EXAMPLES.iter() {
        let original_schema = Schema::parse_str(raw_schema).unwrap();
        // TODO - Is documentation part of PCF?
        // TODO - PCF should possibly be redone as it could just work on the serde tree
        // TODO - And has not historically supported documentation
        //let round_trip_schema =
        //    Schema::parse_str(original_schema.canonical_form().as_str()).unwrap();
        let equiv_schema = serde_json::to_string(&original_schema).unwrap();
        let round_trip_schema = Schema::parse_str(&equiv_schema).unwrap();
        assert_eq!(original_schema, round_trip_schema);
    }
}

// The fullname is determined in one of the following ways:
//  * A name and namespace are both specified.  For example,
//    one might use "name": "X", "namespace": "org.foo"
//    to indicate the fullname "org.foo.X".
//  * A fullname is specified.  If the name specified contains
//    a dot, then it is assumed to be a fullname, and any
//    namespace also specified is ignored.  For example,
//    use "name": "org.foo.X" to indicate the
//    fullname "org.foo.X".
//  * A name only is specified, i.e., a name that contains no
//    dots.  In this case the namespace is taken from the most
//    tightly enclosing schema or protocol.  For example,
//    if "name": "X" is specified, and this occurs
//    within a field of the record definition ///    of "org.foo.Y", then the fullname is "org.foo.X".

// References to previously defined names are as in the latter
// two cases above: if they contain a dot they are a fullname, if
// they do not contain a dot, the namespace is the namespace of
// the enclosing definition.

// Primitive type names have no namespace and their names may
// not be defined in any namespace. A schema may only contain
// multiple definitions of a fullname if the definitions are
// equivalent.

macro_rules! test_namesetup {
    ($name: expr, $ns: expr, $ns_spec: expr, $expected: expr) => {
        let schema = Schema::parse_str(&format!(
            r#"{{
                "name": "{}", "namespace": {}, "aliases": null, "type": "record",
                "fields": [{{"name":"x", "type":["null", "int"]}}]
            }}"#,
            $name, $ns
        ))
        .unwrap();

        match schema.root() {
            SchemaType::Record(record) => {
                let name = record.name();
                let fullname = name.fullname($ns_spec);
                assert_eq!($expected, fullname);
            }
            _ => panic!("Incorrect parse"),
        };
    };
}

#[test]
fn test_fullname_name_and_namespace_specified() {
    test_namesetup!("a", r#""o.a.h""#, None, "o.a.h.a");
}

#[test]
fn test_fullname_fullname_and_namespace_specified() {
    test_namesetup!("a.b.c.d", r#""o.a.h""#, None, "a.b.c.d");
}

#[test]
fn test_fullname_name_and_default_namespace_specified() {
    test_namesetup!("a", "null", Some("b.c.d"), "b.c.d.a");
}

#[test]
fn test_fullname_fullname_and_default_namespace_specified() {
    test_namesetup!("a.b.c.d", "null", Some("o.a.h"), "a.b.c.d");
}

#[test]
fn test_fullname_fullname_namespace_and_default_namespace_specified() {
    test_namesetup!("a.b.c.d", r#""o.a.a""#, Some("o.a.h"), "a.b.c.d");
}

#[test]
fn test_fullname_name_namespace_and_default_namespace_specified() {
    test_namesetup!("a", r#""o.a.a""#, Some("o.a.h"), "o.a.a.a");
}

#[test]
fn test_doc_attributes() {
    fn assert_doc(schema: SchemaType) {
        match schema {
            SchemaType::Enum(enum_) => assert!(enum_.doc().is_some()),
            SchemaType::Record(record) => assert!(record.doc().is_some()),
            _ => (),
        }
    }

    for (raw_schema, _) in DOC_EXAMPLES.iter() {
        let original_schema = Schema::parse_str(raw_schema).unwrap();
        assert_doc(original_schema.root());
        if let SchemaType::Record(record) = original_schema.root() {
            for f in record.iter_fields() {
                assert_doc(f.schema())
            }
        }
    }
}

/*
TODO: (#94) add support for user-defined attributes and uncomment (may need some tweaks to compile)
#[test]
fn test_other_attributes() {
    fn assert_attribute_type(attribute: (String, serde_json::Value)) {
        match attribute.1.as_ref() {
            "cp_boolean" => assert!(attribute.2.is_bool()),
            "cp_int" => assert!(attribute.2.is_i64()),
            "cp_object" => assert!(attribute.2.is_object()),
            "cp_float" => assert!(attribute.2.is_f64()),
            "cp_array" => assert!(attribute.2.is_array()),
        }
    }

    for (raw_schema, _) in OTHER_ATTRIBUTES_EXAMPLES.iter() {
        let schema = Schema::parse_str(raw_schema).unwrap();
        // all inputs have at least some user-defined attributes
        assert!(schema.other_attributes.is_some());
        for prop in schema.other_attributes.unwrap().iter() {
            assert_attribute_type(prop);
        }
        if let Schema::Record { fields, .. } = schema {
           for f in fields {
               // all fields in the record have at least some user-defined attributes
               assert!(f.schema.other_attributes.is_some());
               for prop in f.schema.other_attributes.unwrap().iter() {
                   assert_attribute_type(prop);
               }
           }
        }
    }
}
*/

#[test]
fn test_root_error_is_not_swallowed_on_parse_error() -> Result<(), String> {
    let raw_schema = r#"/not/a/real/file"#;
    let error = Schema::parse_str(raw_schema).unwrap_err();

    // TODO: (#82) this should be a ParseSchemaError wrapping the JSON error
    match error.downcast::<serde_json::error::Error>() {
        Ok(e) => {
            assert!(
                e.to_string().contains("expected value at line 1 column 1"),
                e.to_string()
            );
            Ok(())
        }
        Err(e) => Err(format!("Expected serde_json::error::Error, got {:?}", e)),
    }
}

/*
// TODO: (#93) add support for logical type and attributes and uncomment (may need some tweaks to compile)
#[test]
fn test_decimal_valid_type_attributes() {
    let fixed_decimal = Schema::parse_str(DECIMAL_LOGICAL_TYPE_ATTRIBUTES[0]).unwrap();
    assert_eq!(4, fixed_decimal.get_attribute("precision"));
    assert_eq!(2, fixed_decimal.get_attribute("scale"));
    assert_eq!(2, fixed_decimal.get_attribute("size"));

    let bytes_decimal = Schema::parse_str(DECIMAL_LOGICAL_TYPE_ATTRIBUTES[1]).unwrap();
    assert_eq!(4, bytes_decimal.get_attribute("precision"));
    assert_eq!(0, bytes_decimal.get_attribute("scale"));
}
*/
