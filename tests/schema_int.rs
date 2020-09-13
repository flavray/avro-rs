use std::{fs::File, io::BufReader, path::PathBuf, str::FromStr};

use avro_rs::{schema::*, Error};
use serde_json::{Map, Number, Value};

fn fixture(name: &str) -> Value {
    let mut dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    dir.push("jsonschemas");
    dir.push(name);

    let file = File::open(dir).expect("Invalid fixture file");
    let reader = BufReader::new(file);

    serde_json::from_reader(reader).expect("Invalid fixture json")
}

macro_rules! assert_lookup {
    ($item: expr, $match: pat => $extraction: expr) => {
        match $item {
            $match => $extraction,
            _ => panic!("Incorrect data shape"),
        }
    };
}

macro_rules! assert_field {
    ($fld: expr, $name: expr, $default: expr, $pat: pat) => {
        assert_eq!($fld.name(), $name);
        assert_eq!($fld.default(), $default);
        if let $pat = $fld.schema() {
            $fld.schema()
        } else {
            panic!("Incorrect data shape")
        }
    };

    ($fld: expr, $name: expr, $default: expr, $pat: pat => $extraction: expr) => {
        assert_eq!($fld.name(), $name);
        assert_eq!($fld.default(), $default);
        assert_lookup!($fld.schema(), $pat => $extraction)
    }
}

macro_rules! assert_serialisation {
    ($reference: expr, $schema: expr) => {
        let actual = serde_json::to_value($schema)?;
        if $reference != actual {
            eprintln!("Reference: {}", serde_json::to_string_pretty(&$reference)?);
            eprintln!("Actual: {}", serde_json::to_string_pretty(&actual)?);
            panic!("Serde mismatch");
        }
    };
}

#[test]
fn test_array() -> Result<(), Error> {
    let array_raw = fixture("array");
    let schema = Schema::parse(&array_raw)?;

    assert_lookup!(schema.root(), SchemaType::Array(array) => {
        assert_eq!(array.items(), SchemaType::Int);
        // Not all arrays have names, but this one *was* named
        let name = array.name().unwrap();
        assert_eq!(name.name(), "test");
        assert_eq!(name.namespace(), None);
        assert_eq!(name.aliases(), None);
    });

    assert_serialisation!(array_raw, schema);

    Ok(())
}

#[test]
fn test_aliases() -> Result<(), Error> {
    let aliases_raw = fixture("aliases");
    let schema = Schema::parse(&aliases_raw)?;

    assert_lookup!(schema.root(), SchemaType::Record(record) => {
        assert_eq!(record.name().name(), "LongList");
        assert_eq!(record.name().aliases().unwrap()[0].name(), "LinkedLongs");

        let fields = record.fields();
        assert_eq!(fields.len(), 2);

        assert_field!(fields[0], "value", None, SchemaType::Long);
        assert_field!(fields[1], "next", None, SchemaType::Union(union_) => {
            assert!(union_.is_nullable());
        });
    });

    assert_serialisation!(aliases_raw, schema);

    Ok(())
}

#[test]
fn test_bigrecord() -> Result<(), Error> {
    let bigrecord_raw = fixture("bigrecord");
    let schema = Schema::parse(&bigrecord_raw)?;

    assert_lookup!(schema.root(), SchemaType::Record(record) => {
        assert_eq!(record.doc(), Some("Top level Doc."));
        assert_eq!(record.name().name(), "RootRecord");

        let fields = record.fields();
        assert_eq!(fields.len(), 14);

        assert_field!(fields[0], "mylong", None, SchemaType::Long);
        assert_eq!(fields[0].doc(), Some("mylong field doc."));
        assert_field!(fields[1], "nestedrecord", None, SchemaType::Record(nested) => {
            let nested_fields = nested.fields();
            assert_eq!(nested_fields.len(), 3);
            assert_field!(nested_fields[0], "inval1", None, SchemaType::Double);
            assert_field!(nested_fields[1], "inval2", None, SchemaType::String);
            assert_field!(nested_fields[2], "inval3", None, SchemaType::Int);
        });
        assert_field!(fields[2], "mymap", None, SchemaType::Map(e) => {
            assert_eq!(e.items(), SchemaType::Int);
        });
        assert_field!(fields[3], "recordmap", None, SchemaType::Map(elem) => {
            assert_eq!(elem.items(), fields[1].schema());
        });
        assert_field!(fields[4], "myarray", None, SchemaType::Array(elem) => {
            assert_eq!(elem.items(), SchemaType::Double);
        });
        assert_field!(fields[5], "myenum", None, SchemaType::Enum(enum_) => {
            assert_eq!(enum_.name().name(), "ExampleEnum");
            assert_eq!(enum_.symbols(), vec!["zero", "one", "two", "three"]);
        });
        assert_field!(fields[6], "myunion", None, SchemaType::Union(union_) => {
            let variants = union_.variants();
            assert_eq!(variants.len(), 3);
            assert_eq!(variants[0], SchemaType::Null);
            assert_lookup!(variants[1], SchemaType::Map(map_) => {
                assert_eq!(map_.items(), SchemaType::Int)
            });
            assert_eq!(variants[2], SchemaType::Float);
            assert!(union_.is_nullable());
        });
        assert_field!(fields[7], "anotherunion", None, SchemaType::Union(union_) => {
            let variants = union_.variants();
            assert_eq!(variants.len(), 2);
            assert_eq!(variants[0], SchemaType::Bytes);
            assert_eq!(variants[1], SchemaType::Null);
            assert!(!union_.is_nullable());
        });
        assert_field!(fields[8], "mybool", None, SchemaType::Boolean);
        assert_field!(fields[9], "anothernested", None, x @ SchemaType::Record(_) => {
            assert_eq!(x, fields[1].schema());
        });
        assert_field!(fields[10], "myfixed", None, SchemaType::Fixed(fixed) => {
            assert_eq!(fixed.size(), 16);
            assert_eq!(fixed.name().name(), "md5");
        });
        assert_field!(fields[11], "anotherint", None, SchemaType::Int);
        assert_field!(fields[12], "bytes", None, SchemaType::Bytes);
        assert_field!(fields[13], "null", None, SchemaType::Null);
    });

    assert_serialisation!(bigrecord_raw, schema);

    Ok(())
}

#[test]
fn test_bigrecord2() -> Result<(), Error> {
    let bigrecord2_raw = fixture("bigrecord2");
    let schema = Schema::parse(&bigrecord2_raw)?;

    assert_lookup!(schema.root(), SchemaType::Record(record) => {
        assert_eq!(record.name().name(), "RootRecord");

        let fields = record.fields();
        assert_eq!(fields.len(), 12);

        assert_field!(fields[0], "mylong", None, SchemaType::Double);
        assert_field!(fields[1], "anotherint", None, SchemaType::Int);
        assert_field!(fields[2], "bytes", None, SchemaType::Bytes);
        assert_field!(fields[3], "nestedrecord", None, SchemaType::Record(nested_record) => {
            assert_eq!(nested_record.name().name(), "Nested");
            let nested_fields = nested_record.fields();
            assert_eq!(nested_fields.len(), 3);
            assert_field!(nested_fields[0], "inval3", None, SchemaType::Int);
            assert_field!(nested_fields[1], "inval2", None, SchemaType::String);
            assert_field!(nested_fields[2], "inval1", None, SchemaType::Double);
        });
        assert_field!(fields[4], "mymap", None, SchemaType::Map(map) => {
            assert_eq!(map.items(), SchemaType::Long);
        });
        assert_field!(fields[5], "myarray", None, SchemaType::Array(array) => {
            assert_eq!(array.items(), SchemaType::Double);
        });
        assert_field!(fields[6], "myenum", None, SchemaType::Enum(enum_) => {
            assert_eq!(enum_.name().name(), "ExampleEnum");
            assert_eq!(enum_.symbols(), vec!["three", "two", "one", "zero"]);
        });
        assert_field!(fields[7], "myunion", None, SchemaType::Union(union_) => {
            let variants = union_.variants();
            assert_eq!(variants.len(), 3);
            assert_eq!(variants[0], SchemaType::Null);
            assert_eq!(variants[1], SchemaType::Float);
            assert_lookup!(variants[2], SchemaType::Map(map) => {
                assert_eq!(map.items(), SchemaType::Float);
            })
        });
        assert_field!(fields[8], "anotherunion", None, SchemaType::Bytes);
        assert_field!(fields[9], "anothernested", None, x @ SchemaType::Record(_) => {
            assert_eq!(x, fields[3].schema());
        });
        assert_field!(fields[10], "newbool", None, SchemaType::Boolean);
        assert_field!(fields[11], "myfixed", None, SchemaType::Union(union_) => {
            let variants = union_.variants();
            assert_eq!(variants.len(), 2);
            assert_eq!(variants[0], SchemaType::Float);
            assert_lookup!(variants[1], SchemaType::Fixed(fixed) => {
                assert_eq!(fixed.name().name(), "md5");
                assert_eq!(fixed.size(), 16);
            });
        });
    });

    assert_serialisation!(bigrecord2_raw, schema);

    Ok(())
}

#[test]
fn test_bigrecord_r() -> Result<(), Error> {
    let bigrecord_r_raw = fixture("bigrecord_r");
    let schema = Schema::parse(&bigrecord_r_raw)?;

    assert_lookup!(schema.root(), SchemaType::Record(record) => {
        assert_eq!(record.name().name(), "RootRecord");

        let fields = record.fields();
        assert_eq!(fields.len(), 20);

        assert_field!(fields[0], "mylong", None, SchemaType::Long);
        assert_field!(fields[1], "mymap", None, SchemaType::Map(map) => {
            assert_eq!(map.items(), SchemaType::Int);
        });
        assert_field!(fields[2], "nestedrecord", None, SchemaType::Record(nested) => {
            assert_eq!(nested.name().name(), "Nested");
            let nested_fields = nested.fields();
            assert_eq!(nested_fields.len(), 3);
            assert_field!(nested_fields[0], "inval2", None, SchemaType::String);
            assert_field!(nested_fields[1], "inval1", None, SchemaType::Double);
            assert_field!(nested_fields[2], "inval3", None, SchemaType::Int);
        });
        assert_field!(fields[3], "recordmap", None, SchemaType::Map(map) => {
            assert_eq!(map.items(), fields[2].schema());
        });
        assert_field!(
            fields[4], "withDefaultValue",
            {
                let mut map = Map::new();
                map.insert("s1".to_string(), Value::String("\"sval\\u8352\"".to_string()));
                map.insert("d1".to_string(), Value::Number(Number::from_str("5.67")?));
                map.insert("i1".to_string(), Value::Number(99.into()));
                Some(&Value::Object(map))
            },
            SchemaType::Record(nested) => {
                assert_eq!(nested.name().name(), "WithDefaultValue");
                let nested_fields = nested.fields();
                assert_eq!(nested_fields.len(), 3);
                assert_field!(nested_fields[0], "s1", None, SchemaType::String);
                assert_field!(nested_fields[1], "d1", None, SchemaType::Double);
                assert_field!(nested_fields[2], "i1", None, SchemaType::Int);
            }
        );
        assert_field!(
            fields[5], "union1WithDefaultValue", Some(&Value::String("sval".to_string())),
            SchemaType::Union(union_) => {
                let variants = union_.variants();
                assert_eq!(variants[0], SchemaType::String);
                assert_eq!(variants[1], SchemaType::Int);
                assert!(!union_.is_nullable());
            }
        );
        assert_field!(
            fields[6], "union2WithDefaultValue", Some(&Value::Null),
            SchemaType::Union(union_) => {
                let variants = union_.variants();
                assert_eq!(variants[0], SchemaType::Null);
                assert_eq!(variants[1], SchemaType::String);
                assert!(union_.is_nullable());
            }
        );
        assert_field!(fields[7], "myarray", None, SchemaType::Array(array) => {
            assert_eq!(array.items(), SchemaType::Double)
        });
        assert_field!(
            fields[8], "myarraywithDefaultValue",
            Some(&Value::Array(vec![Value::Number(2.into()), Value::Number(3.into())])),
            SchemaType::Array(array) => assert_eq!(array.items(), SchemaType::Int)
        );
        assert_field!(fields[9], "myenum", None, SchemaType::Enum(enum_) => {
            assert_eq!(enum_.name().name(), "ExampleEnum");
            assert_eq!(enum_.symbols(), vec!["zero", "one", "two", "three"]);
        });
        assert_field!(fields[10], "myunion", Some(&Value::Null), SchemaType::Union(union_) => {
            let variants = union_.variants();
            assert_eq!(variants.len(), 3);
            assert_eq!(variants[0], SchemaType::Null);
            assert_lookup!(variants[1], SchemaType::Map(map) => {
                assert_eq!(map.items(), SchemaType::Int);
            });
            assert_eq!(variants[2], SchemaType::Float);
            assert!(union_.is_nullable());
        });
        assert_field!(fields[11], "anotherunion", None, SchemaType::Union(union_) => {
            let variants = union_.variants();
            assert_eq!(variants.len(), 2);
            assert_eq!(variants[0], SchemaType::Bytes);
            assert_eq!(variants[1], SchemaType::Null);
            assert!(!union_.is_nullable());
        });
        assert_field!(fields[12], "mybool", None, SchemaType::Boolean);
        assert_field!(fields[13], "anothernested", None, r @ SchemaType::Record(_) => {
            assert_eq!(r, fields[2].schema())
        });
        assert_field!(
            fields[14], "rwd",
            {
                let mut map = Map::new();
                let mut submap = Map::new();
                submap.insert("inval2".to_string(), Value::String("hello".to_string()));
                submap.insert("inval1".to_string(), Value::Number(Number::from_str("4.23")?));
                submap.insert("inval3".to_string(), Value::Number(100.into()));
                map.insert("rwd_f1".to_string(), Value::Object(submap));
                Some(&Value::Object(map))
            },
            SchemaType::Record(record) => {
                assert_eq!(record.name().name(), "RecordWithDefault");
                let nested_fields = record.fields();
                assert_eq!(nested_fields.len(), 1);
                assert_lookup!(nested_fields[0].schema(), x @ SchemaType::Record(_) => {
                    assert_eq!(x, fields[2].schema())
                });
            }
        );
        assert_field!(fields[15], "myfixed", None, SchemaType::Fixed(fixed) => {
            assert_eq!(fixed.size(), 16);
            assert_eq!(fixed.name().name(), "md5");
        });
        assert_field!(
            fields[16], "myfixedwithDefaultValue",
            Some(&Value::String("\u{0001}".to_string())),
            SchemaType::Union(union_) => {
                let variants = union_.variants();
                assert_lookup!(variants[0], SchemaType::Fixed(fixed) => {
                    assert_eq!(fixed.name().name(), "val");
                    assert_eq!(fixed.size(), 1);
                });
                assert_eq!(variants[1], SchemaType::Null);
            }
        );
        assert_field!(fields[17], "anotherint", None, SchemaType::Int);
        assert_field!(fields[18], "bytes", None, SchemaType::Bytes);
        assert_field!(
            fields[19], "byteswithDefaultValue",
            Some(&Value::String("\u{00ff}\u{00AA}".to_string())),
            SchemaType::Union(union_) => {
                let variants = union_.variants();
                assert_eq!(variants.len(), 2);
                assert_eq!(variants[0], SchemaType::Bytes);
                assert_eq!(variants[1], SchemaType::Null);
            }
        );
    });

    assert_serialisation!(bigrecord_r_raw, schema);

    Ok(())
}

#[test]
fn test_circulardep() -> Result<(), Error> {
    let circdep_raw = fixture("circulardep");
    let schema = Schema::parse(&circdep_raw)?;

    assert_lookup!(schema.root(), SchemaType::Record(record) => {
        assert_eq!(record.name().name(), "Item");
        let fields = record.fields();

        assert_eq!(fields.len(), 2);
        assert_field!(fields[0], "id", None, SchemaType::String);
        assert_field!(fields[1], "entities", None, SchemaType::Union(union_) => {
            let variants = union_.variants();

            assert_eq!(variants[0], SchemaType::Null);
            assert_lookup!(&variants[1], SchemaType::Record(rec) => {
                assert_eq!(rec.name().name(), "Information");
                let rec_fields = rec.fields();
                assert_eq!(rec_fields.len(), 3);
                assert_field!(rec_fields[0], "id", None, SchemaType::String);
                assert_field!(rec_fields[1], "externalItem", None,
                              circ @ SchemaType::Record(_) => assert_eq!(circ, schema.root()));
                assert_field!(rec_fields[2], "innerUnion", None, SchemaType::Union(inner) => {
                    let inner_variants = inner.variants();
                    assert_eq!(inner_variants.len(), 2);
                    assert_eq!(inner_variants[0], SchemaType::Int);
                    assert_eq!(inner_variants[1], SchemaType::Double);
                    assert!(!inner.is_nullable());
                });
            });

            assert!(union_.is_nullable());
        });
    });

    // TODO - If we support metadata, we should enable the serde test
    // assert_serialisation!(circdep_raw, schema);

    Ok(())
}

#[test]
fn test_crossref() -> Result<(), Error> {
    let crossref_raw = fixture("crossref");
    let schema = Schema::parse(&crossref_raw)?;

    assert_lookup!(schema.root(), SchemaType::Record(record) => {
        assert_eq!(record.name().name(), "A");
        let fields = record.fields();

        assert_eq!(fields.len(), 1);
        assert_field!(fields[0], "edges", None, SchemaType::Array(array) => {
            assert_lookup!(array.items(), SchemaType::Record(edges) => {
                assert_eq!(edges.name().name(), "B");

                let edge_fields = edges.fields();
                assert_eq!(edge_fields.len(), 1);

                assert_field!(edge_fields[0], "child", None, SchemaType::Union(union_) => {
                    let variants = union_.variants();
                    assert_eq!(variants.len(), 2);

                    assert_lookup!(&variants[0], SchemaType::Record(child_rec) => {
                        assert_eq!(child_rec.name().name(), "C");

                        let child_fields = child_rec.fields();
                        assert_eq!(child_fields.len(), 1);

                        assert_field!(child_fields[0], "x", None, SchemaType::Map(map) => {
                            assert_eq!(map.items(), schema.root());
                        });
                    });

                    assert_eq!(variants[1], SchemaType::Int);
                });
            });
        });
    });

    assert_serialisation!(crossref_raw, schema);

    Ok(())
}

#[test]
fn test_empty_record() -> Result<(), Error> {
    let empty_raw = fixture("empty_record");
    let schema = Schema::parse(&empty_raw)?;

    assert_lookup!(schema.root(), SchemaType::Record(record) => {
        assert_eq!(record.name().name(), "Empty");
        assert!(record.fields().is_empty());
    });

    assert_serialisation!(empty_raw, schema);

    Ok(())
}

#[test]
fn test_enum() -> Result<(), Error> {
    let enum_raw = fixture("enum");
    let schema = Schema::parse(&enum_raw)?;

    assert_lookup!(schema.root(), SchemaType::Enum(enum_) => {
        assert_eq!(enum_.name().name(), "myenum");
        assert_eq!(enum_.symbols(), vec!["zero", "int", "two", "three"]);
    });

    assert_serialisation!(enum_raw, schema);

    Ok(())
}

#[test]
fn test_fixed() -> Result<(), Error> {
    let fixed_raw = fixture("fixed");
    let schema = Schema::parse(&fixed_raw)?;

    assert_lookup!(schema.root(), SchemaType::Fixed(fixed) => {
        assert_eq!(fixed.size(), 16);
        assert_eq!(fixed.name().name(), "md5");
    });

    assert_serialisation!(fixed_raw, schema);

    Ok(())
}

#[test]
fn test_int() -> Result<(), Error> {
    let int_raw = fixture("int");
    let schema = Schema::parse(&int_raw)?;
    assert_eq!(schema.root(), SchemaType::Int);
    assert_serialisation!(int_raw, schema);
    Ok(())
}

#[test]
fn test_map() -> Result<(), Error> {
    let map_raw = fixture("map");
    let schema = Schema::parse(&map_raw)?;
    assert_lookup!(schema.root(), SchemaType::Map(map) => {
        assert_eq!(map.name().unwrap().name(), "noname");
        assert_eq!(map.items(), SchemaType::Int);
    });

    // TODO - If we support metadata, we should enable the serde test
    // assert_serialisation!(map_raw, schema);

    Ok(())
}

#[test]
fn test_nested_error() -> Result<(), Error> {
    let error_raw = fixture("nested.error");
    let schema = Schema::parse(&error_raw);
    assert!(schema.is_err());
    Ok(())
}

#[test]
fn test_padded_record() -> Result<(), Error> {
    let padded_record_raw = fixture("padded_record");
    let schema = Schema::parse(&padded_record_raw)?;
    assert_lookup!(schema.root(), SchemaType::Record(record) => {
        assert_eq!(record.name().name(), "PaddedRecord");
        let fields = record.fields();
        assert_eq!(fields.len(), 2);

        assert_field!(fields[0], "index", None, SchemaType::Int);
        assert_field!(fields[1], "padding", None, SchemaType::Bytes);
    });
    assert_serialisation!(padded_record_raw, schema);
    Ok(())
}

#[test]
fn test_primitive_types() -> Result<(), Error> {
    let primitives_raw = fixture("primitivetypes");
    let schema = Schema::parse(&primitives_raw)?;

    assert_lookup!(schema.root(), SchemaType::Record(record) => {
        assert_eq!(record.name().name(), "TestPrimitiveTypes");
        assert_eq!(record.fields().len(), 9);
        assert_eq!(record.field("Null").unwrap().schema(), SchemaType::Null);
        assert_eq!(record.field("Boolean").unwrap().schema(), SchemaType::Boolean);
        assert_eq!(record.field("Int").unwrap().schema(), SchemaType::Int);
        assert_eq!(record.field("Long").unwrap().schema(), SchemaType::Long);
        assert_eq!(record.field("Float").unwrap().schema(), SchemaType::Float);
        assert_eq!(record.field("Double").unwrap().schema(), SchemaType::Double);
        assert_eq!(record.field("Bytes").unwrap().schema(), SchemaType::Bytes);
        assert_eq!(record.field("String").unwrap().schema(), SchemaType::String);
        assert_eq!(record.field("SecondNull").unwrap().schema(), SchemaType::Null);
    });

    assert_serialisation!(primitives_raw, schema);
    Ok(())
}

#[test]
fn test_rec_in_rec() -> Result<(), Error> {
    let rec_in_rec_raw = fixture("recinrec");
    let schema = Schema::parse(&rec_in_rec_raw)?;

    assert_lookup!(schema.root(), SchemaType::Record(record) => {
        assert_eq!(record.name().name(), "Rec1");

        let rec1_fields = record.fields();
        assert_eq!(rec1_fields.len(), 3);

        assert_field!(rec1_fields[0], "val1", None, SchemaType::Long);
        assert_field!(rec1_fields[1], "val2", None, SchemaType::Record(rec2) => {
            assert_eq!(rec2.name().name(), "Rec2");

            let rec2_fields = rec2.fields();
            assert_eq!(rec2_fields.len(), 2);
            assert_field!(rec2_fields[0], "inval1", None, SchemaType::Double);
            assert_field!(rec2_fields[1], "inval2", None, SchemaType::Int);
        });
        assert_field!(rec1_fields[2], "val3", None, SchemaType::Float);
    });

    // TODO - If we support metadata, we should enable the serde test
    // assert_serialisation!(rec_in_rec_raw, schema);

    Ok(())
}

#[test]
fn test_record() -> Result<(), Error> {
    let record_raw = fixture("record");
    let schema = Schema::parse(&record_raw)?;

    assert_lookup!(schema.root(), SchemaType::Record(record) => {
        assert_eq!(record.name().name(), "LongList");

        let fields = record.fields();
        assert_eq!(fields.len(), 2);

        assert_field!(fields[0], "value", None, SchemaType::Long);
        assert_field!(fields[1], "next", None, SchemaType::Int);
    });

    // TODO - If we support metadata, we should enable the serde test
    // assert_serialisation!(record_raw, schema);

    Ok(())
}

#[test]
fn test_record2() -> Result<(), Error> {
    let record2_raw = fixture("record2");
    let schema = Schema::parse(&record2_raw)?;

    assert_lookup!(schema.root(), SchemaType::Record(record) => {
        assert_eq!(record.name().name(), "LongList");

        let fields = record.fields();
        assert_eq!(fields.len(), 3);

        assert_field!(fields[0], "value", None, SchemaType::Long);
        assert_field!(fields[1], "next", None, SchemaType::Union(union_) => {
            let variants = union_.variants();
            assert_eq!(variants, vec![SchemaType::Int, SchemaType::Float]);
        });
        assert_field!(fields[2], "hello", None, SchemaType::Array(array) => {
            assert_eq!(array.items(), SchemaType::Float);
        });
    });

    assert_serialisation!(record2_raw, schema);

    Ok(())
}

#[test]
fn test_recursive() -> Result<(), Error> {
    let recursive_raw = fixture("recursive");
    let schema = Schema::parse(&recursive_raw)?;

    assert_lookup!(schema.root(), SchemaType::Record(record) => {
        assert_eq!(record.name().name(), "LongList");

        let fields = record.fields();
        assert_eq!(fields.len(), 2);

        assert_field!(fields[0], "value", None, SchemaType::Long);
        assert_field!(fields[1], "next", None, SchemaType::Union(union_) => {
            let variants = union_.variants();
            assert_eq!(variants, vec![schema.root(), SchemaType::Null]);
        });
    });

    assert_serialisation!(recursive_raw, schema);

    Ok(())
}

#[test]
fn test_reuse() -> Result<(), Error> {
    let reuse_raw = fixture("reuse");
    let schema = Schema::parse(&reuse_raw)?;

    assert_lookup!(schema.root(), SchemaType::Record(record) => {
        assert_eq!(record.name().name(), "outer");

        let fields = record.fields();
        assert_eq!(fields.len(), 2);

        assert_field!(fields[0], "f1", None, SchemaType::Record(f1) => {
            assert_eq!(f1.name().name(), "F");

            let f1_fields = f1.fields();
            assert_eq!(f1_fields.len(), 2);

            assert_field!(f1_fields[0], "g1", None, SchemaType::Boolean);
            assert_field!(f1_fields[1], "g2", None, SchemaType::Int);
        });

        assert_field!(fields[1], "f2", None, r @ SchemaType::Record(_) => {
            assert_eq!(r, fields[0].schema())
        });
    });

    assert_serialisation!(reuse_raw, schema);

    Ok(())
}

#[test]
fn test_tree1() -> Result<(), Error> {
    let tree1_raw = fixture("tree1");
    let schema = Schema::parse(&tree1_raw)?;

    assert_lookup!(schema.root(), SchemaType::Record(record) => {
        assert_eq!(record.name().name(), "Node");

        let fields = record.fields();
        assert_eq!(fields.len(), 2);

        assert_field!(fields[0], "payload", Some(&Value::Number(0.into())), SchemaType::Int);
        assert_field!(fields[1], "edges", None, SchemaType::Array(array) => {
            assert_lookup!(array.items(), SchemaType::Record(edge) => {
                assert_eq!(edge.name().name(), "Edge");

                let edge_fields = edge.fields();
                assert_eq!(edge_fields.len(), 2);

                assert_field!(edge_fields[0], "child", None, r @ SchemaType::Record(_) => {
                    assert_eq!(r, schema.root())
                });
                assert_field!(edge_fields[1], "label", None, SchemaType::String);
            });
        });
    });

    assert_serialisation!(tree1_raw, schema);

    Ok(())
}

#[test]
fn test_tree2() -> Result<(), Error> {
    let tree2_raw = fixture("tree2");
    let schema = Schema::parse(&tree2_raw)?;

    assert_lookup!(schema.root(), SchemaType::Record(record) => {
        assert_eq!(record.name().name(), "Node");

        let fields = record.fields();
        assert_eq!(fields.len(), 2);

        assert_field!(fields[0], "payload", Some(&Value::Number(0.into())), SchemaType::Int);
        assert_field!(fields[1], "edges", None, SchemaType::Map(map) => {
            assert_eq!(map.items(), schema.root());
        });
    });

    assert_serialisation!(tree2_raw, schema);

    Ok(())
}

#[test]
fn test_tweet() -> Result<(), Error> {
    let tweet_raw = fixture("tweet");
    let schema = Schema::parse(&tweet_raw)?;

    assert_lookup!(schema.root(), SchemaType::Record(record) => {
        assert_eq!(record.name().name(), "AvroTweet");
        assert_eq!(record.name().namespace(), Some("com.bifflabs.grok.model.twitter.avro"));

        let fields = record.fields();
        assert_eq!(fields.len(), 9);

        assert_field!(fields[0], "ID", None, SchemaType::Long);
        assert_field!(fields[1], "text", None, SchemaType::String);
        assert_field!(fields[2], "authorScreenName", None, SchemaType::String);
        assert_field!(fields[3], "authorProfileImageURL", None, SchemaType::String);
        assert_field!(fields[4], "authorUserID", None, SchemaType::Union(union_) => {
            let mut var_it = union_.iter_variants();
            assert_eq!(var_it.next(), Some(SchemaType::Null));
            assert_eq!(var_it.next(), Some(SchemaType::Long));
            assert_eq!(var_it.next(), None);
        });
        assert_field!(fields[5], "location", None, SchemaType::Union(union_) => {
            assert!(union_.is_nullable());
            let variants = union_.variants();
            assert_eq!(variants[0], SchemaType::Null);
            assert_lookup!(&variants[1], SchemaType::Record(location) => {
                assert_eq!(location.name().name(), "AvroPoint");
                assert_eq!(location.name().namespace(), Some("com.bifflabs.grok.model.common.avro"));

                let loc_flds = location.fields();
                assert_eq!(loc_flds.len(), 2);
                assert_field!(loc_flds[0], "latitude", None, SchemaType::Double);
                assert_field!(loc_flds[1], "longitude", None, SchemaType::Double);
            });
        });
        assert_field!(fields[6], "placeID", None, SchemaType::Union(union_) => {
            assert!(union_.is_nullable());
            assert_eq!(union_.variants(), vec![SchemaType::Null, SchemaType::String]);
        });
        assert_field!(fields[7], "createdAt", None, SchemaType::Record(created_at) => {
            assert_eq!(created_at.name().name(), "AvroDateTime");
            assert_eq!(created_at.name().namespace(), Some("com.bifflabs.grok.model.common.avro"));

            let c_flds = created_at.fields();
            assert_eq!(c_flds.len(), 1);
            assert_field!(c_flds[0], "dateTimeString", None, SchemaType::String);
        });
        assert_field!(fields[8], "metadata", None, SchemaType::Record(metadata) => {
            assert_eq!(metadata.name().name(), "AvroTweetMetadata");
            // TODO - Should this be namespaced then?
            //assert_eq!(metadata.name().namespace(), Some("com.bifflabs.grok.model.common.avro"));

            let meta_flds = metadata.fields();
            assert_eq!(meta_flds.len(), 9);

            assert_field!(meta_flds[0], "inReplyToScreenName", None, SchemaType::Record(reply) => {
                assert_eq!(reply.name().name(), "AvroKnowableOptionString");
                assert_eq!(reply.name().namespace(), Some("com.bifflabs.grok.model.common.avro"));

                let reply_flds = reply.fields();
                assert_eq!(reply_flds.len(), 2);
                assert_field!(reply_flds[0], "known", None, SchemaType::Boolean);
                assert_field!(reply_flds[1], "data", None, SchemaType::Union(union_) => {
                    assert!(union_.is_nullable());
                    assert_eq!(union_.variants(), vec![SchemaType::Null, SchemaType::String]);
                });
            });

            assert_field!(meta_flds[1], "mentionedScreenNames", None, SchemaType::Record(mention) => {
                assert_eq!(mention.name().name(), "AvroKnowableListString");
                assert_eq!(mention.name().namespace(), Some("com.bifflabs.grok.model.common.avro"));

                let men_flds = mention.fields();
                assert_eq!(men_flds.len(), 2);
                assert_field!(men_flds[0], "known", None, SchemaType::Boolean);
                assert_field!(men_flds[1], "data", None, SchemaType::Array(array) => {
                    assert_eq!(array.items(), SchemaType::String)
                });
            });

            assert_field!(meta_flds[2], "links", None, val @ SchemaType::Record(_) => {
                assert_eq!(val, meta_flds[1].schema())
            });

            assert_field!(meta_flds[3], "hashtags", None, val @ SchemaType::Record(_) => {
                assert_eq!(val, meta_flds[1].schema())
            });

            assert_field!(meta_flds[4], "isBareCheckin", None, SchemaType::Record(bare) => {
                assert_eq!(bare.name().name(), "AvroKnowableBoolean");
                assert_eq!(bare.name().namespace(), Some("com.bifflabs.grok.model.common.avro"));

                let flds = bare.fields();
                assert_eq!(flds.len(), 2);

                assert_field!(flds[0], "known", None, SchemaType::Boolean);
                assert_field!(flds[1], "data", None, SchemaType::Boolean);
            });

            assert_field!(meta_flds[5], "isBareRetweet", None, val @ SchemaType::Record(_) => {
                assert_eq!(val, meta_flds[4].schema())
            });

            assert_field!(meta_flds[6], "isRetweet", None, val @ SchemaType::Record(_) => {
                assert_eq!(val, meta_flds[4].schema())
            });

            assert_field!(meta_flds[7], "venueID", None, val @ SchemaType::Record(_) => {
                assert_eq!(val, meta_flds[0].schema())
            });

            assert_field!(meta_flds[8], "venuePoint", None, SchemaType::Record(venue) => {
                assert_eq!(venue.name().name(), "AvroKnowableOptionPoint");
                assert_eq!(venue.name().namespace(), Some("com.bifflabs.grok.model.common.avro"));

                let flds = venue.fields();
                assert_eq!(flds.len(), 2);

                assert_field!(flds[0], "known", None, SchemaType::Boolean);
                assert_field!(flds[1], "data", None, SchemaType::Union(union_) => {
                    assert_lookup!(fields[5].schema(), SchemaType::Union(point_) => {
                        let p_variants = point_.variants();
                        let point_type = p_variants[1].clone();
                        assert_eq!(union_.variants(), vec![SchemaType::Null, point_type]);
                    });
                });
            });

        });
    });

    assert_serialisation!(tweet_raw, schema);

    Ok(())
}

#[test]
fn test_union() -> Result<(), Error> {
    let union_raw = fixture("union");
    let schema = Schema::parse(&union_raw)?;

    assert_lookup!(schema.root(), SchemaType::Union(union_) => {
        assert!(!union_.is_nullable());
        let variants = union_.variants();
        assert_eq!(variants[0], SchemaType::Int);
        assert_eq!(variants[1], SchemaType::Long);
        assert_eq!(variants[2], SchemaType::Float);
    });

    assert_serialisation!(union_raw, schema);

    Ok(())
}

#[test]
fn test_union_array_union() -> Result<(), Error> {
    let union_array_union_raw = fixture("union_array_union");
    let schema = Schema::parse(&union_array_union_raw)?;

    assert_lookup!(schema.root(), SchemaType::Record(record) => {
        assert_eq!(record.name().name(), "r1");

        let fields = record.fields();
        assert_eq!(fields.len(), 1);

        assert_field!(fields[0], "f1", None, SchemaType::Union(union_) => {
            assert!(union_.is_nullable());
            let variants = union_.variants();
            assert_eq!(variants.len(), 2);
            assert_eq!(variants[0], SchemaType::Null);

            assert_lookup!(variants[1], SchemaType::Array(array) => {
                assert_lookup!(array.items(), SchemaType::Union(nested) => {
                    assert!(nested.is_nullable());
                    assert_eq!(nested.variants(), vec![SchemaType::Null, SchemaType::Int]);
                });
            });
        });
    });

    assert_serialisation!(union_array_union_raw, schema);

    Ok(())
}

#[test]
fn test_union_conflict() -> Result<(), Error> {
    let union_conflict_raw = fixture("union_conflict");
    let schema = Schema::parse(&union_conflict_raw)?;

    assert_lookup!(schema.root(), SchemaType::Record(record) => {
        assert_eq!(record.name().name(), "uc");

        let fields = record.fields();
        assert_eq!(fields.len(), 3);

        assert_field!(fields[0], "rev_t", None, SchemaType::String);
        assert_field!(fields[1], "data", None, SchemaType::Bytes);
        assert_field!(fields[2], "rev", None, SchemaType::Union(union_) => {
            assert!(!union_.is_nullable());
            assert_eq!(union_.variants(), vec![SchemaType::String, SchemaType::Null]);
        });
    });

    assert_serialisation!(union_conflict_raw, schema);

    Ok(())
}

#[test]
fn union_map_union() -> Result<(), Error> {
    let union_map_union_raw = fixture("union_map_union");
    let schema = Schema::parse(&union_map_union_raw)?;

    assert_lookup!(schema.root(), SchemaType::Record(record) => {
        assert_eq!(record.name().name(), "r1");

        let fields = record.fields();
        assert_eq!(fields.len(), 2);

        assert_field!(fields[0], "id", None, SchemaType::String);
        assert_field!(fields[1], "val", None, SchemaType::Union(union_) => {
            assert!(!union_.is_nullable());

            let variants = union_.variants();
            assert_eq!(variants.len(), 2);

            assert_lookup!(&variants[0], SchemaType::Map(map) => {
                assert_lookup!(map.items(), SchemaType::Record(r3) => {
                    assert_eq!(r3.name().name(), "r3");

                    let r3_flds = r3.fields();
                    assert_eq!(r3_flds.len(), 3);

                    assert_field!(r3_flds[0], "name", None, SchemaType::String);
                    assert_field!(r3_flds[1], "data", None, SchemaType::Bytes);
                    assert_field!(r3_flds[2], "rev", None, SchemaType::Union(rev) => {
                        assert!(!rev.is_nullable());
                        assert_eq!(rev.variants(), vec![SchemaType::String, SchemaType::Null]);
                    });
                });
            });
            assert_eq!(variants[1], SchemaType::Null);
        });
    });

    assert_serialisation!(union_map_union_raw, schema);

    Ok(())
}

#[test]
fn union_with_map() -> Result<(), Error> {
    let union_with_map_raw = fixture("unionwithmap");
    let schema = Schema::parse(&union_with_map_raw)?;

    assert_lookup!(schema.root(), SchemaType::Union(union_) => {
        assert!(!union_.is_nullable());

        let variants = union_.variants();
        assert_eq!(variants.len(), 3);

        assert_eq!(variants[0], SchemaType::Int);
        assert_eq!(variants[1], SchemaType::Long);
        assert_lookup!(variants[2], SchemaType::Map(map) => {
            assert_lookup!(map.items(), SchemaType::Union(u2) => {
                assert!(!u2.is_nullable());
                assert_eq!(u2.variants(), vec![SchemaType::Int, SchemaType::Long]);
            });
        });
    });

    assert_serialisation!(union_with_map_raw, schema);

    Ok(())
}

#[test]
fn test_verbose_int() -> Result<(), Error> {
    let verbose_int_raw = fixture("verboseint");
    let schema = Schema::parse(&verbose_int_raw)?;

    assert_eq!(schema.root(), SchemaType::Int);

    // TODO - If we support metadata, we should enable the serde test
    // assert_serialisation!(verbose_int_raw, schema);
    Ok(())
}

#[test]
fn test_nested() -> Result<(), Error> {
    let nested_raw = fixture("nested");
    let schema = Schema::parse(&nested_raw)?;

    assert_lookup!(schema.root(), SchemaType::Record(record) => {
        let fields = record.fields();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name(), "value");
        assert_eq!(fields[0].schema(), SchemaType::Long);
        assert_eq!(fields[1].name(), "next");

        let union_ = assert_lookup!(fields[1].schema(), SchemaType::Union(data) => data);
        let variants = union_.variants();
        assert_eq!(variants.len(), 2);
        assert_eq!(variants[0], schema.root());
        assert_eq!(variants[1], SchemaType::Null);
    });

    assert_serialisation!(nested_raw, schema);

    Ok(())
}

#[test]
fn test_large_schema() -> Result<(), Error> {
    let large_raw = fixture("large_schema.avsc");
    Schema::parse(&large_raw).map(|_x| ())

    // TODO - If we support metadata, we should enable the serde test
    // assert_serialisation!(large_raw, schema);
}

#[test]
fn test_stress() -> Result<(), Error> {
    let stress_raw = fixture("stress");
    Schema::parse(&stress_raw).map(|_x| ())

    // TODO - If we support metadata, we should enable the serde test
    // assert_serialisation!(large_raw, schema);
}
