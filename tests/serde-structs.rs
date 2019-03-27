
#[macro_use]
extern crate serde;
extern crate avro_rs;


#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct NT(i32, i64, bool);

#[test]
fn serialize_deserialize_newtype() {
    let values = vec![
        NT(32, 64, true),
        NT(23, 46, false),
    ];

    for input in values {
        let avro = avro_rs::to_value(input.clone()).expect("Failed to serialize");
        let output = avro_rs::from_value::<NT>(&avro).expect("Failed to deserialize");
        assert_eq!(input, output);
    }
}


#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct S1 {
    a: i32,
    b: i64,
    c: bool
}

#[test]
fn serialize_deserialize_struct() {
    let values = vec![
        S1 { a: 32, b: 64, c: true },
        S1 { a: 23, b: 46, c: false },
    ];

    for input in values {
        let avro = avro_rs::to_value(input.clone()).expect("Failed to serialize");
        let output = avro_rs::from_value::<S1>(&avro).expect("Failed to deserialize");
        assert_eq!(input, output);
    }
}


#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
enum E1 {
    Unit,
    Newtype(i32, i64, bool),
    Struct1 { a: i32, b: i64, c: bool },
    Struct2 { a: i32, b: i64, c: bool },
}

#[test]
fn serialize_deserialize_union_types() {
    let values = vec![
        E1::Unit,
        E1::Newtype(32, 64, true),
        E1::Struct1 { a: 32, b: 64, c: true },
        E1::Struct2 { a: 23, b: 46, c: false },
    ];

    for input in values {
        let avro = avro_rs::to_value(input.clone()).expect("Failed to serialize");
        let output = avro_rs::from_value::<E1>(&avro).expect("Failed to deserialize");
        assert_eq!(input, output);
    }
}
