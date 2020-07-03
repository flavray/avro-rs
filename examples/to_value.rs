use avro_rs::{to_value, AvroError};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
struct Test {
    a: i64,
    b: String,
}

fn main() -> Result<(), AvroError> {
    let test = Test {
        a: 27,
        b: "foo".to_owned(),
    };
    println!("{:?}", to_value(test)?);

    Ok(())
}
