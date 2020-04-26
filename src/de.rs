//! Logic for serde-compatible deserialization.
use std::collections::{
    hash_map::{Keys, Values},
    HashMap,
};
use std::error;
use std::fmt;
use std::slice::Iter;
use std::sync::Arc;

use serde::{
    de::{self, DeserializeSeed, Error as SerdeError, Visitor},
    forward_to_deserialize_any, Deserialize,
};

use crate::types::Value;

/// Represents errors that could be encountered while deserializing data
#[derive(Clone, Debug, PartialEq)]
pub struct Error {
    message: String,
}

impl de::Error for Error {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Error {
            message: msg.to_string(),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str(&self.to_string())
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        &self.message
    }
}

pub struct Deserializer<'de> {
    input: &'de Value,
}

struct SeqDeserializer<'de> {
    input: Iter<'de, Value>,
}

struct MapDeserializer<'de> {
    input_keys: Keys<'de, String, Value>,
    input_values: Values<'de, String, Value>,
}

struct StructDeserializer<'de> {
    input: Iter<'de, (Arc<String>, Value)>,
    value: Option<&'de Value>,
}

pub struct EnumUnitDeserializer<'a> {
    input: &'a str,
}

pub struct EnumDeserializer<'de> {
    input: &'de [(Arc<String>, Value)],
}

impl<'de> Deserializer<'de> {
    pub fn new(input: &'de Value) -> Self {
        Self { input }
    }
}

impl<'de> SeqDeserializer<'de> {
    pub fn new(input: &'de [Value]) -> Self {
        Self {
            input: input.iter(),
        }
    }
}

impl<'de> MapDeserializer<'de> {
    pub fn new(input: &'de HashMap<String, Value>) -> Self {
        Self {
            input_keys: input.keys(),
            input_values: input.values(),
        }
    }
}

impl<'de> StructDeserializer<'de> {
    pub fn new(input: &'de [(Arc<String>, Value)]) -> Self {
        StructDeserializer {
            input: input.iter(),
            value: None,
        }
    }
}

impl<'a> EnumUnitDeserializer<'a> {
    pub fn new(input: &'a str) -> Self {
        Self { input }
    }
}

impl<'de> EnumDeserializer<'de> {
    pub fn new(input: &'de [(Arc<String>, Value)]) -> Self {
        Self { input }
    }
}

impl<'de> de::EnumAccess<'de> for EnumUnitDeserializer<'de> {
    type Error = Error;
    type Variant = Self;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        Ok((
            seed.deserialize(BorrowedStringDeserializer { input: self.input })?,
            self,
        ))
    }
}

impl<'de> de::VariantAccess<'de> for EnumUnitDeserializer<'de> {
    type Error = Error;

    fn unit_variant(self) -> Result<(), Error> {
        Ok(())
    }

    fn newtype_variant_seed<T>(self, _seed: T) -> Result<T::Value, Error>
    where
        T: DeserializeSeed<'de>,
    {
        Err(Error::custom("Unexpected Newtype variant"))
    }

    fn tuple_variant<V>(self, _len: usize, _visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::custom("Unexpected tuple variant"))
    }

    fn struct_variant<V>(
        self,
        _fields: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::custom("Unexpected struct variant"))
    }
}

impl<'de> de::EnumAccess<'de> for EnumDeserializer<'de> {
    type Error = Error;
    type Variant = Self;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        self.input.first().map_or(
            Err(Error::custom("A record must have a least one field")),
            |(key, value)| match (key.as_ref().as_str(), value) {
                ("type", Value::String(input)) => Ok((
                    seed.deserialize(BorrowedStringDeserializer { input })?,
                    self,
                )),
                (field, Value::String(_)) => Err(Error::custom(format!(
                    "Expected first field named 'type': got '{}' instead",
                    field
                ))),
                (_, _) => Err(Error::custom(format!(
                    "Expected first field of type String for the type name"
                ))),
            },
        )
    }
}

impl<'de> de::VariantAccess<'de> for EnumDeserializer<'de> {
    type Error = Error;

    fn unit_variant(self) -> Result<(), Error> {
        Ok(())
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value, Error>
    where
        T: DeserializeSeed<'de>,
    {
        self.input.get(1).map_or(
            Err(Error::custom(
                "Expected a newtype variant, got nothing instead.",
            )),
            |item| seed.deserialize(&Deserializer::new(&item.1)),
        )
    }

    fn tuple_variant<V>(self, _len: usize, visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        self.input.get(1).map_or(
            Err(Error::custom(
                "Expected a tuple variant, got nothing instead.",
            )),
            |item| de::Deserializer::deserialize_seq(&Deserializer::new(&item.1), visitor),
        )
    }

    fn struct_variant<V>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        self.input.get(1).map_or(
            Err(Error::custom("Expected a struct variant, got nothing")),
            |item| {
                de::Deserializer::deserialize_struct(
                    &Deserializer::new(&item.1),
                    "",
                    fields,
                    visitor,
                )
            },
        )
    }
}

impl<'a, 'de> de::Deserializer<'de> for &'a Deserializer<'de> {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.input {
            Value::Null => visitor.visit_unit(),
            &Value::Boolean(b) => visitor.visit_bool(b),
            Value::Int(i) | Value::Date(i) | Value::TimeMillis(i) => visitor.visit_i32(*i),
            Value::Long(i)
            | Value::TimeMicros(i)
            | Value::TimestampMillis(i)
            | Value::TimestampMicros(i) => visitor.visit_i64(*i),
            &Value::Float(f) => visitor.visit_f32(f),
            &Value::Double(d) => visitor.visit_f64(d),
            Value::Union(u) => match **u {
                Value::Null => visitor.visit_unit(),
                Value::Boolean(b) => visitor.visit_bool(b),
                Value::Int(i) => visitor.visit_i32(i),
                Value::Long(i) => visitor.visit_i64(i),
                Value::Float(f) => visitor.visit_f32(f),
                Value::Double(d) => visitor.visit_f64(d),
                _ => Err(Error::custom("Unsupported union")),
            },
            Value::Record(ref fields) => visitor.visit_map(StructDeserializer::new(fields)),
            Value::Array(ref fields) => visitor.visit_seq(SeqDeserializer::new(fields)),
            value => Err(Error::custom(format!(
                "incorrect value of type: {:?}",
                crate::schema::SchemaKind::from(value)
            ))),
        }
    }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64
    }

    fn deserialize_char<V>(self, _: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::custom("avro does not support char"))
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match *self.input {
            Value::String(ref s) => visitor.visit_borrowed_str(s),
            Value::Bytes(ref bytes) | Value::Fixed(_, ref bytes) => ::std::str::from_utf8(bytes)
                .map_err(|e| Error::custom(e.to_string()))
                .and_then(|s| visitor.visit_str(s)),
            _ => Err(Error::custom("not a string|bytes|fixed")),
        }
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match *self.input {
            Value::String(ref s) => visitor.visit_borrowed_str(s),
            Value::Bytes(ref bytes) | Value::Fixed(_, ref bytes) => {
                String::from_utf8(bytes.to_owned())
                    .map_err(|e| Error::custom(e.to_string()))
                    .and_then(|s| visitor.visit_string(s))
            }
            Value::Union(ref x) => match **x {
                Value::String(ref s) => visitor.visit_string(s.to_owned()),
                _ => Err(Error::custom("not a string|bytes|fixed")),
            },
            _ => Err(Error::custom("not a string|bytes|fixed")),
        }
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match *self.input {
            Value::String(ref s) => visitor.visit_bytes(s.as_bytes()),
            Value::Bytes(ref bytes) | Value::Fixed(_, ref bytes) => visitor.visit_bytes(bytes),
            _ => Err(Error::custom("not a string|bytes|fixed")),
        }
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match *self.input {
            Value::String(ref s) => visitor.visit_byte_buf(s.clone().into_bytes()),
            Value::Bytes(ref bytes) | Value::Fixed(_, ref bytes) => {
                visitor.visit_byte_buf(bytes.to_owned())
            }
            _ => Err(Error::custom("not a string|bytes|fixed")),
        }
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match *self.input {
            Value::Union(ref inner) if inner.as_ref() == &Value::Null => visitor.visit_none(),
            Value::Union(ref inner) => visitor.visit_some(&Deserializer::new(inner)),
            _ => Err(Error::custom("not a union")),
        }
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match *self.input {
            Value::Null => visitor.visit_unit(),
            _ => Err(Error::custom("not a null")),
        }
    }

    fn deserialize_unit_struct<V>(
        self,
        _: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_unit(visitor)
    }

    fn deserialize_newtype_struct<V>(
        self,
        _: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match *self.input {
            Value::Array(ref items) => visitor.visit_seq(SeqDeserializer::new(items)),
            Value::Union(ref inner) => match **inner {
                Value::Array(ref items) => visitor.visit_seq(SeqDeserializer::new(items)),
                _ => Err(Error::custom("not an array")),
            },
            _ => Err(Error::custom("not an array")),
        }
    }

    fn deserialize_tuple<V>(self, _: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    fn deserialize_tuple_struct<V>(
        self,
        _: &'static str,
        _: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.input {
            Value::Map(items) => visitor.visit_map(MapDeserializer::new(items)),
            _ => Err(Error::custom("not a map")),
        }
    }

    fn deserialize_struct<V>(
        self,
        _: &'static str,
        _: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match *self.input {
            Value::Record(ref fields) => visitor.visit_map(StructDeserializer::new(fields)),
            Value::Union(ref inner) => match **inner {
                Value::Record(ref fields) => visitor.visit_map(StructDeserializer::new(fields)),
                _ => Err(Error::custom("not a record")),
            },
            _ => Err(Error::custom("not a record")),
        }
    }

    fn deserialize_enum<V>(
        self,
        _: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match *self.input {
            // This branch can be anything...
            Value::Record(ref fields) => visitor.visit_enum(EnumDeserializer::new(&fields)),
            // This has to be a unit Enum
            Value::Enum(_index, ref field) => visitor.visit_enum(EnumUnitDeserializer::new(&field)),
            _ => Err(Error::custom("not an enum")),
        }
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_any(visitor)
    }
}

impl<'de> de::SeqAccess<'de> for SeqDeserializer<'de> {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        self.input
            .next()
            .map(|item| seed.deserialize(&Deserializer::new(&item)))
            .transpose()
    }
}

impl<'de> de::MapAccess<'de> for MapDeserializer<'de> {
    type Error = Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        self.input_keys
            .next()
            .map(|input| seed.deserialize(BorrowedStringDeserializer { input }))
            .transpose()
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        let value = self
            .input_values
            .next()
            .ok_or_else(|| Error::custom("should not happen - too many values"))?;
        seed.deserialize(&Deserializer::new(value))
    }
}

impl<'de> de::MapAccess<'de> for StructDeserializer<'de> {
    type Error = Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        self.input
            .next()
            .map(|(input, value)| {
                self.value = Some(value);
                seed.deserialize(BorrowedStringDeserializer { input })
            })
            .transpose()
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        let value = self
            .value
            .take()
            .ok_or_else(|| Error::custom("should not happen - too many values"))?;
        seed.deserialize(&Deserializer::new(value))
    }
}

struct BorrowedStringDeserializer<'input> {
    input: &'input str,
}

impl<'de> de::Deserializer<'de> for BorrowedStringDeserializer<'de> {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_str(self.input.as_ref())
    }

    forward_to_deserialize_any! {
        bool u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 char str string unit option
        seq bytes byte_buf map unit_struct newtype_struct
        tuple_struct struct tuple enum identifier ignored_any
    }
}

/// Interpret a `Value` as an instance of type `D`.
///
/// This conversion can fail if the structure of the `Value` does not match the
/// structure expected by `D`.
pub fn from_value<'de, D: Deserialize<'de>>(value: &'de Value) -> Result<D, Error> {
    let de = Deserializer::new(value);
    D::deserialize(&de)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;

    #[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
    struct Test {
        a: i64,
        b: String,
    }

    #[derive(Debug, Deserialize, Serialize, PartialEq)]
    struct TestInner {
        a: Test,
        b: i32,
    }

    #[derive(Debug, Deserialize, Serialize, PartialEq)]
    struct TestUnitExternalEnum {
        a: UnitExternalEnum,
    }

    #[derive(Debug, Deserialize, Serialize, PartialEq)]
    enum UnitExternalEnum {
        Val1,
        Val2,
    }

    #[derive(Debug, Deserialize, Serialize, PartialEq)]
    struct TestUnitInternalEnum {
        a: UnitInternalEnum,
    }

    #[derive(Debug, Deserialize, Serialize, PartialEq)]
    #[serde(tag = "t")]
    enum UnitInternalEnum {
        Val1,
        Val2,
    }

    #[derive(Debug, Deserialize, Serialize, PartialEq)]
    struct TestUnitAdjacentEnum {
        a: UnitAdjacentEnum,
    }

    #[derive(Debug, Deserialize, Serialize, PartialEq)]
    #[serde(tag = "t", content = "v")]
    enum UnitAdjacentEnum {
        Val1,
        Val2,
    }

    #[derive(Debug, Deserialize, Serialize, PartialEq)]
    struct TestUnitUntaggedEnum {
        a: UnitUntaggedEnum,
    }

    #[derive(Debug, Deserialize, Serialize, PartialEq)]
    #[serde(untagged)]
    enum UnitUntaggedEnum {
        Val1,
        Val2,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct TestSingleValueExternalEnum {
        a: SingleValueExternalEnum,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    enum SingleValueExternalEnum {
        Double(f64),
        String(String),
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct TestStructExternalEnum {
        a: StructExternalEnum,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    enum StructExternalEnum {
        Val1 { x: f32, y: f32 },
        Val2 { x: f32, y: f32 },
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct TestTupleExternalEnum {
        a: TupleExternalEnum,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    enum TupleExternalEnum {
        Val1(f32, f32),
        Val2(f32, f32, f32),
    }

    #[test]
    fn test_from_value() {
        let test = Value::Record(vec![
            (Arc::new("a".to_owned()), Value::Long(27)),
            (Arc::new("b".to_owned()), Value::String("foo".to_owned())),
        ]);
        let expected = Test {
            a: 27,
            b: "foo".to_owned(),
        };
        let final_value: Test = from_value(&test).unwrap();
        assert_eq!(final_value, expected);

        let test_inner = Value::Record(vec![
            (
                Arc::new("a".to_owned()),
                Value::Record(vec![
                    (Arc::new("a".to_owned()), Value::Long(27)),
                    (Arc::new("b".to_owned()), Value::String("foo".to_owned())),
                ]),
            ),
            (Arc::new("b".to_owned()), Value::Int(35)),
        ]);

        let expected_inner = TestInner {
            a: expected.clone(),
            b: 35,
        };
        let final_value: TestInner = from_value(&test_inner).unwrap();
        assert_eq!(final_value, expected_inner)
    }
    #[test]
    fn test_from_value_unit_enum() {
        let expected = TestUnitExternalEnum {
            a: UnitExternalEnum::Val1,
        };

        let test = Value::Record(vec![(
            Arc::new("a".to_owned()),
            Value::Enum(0, Arc::new("Val1".to_owned())),
        )]);
        let final_value: TestUnitExternalEnum = from_value(&test).unwrap();
        assert_eq!(
            final_value, expected,
            "Error deserializing unit external enum"
        );

        let expected = TestUnitInternalEnum {
            a: UnitInternalEnum::Val1,
        };

        let test = Value::Record(vec![(
            Arc::new("a".to_owned()),
            Value::Record(vec![(
                Arc::new("t".to_owned()),
                Value::String("Val1".to_owned()),
            )]),
        )]);
        let final_value: TestUnitInternalEnum = from_value(&test).unwrap();
        assert_eq!(
            final_value, expected,
            "Error deserializing unit internal enum"
        );
        let expected = TestUnitAdjacentEnum {
            a: UnitAdjacentEnum::Val1,
        };

        let test = Value::Record(vec![(
            Arc::new("a".to_owned()),
            Value::Record(vec![(
                Arc::new("t".to_owned()),
                Value::String("Val1".to_owned()),
            )]),
        )]);
        let final_value: TestUnitAdjacentEnum = from_value(&test).unwrap();
        assert_eq!(
            final_value, expected,
            "Error deserializing unit adjacent enum"
        );
        let expected = TestUnitUntaggedEnum {
            a: UnitUntaggedEnum::Val1,
        };

        let test = Value::Record(vec![(Arc::new("a".to_owned()), Value::Null)]);
        let final_value: TestUnitUntaggedEnum = from_value(&test).unwrap();
        assert_eq!(
            final_value, expected,
            "Error deserializing unit untagged enum"
        );
    }

    #[test]
    fn test_from_value_single_value_enum() {
        let expected = TestSingleValueExternalEnum {
            a: SingleValueExternalEnum::Double(64.0),
        };

        let test = Value::Record(vec![(
            Arc::new("a".to_owned()),
            Value::Record(vec![
                (
                    Arc::new("type".to_owned()),
                    Value::String("Double".to_owned()),
                ),
                (
                    Arc::new("value".to_owned()),
                    Value::Union(Box::new(Value::Double(64.0))),
                ),
            ]),
        )]);
        let final_value: TestSingleValueExternalEnum = from_value(&test).unwrap();
        assert_eq!(
            final_value, expected,
            "Error deserializing single value external enum(union)"
        );
    }

    #[test]
    fn test_from_value_struct_enum() {
        let expected = TestStructExternalEnum {
            a: StructExternalEnum::Val1 { x: 1.0, y: 2.0 },
        };

        let test = Value::Record(vec![(
            Arc::new("a".to_owned()),
            Value::Record(vec![
                (
                    Arc::new("type".to_owned()),
                    Value::String("Val1".to_owned()),
                ),
                (
                    Arc::new("value".to_owned()),
                    Value::Union(Box::new(Value::Record(vec![
                        (Arc::new("x".to_owned()), Value::Float(1.0)),
                        (Arc::new("y".to_owned()), Value::Float(2.0)),
                    ]))),
                ),
            ]),
        )]);
        let final_value: TestStructExternalEnum = from_value(&test).unwrap();
        assert_eq!(
            final_value, expected,
            "error deserializing struct external enum(union)"
        );
    }

    #[test]
    fn test_from_value_tuple_enum() {
        let expected = TestTupleExternalEnum {
            a: TupleExternalEnum::Val1(1.0, 2.0),
        };

        let test = Value::Record(vec![(
            Arc::new("a".to_owned()),
            Value::Record(vec![
                (
                    Arc::new("type".to_owned()),
                    Value::String("Val1".to_owned()),
                ),
                (
                    Arc::new("value".to_owned()),
                    Value::Union(Box::new(Value::Array(vec![
                        Value::Float(1.0),
                        Value::Float(2.0),
                    ]))),
                ),
            ]),
        )]);
        let final_value: TestTupleExternalEnum = from_value(&test).unwrap();
        assert_eq!(
            final_value, expected,
            "error serializing tuple external enum(union)"
        );
    }

    type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

    #[test]
    fn test_date() -> TestResult<()> {
        let raw_value = 1;
        let value = Value::Date(raw_value);
        let result = crate::from_value::<i32>(&value)?;
        assert_eq!(result, raw_value);
        Ok(())
    }

    #[test]
    fn test_time_millis() -> TestResult<()> {
        let raw_value = 1;
        let value = Value::TimeMillis(raw_value);
        let result = crate::from_value::<i32>(&value)?;
        assert_eq!(result, raw_value);
        Ok(())
    }

    #[test]
    fn test_time_micros() -> TestResult<()> {
        let raw_value = 1;
        let value = Value::TimeMicros(raw_value);
        let result = crate::from_value::<i64>(&value)?;
        assert_eq!(result, raw_value);
        Ok(())
    }

    #[test]
    fn test_timestamp_millis() -> TestResult<()> {
        let raw_value = 1;
        let value = Value::TimestampMillis(raw_value);
        let result = crate::from_value::<i64>(&value)?;
        assert_eq!(result, raw_value);
        Ok(())
    }

    #[test]
    fn test_timestamp_micros() -> TestResult<()> {
        let raw_value = 1;
        let value = Value::TimestampMicros(raw_value);
        let result = crate::from_value::<i64>(&value)?;
        assert_eq!(result, raw_value);
        Ok(())
    }
}
