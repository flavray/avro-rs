use failure::Error;

#[derive(Debug, PartialEq, Clone)]
pub struct RawUuid<T>(T);

#[cfg(feature = "safe-uuid")]
impl RawUuid<::uuid::Uuid> {
    pub(crate) fn to_string(&self) -> String {
        self.0.to_string()
    }

    pub(crate) fn parse_str(input: &str) -> Result<Self, Error> {
        Ok(Self(uuid::Uuid::parse_str(input)?))
    }
}

#[cfg(not(feature = "safe-uuid"))]
impl RawUuid<String> {
    pub(crate) fn to_string(&self) -> String {
        self.0.clone()
    }

    pub(crate) fn parse_str(input: &str) -> Result<Self, Error> {
        Ok(Self(input.to_owned()))
    }
}

#[cfg(feature = "safe-uuid")]
pub type Uuid = RawUuid<::uuid::Uuid>;

#[cfg(not(feature = "safe-uuid"))]
pub type Uuid = RawUuid<String>;
