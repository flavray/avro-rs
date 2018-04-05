extern crate failure;
extern crate libflate;
extern crate rand;
#[macro_use]
extern crate serde;
extern crate serde_json;
#[cfg(feature = "snappy")]
extern crate snap;

pub mod codec;
pub mod de;
pub mod decode;
pub mod encode;
pub mod reader;
pub mod schema;
pub mod ser;
pub mod types;
mod util;
pub mod writer;

pub use codec::Codec;

pub use de::from_value;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
