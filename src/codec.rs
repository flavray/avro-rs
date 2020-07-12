//! Logic for all supported compression codecs in Avro.
use crate::{
    errors::AvroResult,
    types::Value,
};
use libflate::deflate::{Decoder, Encoder};
use std::io::{Read, Write};
use strum_macros::{EnumString, IntoStaticStr};

/// The compression codec used to compress blocks.
#[derive(Clone, Copy, Debug, PartialEq, EnumString, IntoStaticStr)]
#[strum(serialize_all = "kebab_case")]
pub enum Codec {
    /// The `Null` codec simply passes through data uncompressed.
    Null,
    /// The `Deflate` codec writes the data block using the deflate algorithm
    /// as specified in RFC 1951, and typically implemented using the zlib library.
    /// Note that this format (unlike the "zlib format" in RFC 1950) does not have a checksum.
    Deflate,
    #[cfg(feature = "snappy")]
    /// The `Snappy` codec uses Google's [Snappy](http://google.github.io/snappy/)
    /// compression library. Each compressed block is followed by the 4-byte, big-endian
    /// CRC32 checksum of the uncompressed data in the block.
    Snappy,
}

impl From<Codec> for Value {
    fn from(value: Codec) -> Self {
        Self::Bytes(<&str>::from(value).as_bytes().to_vec())
    }
}

impl Codec {
    /// Compress a stream of bytes in-place.
    pub fn compress(self, stream: &mut Vec<u8>) -> AvroResult<()> {
        match self {
            Codec::Null => (),
            Codec::Deflate => {
                let mut encoder = Encoder::new(Vec::new());
                encoder.write_all(stream)?;
                // Deflate errors seem to just be io::Error
                *stream = encoder.finish().into_result()?;
            }
            #[cfg(feature = "snappy")]
            Codec::Snappy => {
                use byteorder::ByteOrder;

                let mut encoded: Vec<u8> = vec![0; snap::max_compress_len(stream.len())];
                let compressed_size =
                    snap::Encoder::new().compress(&stream[..], &mut encoded[..])?;

                let crc = crc::crc32::checksum_ieee(&stream[..]);
                byteorder::BigEndian::write_u32(&mut encoded[compressed_size..], crc);
                encoded.truncate(compressed_size + 4);

                *stream = encoded;
            }
        };

        Ok(())
    }

    /// Decompress a stream of bytes in-place.
    pub fn decompress(self, stream: &mut Vec<u8>) -> AvroResult<()> {
        *stream = match self {
            Codec::Null => return Ok(()),
            Codec::Deflate => {
                let mut decoded = Vec::new();
                let mut decoder = Decoder::new(&stream[..]);
                decoder.read_to_end(&mut decoded)?;
                decoded
            }
            #[cfg(feature = "snappy")]
            Codec::Snappy => {
                use crate::Error;
                use byteorder::ByteOrder;

                let decompressed_size = snap::decompress_len(&stream[..stream.len() - 4])?;
                let mut decoded = vec![0; decompressed_size];
                snap::Decoder::new().decompress(&stream[..stream.len() - 4], &mut decoded[..])?;

                let expected_crc = byteorder::BigEndian::read_u32(&stream[stream.len() - 4..]);
                let actual_crc = crc::crc32::checksum_ieee(&decoded);

                if expected_crc != actual_crc {
                    return Err(Error::SnappyCrcError {
                        expected: expected_crc,
                        found: actual_crc,
                    });
                }
                decoded
            }
        };
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const INPUT: &[u8] = b"theanswertolifetheuniverseandeverythingis42theanswertolifetheuniverseandeverythingis4theanswertolifetheuniverseandeverythingis2";

    #[test]
    fn null_compress_and_decompress() {
        let codec = Codec::Null;
        let mut stream = INPUT.to_vec();
        codec.compress(&mut stream).unwrap();
        assert_eq!(INPUT, stream.as_slice());
        codec.decompress(&mut stream).unwrap();
        assert_eq!(INPUT, stream.as_slice());
    }

    #[test]
    fn deflate_compress_and_decompress() {
        let codec = Codec::Deflate;
        let mut stream = INPUT.to_vec();
        codec.compress(&mut stream).unwrap();
        assert_ne!(INPUT, stream.as_slice());
        assert!(INPUT.len() > stream.len());
        codec.decompress(&mut stream).unwrap();
        assert_eq!(INPUT, stream.as_slice());
    }

    #[cfg(feature = "snappy")]
    #[test]
    fn snappy_compress_and_decompress() {
        let codec = Codec::Snappy;
        let mut stream = INPUT.to_vec();
        codec.compress(&mut stream).unwrap();
        assert_ne!(INPUT, stream.as_slice());
        assert!(INPUT.len() > stream.len());
        codec.decompress(&mut stream).unwrap();
        assert_eq!(INPUT, stream.as_slice());
    }

    #[test]
    fn codec_to_str() {
        assert_eq!(<&str>::from(Codec::Null), "null");
        assert_eq!(<&str>::from(Codec::Deflate), "deflate");

        #[cfg(feature = "snappy")]
        assert_eq!(<&str>::from(Codec::Snappy), "snappy");
    }

    #[test]
    fn codec_from_str() {
        use std::str::FromStr;

        assert_eq!(Codec::from_str("null").unwrap(), Codec::Null);
        assert_eq!(Codec::from_str("deflate").unwrap(), Codec::Deflate);

        #[cfg(feature = "snappy")]
        assert_eq!(Codec::from_str("snappy").unwrap(), Codec::Snappy);

        assert!(Codec::from_str("not a codec").is_err());
    }
}
