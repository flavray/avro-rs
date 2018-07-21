# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
## Added
- Support for Parsing Canonical Form
- `to_value` to serialize anything that implements Serialize into a Value

## [0.4.1] - 2018-06-17
### Changed
- Implememented clippy suggestions

## [0.4.0] - 2018-06-17
### Changed
- Many performance improvements to both encoding and decoding
### Added
- New public method extend_from_slice for Writer
- serde_json benchmark for comparison
- bench_from_file function and a file from the goavro repository for comparison

## [0.3.2] - 2018-06-07
### Added
- Some missing serialization fields for Schema::Record

## [0.3.1] - 2018-06-02
### Fixed
- Encode/decode Union values with a leading zig-zag long

## [0.3.0] - 2018-05-29
### Changed
- Move from string as errors to custom fail types

### Fixed
- Avoid reading the first item over and over in Reader

## [0.2.0] - 2018-05-22
### Added
- `from_avro_datum` to decode Avro-encoded bytes into a `Value`
- Documentation for `from_value`

## [0.1.1] - 2018-05-16
- Initial release
