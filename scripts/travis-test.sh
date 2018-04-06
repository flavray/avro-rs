#!/bin/bash
set -ev
make release
make test
[ "$TRAVIS_RUST_VERSION" = "nightly" ] && make benchmark
