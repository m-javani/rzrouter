#!/usr/bin/env bash

set -euo pipefail

# cargo clean --release
cargo build  --release

strip --strip-all target/release/rzrouter

upx --best --lzma target/release/rzrouter

ls -lh target/release/rzrouter

cp target/release/rzrouter .