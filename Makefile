setup:
	cargo install cargo-audit cargo-udeps

audit:
	cargo audit
	cargo udeps --workspace

clean:
	cargo clean

build:
	cargo build

clippy:
	cargo clippy --workspace --tests --all-features -- -D warnings

fmt:
	cargo fmt

fmt-check:
	cargo fmt --all -- --check

lint: fmt-check clippy

pre-commit: lint build audit
