set shell := ["bash", "-uc"]

nightly := `grep nightly rust-toolchain.toml | sed -r 's/# nightly = "(.*)"/\1/'`

check:
	cargo check --tests

fmt:
    cargo +{{nightly}} fmt

fmt-check:
    cargo +{{nightly}} fmt --check

fix:
	cargo fix --tests --allow-dirty --allow-staged

lint:
	cargo clippy --tests --no-deps -- -D warnings

lint-fix:
	cargo clippy --tests --no-deps --allow-dirty --allow-staged --fix

test:
	cargo test --test integration_test -- --no-capture

doc:
	cargo doc --no-deps

all: check fmt lint test doc
