.PHONY: check fmt-check lint test

check: fmt-check lint test

fmt-check:
	cargo fmt --all --check

lint:
	cargo clippy --workspace --all-targets --locked -- -D warnings

test:
	cargo test --workspace --locked
