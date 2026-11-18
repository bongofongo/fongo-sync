.PHONY: check fmt-check lint test proof

BEND := $(shell command -v bend || echo $(HOME)/.bend/bin/bend)

check: fmt-check lint test proof

fmt-check:
	cargo fmt --all --check

lint:
	cargo clippy --workspace --all-targets --locked -- -D warnings

test:
	cargo test --workspace --locked

proof:
	@$(BEND) PROOF.bend | tee /dev/stderr | grep -q '^ALL PROOFS CHECK'
