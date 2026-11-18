.PHONY: check fmt-check lint test wasm proof verdict

BEND := $(shell command -v bend || echo $(HOME)/.bend/bin/bend)

check: fmt-check lint test wasm proof

fmt-check:
	cargo fmt --all --check

lint:
	cargo clippy --workspace --all-targets --all-features --locked -- -D warnings

test:
	cargo test --workspace --all-features --locked

wasm:
	cargo build -p fongo-sync-protocol --target wasm32-unknown-unknown --locked

proof:
	@$(BEND) PROOF.bend | tee /dev/stderr | grep -q '^ALL PROOFS CHECK'

# Rechecks the proofs with BendTT's kernel, which needs Lean on PATH.
verdict:
	@$(BEND) PROOF.bend --verdict | tee /dev/stderr | grep -q '^ALL PROOFS CHECK'
