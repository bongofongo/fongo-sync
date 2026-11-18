.PHONY: check fmt-check lint test wasm proof lean

BEND := $(shell command -v bend || echo $(HOME)/.bend/bin/bend)
LAKE := $(shell command -v lake || echo $(HOME)/.elan/bin/lake)

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

# Not part of `check`: it needs a Lean toolchain (see lean/lean-toolchain).
lean:
	cd lean && $(LAKE) build
	@! grep -rnwE 'sorry|admit|axiom|native_decide' lean/FongoSync lean/FongoSync.lean
	@cd lean && $(LAKE) env lean Audit.lean | tee /dev/stderr | { ! grep -q sorryAx; }
