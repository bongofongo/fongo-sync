.PHONY: check fmt-check lint test wasm proof verdict lean

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
	@grep -oE '^theorem [^ ]+' lean/FongoSync/Laws.lean \
		| sed 's/^theorem /#print axioms FongoSync.Laws./' > lean/.lake/laws.txt
	@grep '^#print axioms' lean/Audit.lean | cmp -s - lean/.lake/laws.txt \
		|| { echo 'lean/Audit.lean must list exactly the theorems of lean/FongoSync/Laws.lean' >&2; exit 1; }
	@cd lean && $(LAKE) env lean Audit.lean > .lake/axioms.txt && cat .lake/axioms.txt
	@! grep -oE '\[.*\]' lean/.lake/axioms.txt | tr -d '[] ' | tr ',' '\n' \
		| grep -vxE 'propext|Classical\.choice|Quot\.sound'

# Rechecks the proofs with BendTT's kernel, which needs Lean on PATH.
verdict:
	@$(BEND) PROOF.bend --verdict | tee /dev/stderr | grep -q '^ALL PROOFS CHECK'
