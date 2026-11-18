import FongoSync

/-! Prints the axioms each headline theorem rests on. `make lean` fails if `sorryAx` appears. -/

#print axioms FongoSync.Clock.tick_after_any_history
#print axioms FongoSync.Clock.observe_then_tick
#print axioms FongoSync.Clock.observe_ignores
#print axioms FongoSync.merge_comm
#print axioms FongoSync.merge_assoc
#print axioms FongoSync.merge_idem
#print axioms FongoSync.fold_apply_eq_of_mem_iff
#print axioms FongoSync.fold_apply_perm
#print axioms FongoSync.fold_apply_dup
#print axioms FongoSync.merged_eq_of_mem_iff
#print axioms FongoSync.merged_perm
#print axioms FongoSync.merged_dup
#print axioms FongoSync.canonical_spec
#print axioms FongoSync.canonical_eq_iff
#print axioms FongoSync.canonical_eq_of_mem_iff
#print axioms FongoSync.view_eq_of_mem_iff
#print axioms FongoSync.view_perm
#print axioms FongoSync.view_dup
#print axioms FongoSync.apply_lww_clock
#print axioms FongoSync.order_matters_without_unique_clocks
#print axioms FongoSync.State.foldl_swap
#print axioms FongoSync.State.foldl_twice
#print axioms FongoSync.gidSet_eq_of_mem_iff
#print axioms FongoSync.view_from_inputs
#print axioms FongoSync.view_swap
#print axioms FongoSync.view_twice
