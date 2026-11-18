import FongoSync

/-! Prints the axioms each law in `FongoSync/Laws.lean` rests on, in the order stated there.
`make lean` fails if this list differs from that file or if `sorryAx` appears. -/

#print axioms FongoSync.Laws.tick_later
#print axioms FongoSync.Laws.observe_ignores_unencodable
#print axioms FongoSync.Laws.tick_after_observe
#print axioms FongoSync.Laws.tick_after_any_history
#print axioms FongoSync.Laws.merge_comm
#print axioms FongoSync.Laws.merge_assoc
#print axioms FongoSync.Laws.merge_idem
#print axioms FongoSync.Laws.lww_keeps_later_clock
#print axioms FongoSync.Laws.apply_twice
#print axioms FongoSync.Laws.apply_any_order
#print axioms FongoSync.Laws.max_any_order
#print axioms FongoSync.Laws.register_set
#print axioms FongoSync.Laws.state_swap
#print axioms FongoSync.Laws.state_twice
#print axioms FongoSync.Laws.state_set
#print axioms FongoSync.Laws.unique_clocks_needed
#print axioms FongoSync.Laws.canonical_is_smallest
#print axioms FongoSync.Laws.canonical_same_iff
#print axioms FongoSync.Laws.groups_swap
#print axioms FongoSync.Laws.groups_twice
#print axioms FongoSync.Laws.groups_set
#print axioms FongoSync.Laws.view_from_inputs
#print axioms FongoSync.Laws.view_swap_writes
#print axioms FongoSync.Laws.view_swap_alias
#print axioms FongoSync.Laws.view_twice
#print axioms FongoSync.Laws.view_set
