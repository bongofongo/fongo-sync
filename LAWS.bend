# What the merge rules must guarantee. Proven in PROOF.bend; `bend PROOF.bend` checks them.

import Base
import ./model.bend as M

# A merge never moves a field's clock backwards: it ends at the later of the two.
law merge_keeps_later_clock:
  for +held: M.Entry
  for +incoming: M.Entry
  {M.Entry.clock(M.merge(held, incoming)) == Nat.max(M.Entry.clock(held), M.Entry.clock(incoming)) : Nat}

# Receiving the same change twice is the same as receiving it once.
law apply_twice:
  for held: Maybe<&2, M.Entry>
  for +c: M.Entry
  {M.apply(M.apply(held, c), c) == M.apply(held, c) : Maybe<&2, M.Entry>}

# Two changes with different clocks may arrive in either order.
law apply_any_order:
  for held: Maybe<&2, M.Entry>
  for +a: M.Entry
  for +b: M.Entry
  for ne: {M.Entry.clock(a) != M.Entry.clock(b) : Nat}
  {M.apply(M.apply(held, a), b) == M.apply(M.apply(held, b), a) : Maybe<&2, M.Entry>}

# Every reading a clock issues is later than the one before, whatever the wall time says.
law tick_is_later:
  for +last: M.Stamp
  for +now: Nat
  {M.Stamp.cmp(last, M.tick(last, now)) == LT{} : Cmp}
