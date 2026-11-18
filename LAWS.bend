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

# Under `Strategy::Max`, two changes with different clocks may arrive in either order.
law apply_max_any_order:
  for held: Maybe<&2, M.Entry>
  for +a: M.Entry
  for +b: M.Entry
  for ne: {M.Entry.clock(a) != M.Entry.clock(b) : Nat}
  {M.apply_by(M.Max{}, M.apply_by(M.Max{}, held, a), b) == M.apply_by(M.Max{}, M.apply_by(M.Max{}, held, b), a) : Maybe<&2, M.Entry>}

# Under `Strategy::Max`, receiving the same change twice is the same as receiving it once.
law apply_max_twice:
  for held: Maybe<&2, M.Entry>
  for +c: M.Entry
  {M.apply_by(M.Max{}, M.apply_by(M.Max{}, held, c), c) == M.apply_by(M.Max{}, held, c) : Maybe<&2, M.Entry>}

# Swapping two neighbouring changes with different clocks anywhere in a delivery leaves every register the same.
law fold_any_order:
  for pre: List<&2, M.Write>
  for +a: M.Write
  for +b: M.Write
  for post: List<&2, M.Write>
  for rs: List<&2, M.Reg>
  for ne: {M.Write.clock(a) != M.Write.clock(b) : Nat}
  {M.fold(List.append(&2, M.Write, pre, a <> b <> post), rs) == M.fold(List.append(&2, M.Write, pre, b <> a <> post), rs) : List<&2, M.Reg>}

# Receiving a change twice in a row anywhere in a delivery leaves every register as receiving it once.
law fold_twice:
  for pre: List<&2, M.Write>
  for +a: M.Write
  for post: List<&2, M.Write>
  for rs: List<&2, M.Reg>
  {M.fold(List.append(&2, M.Write, pre, a <> a <> post), rs) == M.fold(List.append(&2, M.Write, pre, a <> post), rs) : List<&2, M.Reg>}

# Swapping two neighbouring aliases anywhere leaves every gid's group label the same.
law labels_any_order:
  for +n: Nat
  for +pre: List<&2, M.Edge>
  for +x: M.Edge
  for +y: M.Edge
  for +post: List<&2, M.Edge>
  {M.labels(n, List.append(&2, M.Edge, pre, x <> y <> post)) == M.labels(n, List.append(&2, M.Edge, pre, y <> x <> post)) : List<&2, Nat>}

# An alias received twice in a row labels every gid as receiving it once.
law labels_twice:
  for +n: Nat
  for +pre: List<&2, M.Edge>
  for +x: M.Edge
  for +post: List<&2, M.Edge>
  {M.labels(n, List.append(&2, M.Edge, pre, x <> x <> post)) == M.labels(n, List.append(&2, M.Edge, pre, x <> post)) : List<&2, Nat>}

# Two replicas whose registers agree and whose groups agree show the same view.
law view_from_registers_and_groups:
  for +n: Nat
  for ws: List<&2, M.Write>
  for vs: List<&2, M.Write>
  for rs: List<&2, M.Reg>
  for es: List<&2, M.Edge>
  for fs: List<&2, M.Edge>
  for same_registers: {M.fold(ws, rs) == M.fold(vs, rs) : List<&2, M.Reg>}
  for same_groups: {M.labels(n, es) == M.labels(n, fs) : List<&2, Nat>}
  {M.view(n, ws, rs, es) == M.view(n, vs, rs, fs) : List<&2, Maybe<&2, Maybe<&2, M.Entry>>>}

# Swapping two neighbouring writes with different clocks anywhere in a delivery leaves the view the same.
law view_any_order_of_writes:
  for +n: Nat
  for +pre: List<&2, M.Write>
  for +a: M.Write
  for +b: M.Write
  for +post: List<&2, M.Write>
  for +rs: List<&2, M.Reg>
  for +es: List<&2, M.Edge>
  for ne: {M.Write.clock(a) != M.Write.clock(b) : Nat}
  {M.view(n, List.append(&2, M.Write, pre, a <> b <> post), rs, es) == M.view(n, List.append(&2, M.Write, pre, b <> a <> post), rs, es) : List<&2, Maybe<&2, Maybe<&2, M.Entry>>>}

# Swapping two neighbouring aliases anywhere leaves the view the same.
law view_any_order_of_aliases:
  for +n: Nat
  for +ws: List<&2, M.Write>
  for +rs: List<&2, M.Reg>
  for +pre: List<&2, M.Edge>
  for +x: M.Edge
  for +y: M.Edge
  for +post: List<&2, M.Edge>
  {M.view(n, ws, rs, List.append(&2, M.Edge, pre, x <> y <> post)) == M.view(n, ws, rs, List.append(&2, M.Edge, pre, y <> x <> post)) : List<&2, Maybe<&2, Maybe<&2, M.Entry>>>}

# Receiving a write twice in a row leaves the view as receiving it once.
law view_write_twice:
  for +n: Nat
  for +pre: List<&2, M.Write>
  for +a: M.Write
  for +post: List<&2, M.Write>
  for +rs: List<&2, M.Reg>
  for +es: List<&2, M.Edge>
  {M.view(n, List.append(&2, M.Write, pre, a <> a <> post), rs, es) == M.view(n, List.append(&2, M.Write, pre, a <> post), rs, es) : List<&2, Maybe<&2, Maybe<&2, M.Entry>>>}

# Receiving an alias twice in a row leaves the view as receiving it once.
law view_alias_twice:
  for +n: Nat
  for +ws: List<&2, M.Write>
  for +rs: List<&2, M.Reg>
  for +pre: List<&2, M.Edge>
  for +x: M.Edge
  for +post: List<&2, M.Edge>
  {M.view(n, ws, rs, List.append(&2, M.Edge, pre, x <> x <> post)) == M.view(n, ws, rs, List.append(&2, M.Edge, pre, x <> post)) : List<&2, Maybe<&2, Maybe<&2, M.Entry>>>}
