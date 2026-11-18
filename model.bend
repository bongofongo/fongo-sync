# A model of the merge rules in crates/core, small enough to prove things about.
# A clock is a Nat here; in the Rust it is an Hlc, which is totally ordered the same way.

import Base

# What a replica holds for one field: the value and the clock of the write that set it.
type Entry is Data:
  Entry{clock: Nat, value: Nat}

def Entry.clock(e: Entry) -> Nat:
  match e:
    case Entry{clock, value}:
      clock

# `y` when clock `b` is later than clock `a`, else `x`. A tie keeps `x`.
def later(-T: Type, a: Nat, b: Nat, x: T, y: T) -> T:
  match a b:
    case 0n 0n:
      x
    case 0n 1n+q:
      y
    case 1n+p 0n:
      x
    case 1n+p 1n+q:
      later(T, p, q, x, y)

# `wins` under `Strategy::Lww`: the incoming write replaces the held one only if it is later.
def merge(+held: Entry, +incoming: Entry) -> Entry:
  later(Entry, Entry.clock(held), Entry.clock(incoming), held, incoming)

# `State::apply` for one field, which may hold nothing yet.
def apply(held: Maybe<&2, Entry>, +incoming: Entry) -> Maybe<&2, Entry>:
  match held:
    case None{}:
      Some{incoming}
    case Some{+value}:
      Some{merge(value, incoming)}

# A clock reading without its node: wall time, then a counter.
type Stamp is Data:
  Stamp{wall: Nat, counter: Nat}

def lex(c: Cmp, d: Cmp) -> Cmp:
  match c:
    case LT{}:
      LT{}
    case EQ{}:
      d
    case GT{}:
      GT{}

def Stamp.cmp(a: Stamp, b: Stamp) -> Cmp:
  match a b:
    case Stamp{aw, ak} Stamp{bw, bk}:
      lex(Nat.cmp(aw, bw), Nat.cmp(ak, bk))

# `Clock::tick`: a wall time past the last reading starts a new count, else the count goes up.
def tick(+last: Stamp, +now: Nat) -> Stamp:
  match last:
    case Stamp{wall, counter}:
      later(Stamp, wall, now, Stamp{wall, 1n+counter}, Stamp{now, 0n})

def Entry.value(e: Entry) -> Nat:
  match e:
    case Entry{clock, value}:
      value

# `y` when value `bv` is greater than `av`, or equal with clock `bc` later than `ac`, else `x`.
def above(-T: Type, av: Nat, bv: Nat, ac: Nat, bc: Nat, x: T, y: T) -> T:
  match av bv:
    case 0n 0n:
      later(T, ac, bc, x, y)
    case 0n 1n+q:
      y
    case 1n+p 0n:
      x
    case 1n+p 1n+q:
      above(T, p, q, ac, bc, x, y)

# `wins` under `Strategy::Max`: the greater value replaces the held one, and an equal value
# replaces it only if it is later.
def merge_max(+held: Entry, +incoming: Entry) -> Entry:
  above(Entry, Entry.value(held), Entry.value(incoming), Entry.clock(held), Entry.clock(incoming), held, incoming)

# How a field merges: `Strategy::Lww` or `Strategy::Max`.
type Strat is Data:
  Lww{}
  Max{}

def merge_by(s: Strat, +held: Entry, +incoming: Entry) -> Entry:
  match s:
    case Lww{}:
      merge(held, incoming)
    case Max{}:
      merge_max(held, incoming)

# `State::apply` for one field that merges by `s`.
def apply_by(+s: Strat, held: Maybe<&2, Entry>, +incoming: Entry) -> Maybe<&2, Entry>:
  match held:
    case None{}:
      Some{incoming}
    case Some{+value}:
      Some{merge_by(s, value, incoming)}

# One field's register: how it merges, and what it holds so far.
type Reg is Data:
  Reg{strat: Strat, held: Maybe<&2, Entry>}

# A change: the position of the register it writes, and what it writes there.
type Write is Data:
  Write{key: Nat, entry: Entry}

def Write.clock(w: Write) -> Nat:
  match w:
    case Write{key, entry}:
      Entry.clock(entry)

def Reg.put(r: Reg, +e: Entry) -> Reg:
  match r:
    case Reg{+s, h}:
      Reg{s, apply_by(s, h, e)}

# A replica's registers after one change. A change to a position with no register is dropped.
def put(rs: List<&2, Reg>, k: Nat, +e: Entry) -> List<&2, Reg>:
  match rs k:
    case Nil{} _:
      Nil{}
    case r <> t 0n:
      Reg.put(r, e) <> t
    case r <> t 1n+p:
      r <> put(t, p, e)

def write(w: Write, rs: List<&2, Reg>) -> List<&2, Reg>:
  match w:
    case Write{k, e}:
      put(rs, k, e)

# A replica's registers after the changes `ws`, applied in the order they arrive.
def fold(ws: List<&2, Write>, rs: List<&2, Reg>) -> List<&2, Reg>:
  match ws:
    case Nil{}:
      rs
    case w <> t:
      fold(t, write(w, rs))

# An alias: gids `a` and `b` of one collection are one entity. A gid is a Nat here.
type Edge is Data:
  Edge{a: Nat, b: Nat}

# `ls[i]`; 0 past the end, which `labels` never reads.
def look(ls: List<&2, Nat>, i: Nat) -> Nat:
  match ls i:
    case Nil{} _:
      0n
    case h <> t 0n:
      h
    case h <> t 1n+p:
      look(t, p)

def offer(hit_a: Bool, hit_b: Bool, +la: Nat, +lb: Nat) -> Maybe<&2, Nat>:
  match hit_a:
    case True{}:
      Some{lb}
    case False{}:
      match hit_b:
        case True{}:
          Some{la}
        case False{}:
          None{}

# The label edge `e` offers gid `x`: its other end's, if `x` is an end.
def offer_of(e: Edge, +x: Nat, +ls: List<&2, Nat>) -> Maybe<&2, Nat>:
  match e:
    case Edge{+a, +b}:
      offer(Nat.is_eq(a, x), Nat.is_eq(b, x), look(ls, a), look(ls, b))

def lower(m: Maybe<&2, Nat>, +best: Nat) -> Nat:
  match m:
    case None{}:
      best
    case Some{v}:
      Nat.min(best, v)

# The smallest of `best` and every label the edges `es` offer gid `x`.
def scan(es: List<&2, Edge>, +x: Nat, +ls: List<&2, Nat>, best: Nat) -> Nat:
  match es:
    case Nil{}:
      best
    case e <> t:
      scan(t, x, ls, lower(offer_of(e, x, ls), best))

# One round: every gid in `xs` takes the smallest label among its own and its neighbours'.
def relabel(xs: List<&2, Nat>, +es: List<&2, Edge>, +ls: List<&2, Nat>) -> List<&2, Nat>:
  match xs:
    case Nil{}:
      Nil{}
    case Con{+x, t}:
      scan(es, x, ls, look(ls, x)) <> relabel(t, es, ls)

def rounds(k: Nat, +xs: List<&2, Nat>, +es: List<&2, Edge>, ls: List<&2, Nat>) -> List<&2, Nat>:
  match k:
    case 0n:
      ls
    case 1n+p:
      relabel(xs, es, rounds(p, xs, es, ls))

# Each of the gids `0..n` labelled with the smallest gid it reaches through the aliases `es`: two
# gids are in one group when their labels are equal. `n` rounds reach across any group.
def labels(+n: Nat, +es: List<&2, Edge>) -> List<&2, Nat>:
  rounds(n, List.range(n), es, List.range(n))

# One gid's registers: `$deleted` (1 is deleted), its owner (0 is none, 1+o is gid o) and one
# field. In the registers a replica holds, gid g's are at positions 3g, 3g+1 and 3g+2.
type Row is Data:
  Row{deleted: Reg, owner: Reg, field: Reg}

def rows(rs: List<&2, Reg>) -> List<&2, Row>:
  match rs:
    case Con{d, Con{o, Con{f, t}}}:
      Row{d, o, f} <> rows(t)
    case _:
      Nil{}

def Reg.value(r: Reg) -> Maybe<&2, Nat>:
  match r:
    case Reg{s, h}:
      match h:
        case None{}:
          None{}
        case Some{e}:
          Some{Entry.value(e)}

def is_one(m: Maybe<&2, Nat>) -> Bool:
  match m:
    case None{}:
      False{}
    case Some{v}:
      match v:
        case 0n:
          False{}
        case 1n+p:
          Nat.is_eq(p, 0n)

def at(bs: List<&2, Bool>, i: Nat) -> Bool:
  match bs i:
    case Nil{} _:
      False{}
    case h <> t 0n:
      h
    case h <> t 1n+p:
      at(t, p)

# Whether some gid labelled `g` is live.
def group_live(+g: Nat, ls: List<&2, Nat>, live: List<&2, Bool>) -> Bool:
  match ls live:
    case Con{l, lt} Con{b, bt}:
      Bool.or(Bool.and(Nat.is_eq(l, g), b), group_live(g, lt, bt))
    case _ _:
      False{}

def owner_ok(m: Maybe<&2, Nat>, +ls: List<&2, Nat>, +live: List<&2, Bool>) -> Bool:
  match m:
    case None{}:
      True{}
    case Some{v}:
      match v:
        case 0n:
          True{}
        case 1n+o:
          group_live(look(ls, o), ls, live)

# One round of liveness: a gid is live if it is not deleted and its owner, if it has one, is in a
# group with a member that `live` already counts as live.
def step_live(rs: List<&2, Row>, +ls: List<&2, Nat>, +live: List<&2, Bool>) -> List<&2, Bool>:
  match rs:
    case Nil{}:
      Nil{}
    case Con{Row{d, o, f}, t}:
      Bool.and(Bool.not(is_one(Reg.value(d))), owner_ok(Reg.value(o), ls, live)) <> step_live(t, ls, live)

def lives(k: Nat, +rs: List<&2, Row>, +ls: List<&2, Nat>, live: List<&2, Bool>) -> List<&2, Bool>:
  match k:
    case 0n:
      live
    case 1n+p:
      step_live(rs, ls, lives(p, rs, ls, live))

# The least set of live gids: `n` rounds from none, enough for any chain of owners.
def liveness(+n: Nat, +rs: List<&2, Row>, +ls: List<&2, Nat>) -> List<&2, Bool>:
  lives(n, rs, ls, List.replicate(Bool, n, False{}))

def take(hit: Bool, r: Reg, acc: Maybe<&2, Entry>) -> Maybe<&2, Entry>:
  match hit:
    case False{}:
      acc
    case True{}:
      match r:
        case Reg{+s, h}:
          match h:
            case None{}:
              acc
            case Some{+e}:
              apply_by(s, acc, e)

# The field's winner among the live members of the group labelled `g`.
def gather(rs: List<&2, Row>, ls: List<&2, Nat>, live: List<&2, Bool>, +g: Nat, acc: Maybe<&2, Entry>) -> Maybe<&2, Entry>:
  match rs ls live:
    case Con{Row{d, o, f}, rt} Con{l, lt} Con{b, bt}:
      gather(rt, lt, bt, g, take(Bool.and(Nat.is_eq(l, g), b), f, acc))
    case _ _ _:
      acc

def shown(visible: Bool, +rs: List<&2, Row>, +ls: List<&2, Nat>, +live: List<&2, Bool>, +g: Nat) -> Maybe<&2, Maybe<&2, Entry>>:
  match visible:
    case False{}:
      None{}
    case True{}:
      Some{gather(rs, ls, live, g, None{})}

def show(xs: List<&2, Nat>, +rs: List<&2, Row>, +ls: List<&2, Nat>, +live: List<&2, Bool>) -> List<&2, Maybe<&2, Maybe<&2, Entry>>>:
  match xs:
    case Nil{}:
      Nil{}
    case Con{+x, t}:
      shown(Bool.and(Nat.is_eq(look(ls, x), x), group_live(x, ls, live)), rs, ls, live, x) <> show(t, rs, ls, live)

# For each gid: nothing unless it names a live group, else the group's field.
def present(+n: Nat, rs: List<&2, Reg>, +ls: List<&2, Nat>) -> List<&2, Maybe<&2, Maybe<&2, Entry>>>:
  +rows = rows(rs)
  show(List.range(n), rows, ls, liveness(n, rows, ls))

# What a replica that started from `rs` shows for gids `0..n`, once it has received the writes
# `ws` and the aliases `es`.
def view(+n: Nat, ws: List<&2, Write>, rs: List<&2, Reg>, +es: List<&2, Edge>) -> List<&2, Maybe<&2, Maybe<&2, Entry>>>:
  present(n, fold(ws, rs), labels(n, es))
