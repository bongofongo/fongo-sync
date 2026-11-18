import Base
import ./model.bend as M
import ./LAWS.bend as Laws

law later_same:
  for -T: Type
  for a: Nat
  for b: Nat
  for -x: T
  {M.later(T, a, b, x, x) == x : T}

def later_same(T, a, b, x):
  match a b:
    case 0n 0n:
      {==}
    case 0n 1n+q:
      {==}
    case 1n+p 0n:
      {==}
    case 1n+p 1n+q:
      later_same(T, p, q, x)

# Choosing, then applying a function, is applying it to both and then choosing.
law later_fn:
  for -T: Type
  for -U: Type
  for -f: T -> U
  for a: Nat
  for b: Nat
  for -x: T
  for -y: T
  {f(M.later(T, a, b, x, y)) == M.later(U, a, b, f(x), f(y)) : U}

def later_fn(T, U, f, a, b, x, y):
  match a b:
    case 0n 0n:
      {==}
    case 0n 1n+q:
      {==}
    case 1n+p 0n:
      {==}
    case 1n+p 1n+q:
      later_fn(T, U, f, p, q, x, y)

law later_swap:
  for -T: Type
  for a: Nat
  for b: Nat
  for -x: T
  for -y: T
  for ne: {a != b : Nat}
  {M.later(T, a, b, x, y) == M.later(T, b, a, y, x) : T}

def later_swap(T, a, b, x, y, ne):
  match a b:
    case 0n 0n:
      Empty.absurd({M.later(T, 0n, 0n, x, y) == M.later(T, 0n, 0n, y, x) : T}, ne({==}))
    case 0n 1n+q:
      {==}
    case 1n+p 0n:
      {==}
    case 1n+p 1n+q:
      later_swap(T, p, q, x, y, e => ne(Equal.cong(Nat, Nat, n => 1n+n, p, q, e)))

law later_max:
  for +a: Nat
  for +b: Nat
  {M.later(Nat, a, b, a, b) == Nat.max(a, b) : Nat}

def later_max(a, b):
  match a b:
    case 0n 0n:
      {==}
    case 0n 1n+q:
      {==}
    case 1n+p 0n:
      {==}
    case 1n+p 1n+q:
      %later_max(p, q) : {M.later(Nat, p, q, 1n+p, 1n+q) == 1n+_ : Nat}
      Equal.sym(Nat, 1n+M.later(Nat, p, q, p, q), M.later(Nat, p, q, 1n+p, 1n+q), later_fn(Nat, Nat, n => 1n+n, p, q, p, q))

law later_twice:
  for -T: Type
  for s: Nat
  for +c: Nat
  for -x: T
  for -y: T
  {M.later(T, Nat.max(s, c), c, M.later(T, s, c, x, y), y) == M.later(T, s, c, x, y) : T}

def later_twice(T, s, c, x, y):
  match s c:
    case 0n 0n:
      {==}
    case 0n 1n+q:
      later_same(T, q, q, y)
    case 1n+p 0n:
      {==}
    case 1n+p 1n+q:
      later_twice(T, p, q, x, y)

# The heart of convergence: held at clock s, then a, then b, equals held, then b, then a.
law later_order:
  for -T: Type
  for s: Nat
  for a: Nat
  for b: Nat
  for -x: T
  for -y: T
  for -z: T
  for ne: {a != b : Nat}
  {M.later(T, Nat.max(s, a), b, M.later(T, s, a, x, y), z) == M.later(T, Nat.max(s, b), a, M.later(T, s, b, x, z), y) : T}

def later_order(T, s, a, b, x, y, z, ne):
  match s a b:
    case 0n 0n 0n:
      Empty.absurd({M.later(T, Nat.max(0n, 0n), 0n, M.later(T, 0n, 0n, x, y), z) == M.later(T, Nat.max(0n, 0n), 0n, M.later(T, 0n, 0n, x, z), y) : T}, ne({==}))
    case 0n 0n 1n+r:
      {==}
    case 0n 1n+q 0n:
      {==}
    case 0n 1n+q 1n+r:
      later_swap(T, q, r, y, z, e => ne(Equal.cong(Nat, Nat, n => 1n+n, q, r, e)))
    case 1n+p 0n 0n:
      Empty.absurd({M.later(T, Nat.max(1n+p, 0n), 0n, M.later(T, 1n+p, 0n, x, y), z) == M.later(T, Nat.max(1n+p, 0n), 0n, M.later(T, 1n+p, 0n, x, z), y) : T}, ne({==}))
    case 1n+p 0n 1n+r:
      {==}
    case 1n+p 1n+q 0n:
      {==}
    case 1n+p 1n+q 1n+r:
      later_order(T, p, q, r, x, y, z, e => ne(Equal.cong(Nat, Nat, n => 1n+n, q, r, e)))

def Laws.merge_keeps_later_clock(held, incoming):
  match held incoming:
    case M.Entry{hc, hv} M.Entry{ic, iv}:
      Equal.trans(Nat, M.Entry.clock(M.later(M.Entry, hc, ic, M.Entry{hc, hv}, M.Entry{ic, iv})), M.later(Nat, hc, ic, hc, ic), Nat.max(hc, ic), later_fn(M.Entry, Nat, e => M.Entry.clock(e), hc, ic, M.Entry{hc, hv}, M.Entry{ic, iv}), later_max(hc, ic))

law merge_same:
  for +c: M.Entry
  {M.merge(c, c) == c : M.Entry}

def merge_same(c):
  match c:
    case M.Entry{cc, cv}:
      later_same(M.Entry, cc, cc, M.Entry{cc, cv})

law merge_swap:
  for +a: M.Entry
  for +b: M.Entry
  for ne: {M.Entry.clock(a) != M.Entry.clock(b) : Nat}
  {M.merge(a, b) == M.merge(b, a) : M.Entry}

def merge_swap(a, b, ne):
  match a b:
    case M.Entry{ac, av} M.Entry{bc, bv}:
      later_swap(M.Entry, ac, bc, M.Entry{ac, av}, M.Entry{bc, bv}, ne)

law merge_twice:
  for +h: M.Entry
  for +c: M.Entry
  {M.merge(M.merge(h, c), c) == M.merge(h, c) : M.Entry}

def merge_twice(h, c):
  match h c:
    case M.Entry{hc, hv} M.Entry{cc, cv}:
      %Equal.sym(Nat, M.Entry.clock(M.later(M.Entry, hc, cc, M.Entry{hc, hv}, M.Entry{cc, cv})), Nat.max(hc, cc), Laws.merge_keeps_later_clock(M.Entry{hc, hv}, M.Entry{cc, cv})) : {M.later(M.Entry, _, cc, M.later(M.Entry, hc, cc, M.Entry{hc, hv}, M.Entry{cc, cv}), M.Entry{cc, cv}) == M.later(M.Entry, hc, cc, M.Entry{hc, hv}, M.Entry{cc, cv}) : M.Entry}
      later_twice(M.Entry, hc, cc, M.Entry{hc, hv}, M.Entry{cc, cv})

law merge_order:
  for +h: M.Entry
  for +a: M.Entry
  for +b: M.Entry
  for ne: {M.Entry.clock(a) != M.Entry.clock(b) : Nat}
  {M.merge(M.merge(h, a), b) == M.merge(M.merge(h, b), a) : M.Entry}

def merge_order(h, a, b, ne):
  match h a b:
    case M.Entry{hc, hv} M.Entry{ac, av} M.Entry{bc, bv}:
      %Equal.sym(Nat, M.Entry.clock(M.later(M.Entry, hc, ac, M.Entry{hc, hv}, M.Entry{ac, av})), Nat.max(hc, ac), Laws.merge_keeps_later_clock(M.Entry{hc, hv}, M.Entry{ac, av})) : {M.later(M.Entry, _, bc, M.later(M.Entry, hc, ac, M.Entry{hc, hv}, M.Entry{ac, av}), M.Entry{bc, bv}) == M.later(M.Entry, M.Entry.clock(M.later(M.Entry, hc, bc, M.Entry{hc, hv}, M.Entry{bc, bv})), ac, M.later(M.Entry, hc, bc, M.Entry{hc, hv}, M.Entry{bc, bv}), M.Entry{ac, av}) : M.Entry}
      %Equal.sym(Nat, M.Entry.clock(M.later(M.Entry, hc, bc, M.Entry{hc, hv}, M.Entry{bc, bv})), Nat.max(hc, bc), Laws.merge_keeps_later_clock(M.Entry{hc, hv}, M.Entry{bc, bv})) : {M.later(M.Entry, Nat.max(hc, ac), bc, M.later(M.Entry, hc, ac, M.Entry{hc, hv}, M.Entry{ac, av}), M.Entry{bc, bv}) == M.later(M.Entry, _, ac, M.later(M.Entry, hc, bc, M.Entry{hc, hv}, M.Entry{bc, bv}), M.Entry{ac, av}) : M.Entry}
      later_order(M.Entry, hc, ac, bc, M.Entry{hc, hv}, M.Entry{ac, av}, M.Entry{bc, bv}, ne)

def Laws.apply_twice(held, c):
  match held:
    case None{}:
      Equal.cong(M.Entry, Maybe<&2, M.Entry>, e => Some{e}, M.merge(c, c), c, merge_same(c))
    case Some{+value}:
      Equal.cong(M.Entry, Maybe<&2, M.Entry>, e => Some{e}, M.merge(M.merge(value, c), c), M.merge(value, c), merge_twice(value, c))

def Laws.apply_any_order(held, a, b, ne):
  match held:
    case None{}:
      Equal.cong(M.Entry, Maybe<&2, M.Entry>, e => Some{e}, M.merge(a, b), M.merge(b, a), merge_swap(a, b, ne))
    case Some{+value}:
      Equal.cong(M.Entry, Maybe<&2, M.Entry>, e => Some{e}, M.merge(M.merge(value, a), b), M.merge(M.merge(value, b), a), merge_order(value, a, b, ne))

law cmp_refl:
  for a: Nat
  {Nat.cmp(a, a) == EQ{} : Cmp}

def cmp_refl(a):
  match a:
    case 0n:
      {==}
    case 1n+p:
      cmp_refl(p)

law cmp_succ:
  for a: Nat
  {Nat.cmp(a, 1n+a) == LT{} : Cmp}

def cmp_succ(a):
  match a:
    case 0n:
      {==}
    case 1n+p:
      cmp_succ(p)

# When the wall time is not past the last reading the first answer is taken, and when it is,
# the comparison of walls alone already says "earlier".
law later_lt:
  for w: Nat
  for now: Nat
  for -c: Cmp
  {M.later(Cmp, w, now, LT{}, M.lex(Nat.cmp(w, now), c)) == LT{} : Cmp}

def later_lt(w, now, c):
  match w now:
    case 0n 0n:
      {==}
    case 0n 1n+q:
      {==}
    case 1n+p 0n:
      {==}
    case 1n+p 1n+q:
      later_lt(p, q, c)

law tick_cases:
  for +w: Nat
  for +k: Nat
  for +now: Nat
  {M.later(Cmp, w, now, M.lex(Nat.cmp(w, w), Nat.cmp(k, 1n+k)), M.lex(Nat.cmp(w, now), Nat.cmp(k, 0n))) == LT{} : Cmp}

def tick_cases(w, k, now):
  %Equal.sym(Cmp, Nat.cmp(w, w), EQ{}, cmp_refl(w)) : {M.later(Cmp, w, now, M.lex(_, Nat.cmp(k, 1n+k)), M.lex(Nat.cmp(w, now), Nat.cmp(k, 0n))) == LT{} : Cmp}
  %Equal.sym(Cmp, Nat.cmp(k, 1n+k), LT{}, cmp_succ(k)) : {M.later(Cmp, w, now, _, M.lex(Nat.cmp(w, now), Nat.cmp(k, 0n))) == LT{} : Cmp}
  later_lt(w, now, Nat.cmp(k, 0n))

def Laws.tick_is_later(last, now):
  match last:
    case M.Stamp{w, k}:
      Equal.trans(Cmp, M.Stamp.cmp(M.Stamp{w, k}, M.later(M.Stamp, w, now, M.Stamp{w, 1n+k}, M.Stamp{now, 0n})), M.later(Cmp, w, now, M.lex(Nat.cmp(w, w), Nat.cmp(k, 1n+k)), M.lex(Nat.cmp(w, now), Nat.cmp(k, 0n))), LT{}, later_fn(M.Stamp, Cmp, t => M.Stamp.cmp(M.Stamp{w, k}, t), w, now, M.Stamp{w, 1n+k}, M.Stamp{now, 0n}), tick_cases(w, k, now))
