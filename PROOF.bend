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

law above_same:
  for -T: Type
  for av: Nat
  for bv: Nat
  for ac: Nat
  for bc: Nat
  for -x: T
  {M.above(T, av, bv, ac, bc, x, x) == x : T}

def above_same(T, av, bv, ac, bc, x):
  match av bv:
    case 0n 0n:
      later_same(T, ac, bc, x)
    case 0n 1n+q:
      {==}
    case 1n+p 0n:
      {==}
    case 1n+p 1n+q:
      above_same(T, p, q, ac, bc, x)

law above_fn:
  for -T: Type
  for -U: Type
  for -f: T -> U
  for av: Nat
  for bv: Nat
  for ac: Nat
  for bc: Nat
  for -x: T
  for -y: T
  {f(M.above(T, av, bv, ac, bc, x, y)) == M.above(U, av, bv, ac, bc, f(x), f(y)) : U}

def above_fn(T, U, f, av, bv, ac, bc, x, y):
  match av bv:
    case 0n 0n:
      later_fn(T, U, f, ac, bc, x, y)
    case 0n 1n+q:
      {==}
    case 1n+p 0n:
      {==}
    case 1n+p 1n+q:
      above_fn(T, U, f, p, q, ac, bc, x, y)

law above_max:
  for +av: Nat
  for +bv: Nat
  for +ac: Nat
  for +bc: Nat
  {M.above(Nat, av, bv, ac, bc, av, bv) == Nat.max(av, bv) : Nat}

def above_max(av, bv, ac, bc):
  match av bv:
    case 0n 0n:
      later_same(Nat, ac, bc, 0n)
    case 0n 1n+q:
      {==}
    case 1n+p 0n:
      {==}
    case 1n+p 1n+q:
      %above_max(p, q, ac, bc) : {M.above(Nat, p, q, ac, bc, 1n+p, 1n+q) == 1n+_ : Nat}
      Equal.sym(Nat, 1n+M.above(Nat, p, q, ac, bc, p, q), M.above(Nat, p, q, ac, bc, 1n+p, 1n+q), above_fn(Nat, Nat, n => 1n+n, p, q, ac, bc, p, q))

law above_swap:
  for -T: Type
  for av: Nat
  for bv: Nat
  for ac: Nat
  for bc: Nat
  for -x: T
  for -y: T
  for ne: {ac != bc : Nat}
  {M.above(T, av, bv, ac, bc, x, y) == M.above(T, bv, av, bc, ac, y, x) : T}

def above_swap(T, av, bv, ac, bc, x, y, ne):
  match av bv:
    case 0n 0n:
      later_swap(T, ac, bc, x, y, ne)
    case 0n 1n+q:
      {==}
    case 1n+p 0n:
      {==}
    case 1n+p 1n+q:
      above_swap(T, p, q, ac, bc, x, y, ne)

law above_order:
  for -T: Type
  for hv: Nat
  for av: Nat
  for bv: Nat
  for +hc: Nat
  for +ac: Nat
  for +bc: Nat
  for -x: T
  for -y: T
  for -z: T
  for ne: {ac != bc : Nat}
  {M.above(T, Nat.max(hv, av), bv, M.above(Nat, hv, av, hc, ac, hc, ac), bc, M.above(T, hv, av, hc, ac, x, y), z) == M.above(T, Nat.max(hv, bv), av, M.above(Nat, hv, bv, hc, bc, hc, bc), ac, M.above(T, hv, bv, hc, bc, x, z), y) : T}

def above_order(T, hv, av, bv, hc, ac, bc, x, y, z, ne):
  match hv av bv:
    case 0n 0n 0n:
      %Equal.sym(Nat, M.later(Nat, hc, ac, hc, ac), Nat.max(hc, ac), later_max(hc, ac)) : {M.later(T, _, bc, M.later(T, hc, ac, x, y), z) == M.later(T, M.later(Nat, hc, bc, hc, bc), ac, M.later(T, hc, bc, x, z), y) : T}
      %Equal.sym(Nat, M.later(Nat, hc, bc, hc, bc), Nat.max(hc, bc), later_max(hc, bc)) : {M.later(T, Nat.max(hc, ac), bc, M.later(T, hc, ac, x, y), z) == M.later(T, _, ac, M.later(T, hc, bc, x, z), y) : T}
      later_order(T, hc, ac, bc, x, y, z, ne)
    case 0n 0n 1n+r:
      {==}
    case 0n 1n+q 0n:
      {==}
    case 0n 1n+q 1n+r:
      above_swap(T, q, r, ac, bc, y, z, ne)
    case 1n+p 0n 0n:
      {==}
    case 1n+p 0n 1n+r:
      {==}
    case 1n+p 1n+q 0n:
      {==}
    case 1n+p 1n+q 1n+r:
      above_order(T, p, q, r, hc, ac, bc, x, y, z, ne)

law above_twice:
  for -T: Type
  for hv: Nat
  for +cv: Nat
  for +hc: Nat
  for +cc: Nat
  for -x: T
  for -y: T
  {M.above(T, Nat.max(hv, cv), cv, M.above(Nat, hv, cv, hc, cc, hc, cc), cc, M.above(T, hv, cv, hc, cc, x, y), y) == M.above(T, hv, cv, hc, cc, x, y) : T}

def above_twice(T, hv, cv, hc, cc, x, y):
  match hv cv:
    case 0n 0n:
      %Equal.sym(Nat, M.later(Nat, hc, cc, hc, cc), Nat.max(hc, cc), later_max(hc, cc)) : {M.later(T, _, cc, M.later(T, hc, cc, x, y), y) == M.later(T, hc, cc, x, y) : T}
      later_twice(T, hc, cc, x, y)
    case 0n 1n+q:
      above_same(T, q, q, cc, cc, y)
    case 1n+p 0n:
      {==}
    case 1n+p 1n+q:
      above_twice(T, p, q, hc, cc, x, y)

law merge_max_value:
  for +h: M.Entry
  for +a: M.Entry
  {M.Entry.value(M.merge_max(h, a)) == Nat.max(M.Entry.value(h), M.Entry.value(a)) : Nat}

def merge_max_value(h, a):
  Equal.trans(Nat, M.Entry.value(M.merge_max(h, a)), M.above(Nat, M.Entry.value(h), M.Entry.value(a), M.Entry.clock(h), M.Entry.clock(a), M.Entry.value(h), M.Entry.value(a)), Nat.max(M.Entry.value(h), M.Entry.value(a)), above_fn(M.Entry, Nat, e => M.Entry.value(e), M.Entry.value(h), M.Entry.value(a), M.Entry.clock(h), M.Entry.clock(a), h, a), above_max(M.Entry.value(h), M.Entry.value(a), M.Entry.clock(h), M.Entry.clock(a)))

law merge_max_clock:
  for +h: M.Entry
  for +a: M.Entry
  {M.Entry.clock(M.merge_max(h, a)) == M.above(Nat, M.Entry.value(h), M.Entry.value(a), M.Entry.clock(h), M.Entry.clock(a), M.Entry.clock(h), M.Entry.clock(a)) : Nat}

def merge_max_clock(h, a):
  above_fn(M.Entry, Nat, e => M.Entry.clock(e), M.Entry.value(h), M.Entry.value(a), M.Entry.clock(h), M.Entry.clock(a), h, a)

law merge_max_same:
  for +c: M.Entry
  {M.merge_max(c, c) == c : M.Entry}

def merge_max_same(c):
  above_same(M.Entry, M.Entry.value(c), M.Entry.value(c), M.Entry.clock(c), M.Entry.clock(c), c)

law merge_max_swap:
  for +a: M.Entry
  for +b: M.Entry
  for ne: {M.Entry.clock(a) != M.Entry.clock(b) : Nat}
  {M.merge_max(a, b) == M.merge_max(b, a) : M.Entry}

def merge_max_swap(a, b, ne):
  above_swap(M.Entry, M.Entry.value(a), M.Entry.value(b), M.Entry.clock(a), M.Entry.clock(b), a, b, ne)

law merge_max_twice:
  for +h: M.Entry
  for +c: M.Entry
  {M.merge_max(M.merge_max(h, c), c) == M.merge_max(h, c) : M.Entry}

def merge_max_twice(h, c):
  %Equal.sym(Nat, M.Entry.value(M.merge_max(h, c)), Nat.max(M.Entry.value(h), M.Entry.value(c)), merge_max_value(h, c)) : {M.above(M.Entry, _, M.Entry.value(c), M.Entry.clock(M.merge_max(h, c)), M.Entry.clock(c), M.merge_max(h, c), c) == M.merge_max(h, c) : M.Entry}
  %Equal.sym(Nat, M.Entry.clock(M.merge_max(h, c)), M.above(Nat, M.Entry.value(h), M.Entry.value(c), M.Entry.clock(h), M.Entry.clock(c), M.Entry.clock(h), M.Entry.clock(c)), merge_max_clock(h, c)) : {M.above(M.Entry, Nat.max(M.Entry.value(h), M.Entry.value(c)), M.Entry.value(c), _, M.Entry.clock(c), M.merge_max(h, c), c) == M.merge_max(h, c) : M.Entry}
  above_twice(M.Entry, M.Entry.value(h), M.Entry.value(c), M.Entry.clock(h), M.Entry.clock(c), h, c)

law merge_max_order:
  for +h: M.Entry
  for +a: M.Entry
  for +b: M.Entry
  for ne: {M.Entry.clock(a) != M.Entry.clock(b) : Nat}
  {M.merge_max(M.merge_max(h, a), b) == M.merge_max(M.merge_max(h, b), a) : M.Entry}

def merge_max_order(h, a, b, ne):
  %Equal.sym(Nat, M.Entry.value(M.merge_max(h, a)), Nat.max(M.Entry.value(h), M.Entry.value(a)), merge_max_value(h, a)) : {M.above(M.Entry, _, M.Entry.value(b), M.Entry.clock(M.merge_max(h, a)), M.Entry.clock(b), M.merge_max(h, a), b) == M.merge_max(M.merge_max(h, b), a) : M.Entry}
  %Equal.sym(Nat, M.Entry.clock(M.merge_max(h, a)), M.above(Nat, M.Entry.value(h), M.Entry.value(a), M.Entry.clock(h), M.Entry.clock(a), M.Entry.clock(h), M.Entry.clock(a)), merge_max_clock(h, a)) : {M.above(M.Entry, Nat.max(M.Entry.value(h), M.Entry.value(a)), M.Entry.value(b), _, M.Entry.clock(b), M.merge_max(h, a), b) == M.merge_max(M.merge_max(h, b), a) : M.Entry}
  %Equal.sym(Nat, M.Entry.value(M.merge_max(h, b)), Nat.max(M.Entry.value(h), M.Entry.value(b)), merge_max_value(h, b)) : {M.above(M.Entry, Nat.max(M.Entry.value(h), M.Entry.value(a)), M.Entry.value(b), M.above(Nat, M.Entry.value(h), M.Entry.value(a), M.Entry.clock(h), M.Entry.clock(a), M.Entry.clock(h), M.Entry.clock(a)), M.Entry.clock(b), M.merge_max(h, a), b) == M.above(M.Entry, _, M.Entry.value(a), M.Entry.clock(M.merge_max(h, b)), M.Entry.clock(a), M.merge_max(h, b), a) : M.Entry}
  %Equal.sym(Nat, M.Entry.clock(M.merge_max(h, b)), M.above(Nat, M.Entry.value(h), M.Entry.value(b), M.Entry.clock(h), M.Entry.clock(b), M.Entry.clock(h), M.Entry.clock(b)), merge_max_clock(h, b)) : {M.above(M.Entry, Nat.max(M.Entry.value(h), M.Entry.value(a)), M.Entry.value(b), M.above(Nat, M.Entry.value(h), M.Entry.value(a), M.Entry.clock(h), M.Entry.clock(a), M.Entry.clock(h), M.Entry.clock(a)), M.Entry.clock(b), M.merge_max(h, a), b) == M.above(M.Entry, Nat.max(M.Entry.value(h), M.Entry.value(b)), M.Entry.value(a), _, M.Entry.clock(a), M.merge_max(h, b), a) : M.Entry}
  above_order(M.Entry, M.Entry.value(h), M.Entry.value(a), M.Entry.value(b), M.Entry.clock(h), M.Entry.clock(a), M.Entry.clock(b), h, a, b, ne)

law apply_by_any_order:
  for +s: M.Strat
  for held: Maybe<&2, M.Entry>
  for +a: M.Entry
  for +b: M.Entry
  for ne: {M.Entry.clock(a) != M.Entry.clock(b) : Nat}
  {M.apply_by(s, M.apply_by(s, held, a), b) == M.apply_by(s, M.apply_by(s, held, b), a) : Maybe<&2, M.Entry>}

def apply_by_any_order(s, held, a, b, ne):
  match s held:
    case M.Lww{} None{}:
      Equal.cong(M.Entry, Maybe<&2, M.Entry>, e => Some{e}, M.merge(a, b), M.merge(b, a), merge_swap(a, b, ne))
    case M.Lww{} Some{+v}:
      Equal.cong(M.Entry, Maybe<&2, M.Entry>, e => Some{e}, M.merge(M.merge(v, a), b), M.merge(M.merge(v, b), a), merge_order(v, a, b, ne))
    case M.Max{} None{}:
      Equal.cong(M.Entry, Maybe<&2, M.Entry>, e => Some{e}, M.merge_max(a, b), M.merge_max(b, a), merge_max_swap(a, b, ne))
    case M.Max{} Some{+v}:
      Equal.cong(M.Entry, Maybe<&2, M.Entry>, e => Some{e}, M.merge_max(M.merge_max(v, a), b), M.merge_max(M.merge_max(v, b), a), merge_max_order(v, a, b, ne))

law apply_by_twice:
  for +s: M.Strat
  for held: Maybe<&2, M.Entry>
  for +c: M.Entry
  {M.apply_by(s, M.apply_by(s, held, c), c) == M.apply_by(s, held, c) : Maybe<&2, M.Entry>}

def apply_by_twice(s, held, c):
  match s held:
    case M.Lww{} None{}:
      Equal.cong(M.Entry, Maybe<&2, M.Entry>, e => Some{e}, M.merge(c, c), c, merge_same(c))
    case M.Lww{} Some{+v}:
      Equal.cong(M.Entry, Maybe<&2, M.Entry>, e => Some{e}, M.merge(M.merge(v, c), c), M.merge(v, c), merge_twice(v, c))
    case M.Max{} None{}:
      Equal.cong(M.Entry, Maybe<&2, M.Entry>, e => Some{e}, M.merge_max(c, c), c, merge_max_same(c))
    case M.Max{} Some{+v}:
      Equal.cong(M.Entry, Maybe<&2, M.Entry>, e => Some{e}, M.merge_max(M.merge_max(v, c), c), M.merge_max(v, c), merge_max_twice(v, c))

def Laws.apply_max_any_order(held, a, b, ne):
  apply_by_any_order(M.Max{}, held, a, b, ne)

def Laws.apply_max_twice(held, c):
  apply_by_twice(M.Max{}, held, c)

law reg_any_order:
  for r: M.Reg
  for +a: M.Entry
  for +b: M.Entry
  for ne: {M.Entry.clock(a) != M.Entry.clock(b) : Nat}
  {M.Reg.put(M.Reg.put(r, a), b) == M.Reg.put(M.Reg.put(r, b), a) : M.Reg}

def reg_any_order(r, a, b, ne):
  match r:
    case M.Reg{+s, h}:
      Equal.cong(Maybe<&2, M.Entry>, M.Reg, m => M.Reg{s, m}, M.apply_by(s, M.apply_by(s, h, a), b), M.apply_by(s, M.apply_by(s, h, b), a), apply_by_any_order(s, h, a, b, ne))

law reg_twice:
  for r: M.Reg
  for +c: M.Entry
  {M.Reg.put(M.Reg.put(r, c), c) == M.Reg.put(r, c) : M.Reg}

def reg_twice(r, c):
  match r:
    case M.Reg{+s, h}:
      Equal.cong(Maybe<&2, M.Entry>, M.Reg, m => M.Reg{s, m}, M.apply_by(s, M.apply_by(s, h, c), c), M.apply_by(s, h, c), apply_by_twice(s, h, c))

law put_any_order:
  for rs: List<&2, M.Reg>
  for ka: Nat
  for kb: Nat
  for +a: M.Entry
  for +b: M.Entry
  for ne: {M.Entry.clock(a) != M.Entry.clock(b) : Nat}
  {M.put(M.put(rs, ka, a), kb, b) == M.put(M.put(rs, kb, b), ka, a) : List<&2, M.Reg>}

def put_any_order(rs, ka, kb, a, b, ne):
  match rs ka kb:
    case Nil{} _ _:
      {==}
    case r <> t 0n 0n:
      Equal.cong(M.Reg, List<&2, M.Reg>, x => x <> t, M.Reg.put(M.Reg.put(r, a), b), M.Reg.put(M.Reg.put(r, b), a), reg_any_order(r, a, b, ne))
    case r <> t 0n 1n+q:
      {==}
    case r <> t 1n+p 0n:
      {==}
    case r <> t 1n+p 1n+q:
      Equal.cong(List<&2, M.Reg>, List<&2, M.Reg>, x => r <> x, M.put(M.put(t, p, a), q, b), M.put(M.put(t, q, b), p, a), put_any_order(t, p, q, a, b, ne))

law put_twice:
  for rs: List<&2, M.Reg>
  for k: Nat
  for +c: M.Entry
  {M.put(M.put(rs, k, c), k, c) == M.put(rs, k, c) : List<&2, M.Reg>}

def put_twice(rs, k, c):
  match rs k:
    case Nil{} _:
      {==}
    case r <> t 0n:
      Equal.cong(M.Reg, List<&2, M.Reg>, x => x <> t, M.Reg.put(M.Reg.put(r, c), c), M.Reg.put(r, c), reg_twice(r, c))
    case r <> t 1n+p:
      Equal.cong(List<&2, M.Reg>, List<&2, M.Reg>, x => r <> x, M.put(M.put(t, p, c), p, c), M.put(t, p, c), put_twice(t, p, c))

law write_any_order:
  for +a: M.Write
  for +b: M.Write
  for rs: List<&2, M.Reg>
  for ne: {M.Write.clock(a) != M.Write.clock(b) : Nat}
  {M.write(b, M.write(a, rs)) == M.write(a, M.write(b, rs)) : List<&2, M.Reg>}

def write_any_order(a, b, rs, ne):
  match a b:
    case M.Write{ka, ea} M.Write{kb, eb}:
      put_any_order(rs, ka, kb, ea, eb, ne)

law write_twice:
  for +a: M.Write
  for rs: List<&2, M.Reg>
  {M.write(a, M.write(a, rs)) == M.write(a, rs) : List<&2, M.Reg>}

def write_twice(a, rs):
  match a:
    case M.Write{k, e}:
      put_twice(rs, k, e)

def Laws.fold_any_order(pre, a, b, post, rs, ne):
  match pre:
    case Nil{}:
      Equal.cong(List<&2, M.Reg>, List<&2, M.Reg>, x => M.fold(post, x), M.write(b, M.write(a, rs)), M.write(a, M.write(b, rs)), write_any_order(a, b, rs, ne))
    case w <> t:
      Laws.fold_any_order(t, a, b, post, M.write(w, rs), ne)

def Laws.fold_twice(pre, a, post, rs):
  match pre:
    case Nil{}:
      Equal.cong(List<&2, M.Reg>, List<&2, M.Reg>, x => M.fold(post, x), M.write(a, M.write(a, rs)), M.write(a, rs), write_twice(a, rs))
    case w <> t:
      Laws.fold_twice(t, a, post, M.write(w, rs))

law min_swap:
  for +b: Nat
  for +x: Nat
  for +y: Nat
  {Nat.min(Nat.min(b, x), y) == Nat.min(Nat.min(b, y), x) : Nat}

def min_swap(b, x, y):
  match b x y:
    case 0n 0n 0n:
      {==}
    case 0n 0n 1n+r:
      {==}
    case 0n 1n+q 0n:
      {==}
    case 0n 1n+q 1n+r:
      {==}
    case 1n+p 0n 0n:
      {==}
    case 1n+p 0n 1n+r:
      {==}
    case 1n+p 1n+q 0n:
      {==}
    case 1n+p 1n+q 1n+r:
      Equal.cong(Nat, Nat, n => 1n+n, Nat.min(Nat.min(p, q), r), Nat.min(Nat.min(p, r), q), min_swap(p, q, r))

law min_twice:
  for +b: Nat
  for +x: Nat
  {Nat.min(Nat.min(b, x), x) == Nat.min(b, x) : Nat}

def min_twice(b, x):
  match b x:
    case 0n 0n:
      {==}
    case 0n 1n+q:
      {==}
    case 1n+p 0n:
      {==}
    case 1n+p 1n+q:
      Equal.cong(Nat, Nat, n => 1n+n, Nat.min(Nat.min(p, q), q), Nat.min(p, q), min_twice(p, q))

law lower_swap:
  for m: Maybe<&2, Nat>
  for k: Maybe<&2, Nat>
  for +best: Nat
  {M.lower(k, M.lower(m, best)) == M.lower(m, M.lower(k, best)) : Nat}

def lower_swap(m, k, best):
  match m k:
    case None{} None{}:
      {==}
    case None{} Some{w}:
      {==}
    case Some{v} None{}:
      {==}
    case Some{v} Some{w}:
      min_swap(best, v, w)

law lower_twice:
  for m: Maybe<&2, Nat>
  for +best: Nat
  {M.lower(m, M.lower(m, best)) == M.lower(m, best) : Nat}

def lower_twice(m, best):
  match m:
    case None{}:
      {==}
    case Some{+v}:
      min_twice(best, v)

law scan_any_order:
  for pre: List<&2, M.Edge>
  for +e: M.Edge
  for +f: M.Edge
  for +post: List<&2, M.Edge>
  for +x: Nat
  for +ls: List<&2, Nat>
  for best: Nat
  {M.scan(List.append(&2, M.Edge, pre, e <> f <> post), x, ls, best) == M.scan(List.append(&2, M.Edge, pre, f <> e <> post), x, ls, best) : Nat}

def scan_any_order(pre, e, f, post, x, ls, best):
  match pre:
    case Nil{}:
      Equal.cong(Nat, Nat, b => M.scan(post, x, ls, b), M.lower(M.offer_of(f, x, ls), M.lower(M.offer_of(e, x, ls), best)), M.lower(M.offer_of(e, x, ls), M.lower(M.offer_of(f, x, ls), best)), lower_swap(M.offer_of(e, x, ls), M.offer_of(f, x, ls), best))
    case w <> t:
      scan_any_order(t, e, f, post, x, ls, M.lower(M.offer_of(w, x, ls), best))

law scan_twice:
  for pre: List<&2, M.Edge>
  for +e: M.Edge
  for +post: List<&2, M.Edge>
  for +x: Nat
  for +ls: List<&2, Nat>
  for best: Nat
  {M.scan(List.append(&2, M.Edge, pre, e <> e <> post), x, ls, best) == M.scan(List.append(&2, M.Edge, pre, e <> post), x, ls, best) : Nat}

def scan_twice(pre, e, post, x, ls, best):
  match pre:
    case Nil{}:
      Equal.cong(Nat, Nat, b => M.scan(post, x, ls, b), M.lower(M.offer_of(e, x, ls), M.lower(M.offer_of(e, x, ls), best)), M.lower(M.offer_of(e, x, ls), best), lower_twice(M.offer_of(e, x, ls), best))
    case w <> t:
      scan_twice(t, e, post, x, ls, M.lower(M.offer_of(w, x, ls), best))

law relabel_any_order:
  for xs: List<&2, Nat>
  for +pre: List<&2, M.Edge>
  for +e: M.Edge
  for +f: M.Edge
  for +post: List<&2, M.Edge>
  for +ls: List<&2, Nat>
  {M.relabel(xs, List.append(&2, M.Edge, pre, e <> f <> post), ls) == M.relabel(xs, List.append(&2, M.Edge, pre, f <> e <> post), ls) : List<&2, Nat>}

def relabel_any_order(xs, pre, e, f, post, ls):
  match xs:
    case Nil{}:
      {==}
    case Con{+x, t}:
      %relabel_any_order(t, pre, e, f, post, ls) : {M.scan(List.append(&2, M.Edge, pre, e <> f <> post), x, ls, M.look(ls, x)) <> M.relabel(t, List.append(&2, M.Edge, pre, e <> f <> post), ls) == M.scan(List.append(&2, M.Edge, pre, f <> e <> post), x, ls, M.look(ls, x)) <> _ : List<&2, Nat>}
      Equal.cong(Nat, List<&2, Nat>, h => h <> M.relabel(t, List.append(&2, M.Edge, pre, e <> f <> post), ls), M.scan(List.append(&2, M.Edge, pre, e <> f <> post), x, ls, M.look(ls, x)), M.scan(List.append(&2, M.Edge, pre, f <> e <> post), x, ls, M.look(ls, x)), scan_any_order(pre, e, f, post, x, ls, M.look(ls, x)))

law relabel_twice:
  for xs: List<&2, Nat>
  for +pre: List<&2, M.Edge>
  for +e: M.Edge
  for +post: List<&2, M.Edge>
  for +ls: List<&2, Nat>
  {M.relabel(xs, List.append(&2, M.Edge, pre, e <> e <> post), ls) == M.relabel(xs, List.append(&2, M.Edge, pre, e <> post), ls) : List<&2, Nat>}

def relabel_twice(xs, pre, e, post, ls):
  match xs:
    case Nil{}:
      {==}
    case Con{+x, t}:
      %relabel_twice(t, pre, e, post, ls) : {M.scan(List.append(&2, M.Edge, pre, e <> e <> post), x, ls, M.look(ls, x)) <> M.relabel(t, List.append(&2, M.Edge, pre, e <> e <> post), ls) == M.scan(List.append(&2, M.Edge, pre, e <> post), x, ls, M.look(ls, x)) <> _ : List<&2, Nat>}
      Equal.cong(Nat, List<&2, Nat>, h => h <> M.relabel(t, List.append(&2, M.Edge, pre, e <> e <> post), ls), M.scan(List.append(&2, M.Edge, pre, e <> e <> post), x, ls, M.look(ls, x)), M.scan(List.append(&2, M.Edge, pre, e <> post), x, ls, M.look(ls, x)), scan_twice(pre, e, post, x, ls, M.look(ls, x)))

law rounds_any_order:
  for +k: Nat
  for +xs: List<&2, Nat>
  for +pre: List<&2, M.Edge>
  for +e: M.Edge
  for +f: M.Edge
  for +post: List<&2, M.Edge>
  for +ls: List<&2, Nat>
  {M.rounds(k, xs, List.append(&2, M.Edge, pre, e <> f <> post), ls) == M.rounds(k, xs, List.append(&2, M.Edge, pre, f <> e <> post), ls) : List<&2, Nat>}

def rounds_any_order(k, xs, pre, e, f, post, ls):
  match k:
    case 0n:
      {==}
    case 1n+p:
      %rounds_any_order(p, xs, pre, e, f, post, ls) : {M.relabel(xs, List.append(&2, M.Edge, pre, e <> f <> post), M.rounds(p, xs, List.append(&2, M.Edge, pre, e <> f <> post), ls)) == M.relabel(xs, List.append(&2, M.Edge, pre, f <> e <> post), _) : List<&2, Nat>}
      relabel_any_order(xs, pre, e, f, post, M.rounds(p, xs, List.append(&2, M.Edge, pre, e <> f <> post), ls))

law rounds_twice:
  for +k: Nat
  for +xs: List<&2, Nat>
  for +pre: List<&2, M.Edge>
  for +e: M.Edge
  for +post: List<&2, M.Edge>
  for +ls: List<&2, Nat>
  {M.rounds(k, xs, List.append(&2, M.Edge, pre, e <> e <> post), ls) == M.rounds(k, xs, List.append(&2, M.Edge, pre, e <> post), ls) : List<&2, Nat>}

def rounds_twice(k, xs, pre, e, post, ls):
  match k:
    case 0n:
      {==}
    case 1n+p:
      %rounds_twice(p, xs, pre, e, post, ls) : {M.relabel(xs, List.append(&2, M.Edge, pre, e <> e <> post), M.rounds(p, xs, List.append(&2, M.Edge, pre, e <> e <> post), ls)) == M.relabel(xs, List.append(&2, M.Edge, pre, e <> post), _) : List<&2, Nat>}
      relabel_twice(xs, pre, e, post, M.rounds(p, xs, List.append(&2, M.Edge, pre, e <> e <> post), ls))

def Laws.labels_any_order(n, pre, x, y, post):
  rounds_any_order(n, List.range(n), pre, x, y, post, List.range(n))

def Laws.labels_twice(n, pre, x, post):
  rounds_twice(n, List.range(n), pre, x, post, List.range(n))

def Laws.view_from_registers_and_groups(n, ws, vs, rs, es, fs, same_registers, same_groups):
  %same_registers : {M.present(n, M.fold(ws, rs), M.labels(n, es)) == M.present(n, _, M.labels(n, fs)) : List<&2, Maybe<&2, Maybe<&2, M.Entry>>>}
  %same_groups : {M.present(n, M.fold(ws, rs), M.labels(n, es)) == M.present(n, M.fold(ws, rs), _) : List<&2, Maybe<&2, Maybe<&2, M.Entry>>>}
  {==}

def Laws.view_any_order_of_writes(n, pre, a, b, post, rs, es, ne):
  Laws.view_from_registers_and_groups(n, List.append(&2, M.Write, pre, a <> b <> post), List.append(&2, M.Write, pre, b <> a <> post), rs, es, es, Laws.fold_any_order(pre, a, b, post, rs, ne), {==})

def Laws.view_any_order_of_aliases(n, ws, rs, pre, x, y, post):
  Laws.view_from_registers_and_groups(n, ws, ws, rs, List.append(&2, M.Edge, pre, x <> y <> post), List.append(&2, M.Edge, pre, y <> x <> post), {==}, Laws.labels_any_order(n, pre, x, y, post))

def Laws.view_write_twice(n, pre, a, post, rs, es):
  Laws.view_from_registers_and_groups(n, List.append(&2, M.Write, pre, a <> a <> post), List.append(&2, M.Write, pre, a <> post), rs, es, es, Laws.fold_twice(pre, a, post, rs), {==})

def Laws.view_alias_twice(n, ws, rs, pre, x, post):
  Laws.view_from_registers_and_groups(n, ws, ws, rs, List.append(&2, M.Edge, pre, x <> x <> post), List.append(&2, M.Edge, pre, x <> post), {==}, Laws.labels_twice(n, pre, x, post))
