import FongoSync.RunModel

/-!
Prints `runView` and `Full.runView` for a fixed set of cases, which schemas `Schema.Restricted`
refuses, and what clocks issue. `crates/core/examples/view_cases.rs` prints the same from
`model::view`, `Schema::check` and `Clock` in the same form, and `make lean-diff` compares the two.
-/

open FongoSync

def schema : Schema :=
  ⟨"things", [⟨"high", .plain, .max⟩, ⟨"link", .reference, .lww⟩, ⟨"title", .plain, .lww⟩]⟩

def ch (gid : Nat) (field : String) (value : Value) (wall : Nat) : Change Value :=
  ⟨"things", gid, field, value, ⟨wall, 0, 9⟩⟩

def cases : List (String × List (Change Value)) := [
  ("later write wins", [ch 1 "title" (.text "a") 1, ch 1 "title" (.text "b") 2]),
  ("greater value wins", [ch 1 "high" (.int 3) 2, ch 1 "high" (.int 9) 1]),
  ("alias joins", [ch 1 "title" (.text "a") 1, ch 2 "title" (.text "b") 2, ch 2 ALIAS (.ref 1) 3]),
  ("deleted", [ch 1 "title" (.text "a") 1, ch 1 DELETED (.int 1) 2]),
  ("restored", [ch 1 "title" (.text "a") 1, ch 1 DELETED (.int 1) 2, ch 1 DELETED (.int 0) 3]),
  ("deleted member", [ch 1 "title" (.text "a") 1, ch 2 "title" (.text "b") 2,
    ch 2 ALIAS (.ref 1) 3, ch 2 DELETED (.int 1) 4]),
  ("references", [ch 1 "link" (.ref 3) 1, ch 2 "title" (.text "x") 2, ch 3 ALIAS (.ref 2) 3,
    ch 4 "link" (.ref 7) 4, ch 5 "link" (.int 1) 5]),
  ("reference to deleted", [ch 1 "link" (.ref 2) 1, ch 2 DELETED (.int 1) 2]),
  ("ignored aliases", [ch 1 "title" (.text "a") 1, ch 1 ALIAS (.ref 1) 2, ch 2 ALIAS (.ref 8) 3,
    ch 3 ALIAS (.int 1) 4]),
  ("chain", [ch 5 "title" (.text "e") 1, ch 4 ALIAS (.ref 5) 2, ch 3 ALIAS (.ref 4) 3,
    ch 3 "high" (.int 2) 4, ch 4 "high" (.text "z") 5]),
  ("tombstones neither 0 nor 1", [ch 1 "title" (.text "a") 1, ch 1 DELETED (.int 1) 2,
    ch 1 DELETED (.int 2) 3, ch 2 "title" (.text "b") 4, ch 2 DELETED (.text "1") 5,
    ch 3 DELETED (.int (-1)) 6, ch 4 DELETED .null 7]),
  ("an exact tie goes to the greater value", [ch 1 "title" (.text "b") 1,
    ch 1 "title" (.text "a") 1, ch 2 "title" (.text "a") 1, ch 2 "title" (.text "b") 1,
    ch 3 "high" (.int 4) 1, ch 3 "link" (.ref 2) 1, ch 3 "link" (.ref 1) 1]),
  ("a tie between members goes to the greater value", [ch 1 "title" (.text "a") 1,
    ch 2 "title" (.text "b") 1, ch 2 ALIAS (.ref 1) 2]),
  ("a tombstone tie deletes", [ch 1 "title" (.text "a") 1, ch 1 DELETED (.int 0) 2,
    ch 1 DELETED (.int 1) 2, ch 2 "title" (.text "b") 1, ch 2 DELETED (.int 1) 2,
    ch 2 DELETED (.int 0) 2])
]

def showValue : Value → String
  | .null => "null"
  | .int i => s!"int:{i}"
  | .text s => s!"text:{s}"
  | .blob b => s!"blob:{b}"
  | .ref g => s!"ref:{g}"

def render (cs : List (Change Value)) : List String :=
  (known schema cs).filterMap fun r =>
    (runView schema cs r).map fun row =>
      s!"{r} " ++ " ".intercalate (row.map fun (f, v) => s!"{f}={showValue v}")

def reading (wall counter node : Nat) : Hlc := ⟨wall, counter, node⟩

/-- A clock's start and what it is asked to do. -/
def clockCases : List (String × Hlc × List Event) := [
  ("ticks and observes", (reading 0 0 1, [.tick 1000, .tick 900, .observe (reading 5000 3 2),
    .tick 10, .observe (reading 4000 0 2), .tick 5001])),
  ("full counter carries", (reading 5 COUNTER_MAX 1, [.tick 5, .tick 0])),
  ("carries into the last millisecond", (reading (WALL_MAX - 1) COUNTER_MAX 1, [.tick 0, .tick 0])),
  ("the last millisecond is not observed", (reading 0 0 1, [.tick 1000,
    .observe (reading WALL_MAX 5 7), .tick 1000, .observe (reading WALL_MAX COUNTER_MAX 7),
    .tick 1000])),
  ("beyond the text form is not observed", (reading WALL_MAX 5 1,
    [.observe (reading (WALL_MAX + 1) 0 2), .tick 0, .observe (reading (2 ^ 64 - 1) 0 2), .tick 0])),
  ("a clock that issued every reading repeats the last", (reading WALL_MAX COUNTER_MAX 1,
    [.tick 0, .tick WALL_MAX, .tick (2 ^ 64 - 1)]))
]

def showReading (h : Hlc) : String := s!"{h.wall} {h.counter} {h.node}"

/-- After each event, the reading a tick issued or the clock after an observation. -/
def runClock (c : Clock) : List Event → List String
  | [] => []
  | .tick wall :: rest =>
    let (r, c') := c.tick wall
    s!"tick {wall}: {showReading r}" :: runClock c' rest
  | .observe remote :: rest =>
    let c' := c.observe remote
    s!"observe {showReading remote}: {showReading c'.last}" :: runClock c' rest

/-! ## `Gid::derive`: SHA-256 of the collection and the key, cut to sixteen bytes -/

def sha256K : Array UInt32 := #[
  0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
  0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
  0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
  0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
  0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
  0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
  0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
  0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2]

def rotr (x : UInt32) (n : UInt32) : UInt32 := (x >>> n) ||| (x <<< (32 - n))

/-- `n` as `width` big-endian bytes. -/
def bigEndian (width n : Nat) : List UInt8 :=
  (List.range width).reverse.map fun i => (n / 256 ^ i % 256).toUInt8

def word (b : List UInt8) (i : Nat) : UInt32 :=
  (List.range 4).foldl (fun w j => (w <<< 8) ||| (b.getD (4 * i + j) 0).toUInt32) 0

def compress (h : Array UInt32) (block : List UInt8) : Array UInt32 := Id.run do
  let mut w : Array UInt32 := (List.range 16).toArray.map (word block)
  for t in [16:64] do
    let s0 := rotr w[t-15]! 7 ^^^ rotr w[t-15]! 18 ^^^ (w[t-15]! >>> 3)
    let s1 := rotr w[t-2]! 17 ^^^ rotr w[t-2]! 19 ^^^ (w[t-2]! >>> 10)
    w := w.push (w[t-16]! + s0 + w[t-7]! + s1)
  let mut a := h[0]!
  let mut b := h[1]!
  let mut c := h[2]!
  let mut d := h[3]!
  let mut e := h[4]!
  let mut f := h[5]!
  let mut g := h[6]!
  let mut k := h[7]!
  for t in [0:64] do
    let t1 := k + (rotr e 6 ^^^ rotr e 11 ^^^ rotr e 25) + ((e &&& f) ^^^ (~~~e &&& g)) +
      sha256K[t]! + w[t]!
    let t2 := (rotr a 2 ^^^ rotr a 13 ^^^ rotr a 22) + ((a &&& b) ^^^ (a &&& c) ^^^ (b &&& c))
    k := g
    g := f
    f := e
    e := d + t1
    d := c
    c := b
    b := a
    a := t1 + t2
  return #[h[0]! + a, h[1]! + b, h[2]! + c, h[3]! + d, h[4]! + e, h[5]! + f, h[6]! + g, h[7]! + k]

def sha256 (msg : List UInt8) : List UInt8 :=
  let padded := msg ++ [0x80] ++ List.replicate ((64 + 55 - msg.length % 64) % 64) 0 ++
    bigEndian 8 (8 * msg.length)
  let h := (List.range (padded.length / 64)).foldl
    (fun h i => compress h ((padded.drop (64 * i)).take 64))
    #[0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
      0x5be0cd19]
  h.toList.flatMap fun x => bigEndian 4 x.toNat

/-- `Value::encode`: a tag, the payload's length, the payload. -/
def encode (v : Value) : List UInt8 :=
  let (tag, bytes) : UInt8 × List UInt8 := match v with
    | .null => (0, [])
    | .int i => (1, bigEndian 8 (i % 2 ^ 64).toNat)
    | .text t => (3, t.toUTF8.toList)
    | .blob b => (4, b)
    | .ref g => (5, bigEndian 16 g)
  tag :: bigEndian 8 bytes.length ++ bytes

def derive (collection : String) (key : List Value) : Gid :=
  let digest := sha256 (bigEndian 8 collection.utf8ByteSize ++ collection.toUTF8.toList ++
    key.flatMap encode)
  (digest.take 16).foldl (fun n b => 256 * n + b.toNat) 0

section Schemas

open Full

def plainLww (name : String) : Full.Field := ⟨name, .plain, .lww⟩

def things (fields : List Full.Field) (together : List (List String)) : Full.Collection :=
  ⟨"things", fields, together, none⟩

def shelves : Full.Collection := ⟨"shelves", [plainLww "name"], [], none⟩

/-- Schemas that differ only in the rules `Schema.Restricted` states. -/
def schemaCases : List (String × List Full.Collection) := [
  ("a set of plain fields", [things [plainLww "title", plainLww "subtitle"] [["title", "subtitle"]]]),
  ("a reference in a set", [things [plainLww "title", ⟨"link", .reference "things", .lww⟩]
    [["title", "link"]]]),
  ("an owner in a set", [shelves, things [plainLww "title", ⟨"shelf", .owner "shelves", .lww⟩]
    [["title", "shelf"]]]),
  ("a Max field in a set", [things [plainLww "title", ⟨"high", .plain, .max⟩] [["title", "high"]]]),
  ("a field in two sets", [things [plainLww "title", plainLww "subtitle"]
    [["title"], ["title", "subtitle"]]]),
  ("an undeclared field in a set", [things [plainLww "title"] [["title", "subtitle"]]]),
  ("an empty set", [things [plainLww "title"] [[]]]),
  ("a collection owning its own rows", [things [⟨"parent", .owner "things", .lww⟩] []]),
  ("a key referring to its own collection",
    [⟨"things", [⟨"parent", .reference "things", .lww⟩], [], some ["parent"]⟩]),
  ("keys referring round two collections",
    [⟨"a", [⟨"b", .reference "b", .lww⟩], [], some ["b"]⟩,
     ⟨"b", [⟨"a", .reference "a", .lww⟩], [], some ["a"]⟩]),
  ("keys referring round three collections",
    [⟨"a", [⟨"b", .reference "b", .lww⟩], [], some ["b"]⟩,
     ⟨"b", [⟨"c", .reference "c", .lww⟩], [], some ["c"]⟩,
     ⟨"c", [⟨"a", .reference "a", .lww⟩], [], some ["a"]⟩]),
  ("an owner in a key leading round",
    [⟨"a", [⟨"b", .owner "b", .lww⟩], [], some ["b"]⟩,
     ⟨"b", [⟨"a", .reference "a", .lww⟩], [], some ["a"]⟩]),
  ("a key chain going down",
    [⟨"a", [⟨"b", .reference "b", .lww⟩], [], some ["b"]⟩,
     ⟨"b", [⟨"c", .reference "c", .lww⟩], [], some ["c"]⟩,
     ⟨"c", [plainLww "name"], [], some ["name"]⟩]),
  ("a loop through a minted collection",
    [⟨"a", [⟨"b", .reference "b", .lww⟩], [], some ["b"]⟩,
     ⟨"b", [⟨"a", .reference "a", .lww⟩], [], none⟩]),
  ("a reference outside the key leading round",
    [⟨"a", [plainLww "name", ⟨"b", .reference "b", .lww⟩], [], some ["name"]⟩,
     ⟨"b", [⟨"a", .reference "a", .lww⟩], [], some ["a"]⟩])
]

end Schemas

/-! ## The view over several collections -/

section Full

open Full

def thingsFull : Full.Schema :=
  ⟨[⟨"things", [⟨"high", .plain, .max⟩, ⟨"link", .reference "things", .lww⟩,
    ⟨"title", .plain, .lww⟩], [], none⟩], derive⟩

/-- Collections in name order, as `model::View` lists them. -/
def library : Full.Schema :=
  ⟨[⟨"authors", [⟨"born", .plain, .lww⟩, ⟨"name", .plain, .lww⟩], [], some ["name"]⟩,
    ⟨"books", [⟨"author", .reference "authors", .lww⟩, ⟨"pages", .plain, .max⟩,
      ⟨"title", .plain, .lww⟩], [], some ["author", "title"]⟩,
    ⟨"notes", [⟨"book", .owner "books", .lww⟩, ⟨"page", .plain, .lww⟩, ⟨"text", .plain, .lww⟩],
      [["text", "page"]], none⟩], derive⟩

def at_ (collection : String) (gid : Nat) (field : String) (value : Value) (wall : Nat) :
    Change Value :=
  ⟨collection, gid, field, value, ⟨wall, 0, 9⟩⟩

def author (name : String) : Gid := derive "authors" [.text name]

def book (a : Gid) (title : String) : Gid := derive "books" [.ref a, .text title]

/-- Writes `name` to an author's key field at clocks `1, 2, …`, then the honest name. -/
def rewrittenFirst (name : String) (others : List String) : List (Change Value) :=
  (others.zipIdx.map fun (o, i) => at_ "authors" (author name) "name" (.text o) (i + 1)) ++
    [at_ "authors" (author name) "name" (.text name) (others.length + 1),
     at_ "authors" (author name) "born" (.int 1900) 50]

def fullCases : List (String × List (Change Value)) := [
  ("natural keys and a together set", [
    at_ "authors" (author "Ann") "name" (.text "Ann") 1,
    at_ "authors" (author "Ann") "born" (.int 1819) 2,
    at_ "books" (book (author "Ann") "Middlemarch") "author" (.ref (author "Ann")) 3,
    at_ "books" (book (author "Ann") "Middlemarch") "title" (.text "Middlemarch") 3,
    at_ "books" (book (author "Ann") "Middlemarch") "pages" (.int 880) 4,
    at_ "notes" 100 "book" (.ref (book (author "Ann") "Middlemarch")) 5,
    at_ "notes" 100 "text" (.text "Dorothea") 5,
    at_ "notes" 100 "page" (.int 12) 5,
    at_ "notes" 100 "text" (.text "Casaubon") 6,
    at_ "notes" 101 "book" (.ref 7) 7,
    at_ "notes" 101 "text" (.text "orphan") 7]),
  ("key rewritten after it was written", [
    at_ "authors" (author "Ann") "name" (.text "Ann") 1] ++
    ((List.range 6).map fun i => at_ "authors" (author "Ann") "name" (.text s!"X{i}") (i + 2))),
  ("four rewrites before the key hide it",
    rewrittenFirst "Bob" ["W1", "W2", "W3", "W4"] ++ [
    at_ "books" (book (author "Bob") "Persuasion") "author" (.ref (author "Bob")) 60,
    at_ "books" (book (author "Bob") "Persuasion") "title" (.text "Persuasion") 60]),
  ("three rewrites before the key do not", rewrittenFirst "Cy" ["W1", "W2", "W3"]),
  ("a write counts once however often it arrives",
    at_ "authors" (author "Dee") "name" (.text "W1") 1 :: rewrittenFirst "Dee" ["W1", "W2", "W3"]),
  ("ties go to the smaller value", [
    at_ "authors" (author "Eve") "name" (.text "W1") 1,
    at_ "authors" (author "Eve") "name" (.text "W2") 2,
    at_ "authors" (author "Eve") "name" (.text "W3") 3,
    at_ "authors" (author "Eve") "name" (.text "Zed") 4,
    at_ "authors" (author "Eve") "name" (.text "Eve") 4,
    at_ "authors" (author "Fay") "name" (.text "W1") 1,
    at_ "authors" (author "Fay") "name" (.text "W2") 2,
    at_ "authors" (author "Fay") "name" (.text "W3") 3,
    at_ "authors" (author "Fay") "name" (.text "Fay") 4,
    at_ "authors" (author "Fay") "name" (.text "Abe") 4]),
  ("keys equal through an alias", [
    at_ "authors" (author "Ann") "name" (.text "Ann") 1,
    at_ "authors" (author "A. Ann") "name" (.text "A. Ann") 2,
    at_ "authors" (author "A. Ann") ALIAS (.ref (author "Ann")) 3,
    at_ "books" (book (author "Ann") "Middlemarch") "author" (.ref (author "Ann")) 4,
    at_ "books" (book (author "Ann") "Middlemarch") "title" (.text "Middlemarch") 4,
    at_ "books" (book (author "A. Ann") "Middlemarch") "author" (.ref (author "A. Ann")) 5,
    at_ "books" (book (author "A. Ann") "Middlemarch") "title" (.text "Middlemarch") 5,
    at_ "books" (book (author "A. Ann") "Middlemarch") "pages" (.int 900) 6]),
  ("a together set tied at one clock shows each field's greater value", [
    at_ "notes" 104 "text" (.text "A") 5,
    at_ "notes" 104 "page" (.int 1) 5,
    at_ "notes" 104 "page" (.int 0) 5,
    at_ "notes" 104 "text" (.text "B") 5]),
  ("clocks beyond the text form", [
    at_ "authors" (author "Gus") "name" (.text "Gus") (WALL_MAX + 1),
    at_ "notes" 102 "text" (.text "late") (WALL_MAX + 1),
    at_ "notes" 103 "text" (.text "edge") WALL_MAX])
]

def sortRow (row : Full.Row) : Full.Row :=
  row.mergeSort fun a b => compare a.1 b.1 != .gt

def renderFull (sc : Full.Schema) (cs : List (Change Value)) : List String :=
  sc.collections.flatMap fun c =>
    (gids sc (candidates sc cs)).filterMap fun r =>
      (runView sc cs c.name r).map fun row =>
        s!"{c.name} {r} " ++ " ".intercalate ((sortRow row).map fun (f, v) => s!"{f}={showValue v}")

end Full

def main : IO Unit := do
  for (name, cs) in cases do
    IO.println s!"case {name}"
    for line in render cs do
      IO.println line
  for (name, cs) in cases do
    IO.println s!"full case {name}"
    for line in renderFull thingsFull (cs.map fun c => { c with collection := "things" }) do
      IO.println line
  for (name, cs) in fullCases do
    IO.println s!"full case {name}"
    for line in renderFull library cs do
      IO.println line
  for (name, collections) in schemaCases do
    let sc : Full.Schema := ⟨collections, fun _ _ => 0⟩
    IO.println s!"schema {name}: {if decide sc.Restricted then "accepted" else "refused"}"
  for (name, start, events) in clockCases do
    IO.println s!"clock {name}"
    for line in runClock ⟨start⟩ events do
      IO.println line

