//! Prints `model::view` for a fixed set of cases, one line per row, which schemas
//! `Schema::check` refuses, and what clocks issue. `lean/Cases.lean` prints the same from the Lean
//! model in the same form, and `make lean-diff` compares the two.

use fongo_sync_core::model::{ALIAS, Collection, FieldKind, Schema, view};
use fongo_sync_core::{Change, Clock, DELETED, Gid, Hlc, Strategy, Value};

const WALL_MAX: u64 = 0xFFFF_FFFF_FFFF;

fn gid(n: u128) -> Gid {
    Gid(n.to_be_bytes())
}

fn ch(g: u128, field: &str, value: Value, wall_ms: u64) -> Change {
    Change {
        collection: "things".into(),
        gid: gid(g),
        field: field.into(),
        value,
        clock: Hlc {
            wall_ms,
            counter: 0,
            node: gid(9),
        },
    }
}

fn at(collection: &str, g: Gid, field: &str, value: Value, wall_ms: u64) -> Change {
    Change {
        collection: collection.into(),
        gid: g,
        field: field.into(),
        value,
        clock: reading(wall_ms, 0, 9),
    }
}

fn text(s: &str) -> Value {
    Value::Text(s.into())
}

fn show(value: &Value) -> String {
    match value {
        Value::Null => "null".into(),
        Value::Int(i) => format!("int:{i}"),
        Value::Text(s) => format!("text:{s}"),
        Value::Ref(g) => format!("ref:{}", u128::from_be_bytes(g.0)),
        other => panic!("no Lean counterpart for {other:?}"),
    }
}

fn reading(wall_ms: u64, counter: u32, node: u128) -> Hlc {
    Hlc {
        wall_ms,
        counter,
        node: gid(node),
    }
}

fn show_reading(h: &Hlc) -> String {
    format!(
        "{} {} {}",
        h.wall_ms,
        h.counter,
        u128::from_be_bytes(h.node.0)
    )
}

fn schema_cases() -> Vec<(&'static str, Vec<Collection>)> {
    let things = || Collection::minted("things").plain("title", Strategy::Lww);
    let shelves = Collection::minted("shelves").plain("name", Strategy::Lww);
    vec![
        (
            "a set of plain fields",
            vec![
                things()
                    .plain("subtitle", Strategy::Lww)
                    .together(&["title", "subtitle"]),
            ],
        ),
        (
            "a reference in a set",
            vec![
                things()
                    .reference("link", "things")
                    .together(&["title", "link"]),
            ],
        ),
        (
            "an owner in a set",
            vec![
                shelves,
                things()
                    .owner("shelf", "shelves")
                    .together(&["title", "shelf"]),
            ],
        ),
        (
            "a Max field in a set",
            vec![
                things()
                    .plain("high", Strategy::Max)
                    .together(&["title", "high"]),
            ],
        ),
        (
            "a field in two sets",
            vec![
                things()
                    .plain("subtitle", Strategy::Lww)
                    .together(&["title"])
                    .together(&["title", "subtitle"]),
            ],
        ),
        (
            "an undeclared field in a set",
            vec![things().together(&["title", "subtitle"])],
        ),
        ("an empty set", vec![things().together(&[])]),
        (
            "a collection owning its own rows",
            vec![Collection::minted("things").owner("parent", "things")],
        ),
        (
            "a key referring to its own collection",
            vec![Collection::natural("things", &["parent"]).reference("parent", "things")],
        ),
        (
            "keys referring round two collections",
            vec![
                Collection::natural("a", &["b"]).reference("b", "b"),
                Collection::natural("b", &["a"]).reference("a", "a"),
            ],
        ),
        (
            "keys referring round three collections",
            vec![
                Collection::natural("a", &["b"]).reference("b", "b"),
                Collection::natural("b", &["c"]).reference("c", "c"),
                Collection::natural("c", &["a"]).reference("a", "a"),
            ],
        ),
        (
            "an owner in a key leading round",
            vec![
                Collection::natural("a", &["b"]).owner("b", "b"),
                Collection::natural("b", &["a"]).reference("a", "a"),
            ],
        ),
        (
            "a key chain going down",
            vec![
                Collection::natural("a", &["b"]).reference("b", "b"),
                Collection::natural("b", &["c"]).reference("c", "c"),
                Collection::natural("c", &["name"]).plain("name", Strategy::Lww),
            ],
        ),
        (
            "a loop through a minted collection",
            vec![
                Collection::natural("a", &["b"]).reference("b", "b"),
                Collection::minted("b").reference("a", "a"),
            ],
        ),
        (
            "a reference outside the key leading round",
            vec![
                Collection::natural("a", &["name"])
                    .plain("name", Strategy::Lww)
                    .reference("b", "b"),
                Collection::natural("b", &["a"]).reference("a", "a"),
            ],
        ),
        (
            "a collection declared twice",
            vec![
                Collection::minted("shelves").plain("name", Strategy::Lww),
                Collection::minted("shelves").plain("name", Strategy::Lww),
            ],
        ),
        (
            "a field declared twice",
            vec![things().plain("title", Strategy::Lww)],
        ),
        (
            "a reserved field name",
            vec![things().plain("$deleted", Strategy::Lww)],
        ),
        (
            "a reference to an unknown collection",
            vec![things().reference("link", "shelves")],
        ),
        (
            "an owner in an unknown collection",
            vec![things().owner("shelf", "shelves")],
        ),
        (
            "an empty key",
            vec![Collection::natural("things", &[]).plain("title", Strategy::Lww)],
        ),
        (
            "an undeclared key field",
            vec![Collection::natural("things", &["name"]).plain("title", Strategy::Lww)],
        ),
        (
            "owners round two collections",
            vec![
                Collection::minted("a").owner("b", "b"),
                Collection::minted("b").owner("a", "a"),
            ],
        ),
        (
            "owners round three collections",
            vec![
                Collection::minted("a").owner("b", "b"),
                Collection::minted("b").owner("c", "c"),
                Collection::minted("c").owner("a", "a"),
            ],
        ),
        (
            "an owner chain going down",
            vec![
                Collection::minted("a").owner("b", "b"),
                Collection::minted("b").owner("c", "c"),
                Collection::minted("c").plain("name", Strategy::Lww),
            ],
        ),
        (
            "an owner and a reference round two collections",
            vec![
                Collection::minted("a").owner("b", "b"),
                Collection::minted("b").reference("a", "a"),
            ],
        ),
    ]
}

fn author(name: &str) -> Gid {
    Gid::derive("authors", &[text(name)])
}

fn book(a: Gid, title: &str) -> Gid {
    Gid::derive("books", &[Value::Ref(a), text(title)])
}

fn library() -> Schema {
    Schema::new(vec![
        Collection::natural("authors", &["name"])
            .plain("born", Strategy::Lww)
            .plain("name", Strategy::Lww),
        Collection::natural("books", &["author", "title"])
            .reference("author", "authors")
            .plain("pages", Strategy::Max)
            .plain("title", Strategy::Lww),
        Collection::minted("notes")
            .owner("book", "books")
            .plain("page", Strategy::Lww)
            .plain("text", Strategy::Lww)
            .together(&["text", "page"]),
    ])
}

/// Writes `name` to an author's key field at clocks 1, 2, …, then the honest name.
fn rewritten_first(name: &str, others: &[&str]) -> Vec<Change> {
    let a = author(name);
    let mut changes: Vec<Change> = others
        .iter()
        .enumerate()
        .map(|(i, o)| at("authors", a, "name", text(o), i as u64 + 1))
        .collect();
    changes.push(at(
        "authors",
        a,
        "name",
        text(name),
        others.len() as u64 + 1,
    ));
    changes.push(at("authors", a, "born", Value::Int(1900), 50));
    changes
}

fn full_cases() -> Vec<(&'static str, Vec<Change>)> {
    let ann = author("Ann");
    let middlemarch = book(ann, "Middlemarch");
    let bob = author("Bob");
    let persuasion = book(bob, "Persuasion");
    let eve = author("Eve");
    let fay = author("Fay");
    let a_ann = author("A. Ann");
    let a_middlemarch = book(a_ann, "Middlemarch");
    vec![
        (
            "natural keys and a together set",
            vec![
                at("authors", ann, "name", text("Ann"), 1),
                at("authors", ann, "born", Value::Int(1819), 2),
                at("books", middlemarch, "author", Value::Ref(ann), 3),
                at("books", middlemarch, "title", text("Middlemarch"), 3),
                at("books", middlemarch, "pages", Value::Int(880), 4),
                at("notes", gid(100), "book", Value::Ref(middlemarch), 5),
                at("notes", gid(100), "text", text("Dorothea"), 5),
                at("notes", gid(100), "page", Value::Int(12), 5),
                at("notes", gid(100), "text", text("Casaubon"), 6),
                at("notes", gid(101), "book", Value::Ref(gid(7)), 7),
                at("notes", gid(101), "text", text("orphan"), 7),
            ],
        ),
        (
            "key rewritten after it was written",
            std::iter::once(at("authors", ann, "name", text("Ann"), 1))
                .chain((0..6).map(|i| at("authors", ann, "name", text(&format!("X{i}")), i + 2)))
                .collect(),
        ),
        (
            "four rewrites before the key hide it",
            rewritten_first("Bob", &["W1", "W2", "W3", "W4"])
                .into_iter()
                .chain([
                    at("books", persuasion, "author", Value::Ref(bob), 60),
                    at("books", persuasion, "title", text("Persuasion"), 60),
                ])
                .collect(),
        ),
        (
            "three rewrites before the key do not",
            rewritten_first("Cy", &["W1", "W2", "W3"]),
        ),
        (
            "a write counts once however often it arrives",
            std::iter::once(at("authors", author("Dee"), "name", text("W1"), 1))
                .chain(rewritten_first("Dee", &["W1", "W2", "W3"]))
                .collect(),
        ),
        (
            "ties go to the smaller value",
            vec![
                at("authors", eve, "name", text("W1"), 1),
                at("authors", eve, "name", text("W2"), 2),
                at("authors", eve, "name", text("W3"), 3),
                at("authors", eve, "name", text("Zed"), 4),
                at("authors", eve, "name", text("Eve"), 4),
                at("authors", fay, "name", text("W1"), 1),
                at("authors", fay, "name", text("W2"), 2),
                at("authors", fay, "name", text("W3"), 3),
                at("authors", fay, "name", text("Fay"), 4),
                at("authors", fay, "name", text("Abe"), 4),
            ],
        ),
        (
            "keys equal through an alias",
            vec![
                at("authors", ann, "name", text("Ann"), 1),
                at("authors", a_ann, "name", text("A. Ann"), 2),
                at("authors", a_ann, ALIAS, Value::Ref(ann), 3),
                at("books", middlemarch, "author", Value::Ref(ann), 4),
                at("books", middlemarch, "title", text("Middlemarch"), 4),
                at("books", a_middlemarch, "author", Value::Ref(a_ann), 5),
                at("books", a_middlemarch, "title", text("Middlemarch"), 5),
                at("books", a_middlemarch, "pages", Value::Int(900), 6),
            ],
        ),
        (
            "a together set tied at one clock shows each field's greater value",
            vec![
                at("notes", gid(104), "text", text("A"), 5),
                at("notes", gid(104), "page", Value::Int(1), 5),
                at("notes", gid(104), "page", Value::Int(0), 5),
                at("notes", gid(104), "text", text("B"), 5),
            ],
        ),
        (
            "clocks beyond the text form",
            vec![
                at("authors", author("Gus"), "name", text("Gus"), WALL_MAX + 1),
                at("notes", gid(102), "text", text("late"), WALL_MAX + 1),
                at("notes", gid(103), "text", text("edge"), WALL_MAX),
            ],
        ),
    ]
}

fn print_full(schema: &Schema, changes: &[Change]) {
    for (collection, rows) in &view(schema, changes).collections {
        for (g, row) in rows {
            let fields: Vec<String> = row
                .iter()
                .map(|(f, v)| format!("{f}={}", show(v)))
                .collect();
            println!(
                "{collection} {} {}",
                u128::from_be_bytes(g.0),
                fields.join(" ")
            );
        }
    }
}

enum Event {
    Tick(u64),
    Observe(Hlc),
}

fn clock_cases() -> Vec<(&'static str, Hlc, Vec<Event>)> {
    use Event::{Observe, Tick};
    vec![
        (
            "ticks and observes",
            reading(0, 0, 1),
            vec![
                Tick(1000),
                Tick(900),
                Observe(reading(5000, 3, 2)),
                Tick(10),
                Observe(reading(4000, 0, 2)),
                Tick(5001),
            ],
        ),
        (
            "full counter carries",
            reading(5, u32::MAX, 1),
            vec![Tick(5), Tick(0)],
        ),
        (
            "carries into the last millisecond",
            reading(WALL_MAX - 1, u32::MAX, 1),
            vec![Tick(0), Tick(0)],
        ),
        (
            "the last millisecond is not observed",
            reading(0, 0, 1),
            vec![
                Tick(1000),
                Observe(reading(WALL_MAX, 5, 7)),
                Tick(1000),
                Observe(reading(WALL_MAX, u32::MAX, 7)),
                Tick(1000),
            ],
        ),
        (
            "beyond the text form is not observed",
            reading(WALL_MAX, 5, 1),
            vec![
                Observe(reading(WALL_MAX + 1, 0, 2)),
                Tick(0),
                Observe(reading(u64::MAX, 0, 2)),
                Tick(0),
            ],
        ),
        (
            "a clock that issued every reading repeats the last",
            reading(WALL_MAX, u32::MAX, 1),
            vec![Tick(0), Tick(WALL_MAX), Tick(u64::MAX)],
        ),
    ]
}

fn main() {
    let schema = Schema::new(vec![
        Collection::minted("things")
            .plain("high", Strategy::Max)
            .field("link", FieldKind::Reference("things".into()), Strategy::Lww)
            .plain("title", Strategy::Lww),
    ]);
    schema.check().expect("valid schema");
    let cases: Vec<(&str, Vec<Change>)> = vec![
        (
            "later write wins",
            vec![ch(1, "title", text("a"), 1), ch(1, "title", text("b"), 2)],
        ),
        (
            "greater value wins",
            vec![
                ch(1, "high", Value::Int(3), 2),
                ch(1, "high", Value::Int(9), 1),
            ],
        ),
        (
            "alias joins",
            vec![
                ch(1, "title", text("a"), 1),
                ch(2, "title", text("b"), 2),
                ch(2, ALIAS, Value::Ref(gid(1)), 3),
            ],
        ),
        (
            "deleted",
            vec![
                ch(1, "title", text("a"), 1),
                ch(1, DELETED, Value::Int(1), 2),
            ],
        ),
        (
            "restored",
            vec![
                ch(1, "title", text("a"), 1),
                ch(1, DELETED, Value::Int(1), 2),
                ch(1, DELETED, Value::Int(0), 3),
            ],
        ),
        (
            "deleted member",
            vec![
                ch(1, "title", text("a"), 1),
                ch(2, "title", text("b"), 2),
                ch(2, ALIAS, Value::Ref(gid(1)), 3),
                ch(2, DELETED, Value::Int(1), 4),
            ],
        ),
        (
            "references",
            vec![
                ch(1, "link", Value::Ref(gid(3)), 1),
                ch(2, "title", text("x"), 2),
                ch(3, ALIAS, Value::Ref(gid(2)), 3),
                ch(4, "link", Value::Ref(gid(7)), 4),
                ch(5, "link", Value::Int(1), 5),
            ],
        ),
        (
            "reference to deleted",
            vec![
                ch(1, "link", Value::Ref(gid(2)), 1),
                ch(2, DELETED, Value::Int(1), 2),
            ],
        ),
        (
            "ignored aliases",
            vec![
                ch(1, "title", text("a"), 1),
                ch(1, ALIAS, Value::Ref(gid(1)), 2),
                ch(2, ALIAS, Value::Ref(gid(8)), 3),
                ch(3, ALIAS, Value::Int(1), 4),
            ],
        ),
        (
            "chain",
            vec![
                ch(5, "title", text("e"), 1),
                ch(4, ALIAS, Value::Ref(gid(5)), 2),
                ch(3, ALIAS, Value::Ref(gid(4)), 3),
                ch(3, "high", Value::Int(2), 4),
                ch(4, "high", text("z"), 5),
            ],
        ),
        (
            "tombstones neither 0 nor 1",
            vec![
                ch(1, "title", text("a"), 1),
                ch(1, DELETED, Value::Int(1), 2),
                ch(1, DELETED, Value::Int(2), 3),
                ch(2, "title", text("b"), 4),
                ch(2, DELETED, text("1"), 5),
                ch(3, DELETED, Value::Int(-1), 6),
                ch(4, DELETED, Value::Null, 7),
            ],
        ),
        (
            "an exact tie goes to the greater value",
            vec![
                ch(1, "title", text("b"), 1),
                ch(1, "title", text("a"), 1),
                ch(2, "title", text("a"), 1),
                ch(2, "title", text("b"), 1),
                ch(3, "high", Value::Int(4), 1),
                ch(3, "link", Value::Ref(gid(2)), 1),
                ch(3, "link", Value::Ref(gid(1)), 1),
            ],
        ),
        (
            "a tie between members goes to the greater value",
            vec![
                ch(1, "title", text("a"), 1),
                ch(2, "title", text("b"), 1),
                ch(2, ALIAS, Value::Ref(gid(1)), 2),
            ],
        ),
        (
            "a tombstone tie deletes",
            vec![
                ch(1, "title", text("a"), 1),
                ch(1, DELETED, Value::Int(0), 2),
                ch(1, DELETED, Value::Int(1), 2),
                ch(2, "title", text("b"), 1),
                ch(2, DELETED, Value::Int(1), 2),
                ch(2, DELETED, Value::Int(0), 2),
            ],
        ),
    ];
    for (name, changes) in &cases {
        println!("case {name}");
        for (g, row) in view(&schema, changes).rows("things") {
            let fields: Vec<String> = row
                .iter()
                .map(|(f, v)| format!("{f}={}", show(v)))
                .collect();
            println!("{} {}", u128::from_be_bytes(g.0), fields.join(" "));
        }
    }
    for (name, changes) in &cases {
        println!("full case {name}");
        print_full(&schema, changes);
    }
    let library = library();
    library.check().expect("valid schema");
    for (name, changes) in &full_cases() {
        println!("full case {name}");
        print_full(&library, changes);
    }
    for (name, collections) in schema_cases() {
        let verdict = match Schema::new(collections).check() {
            Ok(()) => "accepted",
            Err(_) => "refused",
        };
        println!("schema {name}: {verdict}");
    }
    for (name, start, events) in clock_cases() {
        println!("clock {name}");
        let mut clock = Clock::resume(start);
        for event in events {
            match event {
                Event::Tick(wall_ms) => {
                    let r = clock.tick(wall_ms);
                    println!("tick {wall_ms}: {}", show_reading(&r));
                }
                Event::Observe(remote) => {
                    clock.observe(&remote);
                    println!(
                        "observe {}: {}",
                        show_reading(&remote),
                        show_reading(&clock.last())
                    );
                }
            }
        }
    }
}
