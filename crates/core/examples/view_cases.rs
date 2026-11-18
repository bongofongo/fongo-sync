//! Prints `model::view` for a fixed set of cases, one line per row. `lean/Cases.lean` prints the
//! Lean model's view of the same cases in the same form, and `make lean-diff` compares the two.

use fongo_sync_core::model::{ALIAS, Collection, FieldKind, Schema, view};
use fongo_sync_core::{Change, DELETED, Gid, Hlc, Strategy, Value};

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
}
