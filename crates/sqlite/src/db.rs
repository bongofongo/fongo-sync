//! The only place that talks to the database driver.

use fongo_sync_core::Value;
use sqlx::sqlite::{SqliteArguments, SqliteRow};
use sqlx::{Row, Sqlite, SqliteConnection, ValueRef};

type Query<'q> = sqlx::query::Query<'q, Sqlite, SqliteArguments<'q>>;

fn bound<'q>(sql: &'q str, params: &'q [Value]) -> Query<'q> {
    let mut query = sqlx::query(sql);
    for param in params {
        query = match param {
            Value::Null => query.bind(None::<i64>),
            Value::Int(v) => query.bind(*v),
            Value::Real(v) => query.bind(*v),
            Value::Text(v) => query.bind(v.as_str()),
            Value::Blob(v) => query.bind(v.as_slice()),
            Value::Ref(v) => query.bind(v.0.to_vec()),
        };
    }
    query
}

pub(crate) async fn exec(
    conn: &mut SqliteConnection,
    sql: &str,
    params: &[Value],
) -> sqlx::Result<u64> {
    Ok(bound(sql, params)
        .execute(&mut *conn)
        .await?
        .rows_affected())
}

pub(crate) async fn query(
    conn: &mut SqliteConnection,
    sql: &str,
    params: &[Value],
) -> sqlx::Result<Vec<Vec<Value>>> {
    let rows = bound(sql, params).fetch_all(&mut *conn).await?;
    rows.iter()
        .map(|row| (0..row.len()).map(|i| decode(row, i)).collect())
        .collect()
}

fn decode(row: &SqliteRow, i: usize) -> sqlx::Result<Value> {
    if row.try_get_raw(i)?.is_null() {
        return Ok(Value::Null);
    }
    if let Ok(v) = row.try_get::<i64, _>(i) {
        return Ok(Value::Int(v));
    }
    if let Ok(v) = row.try_get::<f64, _>(i) {
        return Ok(Value::Real(v));
    }
    if let Ok(v) = row.try_get::<String, _>(i) {
        return Ok(Value::Text(v));
    }
    row.try_get::<Vec<u8>, _>(i).map(Value::Blob)
}
