//! The only place that talks to SQLite. Every operation locks the connection and works on its
//! raw handle synchronously, so that the evaluator's questions are answered by direct queries
//! inside the operation's transaction. Prepared statements are kept for the length of one
//! operation.

use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::{CStr, c_char, c_int};
use std::ptr::{self, NonNull};

use fongo_sync_core::{Gid, Value};
use libsqlite3_sys as ffi;

use crate::error::{Error, Result};

pub(crate) struct Db {
    raw: NonNull<ffi::sqlite3>,
    statements: RefCell<HashMap<String, NonNull<ffi::sqlite3_stmt>>>,
    /// The first failure of a read made where no error can be returned (an answer to the
    /// evaluator); every later step of the operation fails with it.
    failed: RefCell<Option<Error>>,
}

impl Db {
    /// # Safety
    /// `raw` must be an open connection that nothing else uses while this lives.
    pub(crate) unsafe fn new(raw: NonNull<ffi::sqlite3>) -> Db {
        Db {
            raw,
            statements: RefCell::new(HashMap::new()),
            failed: RefCell::new(None),
        }
    }

    fn failure(&self) -> Error {
        // SAFETY: the handle is open and used by this thread only.
        unsafe {
            let code = ffi::sqlite3_extended_errcode(self.raw.as_ptr());
            let message = CStr::from_ptr(ffi::sqlite3_errmsg(self.raw.as_ptr()))
                .to_string_lossy()
                .into_owned();
            Error::Sqlite { code, message }
        }
    }

    fn statement(&self, sql: &str) -> Result<NonNull<ffi::sqlite3_stmt>> {
        if let Some(stmt) = self.statements.borrow().get(sql) {
            return Ok(*stmt);
        }
        let mut stmt = ptr::null_mut();
        let len =
            c_int::try_from(sql.len()).map_err(|_| Error::Corrupt("statement too long".into()))?;
        // SAFETY: `sql` outlives the call, which copies what it needs.
        let status = unsafe {
            ffi::sqlite3_prepare_v3(
                self.raw.as_ptr(),
                sql.as_ptr().cast::<c_char>(),
                len,
                ffi::SQLITE_PREPARE_PERSISTENT,
                &mut stmt,
                ptr::null_mut(),
            )
        };
        if status != ffi::SQLITE_OK {
            return Err(self.failure());
        }
        let Some(stmt) = NonNull::new(stmt) else {
            return Err(Error::Corrupt(format!("no statement in {sql}")));
        };
        self.statements.borrow_mut().insert(sql.to_string(), stmt);
        Ok(stmt)
    }

    fn bind(&self, stmt: NonNull<ffi::sqlite3_stmt>, params: &[Value]) -> Result<()> {
        let s = stmt.as_ptr();
        for (i, param) in params.iter().enumerate() {
            let at =
                c_int::try_from(i + 1).map_err(|_| Error::Corrupt("too many parameters".into()))?;
            // SAFETY: SQLite copies text and blobs bound as transient before returning.
            let status = unsafe {
                match param {
                    Value::Null => ffi::sqlite3_bind_null(s, at),
                    Value::Int(v) => ffi::sqlite3_bind_int64(s, at, *v),
                    Value::Real(v) => ffi::sqlite3_bind_double(s, at, *v),
                    Value::Text(v) => ffi::sqlite3_bind_text64(
                        s,
                        at,
                        v.as_ptr().cast::<c_char>(),
                        v.len() as u64,
                        ffi::SQLITE_TRANSIENT(),
                        ffi::SQLITE_UTF8 as u8,
                    ),
                    Value::Blob(v) if v.is_empty() => ffi::sqlite3_bind_zeroblob(s, at, 0),
                    Value::Blob(v) => ffi::sqlite3_bind_blob64(
                        s,
                        at,
                        v.as_ptr().cast(),
                        v.len() as u64,
                        ffi::SQLITE_TRANSIENT(),
                    ),
                    Value::Ref(g) => ffi::sqlite3_bind_blob64(
                        s,
                        at,
                        g.0.as_ptr().cast(),
                        16,
                        ffi::SQLITE_TRANSIENT(),
                    ),
                }
            };
            if status != ffi::SQLITE_OK {
                return Err(self.failure());
            }
        }
        Ok(())
    }

    fn column(stmt: *mut ffi::sqlite3_stmt, i: c_int) -> Value {
        // SAFETY: `stmt` has a row, and `i` is one of its columns; text and blob pointers are
        // read before the statement moves on.
        unsafe {
            match ffi::sqlite3_column_type(stmt, i) {
                ffi::SQLITE_INTEGER => Value::Int(ffi::sqlite3_column_int64(stmt, i)),
                ffi::SQLITE_FLOAT => Value::Real(ffi::sqlite3_column_double(stmt, i)),
                ffi::SQLITE_TEXT => {
                    let text = ffi::sqlite3_column_text(stmt, i);
                    let len = ffi::sqlite3_column_bytes(stmt, i) as usize;
                    if text.is_null() {
                        Value::Text(String::new())
                    } else {
                        let bytes = std::slice::from_raw_parts(text, len);
                        Value::Text(String::from_utf8_lossy(bytes).into_owned())
                    }
                }
                ffi::SQLITE_BLOB => {
                    let blob = ffi::sqlite3_column_blob(stmt, i);
                    let len = ffi::sqlite3_column_bytes(stmt, i) as usize;
                    if blob.is_null() {
                        Value::Blob(Vec::new())
                    } else {
                        Value::Blob(std::slice::from_raw_parts(blob.cast::<u8>(), len).to_vec())
                    }
                }
                _ => Value::Null,
            }
        }
    }

    fn run(&self, sql: &str, params: &[Value], mut row: impl FnMut(&[Value])) -> Result<()> {
        if let Some(e) = self.failed.borrow().as_ref() {
            return Err(Error::Corrupt(format!("an earlier read failed: {e}")));
        }
        let stmt = self.statement(sql)?;
        let s = stmt.as_ptr();
        let outcome = self.bind(stmt, params).and_then(|()| {
            // SAFETY: the statement is prepared and bound.
            let width = unsafe { ffi::sqlite3_column_count(s) };
            let mut values = Vec::with_capacity(width as usize);
            loop {
                match unsafe { ffi::sqlite3_step(s) } {
                    ffi::SQLITE_ROW => {
                        values.clear();
                        values.extend((0..width).map(|i| Db::column(s, i)));
                        row(&values);
                    }
                    ffi::SQLITE_DONE => return Ok(()),
                    _ => return Err(self.failure()),
                }
            }
        });
        // SAFETY: resetting a statement that failed returns its error again, already read.
        unsafe {
            ffi::sqlite3_reset(s);
            ffi::sqlite3_clear_bindings(s);
        }
        outcome
    }

    /// The rows `sql` returns.
    pub(crate) fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<Vec<Value>>> {
        let mut out = Vec::new();
        self.run(sql, params, |row| out.push(row.to_vec()))?;
        Ok(out)
    }

    /// How many rows `sql` changed.
    pub(crate) fn exec(&self, sql: &str, params: &[Value]) -> Result<u64> {
        self.run(sql, params, |_| {})?;
        // SAFETY: the handle is open and used by this thread only.
        Ok(unsafe { ffi::sqlite3_changes64(self.raw.as_ptr()) } as u64)
    }

    /// A read whose failure cannot be returned to the caller: it is kept, and the operation fails
    /// at its next step.
    pub(crate) fn answer(&self, sql: &str, params: &[Value]) -> Vec<Vec<Value>> {
        match self.query(sql, params) {
            Ok(rows) => rows,
            Err(e) => {
                self.fail(e);
                Vec::new()
            }
        }
    }

    pub(crate) fn fail(&self, error: Error) {
        self.failed.borrow_mut().get_or_insert(error);
    }

    /// Fails if a read made through [`Db::answer`] failed.
    pub(crate) fn check(&self) -> Result<()> {
        match self.failed.borrow().as_ref() {
            Some(e) => Err(Error::Corrupt(format!("a read failed: {e}"))),
            None => Ok(()),
        }
    }

    /// The longest text or blob this connection stores.
    pub(crate) fn length_limit(&self) -> usize {
        // SAFETY: the handle is open; a negative value reads the limit without changing it.
        let limit = unsafe { ffi::sqlite3_limit(self.raw.as_ptr(), ffi::SQLITE_LIMIT_LENGTH, -1) };
        usize::try_from(limit).unwrap_or(0)
    }

    /// Runs `work` inside a savepoint: everything it does is kept, or none of it is.
    pub(crate) fn atomically<T>(&self, work: impl FnOnce(&Db) -> Result<T>) -> Result<T> {
        self.exec("SAVEPOINT fongo", &[])?;
        let outcome = work(self).and_then(|out| {
            self.check()?;
            self.exec("RELEASE fongo", &[])?;
            Ok(out)
        });
        if outcome.is_err() {
            // An interrupted statement may already have ended the transaction, so neither
            // statement is required to succeed.
            let _ = self.exec_unchecked("ROLLBACK TO fongo");
            let _ = self.exec_unchecked("RELEASE fongo");
        }
        outcome
    }

    /// [`Db::atomically`], taking the write lock first when no transaction is open, so that while
    /// another connection writes it waits in the busy handler rather than fail at its first write.
    pub(crate) fn atomically_writing<T>(&self, work: impl FnOnce(&Db) -> Result<T>) -> Result<T> {
        // SAFETY: the handle is open and used by this thread only.
        if unsafe { ffi::sqlite3_get_autocommit(self.raw.as_ptr()) } == 0 {
            return self.atomically(work);
        }
        self.exec("BEGIN IMMEDIATE", &[])?;
        let outcome = self
            .atomically(work)
            .and_then(|out| self.exec("COMMIT", &[]).map(|_| out));
        if outcome.is_err() {
            let _ = self.exec_unchecked("ROLLBACK");
        }
        outcome
    }

    fn exec_unchecked(&self, sql: &str) -> Result<()> {
        let stmt = self.statement(sql)?;
        // SAFETY: the statement is prepared and takes no parameters.
        unsafe {
            ffi::sqlite3_step(stmt.as_ptr());
            ffi::sqlite3_reset(stmt.as_ptr());
        }
        Ok(())
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        for (_, stmt) in self.statements.borrow_mut().drain() {
            // SAFETY: each statement was prepared on this handle and is finalized once.
            unsafe {
                ffi::sqlite3_finalize(stmt.as_ptr());
            }
        }
    }
}

/// Whether a write to an application's table failed because of what it wrote: a constraint, a
/// trigger of the application's that raised an error, a value too large or of a kind the table
/// or a function refuses. Such a write is refused and its entity held; any other failure (an
/// interruption, I/O, a full disk, a lock) ends the operation.
pub(crate) fn refused(error: &Error) -> bool {
    matches!(error, Error::Sqlite { code, .. } if matches!(
        code & 0xff,
        ffi::SQLITE_CONSTRAINT
            | ffi::SQLITE_ERROR
            | ffi::SQLITE_TOOBIG
            | ffi::SQLITE_MISMATCH
            | ffi::SQLITE_RANGE
    ))
}

/// A kernel value as stored: references as their 16 bytes, NaN as its bits (SQLite would store
/// NULL), each with a kind telling them apart from plain blobs.
pub(crate) fn stored(value: &Value) -> (Value, Value) {
    match value {
        Value::Ref(gid) => (Value::Blob(gid.0.to_vec()), Value::Int(1)),
        Value::Real(v) if v.is_nan() => (
            Value::Blob(v.to_bits().to_be_bytes().to_vec()),
            Value::Int(2),
        ),
        other => (other.clone(), Value::Int(0)),
    }
}

pub(crate) fn loaded(value: &Value, kind: &Value) -> Result<Value> {
    match (kind, value) {
        (Value::Int(0), v) => Ok(v.clone()),
        (Value::Int(1), Value::Blob(bytes)) => Gid::from_slice(bytes)
            .map(Value::Ref)
            .ok_or_else(|| Error::Corrupt("a reference is not 16 bytes".into())),
        (Value::Int(2), Value::Blob(bytes)) => <[u8; 8]>::try_from(bytes.as_slice())
            .map(|b| Value::Real(f64::from_bits(u64::from_be_bytes(b))))
            .map_err(|_| Error::Corrupt("a real is not 8 bytes".into())),
        _ => Err(Error::Corrupt(format!("a stored value of kind {kind:?}"))),
    }
}

pub(crate) fn as_gid(value: &Value) -> Result<Gid> {
    match value {
        Value::Blob(bytes) => {
            Gid::from_slice(bytes).ok_or_else(|| Error::Corrupt("a gid is not 16 bytes".into()))
        }
        _ => Err(Error::Corrupt("a gid is not a blob".into())),
    }
}

pub(crate) fn as_text(value: &Value) -> Result<&str> {
    match value {
        Value::Text(text) => Ok(text),
        _ => Err(Error::Corrupt("expected text".into())),
    }
}

pub(crate) fn as_int(value: &Value) -> Result<i64> {
    match value {
        Value::Int(v) => Ok(*v),
        _ => Err(Error::Corrupt("expected an integer".into())),
    }
}

pub(crate) fn quoted(name: &str) -> String {
    format!("\"{name}\"")
}
