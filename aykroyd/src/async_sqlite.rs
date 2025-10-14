use rusqlite::params_from_iter;

use crate::client::{FromColumnIndexed, FromColumnNamed, ToParam};
use crate::query::StaticQueryText;
use crate::{error, FromRow, Query, QueryOne, Statement};

pub type Error = error::Error<async_sqlite::Error>;

impl<T> FromColumnIndexed<Client> for T
where
    T: rusqlite::types::FromSql,
{
    fn from_column(row: &rusqlite::Row, index: usize) -> Result<Self, Error> {
        row.get(index)
            .map_err(|e| Error::from_column(async_sqlite::Error::Rusqlite(e)))
    }
}

impl<T> FromColumnNamed<Client> for T
where
    T: rusqlite::types::FromSql,
{
    fn from_column(row: &rusqlite::Row, name: &str) -> Result<Self, Error> {
        row.get(name)
            .map_err(|e| Error::from_column(async_sqlite::Error::Rusqlite(e)))
    }
}

impl<T> ToParam<Client> for T
where
    T: rusqlite::types::ToSql + Send + Sync,
{
    fn to_param(&self) -> &(dyn rusqlite::types::ToSql + Send + Sync) {
        self
    }
}

#[derive(Clone)]
pub struct Client(async_sqlite::Client);

impl crate::client::Client for Client {
    type Row<'a> = rusqlite::Row<'a>;
    type Param<'a> = &'a (dyn rusqlite::types::ToSql + Send + Sync);
    type Error = async_sqlite::Error;
}

impl AsMut<async_sqlite::Client> for Client {
    fn as_mut(&mut self) -> &mut async_sqlite::Client {
        &mut self.0
    }
}

impl AsRef<async_sqlite::Client> for Client {
    fn as_ref(&self) -> &async_sqlite::Client {
        &self.0
    }
}

impl From<async_sqlite::Client> for Client {
    fn from(inner: async_sqlite::Client) -> Self {
        Client(inner)
    }
}

impl Client {
    /// Open a database at `path`.
    pub async fn open<P: AsRef<std::path::Path>>(path: P) -> Result<Self, Error> {
        let client = async_sqlite::ClientBuilder::new()
            .path(path.as_ref())
            .open()
            .await
            .map_err(Error::connect)?;
        Ok(Client(client))
    }

    /// Open an in-memory database.
    pub async fn open_in_memory() -> Result<Self, Error> {
        let client = async_sqlite::ClientBuilder::new()
            .path(":memory:")
            .open()
            .await
            .map_err(Error::connect)?;
        Ok(Client(client))
    }

    /// Prepare a static query text (cached prepare on the underlying connection).
    pub async fn prepare<S: StaticQueryText>(&mut self) -> Result<(), Error> {
        // Use client.conn to execute rusqlite prepare_cached on the background connection.
        // We ignore the prepared statement handle and rely on the cache in rusqlite.
        let inner = &self.0;
        inner
            .conn(|conn| conn.prepare_cached(S::QUERY_TEXT).map(|_stmt| ()))
            .await
            .map_err(Error::prepare)?;
        Ok(())
    }

    /// Run a query returning multiple rows.
    pub async fn query<Q: Query<Self> + Send + Sync + 'static>(
        &mut self,
        query: Q,
    ) -> Result<Vec<Q::Row>, Error>
    where
        Q::Row: FromRow<Self> + Send + Sync + 'static,
    {
        let inner = &self.0;
        let rows_vec = inner
            .conn(move |conn| {
                let sql = query.query_text();
                let params = query.to_params().unwrap_or_default();
                let params = params_from_iter(params.iter());

                // This closure runs in the background on the connection's thread,
                // so we can use rusqlite API synchronously here.
                let mut stmt = conn.prepare_cached(&sql)?;
                let mut rows = stmt.query(params)?;

                let mut out = Vec::new();
                while let Some(row) = rows.next()? {
                    out.push(FromRow::from_row(row).unwrap());
                }
                Ok(out)
            })
            .await
            .map_err(Error::query)?;

        Ok(rows_vec)
    }

    /// Query and expect exactly one row.
    pub async fn query_one<Q: QueryOne<Self> + Send + Sync + 'static>(
        &mut self,
        query: Q,
    ) -> Result<Q::Row, Error>
    where
        Q::Row: FromRow<Self> + Send + Sync + 'static,
    {
        let inner = &self.0;
        let maybe_row = inner
            .conn(move |conn| {
                let sql = query.query_text();
                let params = query.to_params().unwrap_or_default();
                let params = params_from_iter(params.iter());

                let mut stmt = conn.prepare_cached(&sql)?;
                let mut rows = stmt.query(params)?;
                let row = rows.next()?.ok_or(rusqlite::Error::QueryReturnedNoRows)?;
                Ok(FromRow::from_row(row).unwrap())
            })
            .await
            .map_err(Error::query)?;

        Ok(maybe_row)
    }

    /// Query optional (0 or 1 rows expected).
    pub async fn query_opt<Q: QueryOne<Self> + Send + Sync + 'static>(
        &mut self,
        query: Q,
    ) -> Result<Option<Q::Row>, Error>
    where
        Q::Row: FromRow<Self> + Send + Sync + 'static,
    {
        let inner = &self.0;
        let opt_row = inner
            .conn(move |conn| {
                let sql = query.query_text();
                let params = query.to_params().unwrap_or_default();
                let params = params_from_iter(params.iter());

                let mut stmt = conn.prepare_cached(&sql)?;
                let mut rows = stmt.query(params)?;

                if let Some(row) = rows.next()? {
                    Ok(Some(FromRow::from_row(row).unwrap()))
                } else {
                    Ok(None)
                }
            })
            .await
            .map_err(Error::query)?;

        Ok(opt_row)
    }

    /// Execute a statement (INSERT/UPDATE/DELETE).
    pub async fn execute<S: Statement<Self> + Send + Sync + 'static>(
        &mut self,
        statement: S,
    ) -> Result<u64, Error> {
        let inner = &self.0;
        let rows_affected = inner
            .conn(move |conn| {
                let sql = statement.query_text();
                let params = statement.to_params().unwrap_or_default();
                let params = params_from_iter(params.iter());

                let mut stmt = conn.prepare_cached(&sql)?;
                let changed = stmt.execute(params)?;
                Ok(changed as u64)
            })
            .await
            .map_err(Error::query)?;

        Ok(rows_affected)
    }

    /// Start a transaction. We issue a `BEGIN` and return a Transaction object
    /// that will send `COMMIT` or `ROLLBACK` on demand.
    pub async fn transaction(&mut self) -> Result<Transaction, Error> {
        // Begin transaction on the underlying single connection.
        let inner = &self.0;
        inner
            .conn(|conn| conn.execute("BEGIN", ()).map(|_| ()))
            .await
            .map_err(Error::transaction)?;

        Ok(Transaction {
            client: self.0.clone(),
            finished: false,
        })
    }
}

/// Transaction object that issues COMMIT/ROLLBACK using the same background connection.
/// This is a simple BEGIN/COMMIT/ROLLBACK wrapper — it relies on the underlying
/// client being a single connection to ensure the transaction applies to subsequent
/// statements executed through the same client.
#[derive(Clone)]
pub struct Transaction {
    client: async_sqlite::Client,
    finished: bool,
}

impl Transaction {
    /// Commit the transaction.
    pub async fn commit(mut self) -> Result<(), Error> {
        if self.finished {
            return Ok(());
        }
        let client = &self.client;
        client
            .conn(|conn| conn.execute("COMMIT", ()).map(|_| ()))
            .await
            .map_err(Error::transaction)?;
        self.finished = true;
        Ok(())
    }

    /// Rollback the transaction.
    pub async fn rollback(mut self) -> Result<(), Error> {
        if self.finished {
            return Ok(());
        }
        let client = &self.client;
        client
            .conn(|conn| conn.execute("ROLLBACK", ()).map(|_| ()))
            .await
            .map_err(Error::transaction)?;
        self.finished = true;
        Ok(())
    }

    /// Prepare a static query text inside the transaction (cached prepare).
    pub async fn prepare<S: StaticQueryText>(&mut self) -> Result<(), Error> {
        let client = &self.client;
        client
            .conn(|conn| conn.prepare_cached(S::QUERY_TEXT).map(|_stmt| ()))
            .await
            .map_err(Error::prepare)?;
        Ok(())
    }

    /// Query inside the transaction (similar to Client::query).
    pub async fn query<Q: Query<Client> + Send + Sync + 'static>(
        &mut self,
        query: Q,
    ) -> Result<Vec<Q::Row>, Error>
    where
        Q::Row: FromRow<Client> + Send + Sync + 'static,
    {
        let client = &self.client;
        let rows_vec = client
            .conn(move |conn| {
                let sql = query.query_text();
                let params = query.to_params().unwrap_or_default();
                let params = params_from_iter(params.iter());

                let mut stmt = conn.prepare_cached(&sql)?;
                let mut rows = stmt.query(params)?;
                let mut out = Vec::new();
                while let Some(row) = rows.next()? {
                    out.push(FromRow::from_row(row).unwrap());
                }
                Ok(out)
            })
            .await
            .map_err(Error::query)?;

        Ok(rows_vec)
    }

    pub async fn query_one<Q: QueryOne<Client> + Send + Sync + 'static>(
        &mut self,
        query: Q,
    ) -> Result<Q::Row, Error>
    where
        Q::Row: FromRow<Client> + Send + Sync + 'static,
    {
        let client = &self.client;
        let row = client
            .conn(move |conn| {
                let sql = query.query_text();
                let params = query.to_params().unwrap_or_default();
                let params = params_from_iter(params.iter());

                let mut stmt = conn.prepare_cached(&sql)?;
                let mut rows = stmt.query(params)?;
                let row = rows.next()?.ok_or(rusqlite::Error::QueryReturnedNoRows)?;
                Ok(FromRow::from_row(row).unwrap())
            })
            .await
            .map_err(Error::query)?;

        Ok(row)
    }

    pub async fn query_opt<Q: QueryOne<Client> + Send + Sync + 'static>(
        &mut self,
        query: Q,
    ) -> Result<Option<Q::Row>, Error>
    where
        Q::Row: FromRow<Client> + Send + Sync + 'static,
    {
        let client = &self.client;
        let opt = client
            .conn(move |conn| {
                let sql = query.query_text();
                let params = query.to_params().unwrap_or_default();
                let params = params_from_iter(params.iter());

                let mut stmt = conn.prepare_cached(&sql)?;
                let mut rows = stmt.query(params)?;
                if let Some(row) = rows.next()? {
                    Ok(Some(FromRow::from_row(row).unwrap()))
                } else {
                    Ok(None)
                }
            })
            .await
            .map_err(Error::query)?;

        Ok(opt)
    }

    pub async fn execute<S: Statement<Client> + Send + Sync + 'static>(
        &mut self,
        statement: S,
    ) -> Result<u64, Error> {
        let client = &self.client;
        let rows_affected = client
            .conn(move |conn| {
                let sql = statement.query_text();
                let params = statement.to_params().unwrap_or_default();
                let params = params_from_iter(params.iter());

                let mut stmt = conn.prepare_cached(&sql)?;
                let changed = stmt.execute(params)?;
                Ok(changed as u64)
            })
            .await
            .map_err(Error::query)?;

        Ok(rows_affected)
    }
}
