#![allow(clippy::redundant_closure)]
//! Sqlite bindings.

use crate::client::{FromColumnIndexed, FromColumnNamed, ToParam};
use crate::query::StaticQueryText;
use crate::{error, FromRow, Query, QueryOne, Statement};

/// The type of errors from a `Client`.
pub type Error = error::Error<turso::Error>;

impl<T> FromColumnIndexed<Client> for T
where
    T: turso_core::types::FromValue,
{
    fn from_column(row: &turso::Row, index: usize) -> Result<Self, Error> {
        row.get(index).map_err(Error::from_column)
    }
}

impl<T> FromColumnNamed<Client> for T
where
    T: turso_core::types::FromValue,
{
    fn from_column(row: &turso::Row, name: &str) -> Result<Self, Error> {
        // row.get(name).map_err(Error::from_column)
        todo!()
    }
}

impl<T> ToParam<Client> for T
where
    T: turso::params::IntoValue + Clone,
{
    fn to_param(&self) -> turso::Value {
        self.clone().into_value().unwrap()
    }
}

/// A synchronous Sqlite client.
#[derive(Debug, Clone)]
pub struct Client(turso::Connection);

impl crate::client::Client for Client {
    type Row<'a> = turso::Row;
    type Param<'a> = turso::Value;
    type Error = turso::Error;
}

impl AsMut<turso::Connection> for Client {
    fn as_mut(&mut self) -> &mut turso::Connection {
        &mut self.0
    }
}

impl AsRef<turso::Connection> for Client {
    fn as_ref(&self) -> &turso::Connection {
        &self.0
    }
}

impl From<turso::Connection> for Client {
    fn from(inner: turso::Connection) -> Self {
        Client(inner)
    }
}

impl Client {
    pub async fn open<P: AsRef<str>>(path: P) -> Result<Self, Error> {
        turso::Builder::new_local(path.as_ref())
            .build()
            .await
            .and_then(|db| db.connect())
            .map(Self)
            .map_err(Error::connect)
    }

    pub async fn open_in_memory() -> Result<Self, Error> {
        turso::Builder::new_local(":memory:")
            .build()
            .await
            .and_then(|db| db.connect())
            .map(Self)
            .map_err(Error::connect)
    }

    pub async fn prepare<S: StaticQueryText>(&self) -> Result<(), Error> {
        self.as_ref()
            .prepare(S::QUERY_TEXT)
            .await
            .map_err(Error::prepare)?;
        Ok(())
    }

    pub async fn query<Q: Query<Self>>(&self, query: &Q) -> Result<Vec<Q::Row>, Error> {
        let params = query.to_params().unwrap_or_default();

        let mut statement = turso::Connection::prepare(self.as_ref(), &query.query_text())
            .await
            .map_err(Error::prepare)?;

        let mut rows = statement.query(params).await.map_err(Error::query)?;

        let mut result = vec![];
        while let Some(row) = rows.next().await.map_err(Error::query)? {
            result.push(FromRow::from_row(&row)?);
        }

        Ok(result)
    }

    pub async fn query_one<Q: QueryOne<Self>>(&self, query: &Q) -> Result<Q::Row, Error> {
        let params = query.to_params().unwrap_or_default();

        let mut statement = turso::Connection::prepare(self.as_mut(), &query.query_text())
            .await
            .map_err(Error::prepare)?;

        let mut rows = statement.query(params).await.map_err(Error::query)?;

        rows.next()
            .await
            .map_err(Error::query)?
            .ok_or_else(|| Error::query(turso::Error::QueryReturnedNoRows))
            .and_then(|row| FromRow::from_row(&row))
    }

    pub async fn query_opt<Q: QueryOne<Self>>(&self, query: &Q) -> Result<Option<Q::Row>, Error> {
        let params = query.to_params().unwrap_or_default();

        let mut statement = turso::Connection::prepare(self.as_ref(), &query.query_text())
            .await
            .map_err(Error::prepare)?;

        let mut rows = statement.query(params).await.map_err(Error::query)?;

        rows.next()
            .await
            .map_err(Error::query)?
            .map(|row| FromRow::from_row(&row))
            .transpose()
    }

    pub async fn execute<S: Statement<Self>>(&self, statement: &S) -> Result<u64, Error> {
        let params = statement.to_params().unwrap_or_default();

        let mut statement = turso::Connection::prepare(self.as_ref(), &statement.query_text())
            .await
            .map_err(Error::prepare)?;

        let rows_affected = statement.execute(params).await.map_err(Error::query)?;

        Ok(rows_affected.try_into().unwrap_or_default())
    }

    pub async fn transaction(&self) -> Result<Transaction<'_>, Error> {
        Ok(Transaction(
            self.0.transaction().await.map_err(Error::transaction)?,
        ))
    }
}

/// A synchronous Sqlite transaction.
///
/// Transactions will implicitly roll back by default when dropped. Use the
/// `commit` method to commit the changes made in the transaction.
#[derive(Debug)]
pub struct Transaction<'a>(turso::transaction::Transaction<'a>);

impl<'a> AsMut<turso::transaction::Transaction<'a>> for Transaction<'a> {
    fn as_mut(&mut self) -> &mut turso::transaction::Transaction<'a> {
        &mut self.0
    }
}

impl<'a> AsRef<turso::transaction::Transaction<'a>> for Transaction<'a> {
    fn as_ref(&self) -> &turso::transaction::Transaction<'a> {
        &self.0
    }
}

impl<'a> Transaction<'a> {
    /// Consumes the transaction, committing all changes made within it.
    pub async fn commit(self) -> Result<(), Error> {
        self.0.commit().await.map_err(Error::transaction)
    }

    /// Rolls the transaction back, discarding all changes made within it.
    ///
    /// This is equivalent to `Transaction`'s `Drop` implementation, but provides any error encountered to the caller.
    pub async fn rollback(self) -> Result<(), Error> {
        self.0.rollback().await.map_err(Error::transaction)
    }

    pub async fn prepare<S: StaticQueryText>(&self) -> Result<(), Error> {
        self.0
            .prepare(S::QUERY_TEXT)
            .await
            .map_err(Error::prepare)?;
        Ok(())
    }

    pub async fn query<Q: Query<Client>>(&self, query: &Q) -> Result<Vec<Q::Row>, Error> {
        let params = query.to_params().unwrap_or_default();

        let mut statement = turso::Connection::prepare(&self.0, &query.query_text())
            .await
            .map_err(Error::prepare)?;

        let mut rows = statement.query(params).await.map_err(Error::query)?;

        let mut result = vec![];
        while let Some(row) = rows.next().await.map_err(Error::query)? {
            result.push(FromRow::from_row(&row)?);
        }

        Ok(result)
    }

    pub async fn query_one<Q: QueryOne<Client>>(&self, query: &Q) -> Result<Q::Row, Error> {
        let params = query.to_params().unwrap_or_default();

        let mut statement = turso::Connection::prepare(&self.0, &query.query_text())
            .await
            .map_err(Error::prepare)?;

        let mut rows = statement.query(params).await.map_err(Error::query)?;

        rows.next()
            .await
            .map_err(Error::query)?
            .ok_or_else(|| Error::query(turso::Error::QueryReturnedNoRows))
            .and_then(|row| FromRow::from_row(&row))
    }

    pub async fn query_opt<Q: QueryOne<Client>>(&self, query: &Q) -> Result<Option<Q::Row>, Error> {
        let params = query.to_params().unwrap();

        let mut statement = turso::Connection::prepare(&self.0, &query.query_text())
            .await
            .map_err(Error::prepare)?;

        let mut rows = statement.query(params).await.map_err(Error::query)?;

        rows.next()
            .await
            .map_err(Error::query)?
            .map(|row| FromRow::from_row(&row))
            .transpose()
    }

    pub async fn execute<S: Statement<Client>>(&self, statement: &S) -> Result<u64, Error> {
        let params = statement.to_params().unwrap_or_default();

        let mut statement = turso::Connection::prepare(&self.0, &statement.query_text())
            .await
            .map_err(Error::prepare)?;

        let rows_affected = statement.execute(params).await.map_err(Error::query)?;

        Ok(rows_affected)
    }
}

// TODO: not derive support
#[cfg(all(test, feature = "derive"))]
mod test {
    use super::*;

    #[derive(Statement)]
    #[aykroyd(
        text = "CREATE TABLE test_turso (id INTEGER PRIMARY KEY AUTOINCREMENT, label TEXT NOT NULL)"
    )]
    struct CreateTodos;

    #[derive(Statement)]
    #[aykroyd(text = "DROP TABLE IF EXISTS test_turso")]
    struct DropTodos;

    #[derive(Statement)]
    #[aykroyd(text = "INSERT INTO test_turso (label) VALUES ($1)")]
    struct InsertTodo<'a>(&'a str);

    #[derive(Query)]
    #[aykroyd(row((i32, String)), text = "SELECT id, label FROM test_turso")]
    struct GetAllTodos;

    #[tokio::test]
    async fn end_to_end_memory() {
        const TODO_TEXT: &str = "get things done, please!";

        let mut client = Client::open_in_memory().await.unwrap();

        client.execute(&CreateTodos).await.unwrap();

        client.execute(&InsertTodo(TODO_TEXT)).await.unwrap();

        let todos = client.query(&GetAllTodos).await.unwrap();
        assert_eq!(1, todos.len());
        assert_eq!(TODO_TEXT, todos[0].1);

        client.execute(&DropTodos).await.unwrap();
    }

    #[tokio::test]
    async fn end_to_end_file() {
        const TODO_TEXT: &str = "get things done, please!";

        let mut client = Client::open("./foobar").await.unwrap();

        client.execute(&DropTodos).await.unwrap();

        client.execute(&CreateTodos).await.unwrap();

        client.execute(&InsertTodo(TODO_TEXT)).await.unwrap();

        let todos = client.query(&GetAllTodos).await.unwrap();
        assert_eq!(1, todos.len());
        assert_eq!(TODO_TEXT, todos[0].1);

        client.execute(&DropTodos).await.unwrap();
    }
}
