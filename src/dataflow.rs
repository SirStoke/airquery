use std::io;
use async_trait::async_trait;
use thiserror::Error;

#[derive(Error, Debug)]
enum OpenError {
    #[error("node internally failed to setup")]
    Internal(#[from] io::Error)
}

#[derive(Error, Debug)]
enum NextError {
    #[error("node internally failed to produce a value")]
    Internal(#[from] io::Error)
}

#[derive(Error, Debug)]
enum CloseError {
    #[error("node internally failed to close")]
    Internal(#[from] io::Error)
}

struct Tuple {
    columns: Vec<String>
}

/// Represents the physical properties describing the tuples emitted by the Node.
/// e.g. columns, sorting, etc.
struct Properties {
    sort_by: Vec<String>,
    columns: Vec<String>
}

/// A Data Flow Node
///
/// This is the classic Volcano interface:
///
/// #open() must be called before #next(), which must be called before #close()
#[async_trait]
trait Node {
    async fn open() -> Result<OpenError, ()>;
    async fn next() -> Result<NextError, Option<Tuple>>;
    async fn close() -> Result<CloseError, ()>;
}