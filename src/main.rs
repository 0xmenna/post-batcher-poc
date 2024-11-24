use anyhow::Result;
use batchposter::BatchPoster;
use types::Config;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let config = Config::default();

    let mut batch_poster = BatchPoster::from(config);
    batch_poster.start().await?;

    Ok(())
}

mod batchposter;
mod db;
mod types;
