use anyhow::Result;
use config::Config;
use post_batch::FeedSubscriber;

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::default();
    let _sequencer_inbox = config::inbox_contract_from(&config);

    let feed_subscriber = FeedSubscriber::new(&config.feed_url)?;
    feed_subscriber.subscribe().await?;

    Ok(())
}

mod config;
mod dataposter;
mod post_batch;
