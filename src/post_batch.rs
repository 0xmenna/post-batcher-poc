use anyhow::Result;
use futures_util::StreamExt;
use tokio_tungstenite::connect_async;
use url::Url;

pub struct FeedSubscriber(Url);

impl FeedSubscriber {
    pub fn new(url: &str) -> Result<Self> {
        let url = Url::parse(url)?;

        Ok(Self(url))
    }

    pub async fn subscribe(&self) -> Result<()> {
        let (ws_stream, _) = connect_async(self.0.to_string()).await?;

        let (_, mut read) = ws_stream.split();

        // Listen for incoming messages and print them
        while let Some(message) = read.next().await {
            let msg = message?;
            if !msg.is_text() {
                continue;
            }
            println!("Received message: {}", msg);
        }

        Ok(())
    }
}
