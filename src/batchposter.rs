use crate::types::{inbox_contract_from, BroadcastMessage, Config, InboxContract};
use anyhow::Result;
use futures_util::StreamExt;
use tokio::{
    sync::mpsc::{self, UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
    time::{self, Interval},
};
use tokio_tungstenite::connect_async;

pub struct BatchPoster {
    feed_handler: SequencerFeedHandler,
    sequencer_inbox: InboxContract,
    poll_interval: Interval,
}

impl From<Config> for BatchPoster {
    fn from(config: Config) -> Self {
        let sequencer_inbox = inbox_contract_from(&config);
        Self {
            sequencer_inbox,
            poll_interval: time::interval(config.poll_interval),
            feed_handler: SequencerFeedHandler::new(config.feed_url),
        }
    }
}

impl BatchPoster {
    pub async fn start(&mut self) -> Result<()> {
        let feed_producer = self.produce_sequencer_feed();

        self.consume_feed().await?;

        feed_producer.await?
    }

    pub async fn consume_feed(&mut self) -> Result<()> {
        let consumer = self.feed_handler.consumer();

        while let Some(broadcast_msg) = consumer.recv().await {
            println!("Received broadcast message: {:?}", broadcast_msg);
            //self.poll_interval.tick().await;
        }

        Ok(())
    }

    pub fn produce_sequencer_feed(&self) -> JoinHandle<Result<()>> {
        let feed_url = self.feed_handler.url.clone();
        let producer = self.feed_handler.producer();
        tokio::spawn(async move {
            // ws connection to read the sequncer's feed
            let (ws_stream, _) = connect_async(&feed_url).await?;

            let (_, mut read) = ws_stream.split();

            // read and produce the feed
            while let Some(message) = read.next().await {
                let msg = message?;
                if !msg.is_text() {
                    continue;
                }
                let json_str = msg.to_text().unwrap();
                let broadcast_msg: BroadcastMessage = serde_json::from_str(json_str)?;
                producer.send(broadcast_msg)?;
            }

            Ok(())
        })
    }
}

type FeedProducer = UnboundedSender<BroadcastMessage>;
type FeedConsumer = UnboundedReceiver<BroadcastMessage>;

struct SequencerFeedHandler {
    url: String,
    producer: FeedProducer,
    consumer: FeedConsumer,
}

impl SequencerFeedHandler {
    pub fn new(url: String) -> Self {
        let (producer, consumer) = mpsc::unbounded_channel();
        Self {
            url,
            producer,
            consumer,
        }
    }

    pub fn producer(&self) -> FeedProducer {
        self.producer.clone()
    }

    pub fn consumer(&mut self) -> &mut FeedConsumer {
        &mut self.consumer
    }
}
