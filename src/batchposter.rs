use crate::types::{
    inbox_contract_from, BatchPosterPosition, BroadcastFeedMessage, BroadcastMessage,
    BuildingBatch, Config, InboxContract,
};
use anyhow::Result;
use futures_util::StreamExt;
use std::{
    mem,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::mpsc::{self, UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
    time::{self, Interval},
};
use tokio_tungstenite::connect_async;

// This PoC implementation is not fault-tolerant:
// It holds all the state in memory and does not have failure recovery mechanisms.
pub struct BatchPoster {
    feed_handler: SequencerFeedHandler,
    sequencer_inbox: InboxContract,
    poll_interval: Interval,
    max_batch_post_interval: Duration,
    building_batch: Option<BuildingBatch>,
    batchposter_position: BatchPosterPosition,
    incoming_messages: Vec<BroadcastFeedMessage>,
    first_incoming_msg_time: Option<SystemTime>,
    msg_count: u64,
}

impl From<Config> for BatchPoster {
    fn from(config: Config) -> Self {
        let sequencer_inbox = inbox_contract_from(&config);

        Self {
            feed_handler: SequencerFeedHandler::new(config.feed_url),
            sequencer_inbox,
            poll_interval: time::interval(config.poll_interval),
            max_batch_post_interval: config.max_batch_post_interval,
            building_batch: Default::default(),
            batchposter_position: BatchPosterPosition {
                msg_count: Default::default(),
                delayed_msg_count: Default::default(),
                next_seq_number: Default::default(),
            },
            incoming_messages: Default::default(),
            first_incoming_msg_time: Default::default(),
            msg_count: Default::default(),
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
            println!("Received message: {:?}", broadcast_msg);
            if !broadcast_msg.messages.is_empty() {
                self.incoming_messages
                    .extend_from_slice(&broadcast_msg.messages);

                self.msg_count += broadcast_msg.messages.len() as u64;
                println!("Total number of messages: {}", self.msg_count);
            }

            if self.first_incoming_msg_time.is_none() {
                // find the first message to set the time of the batch initialization to later force a batch post
                self.first_incoming_msg_time = self.incoming_messages.first().map(|msg| {
                    UNIX_EPOCH + Duration::from_secs(msg.message.message.header.timestamp)
                });
            }

            self.poll_interval.tick().await;
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

    pub async fn maybe_post_batch(&mut self) -> Result<()> {
        let batch_position = self.batchposter_position.clone();

        if self.building_batch.is_none() {
            self.building_batch = Some(BuildingBatch::new(
                batch_position.msg_count,
                batch_position.msg_count,
                batch_position.delayed_msg_count,
            ));
        }

        let force_post_batch = self.first_incoming_msg_time.map_or(false, |msg_time| {
            msg_time.elapsed().unwrap() >= self.max_batch_post_interval
        });

        let dispatchable_messages = mem::take(&mut self.incoming_messages);
        for msg in dispatchable_messages {
            // process the message
        }

        Ok(())
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
