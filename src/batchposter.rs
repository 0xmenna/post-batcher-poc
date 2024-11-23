use crate::types::{
    inbox_contract_from, BatchPosterPosition, BroadcastFeedMessage, BroadcastMessage,
    BuildingBatch, Config, InboxContract, L1_MESSAGE_TYPE_BATCH_POSTING_REPORT,
};
use anyhow::Result;
use ethers::types::{Address, Bytes, U256};
use futures_util::StreamExt;
use std::{
    mem,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::mpsc::{self, UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
    time::{self},
};
use tokio_tungstenite::connect_async;

// This PoC implementation is not fault-tolerant:
// It holds all the state in memory and does not have failure recovery mechanisms.
pub struct BatchPoster {
    feed_handler: SequencerFeedHandler,

    sequencer_inbox: InboxContract,
    max_batch_post_interval: Duration,
    gas_refunder: Address,
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
            feed_handler: SequencerFeedHandler::new(
                config.feed_url,
                config.feed_wait_interval_secs,
                config.retries_count,
            ),
            sequencer_inbox,
            max_batch_post_interval: config.max_batch_post_interval,
            gas_refunder: config.gas_refunder,
            building_batch: Default::default(),
            batchposter_position: BatchPosterPosition {
                msg_count: 1,
                delayed_msg_count: 1,
                next_seq_number: 1,
            },
            incoming_messages: Vec::new(),
            first_incoming_msg_time: None,
            msg_count: 1,
        }
    }
}

impl BatchPoster {
    pub async fn start(&mut self) -> Result<()> {
        let feed_producer = self.produce_sequencer_feed();

        self.consume_feed_and_batch_post().await?;

        feed_producer.await?
    }

    pub fn produce_sequencer_feed(&self) -> JoinHandle<Result<()>> {
        let feed_url = self.feed_handler.url.clone();
        let producer = self.feed_handler.producer();

        let retries = self.feed_handler.retries;
        let mut wait_feed_error =
            time::interval(Duration::from_secs(self.feed_handler.wait_feed_error_secs));

        tokio::spawn(async move {
            // ws connection to read the sequncer's feed
            let mut count = 0;
            let ws_stream = loop {
                wait_feed_error.tick().await;

                let maybe_connection = connect_async(&feed_url).await;
                if let Ok((ws_stream, _)) = maybe_connection {
                    log::info!("Successfully connected to the sequencer's feed");
                    break ws_stream;
                }
                count += 1;
                if count >= retries {
                    return Err(anyhow::anyhow!(
                        "Failed to connect to the sequencer's feed after {} retries",
                        retries
                    ));
                }
                log::info!(
                    "Retrying to connect to the sequencer's feed in {} seconds...",
                    wait_feed_error.period().as_secs()
                );
            };

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

    pub async fn consume_feed_and_batch_post(&mut self) -> Result<()> {
        let consumer = self.feed_handler.consumer();

        log::info!("Waiting for incoming messages...");
        while let Some(broadcast_msg) = consumer.recv().await {
            if !broadcast_msg.messages.is_empty() {
                self.incoming_messages
                    .extend_from_slice(&broadcast_msg.messages);

                self.msg_count += broadcast_msg.messages.len() as u64;
                log::info!("Total number of messages: {}", self.msg_count);
            }

            if self.first_incoming_msg_time.is_none() {
                // find the first message to set the time of the batch initialization to later force a batch post
                self.first_incoming_msg_time = self.incoming_messages.first().map(|msg| {
                    UNIX_EPOCH + Duration::from_secs(msg.message.message.header.timestamp)
                });

                log::info!("Time of a new batch: {:?}", self.first_incoming_msg_time);
            }

            if self.building_batch.is_none() {
                self.building_batch = Some(BuildingBatch::new(
                    self.batchposter_position.msg_count,
                    self.batchposter_position.msg_count,
                    self.batchposter_position.delayed_msg_count,
                ));
            }

            let force_post_batch = self.first_incoming_msg_time.map_or(false, |msg_time| {
                msg_time.elapsed().unwrap() >= self.max_batch_post_interval
            });

            let mut have_useful_message = false;

            let dispatchable_messages = mem::take(&mut self.incoming_messages);

            let maybe_building = self.building_batch.as_mut();
            let building = maybe_building.unwrap();
            for msg in dispatchable_messages {
                log::info!("Processing message: {:?}", msg);
                let msg = msg.message;
                building.segments.add_message(&msg)?;

                if msg.message.header.kind != L1_MESSAGE_TYPE_BATCH_POSTING_REPORT {
                    have_useful_message = true;
                }
                building.msg_count += 1;
            }

            if !force_post_batch || !have_useful_message {
                // the batch isn't full yet and we've posted a batch recently
                // don't post anything for now
                continue;
            }

            let sequencer_msg = building.segments.close_and_get_bytes()?;

            if sequencer_msg.is_none() {
                self.building_batch = None;
                self.first_incoming_msg_time = None;
                continue;
            }

            let _ = add_batch_tx(
                &self.sequencer_inbox,
                self.batchposter_position.next_seq_number,
                sequencer_msg.unwrap(),
                building.segments.delayed_msg(),
                self.gas_refunder,
                self.batchposter_position.msg_count,
                building.msg_count,
            )
            .await?;

            self.batchposter_position = BatchPosterPosition {
                msg_count: building.msg_count,
                delayed_msg_count: building.segments.delayed_msg(),
                next_seq_number: self.batchposter_position.next_seq_number + 1,
            };
            self.building_batch = None;
            self.first_incoming_msg_time = None;
        }

        Err(anyhow::anyhow!(
            "Consumer stopped, incoming feed must not stop"
        ))
    }
}

type FeedProducer = UnboundedSender<BroadcastMessage>;
type FeedConsumer = UnboundedReceiver<BroadcastMessage>;

struct SequencerFeedHandler {
    url: String,
    producer: FeedProducer,
    wait_feed_error_secs: u64,
    retries: u32,
    consumer: FeedConsumer,
}

impl SequencerFeedHandler {
    pub fn new(url: String, wait_feed_error_secs: u64, retries: u32) -> Self {
        let (producer, consumer) = mpsc::unbounded_channel();
        Self {
            url,
            producer,
            wait_feed_error_secs,
            retries,
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

pub async fn add_batch_tx(
    contract: &InboxContract,
    sequence_number: u64,
    data: Vec<u8>,
    after_delayed_msg: u64,
    gas_refunder: Address,
    prev_msg_count: u64,
    new_msg_count: u64,
) -> Result<()> {
    let after_delayed_messages_read: U256 = after_delayed_msg.into();
    let prev_message_count: U256 = prev_msg_count.into();
    let new_message_count: U256 = new_msg_count.into();
    let data = Bytes::from(data);

    let tx = contract.method::<_, ()>(
        "addSequencerL2BatchFromOrigin",
        (
            sequence_number,
            data,
            after_delayed_messages_read,
            gas_refunder,
            prev_message_count,
            new_message_count,
        ),
    )?;

    let _receipt = tx.send().await?;

    Ok(())
}
