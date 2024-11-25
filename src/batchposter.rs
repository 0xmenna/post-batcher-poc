use crate::{
    db::BatchPosterDb,
    types::{
        inbox_contract_from, BatchPosterPosition, BroadcastMessage, BuildingBatch, Config,
        InboxContract, GAS_LIMIT,
    },
};
use anyhow::Result;
use ethers::types::{Address, Bytes, TransactionReceipt, U256};
use futures_util::StreamExt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::{
    sync::mpsc::{self, UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
    time::{self, Interval},
};
use tokio_tungstenite::connect_async;

// PoC implementation of a BatchPoster
pub struct BatchPoster {
    feed_handler: SequencerFeedHandler,
    sequencer_inbox: InboxContract,
    max_batch_post_interval: Duration,
    gas_refunder: Address,
    db: BatchPosterDb,
    poll_interval: Interval,
}

impl From<Config> for BatchPoster {
    fn from(config: Config) -> Self {
        let sequencer_inbox = inbox_contract_from(&config);
        let db = BatchPosterDb::from_path(&config.db_path).unwrap();

        let poll_interval = time::interval(config.poll_interval);
        Self {
            feed_handler: SequencerFeedHandler::new(
                config.feed_url,
                config.feed_wait_interval_secs,
                config.retries_count,
            ),
            sequencer_inbox,
            max_batch_post_interval: config.max_batch_post_interval,
            gas_refunder: config.gas_refunder,
            db,
            poll_interval,
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
                log::info!(
                    "Producing broadcast message of length: {:?}",
                    broadcast_msg.messages.len()
                );
                producer.send(broadcast_msg)?;
            }

            Ok(())
        })
    }

    pub async fn consume_feed_and_batch_post(&mut self) -> Result<()> {
        // The latest batch poster position
        let mut batchposter_position = self.db.read_batch_position()?;
        // The latest sequence number of the batch being posted
        let mut seq_number = self.db.read_seq_number()?;
        // The time of the first message in the current batch
        let mut first_incoming_msg_time = None;
        // The current batch being built
        let mut building_batch = None;

        let consumer = self.feed_handler.consumer();
        // Pull feed messages indefinitely
        loop {
            // check if we need to send the batch
            let send_batch = first_incoming_msg_time.map_or(false, |msg_time: SystemTime| {
                msg_time.elapsed().unwrap() >= self.max_batch_post_interval
            });

            if send_batch {
                let batch: BuildingBatch = building_batch.unwrap();
                let batch_msg_count = batch.msg_count;
                let batch_delayed_msg_count = batch.segments.delayed_msg();

                post_batch(
                    batch,
                    &self.sequencer_inbox,
                    &batchposter_position,
                    self.gas_refunder,
                )
                .await?;

                batchposter_position = BatchPosterPosition {
                    msg_count: batch_msg_count,
                    delayed_msg_count: batch_delayed_msg_count,
                    next_seq_number: batchposter_position.next_seq_number + 1,
                };
                first_incoming_msg_time = None;
                building_batch = None;

                // checkpoint the batch operation
                self.db
                    .write_checkpoint(&batchposter_position, seq_number)?;

                log::info!(
                    "Checkpoint at height {}: {:?}",
                    seq_number,
                    batchposter_position,
                );
            }

            let maybe_broadcast_msg = consumer.try_recv();

            if let Ok(broadcast_msg) = maybe_broadcast_msg {
                for msg in broadcast_msg.messages.iter() {
                    if msg.sequence_number <= seq_number {
                        log::info!(
                            "Skipping message with sequence number: {}",
                            msg.sequence_number
                        );
                        continue;
                    }

                    log::info!(
                        "Processing message with sequence number: {}",
                        msg.sequence_number
                    );

                    if first_incoming_msg_time.is_none() {
                        // Set the time of the first message in the batch
                        first_incoming_msg_time = Some(
                            UNIX_EPOCH + Duration::from_secs(msg.message.message.header.timestamp),
                        );
                    }

                    if building_batch.is_none() {
                        building_batch = Some(BuildingBatch::new(
                            batchposter_position.msg_count,
                            batchposter_position.msg_count,
                            batchposter_position.delayed_msg_count,
                        ));
                    }
                    let building = building_batch.as_mut().unwrap();

                    let msg_with_meta = &msg.message;
                    building.segments.add_message(msg_with_meta)?;

                    building.msg_count += 1;
                    seq_number = msg.sequence_number;
                }
            } else {
                self.poll_interval.tick().await;
            }
        }
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

pub async fn post_batch(
    batch: BuildingBatch,
    contract: &InboxContract,
    batchposter_pos: &BatchPosterPosition,
    gas_refunder: Address,
) -> Result<bool> {
    let after_delayed_msg = batch.segments.delayed_msg();
    let sequencer_msg = batch.segments.close_and_get_bytes()?;

    if sequencer_msg.is_none() {
        return Ok(false);
    }

    post_batch_tx(
        contract,
        batchposter_pos.next_seq_number,
        sequencer_msg.unwrap(),
        after_delayed_msg,
        gas_refunder,
        batchposter_pos.msg_count,
        batch.msg_count,
    )
    .await?;

    Ok(true)
}

pub async fn post_batch_tx(
    contract: &InboxContract,
    sequence_number: u64,
    data: Vec<u8>,
    after_delayed_msg: u64,
    gas_refunder: Address,
    prev_msg_count: u64,
    new_msg_count: u64,
) -> Result<()> {
    let sequence_number: U256 = sequence_number.into();
    let after_delayed_messages_read: U256 = after_delayed_msg.into();
    let prev_message_count: U256 = prev_msg_count.into();
    let new_message_count: U256 = new_msg_count.into();
    let data = Bytes::from(data);

    log::info!(
        "Adding batch with sequence number: {} - after_delayed_messages_read: {} - gas_refunder: {} - prev_message_count: {} - new_message_count: {}",
        sequence_number,
        after_delayed_messages_read,
        gas_refunder,
        prev_message_count,
        new_message_count
    );

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
    // Set a higher gas limit
    let tx = tx.gas(GAS_LIMIT);

    let pending_tx = tx.send().await?;
    log::info!("Batch Transaction sent! Hash: {:?}", pending_tx.tx_hash());

    // Wait for the transaction to be included in a block
    let receipt = pending_tx.await?;
    log_receipt(receipt);

    Ok(())
}

pub fn log_receipt(receipt: Option<TransactionReceipt>) {
    match receipt {
        Some(receipt) => {
            log::info!(
                "Transaction included! Block number: {:?}",
                receipt.block_number
            );
        }
        None => {
            log::info!("Transaction not included");
        }
    }
}
