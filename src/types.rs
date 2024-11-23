use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use brotli::CompressorWriter;
use ethers::{
    abi::Abi,
    contract::Contract,
    middleware::SignerMiddleware,
    providers::{Http, Provider},
    signers::LocalWallet,
    types::{serde_helpers::deserialize_number, Address, H256, U256},
};
use serde::Deserialize;
use serde_json::Value;
use std::{fs, io::Write, sync::Arc, time::Duration};

const BATCH_SEGMENT_KIND_L2_MESSAGE: u8 = 0;
const _BATCH_SEGMENT_KIND_L2_MESSAGE_BROTLI: u8 = 1;
const BATCH_SEGMENT_KIND_DELAYED_MESSAGES: u8 = 2;
const BATCH_SEGMENT_KIND_ADVANCE_TIMESTAMP: u8 = 3;
const BATCH_SEGMENT_KIND_ADVANCE_L1_BLOCK_NUMBER: u8 = 4;

const MAX_DECOMPRESSED_LEN: usize = 1024 * 1024 * 16; // 16 MiB
const MAX_SEGMENTS_PER_SEQUENCER_MESSAGE: usize = 100 * 1024;

const BROTLI_MESSAGE_HEADER_BYTE: u8 = 0;

pub const L1_MESSAGE_TYPE_BATCH_POSTING_REPORT: u8 = 13;

/// Simple configuration for the batch poster
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub l1_url: String,
    pub feed_url: String,
    pub feed_wait_interval_secs: u64,
    pub retries_count: u32,
    pub privkey: String,
    pub sequencer_inbox_address: Address,
    pub contract_abi_path: String,
    pub max_batch_post_interval: Duration,
    pub gas_refunder: Address,
}

/// Default configuration
// Note: Private key is for testing purposes only, does not contain real funds, do not use in production
impl Default for Config {
    fn default() -> Self {
        Self {
            l1_url: "http://138.201.133.213:32780".to_string(),
            feed_url: "ws://138.201.133.213:9642".to_string(),
            feed_wait_interval_secs: 1,
            retries_count: 20,
            privkey: "0x53321db7c1e331d93a11a41d16f004d7ff63972ec8ec7c25db329728ceeb1710"
                .to_string(),
            sequencer_inbox_address: "0xA644B79509328CDf5BF2ebea5ad43071AE3d2c79"
                .parse()
                .unwrap(),
            contract_abi_path: "SequencerInbox.json".to_string(),
            max_batch_post_interval: Duration::from_secs(300),
            gas_refunder: "0x614561D2d143621E126e87831AEF287678B442b8"
                .parse()
                .unwrap(),
        }
    }
}

pub type InboxContract = Contract<SignerMiddleware<Provider<Http>, LocalWallet>>;

pub fn inbox_contract_from(config: &Config) -> InboxContract {
    let provider = Provider::<Http>::try_from(&config.l1_url).unwrap();
    let wallet = config.privkey.parse::<LocalWallet>().unwrap();
    let client = Arc::new(SignerMiddleware::new(provider, wallet));

    let contract_artifact = fs::read_to_string(&config.contract_abi_path).unwrap();
    let json: Value = serde_json::from_str(&contract_artifact).unwrap();

    let abi = serde_json::from_value::<Abi>(json["abi"].clone()).unwrap();

    let sequencer_inbox = Contract::new(config.sequencer_inbox_address, abi, client);

    sequencer_inbox
}

#[derive(Debug, Clone)]
pub struct BatchPosterPosition {
    pub msg_count: u64,
    pub delayed_msg_count: u64,
    pub next_seq_number: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BroadcastMessage {
    pub version: u32,
    pub messages: Vec<BroadcastFeedMessage>,
    pub confirmed_sequence_number_message: Option<ConfirmedSequenceNumberMessage>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfirmedSequenceNumberMessage {
    pub sequence_number: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BroadcastFeedMessage {
    pub sequence_number: u64,
    pub message: MessageWithMetadata,
    pub signature: Option<Signature>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageWithMetadata {
    pub message: L1IncomingMessage,
    pub delayed_messages_read: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct L1IncomingMessage {
    pub header: L1IncomingMessageHeader,
    pub l2_msg: L2Message,
    pub batch_gas_cost: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct L1IncomingMessageHeader {
    pub kind: u8,
    pub sender: Address,
    pub block_number: u64,
    pub timestamp: u64,
    pub request_id: Option<H256>,
    pub base_fee_l1: Option<BigInt>,
}

#[derive(Debug, Clone)]
pub struct BigInt(pub U256);

impl<'de> Deserialize<'de> for BigInt {
    fn deserialize<D>(deserializer: D) -> Result<BigInt, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let n = deserialize_number(deserializer)?;
        Ok(BigInt::from(n))
    }
}

impl AsRef<U256> for BigInt {
    fn as_ref(&self) -> &U256 {
        &self.0
    }
}

impl From<U256> for BigInt {
    fn from(val: U256) -> Self {
        Self(val)
    }
}

pub type L2Message = Base64Bytes;

pub type Signature = Base64Bytes;

#[derive(Debug, Clone)]
pub struct Base64Bytes(pub Vec<u8>);

impl From<Vec<u8>> for Base64Bytes {
    fn from(val: Vec<u8>) -> Self {
        Self(val)
    }
}

impl AsRef<[u8]> for Base64Bytes {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl<'de> Deserialize<'de> for Base64Bytes {
    fn deserialize<D>(deserializer: D) -> Result<Base64Bytes, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let base64_str = String::deserialize(deserializer)?;
        let l2_msg = BASE64
            .decode(&base64_str)
            .map_err(|_| serde::de::Error::custom("Base64 L2Msg has invalid format"))?;

        Ok(Base64Bytes(l2_msg))
    }
}

pub struct BuildingBatch {
    /// Instance that handles the segments of the batch
    pub segments: BatchSegments,
    /// Message count at the start of the batch.
    pub start_msg_count: u64,
    /// Current message count of the batch.
    pub msg_count: u64,
}

impl BuildingBatch {
    pub fn new(start_msg_count: u64, msg_count: u64, first_delayed: u64) -> Self {
        let segments = BatchSegments::new(first_delayed);

        Self {
            segments,
            start_msg_count,
            msg_count,
        }
    }
}

pub struct BatchSegments {
    compressed_writer: CompressorWriter<Vec<u8>>,
    raw_segments: Vec<Vec<u8>>,
    timestamp: u64,
    block_num: u64,
    delayed_msg: u64,
    size_limit: usize,
    compression_level: u32,
    new_uncompressed_size: usize,
    total_uncompressed_size: usize,
    last_compressed_size: usize,
    trailing_headers: usize,
    is_done: bool,
}

impl BatchSegments {
    // impl missing methods
}

impl BatchSegments {
    pub fn new(first_delayed: u64) -> Self {
        let buffer_size = 4096;
        let compression_level = 11;
        let lgwin = 22;

        let compressed_writer =
            CompressorWriter::new(Vec::new(), buffer_size, compression_level, lgwin);

        Self {
            compressed_writer,
            size_limit: 90000,
            compression_level,
            raw_segments: Vec::new(),
            delayed_msg: first_delayed,
            timestamp: 0,
            block_num: 0,
            new_uncompressed_size: 0,
            total_uncompressed_size: 0,
            last_compressed_size: 0,
            trailing_headers: 0,
            is_done: false,
        }
    }
}

impl BatchSegments {
    fn recompress_all(&mut self) -> anyhow::Result<()> {
        self.compressed_writer =
            CompressorWriter::new(Vec::new(), self.size_limit * 2, self.compression_level, 22);
        self.new_uncompressed_size = 0;
        self.total_uncompressed_size = 0;

        for segment in self.raw_segments.clone() {
            self.add_segment_to_compressed(segment)?;
        }

        if self.total_uncompressed_size > MAX_DECOMPRESSED_LEN {
            return Err(anyhow::anyhow!(
                "Batch size exceeds maximum decompressed length"
            ));
        }
        if self.raw_segments.len() >= MAX_SEGMENTS_PER_SEQUENCER_MESSAGE {
            return Err(anyhow::anyhow!(
                "Number of raw segments exceeds maximum allowed"
            ));
        }

        Ok(())
    }

    fn test_for_overflow(&mut self, is_header: bool) -> anyhow::Result<bool> {
        if self.total_uncompressed_size > MAX_DECOMPRESSED_LEN {
            return Ok(true);
        }

        if self.raw_segments.len() >= MAX_SEGMENTS_PER_SEQUENCER_MESSAGE {
            return Ok(true);
        }

        if (self.last_compressed_size + self.new_uncompressed_size) < self.size_limit {
            return Ok(false);
        }

        if is_header || self.raw_segments.len() == self.trailing_headers {
            return Ok(false);
        }

        self.compressed_writer.flush()?;
        self.last_compressed_size = self.compressed_writer.get_ref().len();
        self.new_uncompressed_size = 0;

        if self.last_compressed_size >= self.size_limit {
            return Ok(true);
        }
        Ok(false)
    }

    fn close(&mut self) -> anyhow::Result<()> {
        // Remove trailing headers
        let len = self.raw_segments.len();
        self.raw_segments
            .truncate(len.saturating_sub(self.trailing_headers));
        self.trailing_headers = 0;
        self.recompress_all()?;
        self.is_done = true;

        Ok(())
    }

    fn add_segment_to_compressed(&mut self, segment: Vec<u8>) -> anyhow::Result<()> {
        let encoded = rlp::encode(&segment);
        let len_written = self.compressed_writer.write(&encoded)?;
        self.new_uncompressed_size += len_written;
        self.total_uncompressed_size += len_written;

        Ok(())
    }

    pub fn add_segment(&mut self, segment: Vec<u8>, is_header: bool) -> anyhow::Result<()> {
        if self.is_done {
            return Err(anyhow::anyhow!("Batch segments already closed"));
        }

        self.add_segment_to_compressed(segment.clone())?;

        let overflow = self.test_for_overflow(is_header)?;
        if overflow {
            // current implementation just returns an error if it overflows
            return Err(anyhow::anyhow!("Batch segments overflowed"));

            // self.close()?;
            // return Ok(());
        }

        self.raw_segments.push(segment);
        if is_header {
            self.trailing_headers += 1;
        } else {
            self.trailing_headers = 0;
        }

        Ok(())
    }

    fn prepare_int_segment(&self, val: u64, segment_header: u8) -> Vec<u8> {
        let mut segment = vec![segment_header];
        segment.extend(rlp::encode(&val));
        segment
    }

    fn maybe_add_diff_segment(
        &mut self,
        base: u64,
        new_val: u64,
        segment_header: u8,
    ) -> anyhow::Result<()> {
        if new_val == base {
            return Ok(());
        }
        let diff = new_val - base;
        let segment = self.prepare_int_segment(diff, segment_header);
        self.add_segment(segment, true)?;

        Ok(())
    }

    fn add_delayed_message(&mut self) -> anyhow::Result<()> {
        let segment = vec![BATCH_SEGMENT_KIND_DELAYED_MESSAGES];
        self.add_segment(segment, false)?;
        self.delayed_msg += 1;

        Ok(())
    }

    pub fn add_message(&mut self, msg: &MessageWithMetadata) -> anyhow::Result<()> {
        if self.is_done {
            return Err(anyhow::anyhow!("Batch segments already closed",));
        }

        if msg.delayed_messages_read > self.delayed_msg {
            if msg.delayed_messages_read != self.delayed_msg + 1 {
                return Err(anyhow::anyhow!(
                    "Attempted to add delayed message {} after {}",
                    msg.delayed_messages_read,
                    self.delayed_msg
                ));
            }
            return self.add_delayed_message();
        }

        self.maybe_add_diff_segment(
            self.timestamp,
            msg.message.header.timestamp,
            BATCH_SEGMENT_KIND_ADVANCE_TIMESTAMP,
        )?;

        self.timestamp = msg.message.header.timestamp;

        self.maybe_add_diff_segment(
            self.block_num,
            msg.message.header.block_number,
            BATCH_SEGMENT_KIND_ADVANCE_L1_BLOCK_NUMBER,
        )?;

        self.block_num = msg.message.header.block_number;

        self.add_l2_msg(msg.message.l2_msg.as_ref())
    }

    fn add_l2_msg(&mut self, l2msg: &[u8]) -> anyhow::Result<()> {
        let mut segment = vec![BATCH_SEGMENT_KIND_L2_MESSAGE];
        segment.extend_from_slice(l2msg);
        self.add_segment(segment, false)
    }

    pub fn delayed_msg(&self) -> u64 {
        self.delayed_msg
    }

    pub fn close_and_get_bytes(&mut self) -> anyhow::Result<Option<Vec<u8>>> {
        if !self.is_done {
            self.close()?;
        }

        if self.raw_segments.is_empty() {
            return Ok(None);
        }

        self.compressed_writer.flush()?;
        let compressed_bytes = self.compressed_writer.get_ref();
        let mut full_msg = vec![BROTLI_MESSAGE_HEADER_BYTE];
        full_msg.extend_from_slice(compressed_bytes);
        Ok(Some(full_msg))
    }
}
