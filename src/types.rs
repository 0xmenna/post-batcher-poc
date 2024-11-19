use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
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
use std::{fs, sync::Arc, time::Duration};

/// Simple configuration for the batch poster
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub l1_url: String,
    pub feed_url: String,
    pub privkey: String,
    pub sequencer_inbox_address: Address,
    pub contract_abi_path: String,
    pub poll_interval: Duration,
}

/// Default configuration
// Note: Private key is for testing purposes only, does not contain real funds, do not use in production
impl Default for Config {
    fn default() -> Self {
        Self {
            l1_url: "http://138.201.133.213:32769".to_string(),
            feed_url: "ws://138.201.133.213:9642".to_string(),
            privkey: "0x53321db7c1e331d93a11a41d16f004d7ff63972ec8ec7c25db329728ceeb1710"
                .to_string(),
            sequencer_inbox_address: "0xA644B79509328CDf5BF2ebea5ad43071AE3d2c79"
                .parse()
                .unwrap(),
            contract_abi_path: "SequencerInbox.json".to_string(),
            poll_interval: Duration::from_secs(12),
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

impl BigInt {
    pub fn new(s: &str) -> anyhow::Result<Self> {
        Ok(BigInt(U256::from_dec_str(s)?))
    }
}

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

impl Base64Bytes {
    pub fn to_vec(&self) -> Vec<u8> {
        self.0.clone()
    }
}

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
    segments: Vec<u8>, // TODO: Change to BatchSegments
    /// Message count at the start of the batch.
    start_msg_count: u64,
    /// Message count at the end of the batch.
    msg_count: u64,
    /// Whether the batch has a useful message.
    has_useful_message: bool,
    /// The first non-delayed message in the batch.
    first_non_delayed_msg: MessageWithMetadata,
    /// The first useful message in the batch.
    first_useful_msg: MessageWithMetadata,
}
