use std::{fs, sync::Arc};

use ethers::{
    abi::Abi,
    contract::Contract,
    middleware::SignerMiddleware,
    providers::{Http, Provider},
    signers::LocalWallet,
    types::Address,
};
use serde_json::Value;

/// Simple configuration for the batch poster
pub struct Config {
    pub l1_url: String,
    pub feed_url: String,
    pub privkey: String,
    pub sequencer_inbox_address: Address,
    pub contract_abi_path: String,
}

/// Default configuration
// Note: Private key is for testing purposes only, does not contain real funds, do not use in production
impl Default for Config {
    fn default() -> Self {
        Self {
            l1_url: "http://localhost:8545".to_string(),
            feed_url: "wss://localhost:9642".to_string(),
            privkey: "0x53321db7c1e331d93a11a41d16f004d7ff63972ec8ec7c25db329728ceeb1710"
                .to_string(),
            sequencer_inbox_address: "0xA644B79509328CDf5BF2ebea5ad43071AE3d2c79"
                .parse()
                .unwrap(),
            contract_abi_path: "SequencerInbox.json".to_string(),
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
