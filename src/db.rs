use crate::types::BatchPosterPosition;
use anyhow::Result;
use codec::{Decode, Encode};
use sled::Batch;

pub struct BatchPosterDb {
    db: sled::Db,
}

impl BatchPosterDb {
    pub fn from_path(path: &str) -> Result<Self> {
        let db = sled::open(path)?;
        Ok(Self { db })
    }

    pub fn read_batch_position(&self) -> Result<BatchPosterPosition> {
        let maybe_pos = self.db.get("pos")?;
        if let Some(pos) = maybe_pos {
            let pos = BatchPosterPosition::decode(&mut pos.as_ref())?;
            Ok(pos)
        } else {
            Ok(BatchPosterPosition::default())
        }
    }

    pub fn read_seq_number(&self) -> Result<u64> {
        let maybe_seq = self.db.get("seq")?;
        if let Some(seq) = maybe_seq {
            let seq = u64::decode(&mut seq.as_ref())?;
            Ok(seq)
        } else {
            Ok(0)
        }
    }

    pub fn write_checkpoint(&self, pos: &BatchPosterPosition, seq_num: u64) -> Result<()> {
        let mut batch = Batch::default();
        batch.insert("pos", pos.encode());
        batch.insert("seq", seq_num.encode());
        self.db.apply_batch(batch)?;

        Ok(())
    }
}
