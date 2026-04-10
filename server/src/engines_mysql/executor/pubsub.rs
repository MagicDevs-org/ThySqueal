use super::Executor;
use crate::engines::mysql::error::SqlResult;
use std::collections::{HashMap, HashSet};

#[derive(Default)]
pub struct PubSubState {
    pub subscriptions: HashMap<String, HashSet<String>>, // client_id -> channels
}

impl Executor {
    pub async fn pubsub_subscribe(&self, client_id: String, channel: String) -> SqlResult<()> {
        let mut state = self.pubsub.write().await;
        state
            .subscriptions
            .entry(client_id)
            .or_insert_with(HashSet::new)
            .insert(channel);
        Ok(())
    }

    pub async fn pubsub_unsubscribe(
        &self,
        client_id: String,
        channel: Option<String>,
    ) -> SqlResult<()> {
        let mut state = self.pubsub.write().await;
        if let Some(channels) = state.subscriptions.get_mut(&client_id) {
            match channel {
                Some(ch) => {
                    channels.remove(&ch);
                }
                None => {
                    channels.clear();
                }
            }
        }
        if state
            .subscriptions
            .get(&client_id)
            .map(|s| s.is_empty())
            .unwrap_or(false)
        {
            state.subscriptions.remove(&client_id);
        }
        Ok(())
    }

    pub async fn pubsub_publish(&self, channel: String, _message: String) -> SqlResult<usize> {
        let state = self.pubsub.read().await;
        let count = state
            .subscriptions
            .values()
            .filter(|channels| channels.contains(&channel))
            .count();
        Ok(count)
    }

    pub async fn pubsub_channels(&self) -> SqlResult<Vec<String>> {
        let state = self.pubsub.read().await;
        let mut channels: Vec<String> = state
            .subscriptions
            .values()
            .flat_map(|s| s.iter().cloned())
            .collect();
        channels.sort();
        channels.dedup();
        Ok(channels)
    }
}
