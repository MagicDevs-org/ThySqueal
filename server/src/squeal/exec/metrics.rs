use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Default)]
pub struct ServerMetrics {
    pub uptime_secs: Arc<AtomicU64>,
    pub connections: Arc<AtomicU64>,
    pub questions: Arc<AtomicU64>,
    pub queries: Arc<AtomicU64>,
    pub com_select: Arc<AtomicU64>,
    pub com_insert: Arc<AtomicU64>,
    pub com_update: Arc<AtomicU64>,
    pub com_delete: Arc<AtomicU64>,
}

impl ServerMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn inc_connection(&self) {
        self.connections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_question(&self) {
        self.questions.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_query(&self) {
        self.queries.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_select(&self) {
        self.com_select.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_insert(&self) {
        self.com_insert.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_update(&self) {
        self.com_update.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_delete(&self) {
        self.com_delete.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get_status_vars(&self) -> Vec<(String, String)> {
        let uptime = self.uptime_secs.load(Ordering::Relaxed);
        let connections = self.connections.load(Ordering::Relaxed);
        let questions = self.questions.load(Ordering::Relaxed);
        let queries = self.queries.load(Ordering::Relaxed);
        let com_select = self.com_select.load(Ordering::Relaxed);
        let com_insert = self.com_insert.load(Ordering::Relaxed);
        let com_update = self.com_update.load(Ordering::Relaxed);
        let com_delete = self.com_delete.load(Ordering::Relaxed);

        vec![
            ("Uptime".to_string(), uptime.to_string()),
            ("Uptime_since_flush_status".to_string(), uptime.to_string()),
            ("Threads_connected".to_string(), connections.to_string()),
            ("Threads_running".to_string(), "1".to_string()),
            ("Questions".to_string(), questions.to_string()),
            ("Queries".to_string(), queries.to_string()),
            ("Com_select".to_string(), com_select.to_string()),
            ("Com_insert".to_string(), com_insert.to_string()),
            ("Com_update".to_string(), com_update.to_string()),
            ("Com_delete".to_string(), com_delete.to_string()),
            ("Connections".to_string(), connections.to_string()),
            ("Aborted_connects".to_string(), "0".to_string()),
            ("Bytes_received".to_string(), "0".to_string()),
            ("Bytes_sent".to_string(), "0".to_string()),
            ("Max_used_connections".to_string(), connections.to_string()),
        ]
    }
}
