use std::sync::Arc;

use cyclone_net::schema::{MessageSchema, Schema};

use crate::generated::{CYCLONE_MESSAGES, CYCLONE_SCHEMA_FINGERPRINT};

pub fn schema() -> Arc<Schema> {
    Arc::new(
        Schema::new(
            CYCLONE_SCHEMA_FINGERPRINT,
            CYCLONE_MESSAGES
                .iter()
                .map(|message| {
                    MessageSchema::new(message.id, message.fingerprint, message.prefixes)
                })
                .collect::<Vec<_>>(),
        )
        .expect("cyclonec never writes a schema this rejects"),
    )
}
