use std::sync::Arc;

use fomoxa_net::schema::{MessageSchema, Schema};

use crate::generated::{FOMOXA_MESSAGES, FOMOXA_SCHEMA_FINGERPRINT};

pub fn schema() -> Arc<Schema> {
    Arc::new(
        Schema::new(
            FOMOXA_SCHEMA_FINGERPRINT,
            FOMOXA_MESSAGES
                .iter()
                .map(|message| {
                    MessageSchema::new(message.id, message.fingerprint, message.prefixes)
                })
                .collect::<Vec<_>>(),
        )
        .expect("fomoxac never writes a schema this rejects"),
    )
}
