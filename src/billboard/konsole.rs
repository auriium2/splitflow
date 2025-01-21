use tracing::span::{Attributes, Record};
use tracing::{Event, Id, Metadata, Subscriber};

use serenity::all::ChannelId;
use std::sync::Arc;
use tokio::sync::Mutex;

struct Konsole {
    discord_channel: Arc<Mutex<ChannelId>>,
}

impl Subscriber for Konsole {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, span: &Attributes<'_>) -> Id {
        Id::from_u64(0)
    }

    fn record(&self, _span: &Id, _values: &Record<'_>) {
        // Implement if needed
    }

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {
        // Implement if needed
    }

    fn event(&self, event: &Event<'_>) {
        let discord_channel = Arc::clone(&self.discord_channel);
        let message = format!("Event: {:?}", event);

        tokio::spawn(async move {
            //let _ = channel.send_message(&ctx.http, CreateMessage::new().content(message)).await;
        });
    }

    fn enter(&self, _span: &Id) {
        // Implement if needed
    }

    fn exit(&self, _span: &Id) {
        // Implement if needed
    }
    // ...
}
