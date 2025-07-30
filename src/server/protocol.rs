#![allow(dead_code)]

use serde_json::Value;

/// Discord protocol constants and utilities
/// Mock user data for arRPC Discord bot
pub const MOCK_USER_DATA: fn() -> Value = || {
    serde_json::json!({
        "id": "1045800378228281345",
        "username": "arrpc",
        "discriminator": "0",
        "global_name": "arRPC",
        "avatar": "cfefa4d9839fb4bdf030f91c2a13e95c",
        "avatar_decoration_data": null,
        "bot": false,
        "flags": 0,
        "premium_type": 0
    })
};

/// Discord configuration constants
pub const DISCORD_CONFIG: fn() -> Value = || {
    serde_json::json!({
        "cdn_host": "cdn.discordapp.com",
        "api_endpoint": "//discord.com/api",
        "environment": "production"
    })
};

/// Create Discord READY event payload
pub fn create_ready_payload() -> Value {
    serde_json::json!({
        "v": 1,
        "config": DISCORD_CONFIG(),
        "user": MOCK_USER_DATA()
    })
}

/// Discord RPC error codes
pub mod error_codes {
    /// Invalid invite error code
    pub const INVALID_INVITE: u32 = 4011;

    /// Invalid guild template error code
    pub const INVALID_GUILD_TEMPLATE: u32 = 4017;

    /// Connection callback code
    pub const CONNECTION_CALLBACK: u32 = 1000;
}

/// Get RPC command variants
pub mod commands {
    use super::super::types::RpcCommand;

    pub const DISPATCH: RpcCommand = RpcCommand::Dispatch;
    pub const CONNECTIONS_CALLBACK: RpcCommand = RpcCommand::ConnectionsCallback;
    pub const SET_ACTIVITY: RpcCommand = RpcCommand::SetActivity;
    pub const GUILD_TEMPLATE_BROWSER: RpcCommand = RpcCommand::GuildTemplateBrowser;
    pub const INVITE_BROWSER: RpcCommand = RpcCommand::InviteBrowser;
    pub const DEEP_LINK: RpcCommand = RpcCommand::DeepLink;
}

/// Get RPC event type variants
pub mod events {
    use super::super::types::RpcEventType;

    pub const READY: RpcEventType = RpcEventType::Ready;
    pub const ERROR: RpcEventType = RpcEventType::Error;
}

/// Activity flags
pub mod activity_flags {
    /// Instance flag (1 << 0)
    pub const INSTANCE: u32 = 1;
}

/// Activity type constants
pub mod activity_types {
    /// Playing activity type
    pub const PLAYING: u8 = 0;

    /// Streaming activity type
    pub const STREAMING: u8 = 1;

    /// Listening activity type
    pub const LISTENING: u8 = 2;

    /// Watching activity type
    pub const WATCHING: u8 = 3;

    /// Custom activity type
    pub const CUSTOM: u8 = 4;

    /// Competing activity type
    pub const COMPETING: u8 = 5;
}

/// Discord protocol validation
pub mod validation {
    use anyhow::bail;

    /// Maximum length for activity name
    pub const MAX_NAME_LENGTH: usize = 128;

    /// Maximum length for activity details
    pub const MAX_DETAILS_LENGTH: usize = 128;

    /// Maximum length for activity state
    pub const MAX_STATE_LENGTH: usize = 128;

    /// Maximum number of buttons allowed
    pub const MAX_BUTTONS: usize = 2;

    /// Maximum length for button label
    pub const MAX_BUTTON_LABEL_LENGTH: usize = 32;

    /// Validate activity field lengths
    pub fn validate_activity_field(
        field: &str,
        max_length: usize,
        field_name: &str,
    ) -> Result<(), anyhow::Error> {
        if field.len() > max_length {
            bail!("{field_name} exceeds maximum length of {max_length}")
        } else {
            Ok(())
        }
    }

    /// Validate button URL format
    pub fn validate_button_url(url: &str) -> Result<(), anyhow::Error> {
        if !url.starts_with("http://") && !url.starts_with("https://") {
            bail!("Button URL must start with http:// or https://")
        } else {
            Ok(())
        }
    }
}
