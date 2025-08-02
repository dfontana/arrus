use anyhow::Context;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

/// Socket counter for generating unique IDs
pub type SocketCounter = Arc<Mutex<u32>>;

/// Active socket connections
pub type ActiveSockets = Arc<Mutex<HashMap<u32, SocketInfo>>>;

/// Event channel for internal communication
pub type EventSender = mpsc::UnboundedSender<RpcEvent>;
pub type EventReceiver = mpsc::UnboundedReceiver<RpcEvent>;

/// Transport type identification
#[derive(Clone, Debug)]
pub enum TransportType {
    Ipc,
    WebSocket,
    Process,
}

/// Socket information tracking
#[derive(Clone, Debug)]
pub struct SocketInfo {
    pub socket_id: u32,
    pub client_id: String,
    pub last_pid: Option<u32>,
    pub transport_type: TransportType,
    pub sender: mpsc::UnboundedSender<RpcMessage>,
}

/// RPC event types
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RpcEventType {
    Ready,
    Error,
    #[serde(other)]
    Other,
}

impl std::fmt::Display for RpcEventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            RpcEventType::Ready => "READY",
            RpcEventType::Error => "ERROR",
            RpcEventType::Other => "OTHER",
        };
        write!(f, "{s}")
    }
}

impl From<&str> for RpcEventType {
    fn from(s: &str) -> Self {
        match s {
            "READY" => RpcEventType::Ready,
            "ERROR" => RpcEventType::Error,
            _ => RpcEventType::Other,
        }
    }
}

impl From<String> for RpcEventType {
    fn from(s: String) -> Self {
        RpcEventType::from(s.as_str())
    }
}

/// RPC message structure following Discord protocol
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcMessage {
    #[serde(serialize_with = "serialize_command")]
    pub cmd: RpcCommand,
    pub data: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub evt: Option<RpcEventType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nonce: Option<String>,
}

fn serialize_command<S>(cmd: &RpcCommand, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&cmd.to_string())
}

/// RPC command types
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RpcCommand {
    Dispatch,
    ConnectionsCallback,
    SetActivity,
    GuildTemplateBrowser,
    InviteBrowser,
    DeepLink,
    #[serde(other)]
    Unknown,
}

impl std::fmt::Display for RpcCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            RpcCommand::Dispatch => "DISPATCH",
            RpcCommand::ConnectionsCallback => "CONNECTIONS_CALLBACK",
            RpcCommand::SetActivity => "SET_ACTIVITY",
            RpcCommand::GuildTemplateBrowser => "GUILD_TEMPLATE_BROWSER",
            RpcCommand::InviteBrowser => "INVITE_BROWSER",
            RpcCommand::DeepLink => "DEEP_LINK",
            RpcCommand::Unknown => "UNKNOWN",
        };
        write!(f, "{s}")
    }
}

impl From<&str> for RpcCommand {
    fn from(s: &str) -> Self {
        match s {
            "DISPATCH" => RpcCommand::Dispatch,
            "CONNECTIONS_CALLBACK" => RpcCommand::ConnectionsCallback,
            "SET_ACTIVITY" => RpcCommand::SetActivity,
            "GUILD_TEMPLATE_BROWSER" => RpcCommand::GuildTemplateBrowser,
            "INVITE_BROWSER" => RpcCommand::InviteBrowser,
            "DEEP_LINK" => RpcCommand::DeepLink,
            _ => RpcCommand::Unknown,
        }
    }
}

impl From<String> for RpcCommand {
    fn from(s: String) -> Self {
        RpcCommand::from(s.as_str())
    }
}

/// RPC request structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcRequest {
    #[serde(deserialize_with = "deserialize_command")]
    pub cmd: RpcCommand,
    pub args: Option<Value>,
    pub nonce: Option<String>,
}

fn deserialize_command<'de, D>(deserializer: D) -> Result<RpcCommand, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    Ok(RpcCommand::from(s))
}

/// Activity type enum
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[repr(u8)]
pub enum ActivityType {
    #[default]
    Playing = 0,
    Streaming = 1,
    Listening = 2,
    Watching = 3,
    Custom = 4,
    Competing = 5,
}

impl From<u8> for ActivityType {
    fn from(value: u8) -> Self {
        match value {
            0 => ActivityType::Playing,
            1 => ActivityType::Streaming,
            2 => ActivityType::Listening,
            3 => ActivityType::Watching,
            4 => ActivityType::Custom,
            5 => ActivityType::Competing,
            _ => ActivityType::Playing,
        }
    }
}

impl From<ActivityType> for u8 {
    fn from(activity_type: ActivityType) -> Self {
        activity_type as u8
    }
}

/// Activity structure for Rich Presence
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Activity {
    pub application_id: Option<String>,
    pub name: Option<String>,
    pub details: Option<String>,
    pub state: Option<String>,
    pub timestamps: Option<Timestamps>,
    pub assets: Option<Assets>,
    pub party: Option<Party>,
    pub secrets: Option<Secrets>,
    pub buttons: Option<Vec<Button>>,
    pub instance: Option<bool>,
    #[serde(rename = "type")]
    #[serde(with = "activity_type_serde")]
    pub activity_type: Option<ActivityType>,
}

mod activity_type_serde {
    use super::ActivityType;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(
        activity_type: &Option<ActivityType>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match activity_type {
            Some(activity_type) => serializer.serialize_u8((*activity_type).into()),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<ActivityType>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Option::<u8>::deserialize(deserializer)?;
        Ok(value.map(ActivityType::from))
    }
}

/// Timestamps for activity
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Timestamps {
    pub start: Option<u64>,
    pub end: Option<u64>,
}

/// Assets for activity
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Assets {
    pub large_image: Option<String>,
    pub large_text: Option<String>,
    pub small_image: Option<String>,
    pub small_text: Option<String>,
}

/// Party information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Party {
    pub id: Option<String>,
    pub size: Option<Vec<i32>>,
}

/// Secrets for join/spectate functionality
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Secrets {
    pub join: Option<String>,
    pub spectate: Option<String>,
    #[serde(rename = "match")]
    pub match_secret: Option<String>,
}

/// Button for activity
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Button {
    pub label: String,
    pub url: String,
}

/// Processed activity for internal use
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessedActivity {
    pub application_id: String,
    pub name: String,
    pub details: Option<String>,
    pub state: Option<String>,
    pub timestamps: Option<Timestamps>,
    pub assets: Option<Assets>,
    pub party: Option<Party>,
    pub secrets: Option<Secrets>,
    pub metadata: ActivityMetadata,
    pub flags: u32,
    pub buttons: Option<Vec<String>>,
    #[serde(rename = "type")]
    #[serde(with = "activity_type_serde")]
    pub activity_type: Option<ActivityType>,
}

/// Activity metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivityMetadata {
    pub button_urls: Option<Vec<String>>,
}

/// Arguments for SET_ACTIVITY command
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SetActivityArgs {
    pub activity: Option<Activity>,
    pub pid: Option<u32>,
}

/// Arguments for browser commands
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrowserArgs {
    pub code: String,
}

/// Arguments for deep link command
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeepLinkArgs {
    pub params: String,
}

/// Events emitted by the RPC server
#[derive(Clone, Debug)]
pub enum RpcEvent {
    Connection {
        socket_id: u32,
        socket_info: SocketInfo,
    },
    Disconnection {
        socket_id: u32,
    },
    Message {
        socket_id: u32,
        request: RpcRequest,
    },
    Activity {
        activity: Box<Option<ProcessedActivity>>,
        pid: Option<u32>,
        socket_id: String,
    },
}

/// Transport handlers for different connection types
#[derive(Clone)]
pub struct TransportHandlers {
    pub on_connection: Arc<dyn Fn(SocketConnection) + Send + Sync>,
    pub on_message: Arc<dyn Fn(u32, RpcRequest) + Send + Sync>,
    pub on_close: Arc<dyn Fn(u32) + Send + Sync>,
}

/// Socket connection abstraction
pub struct SocketConnection {
    pub socket_id: u32,
    pub client_id: String,
    pub transport_type: TransportType,
    pub sender: mpsc::UnboundedSender<RpcMessage>,
}

impl SocketConnection {
    pub fn send(&self, message: RpcMessage) -> Result<(), anyhow::Error> {
        self.sender.send(message).context("Failed socket send")
    }
}
