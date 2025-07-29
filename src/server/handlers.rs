use serde_json::Value;
use std::sync::Arc;

use super::types::*;
use crate::error::ArrusError;

/// Process RPC commands
pub fn process_command(
    socket_id: u32,
    request: RpcRequest,
    active_sockets: &ActiveSockets,
    event_tx: &EventSender,
) -> Result<(), ArrusError> {
    let socket_info = { active_sockets.lock().unwrap().get(&socket_id).cloned() };

    let Some(socket_info) = socket_info else {
        return Err(ArrusError::SocketNotFound(socket_id));
    };

    match request.cmd {
        RpcCommand::ConnectionsCallback => {
            handle_connections_callback(socket_info, request.nonce)?;
        }

        RpcCommand::SetActivity => {
            handle_set_activity(
                socket_id,
                socket_info,
                request.args,
                request.nonce,
                active_sockets,
                event_tx,
            )?;
        }

        RpcCommand::GuildTemplateBrowser => {
            handle_guild_template_browser(socket_info, request.args, request.nonce, event_tx)?;
        }

        RpcCommand::InviteBrowser => {
            handle_invite_browser(socket_info, request.args, request.nonce, event_tx)?;
        }

        RpcCommand::DeepLink => {
            handle_deep_link(request.args, event_tx)?;
        }

        RpcCommand::Dispatch => {
            // DISPATCH commands are handled at the transport layer
            eprintln!("DISPATCH command should not reach handlers");
        }

        RpcCommand::Unknown => {
            eprintln!("Unknown command: {}", request.cmd);
        }
    }

    Ok(())
}

/// Handle CONNECTIONS_CALLBACK command
fn handle_connections_callback(
    socket_info: SocketInfo,
    nonce: Option<String>,
) -> Result<(), ArrusError> {
    let response = RpcMessage {
        cmd: RpcCommand::ConnectionsCallback,
        data: Some(serde_json::json!({ "code": 1000 })),
        evt: Some(RpcEventType::Error),
        nonce,
    };

    socket_info
        .sender
        .send(response)
        .map_err(|_| ArrusError::SendError)?;

    Ok(())
}

/// Handle SET_ACTIVITY command
fn handle_set_activity(
    socket_id: u32,
    mut socket_info: SocketInfo,
    args: Option<Value>,
    nonce: Option<String>,
    active_sockets: &ActiveSockets,
    event_tx: &EventSender,
) -> Result<(), ArrusError> {
    let args: SetActivityArgs = args
        .map(serde_json::from_value)
        .transpose()
        .map_err(ArrusError::SerializationError)?
        .unwrap_or_default();

    // Update last PID
    if let Some(pid) = args.pid {
        socket_info.last_pid = Some(pid);
        let mut sockets = active_sockets.lock().unwrap();
        sockets.insert(socket_id, socket_info.clone());
    }

    match args.activity {
        None => {
            // Activity clear
            let response = RpcMessage {
                cmd: RpcCommand::SetActivity,
                data: None,
                evt: None,
                nonce,
            };

            socket_info
                .sender
                .send(response)
                .map_err(|_| ArrusError::SendError)?;

            let _ = event_tx.send(RpcEvent::Activity {
                activity: None,
                pid: args.pid,
                socket_id: socket_id.to_string(),
            });
        }

        Some(activity) => {
            // Process activity
            let processed = process_activity(activity, &socket_info.client_id)?;

            // Send response to client
            let response_data = serde_json::json!({
                "name": "",
                "application_id": socket_info.client_id,
                "type": 0
            });

            let response = RpcMessage {
                cmd: RpcCommand::SetActivity,
                data: Some(response_data),
                evt: None,
                nonce,
            };

            socket_info
                .sender
                .send(response)
                .map_err(|_| ArrusError::SendError)?;

            // Emit activity event
            let _ = event_tx.send(RpcEvent::Activity {
                activity: Some(processed),
                pid: args.pid,
                socket_id: socket_id.to_string(),
            });
        }
    }

    Ok(())
}

/// Process activity data into internal format
fn process_activity(activity: Activity, client_id: &str) -> Result<ProcessedActivity, ArrusError> {
    let mut metadata = ActivityMetadata { button_urls: None };

    let mut button_labels = None;

    // Process buttons
    if let Some(ref buttons) = activity.buttons {
        metadata.button_urls = Some(buttons.iter().map(|b| b.url.clone()).collect());
        button_labels = Some(buttons.iter().map(|b| b.label.clone()).collect());
    }

    // Process timestamps (convert seconds to milliseconds if needed)
    let timestamps = activity.timestamps.map(|mut ts| {
        if let Some(start) = ts.start {
            if needs_ms_conversion(start) {
                ts.start = Some(start * 1000);
            }
        }
        if let Some(end) = ts.end {
            if needs_ms_conversion(end) {
                ts.end = Some(end * 1000);
            }
        }
        ts
    });

    Ok(ProcessedActivity {
        application_id: client_id.to_string(),
        name: activity.name.unwrap_or_default(),
        details: activity.details,
        state: activity.state,
        timestamps,
        assets: activity.assets,
        party: activity.party,
        secrets: activity.secrets,
        metadata,
        flags: if activity.instance.unwrap_or(false) {
            1
        } else {
            0
        },
        buttons: button_labels,
        activity_type: activity.activity_type,
    })
}

/// Check if timestamp needs millisecond conversion
fn needs_ms_conversion(timestamp: u64) -> bool {
    let now_str = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .to_string();
    let ts_str = timestamp.to_string();
    now_str.len() - ts_str.len() > 2
}

/// Handle INVITE_BROWSER command
fn handle_invite_browser(
    socket_info: SocketInfo,
    args: Option<Value>,
    nonce: Option<String>,
    event_tx: &EventSender,
) -> Result<(), ArrusError> {
    let args: BrowserArgs = args
        .ok_or(ArrusError::MissingArgs)
        .and_then(|v| serde_json::from_value(v).map_err(ArrusError::SerializationError))?;

    let callback = create_browser_callback(
        socket_info,
        RpcCommand::InviteBrowser,
        args.code.clone(),
        nonce,
        true,
    );

    let _ = event_tx.send(RpcEvent::Invite {
        code: args.code,
        callback,
    });

    Ok(())
}

/// Handle GUILD_TEMPLATE_BROWSER command
fn handle_guild_template_browser(
    socket_info: SocketInfo,
    args: Option<Value>,
    nonce: Option<String>,
    event_tx: &EventSender,
) -> Result<(), ArrusError> {
    let args: BrowserArgs = args
        .ok_or(ArrusError::MissingArgs)
        .and_then(|v| serde_json::from_value(v).map_err(ArrusError::SerializationError))?;

    let callback = create_browser_callback(
        socket_info,
        RpcCommand::GuildTemplateBrowser,
        args.code.clone(),
        nonce,
        false,
    );

    let _ = event_tx.send(RpcEvent::GuildTemplate {
        code: args.code,
        callback,
    });

    Ok(())
}

/// Create browser callback
fn create_browser_callback(
    socket_info: SocketInfo,
    cmd: RpcCommand,
    code: String,
    nonce: Option<String>,
    is_invite: bool,
) -> Arc<dyn Fn(bool) + Send + Sync> {
    Arc::new(move |is_valid| {
        let response = if is_valid {
            RpcMessage {
                cmd: cmd.clone(),
                data: Some(serde_json::json!({ "code": code })),
                evt: None,
                nonce: nonce.clone(),
            }
        } else {
            let error_code = if is_invite { 4011 } else { 4017 };
            let message = format!(
                "Invalid {} id: {}",
                if is_invite {
                    "invite"
                } else {
                    "guild template"
                },
                code
            );

            RpcMessage {
                cmd: cmd.clone(),
                data: Some(serde_json::json!({
                    "code": error_code,
                    "message": message
                })),
                evt: Some(RpcEventType::Error),
                nonce: nonce.clone(),
            }
        };

        let _ = socket_info.sender.send(response);
    })
}

/// Handle DEEP_LINK command
fn handle_deep_link(args: Option<Value>, event_tx: &EventSender) -> Result<(), ArrusError> {
    let args: DeepLinkArgs = args
        .ok_or(ArrusError::MissingArgs)
        .and_then(|v| serde_json::from_value(v).map_err(ArrusError::SerializationError))?;

    let _ = event_tx.send(RpcEvent::DeepLink {
        params: args.params,
    });

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamp_conversion() {
        // Test seconds to milliseconds conversion
        assert!(needs_ms_conversion(1640000000)); // 2021 timestamp in seconds
        assert!(!needs_ms_conversion(1640000000000)); // 2021 timestamp in milliseconds
    }

    #[test]
    fn test_activity_processing() {
        let activity = Activity {
            name: Some("Test Game".to_string()),
            details: Some("In a match".to_string()),
            timestamps: Some(Timestamps {
                start: Some(1640000000), // Seconds timestamp
                end: None,
            }),
            buttons: Some(vec![Button {
                label: "Join Game".to_string(),
                url: "https://example.com".to_string(),
            }]),
            instance: Some(true),
            ..Default::default()
        };

        let processed = process_activity(activity, "123456789").unwrap();

        assert_eq!(processed.application_id, "123456789");
        assert_eq!(processed.name, "Test Game");
        assert_eq!(processed.flags, 1); // Instance flag
        assert!(processed.timestamps.unwrap().start.unwrap() > 1640000000000); // Converted to ms
        assert!(processed.metadata.button_urls.is_some());
    }
}
