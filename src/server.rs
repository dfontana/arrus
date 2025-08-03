mod types;

use anyhow::bail;
use serde_json::Value;
use tracing::{error, warn};
use types::{
    ActiveSockets, Activity, ActivityMetadata, EventSender, ProcessedActivity, RpcCommand,
    RpcEvent, RpcEventType, RpcMessage, RpcRequest, SetActivityArgs, SocketConnection,
    SocketCounter, SocketInfo,
};

trait Rpc {
    fn hdl_conn(
        &self,
        event_tx: &EventSender,
        active_sockets: &ActiveSockets,
        socket_counter: &SocketCounter,
        socket: SocketConnection,
    ) {
        // Generate unique socket ID
        let socket_id = {
            let mut counter = socket_counter.lock().unwrap();
            *counter += 1;
            *counter
        };

        // Send Discord READY event
        let ready_message = RpcMessage {
            cmd: RpcCommand::DISPATCH,
            data: Some(serde_json::json!({
                "v": 1,
                "config": serde_json::json!({
                    "cdn_host": "cdn.discordapp.com",
                    "api_endpoint": "//discord.com/api",
                    "environment": "production"
                }),
                "user": serde_json::json!({
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
            })),
            evt: Some(RpcEventType::READY),
            nonce: None,
        };

        // Send ready message
        if let Err(e) = socket.send(ready_message) {
            error!("Failed to send READY message: {e}");
            return;
        }

        // Store socket info
        let socket_info = SocketInfo {
            socket_id,
            client_id: socket.client_id.clone(),
            last_pid: None,
            transport_type: socket.transport_type.clone(),
            sender: socket.sender.clone(),
        };

        {
            let mut sockets = active_sockets.lock().unwrap();
            sockets.insert(socket_id, socket_info.clone());
        }

        // Emit connection event
        let _ = event_tx.send(RpcEvent::Connection {
            socket_id,
            socket_info,
        });
    }

    fn hdl_msg(
        &self,
        event_tx: &EventSender,
        active_sockets: &ActiveSockets,
        socket_id: u32,
        request: RpcRequest,
    ) {
        // Emit message event first
        let _ = event_tx.send(RpcEvent::Message {
            socket_id,
            request: request.clone(),
        });

        // Process specific commands
        if let Err(e) = process_command(socket_id, request, &active_sockets, &event_tx) {
            error!("Error processing command: {e}");
        }
    }

    fn hdl_close(&self, event_tx: &EventSender, active_sockets: &ActiveSockets, socket_id: u32) {
        // Get socket info before removal
        let socket_info = {
            let mut sockets = active_sockets.lock().unwrap();
            sockets.remove(&socket_id)
        };

        if let Some(info) = socket_info {
            // Emit activity clear event
            let _ = event_tx.send(RpcEvent::Activity {
                activity: Box::new(None),
                pid: info.last_pid,
                socket_id: socket_id.to_string(),
            });

            // Emit disconnection event
            let _ = event_tx.send(RpcEvent::Disconnection { socket_id });
        }
    }
}

fn process_command(
    socket_id: u32,
    request: RpcRequest,
    active_sockets: &ActiveSockets,
    event_tx: &EventSender,
) -> Result<(), anyhow::Error> {
    let socket_info = { active_sockets.lock().unwrap().get(&socket_id).cloned() };

    let Some(socket_info) = socket_info else {
        bail!("Socket not found {socket_id}");
    };

    match request.cmd {
        RpcCommand::CONNECTIONS_CALLBACK => socket_info.sender.send(RpcMessage {
            cmd: RpcCommand::CONNECTIONS_CALLBACK,
            data: Some(serde_json::json!({ "code": 1000 })),
            evt: Some(RpcEventType::ERROR),
            nonce: request.nonce,
        })?,
        RpcCommand::SET_ACTIVITY => {
            handle_set_activity(
                socket_id,
                socket_info,
                request.args,
                request.nonce,
                active_sockets,
                event_tx,
            )?;
        }
        x => warn!("Unimplemented command requested: {x}"),
    }
    Ok(())
}

fn handle_set_activity(
    socket_id: u32,
    mut socket_info: SocketInfo,
    args: Option<Value>,
    nonce: Option<String>,
    active_sockets: &ActiveSockets,
    event_tx: &EventSender,
) -> Result<(), anyhow::Error> {
    let args: SetActivityArgs = args
        .map(serde_json::from_value)
        .transpose()?
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
                cmd: RpcCommand::SET_ACTIVITY,
                data: None,
                evt: None,
                nonce,
            };

            socket_info.sender.send(response)?;

            let _ = event_tx.send(RpcEvent::Activity {
                activity: Box::new(None),
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
                cmd: RpcCommand::SET_ACTIVITY,
                data: Some(response_data),
                evt: None,
                nonce,
            };

            socket_info.sender.send(response)?;

            // Emit activity event
            let _ = event_tx.send(RpcEvent::Activity {
                activity: Box::new(Some(processed)),
                pid: args.pid,
                socket_id: socket_id.to_string(),
            });
        }
    }

    Ok(())
}

/// Process activity data into internal format
fn process_activity(
    activity: Activity,
    client_id: &str,
) -> Result<ProcessedActivity, anyhow::Error> {
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
