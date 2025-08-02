use super::types::*;
use anyhow::bail;
use serde_json::Value;
use tracing::warn;

pub fn process_command(
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
        RpcCommand::ConnectionsCallback => socket_info.sender.send(RpcMessage {
            cmd: RpcCommand::ConnectionsCallback,
            data: Some(serde_json::json!({ "code": 1000 })),
            evt: Some(RpcEventType::Error),
            nonce: request.nonce,
        })?,
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
                cmd: RpcCommand::SetActivity,
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
                cmd: RpcCommand::SetActivity,
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
