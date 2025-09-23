## Setup

1. `cargo install --path .`
1. `cp arrus.service ~/.config/systemd/user/`
1. `systemctl --user daemon-reload`
1. `systemctl --user enable arrus.service`

### Config
All done via ENV at the moment:
- `ARRUS_DB_BASE_URL`: Points to the discord API, shouldn't need to change from default
- `ARRUS_DB_ENDPOINT`: Points to endpoint on discord API, also shouldn't need to change
- `ARRUS_DB_USER_AGENT`: User agent when making requests to discord (Again, not needed to change)
- `ARRUS_DB_TIMEOUT`: Duration in seconds to wait for http request
- `ARRUS_DB_MAX_RETRIES`: How many times to retry a request to discord before giving up (in one refresh loop)
- `ARRUS_DB_UPDATE_INTERVAL`: How often to check the discord api for updates to the game database. This is cached on your system during boot and only updated after the etag indicates a difference. Suggested to set this to something high. Refresh is checked on first start and every `UPDATE_INTERVAL` there-after.
- `ARRUS_BRIDGE_PORT`: Port to run the bridge on, changing from default will likely not work for Vesktop
- `ARRUS_LOG_LEVEL`: Detail to log at, default is `INFO`.
