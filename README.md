# bentham

An ambient Claude presence for Discord. Not a command bot — it lives in your
server, wakes when something worth its attention happens, lingers while a
conversation is active, and goes back to sleep. It edits its own persona and
behavior over time.

## Architecture

One binary, three layers:

1. **Supervisor** (`src/supervisor.rs`) — owns the Claude sessions, one per
   channel. A dispatcher watches for wake-worthy activity and runs a
   `claude -p` turn per active channel (debounced), resuming that
   channel's session via `--resume`, so each session only ever sees its own room. Sessions rotate
   fresh every `session_max_wakes` wakes; failures back off exponentially and
   3 in a row drop that channel's session.

2. **Discord MCP server** (`src/discord.rs`, `src/mcp.rs`) — a serenity
   gateway feeds an in-process message buffer; a minimal MCP streamable-HTTP
   server on `127.0.0.1:43117/mcp` exposes it to the Claude session:
   - `wait_for_messages` — long-poll; how the bot listens while awake
   - `read_messages`, `send_message`, `add_reaction`, `list_channels`

   Because the gateway lives in the daemon (not a per-session stdio child),
   nothing is missed while Claude is asleep: messages buffer and are delivered
   on the next wake.

3. **Self-amendment** (`src/persona.rs` + MCP tools) —
   - `get_persona` / `set_persona`: rewrite `data/persona.md`, which is
     injected into the system prompt at every wake and is the bot's only
     memory across session rotations.
   - `get_behavior` / `set_behavior`: `data/behavior.json` — which channels to
     watch, wake on all messages vs. mentions/DMs only, optional idle-wake
     timer. Effective immediately.

## Isolation

Every server is a separate bentham: separate persona file
(`data/personas/<guild>.md`), separate behavior, separate opted-user list.
Each DM conversation is likewise its own scope. Enforcement is structural:
every turn gets a one-time session token, and all MCP tools resolve it to that
turn's channel + scope — a session cannot read, post, or remember outside its
scope. Whenever what he may remember changes (persona rewrite, forget_user,
an opt-out), the scope's session transcripts are dropped and the next wake
starts fresh.

## Consent model

Bentham is opt-in at two levels, enforced in the daemon (not by prompting):

- **One consent post per server**: on joining a guild the daemon posts a
  standing notice (in #general, else the system channel, else the first text
  channel). Reacting to it (any emoji) is the only way in; removing the
  reaction opts back out (and burns that server's session transcripts).
  Posting is not watching — every channel stays dormant. On each restart the
  post is edited in place (a "last restart" footer); if the record is lost,
  the post is re-found by its marker line rather than reposted.
- **Channels are dormant** until an opted-in person @mentions him there.
  Dormant channels are dropped at ingest — never buffered, never seen.
- **People are invisible** until they opt in — their messages (and their
  @mentions of him) are stripped at ingest: never stored, never delivered,
  never waking him, and absent from `read_messages` history. Not even
  metadata reaches the model. Exceptions: DMing him is consent, and other
  bots' messages are always visible (and never wake him).
- **`forget_user`** (at someone's request): opts them out, purges their
  buffered messages, drops every channel's session transcript, and directs him
  to scrub them from his persona notes. **`ignore_channel`**: back to dormant.
  State lives in `data/consent.json`, per guild.

## Discord setup (one-time)

1. https://discord.com/developers/applications → **New Application** (name it Bentham).
2. **Bot** tab → **Reset Token** → copy it:
   ```sh
   install -m 600 /dev/null data/discord-token && $EDITOR data/discord-token  # paste token
   ```
3. Same tab, under **Privileged Gateway Intents**: enable **Message Content Intent**.
4. Invite it (replace `APP_ID` with the Application ID from General Information):
   ```
   https://discord.com/oauth2/authorize?client_id=APP_ID&scope=bot&permissions=68672
   ```
   (68672 = View Channels + Send Messages + Read Message History + Add Reactions.)

## Run

```sh
cargo run --release          # uses ./config.toml; pass a path to override
```

Requires a logged-in `claude` CLI for the account that should pay for the
tokens. `RUST_LOG=bentham=debug` for chatter.

## Config

See `config.toml` — model, wake debounce, session rotation, turn timeout, and
which built-in Claude tools are disallowed (default: all of them — he gets
the discord MCP server and nothing else). Runtime
behavior (channels, wake triggers) lives in `data/behavior.json` and is meant
to be adjusted by the bot itself — or by you, by talking to it.

## Cost notes

- `respond_to = "all"` (the default) wakes him for any opted-in person's
  message. Consent gating already bounds who can spend his tokens; if a server
  gets expensive anyway, tell him (or set) `"mentions"` there — then only
  @mentions and DMs wake him, everything else buffering as context.
- Each wake is a `claude -p --resume` turn on the configured model
  (default `sonnet`).

## Files

- `data/personas/<scope>.md` — self-editable identity + memory, one per server/DM
- `data/behaviors.json` — self-editable behavior, per scope
- `data/state.json` — per-channel session ids / wake counts
- `data/consent.json` — active channels + opted-in users
- `data/discord-token` — bot token (gitignored, like all of `data/`)
