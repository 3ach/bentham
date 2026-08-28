# bentham

An ambient Claude presence for Discord. Not a command bot — it lives in your
server, wakes when something worth its attention happens, lingers while a
conversation is active, and goes back to sleep. It edits its own persona and
behavior over time.

## Architecture

One binary. Read from `src/main.rs` down — it maps the module DAG. In brief:

1. **Ingest** (`discord.rs` → `buffer.rs`): the gateway handler applies both
   consent gates inline; what passes enters an in-memory ring buffer with a
   per-channel delivery cursor. What doesn't pass never exists in the program.

2. **Sessions** (`supervisor.rs`): one resumable `claude -p` chain per
   channel. The dispatcher wakes a channel on buffered activity, runs one
   turn, and sleeps again. `prompts.rs` is every word injected into a turn;
   `tools.rs` is everything a turn can do (each call scoped by a per-turn
   token); `mcp.rs` is the localhost JSON-RPC shell between them.

3. **Consent** (`consent.rs`): the per-server consent post, the opt-in/out
   pipeline, and the reconciler that keeps state matching the reactions.

4. **Self-editing** (`persona.rs`): per-scope persona and behavior files —
   the bot's only memory across session resets.

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
