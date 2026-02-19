# Polymarket Websocket Copy Trader

Websocket-first copy trader with:
- FOK order entries/exits
- profit-armed soft stop-loss (example: arm at `+5%`, trigger at `-3.5%`)
- source wallet sell -> close copied position
- hold to market resolution + auto-claim winnings
- market quality filters (volume/liquidity/spread)
- slippage guard vs source wallet price
- position sizing capped at `$20` and `1.25%` of live balance
- built-in web dashboard (overview, open/resolved positions, equity chart)

## Install

```bash
cd polymarket_bot
python -m venv .venv
. .venv/Scripts/activate
pip install -r requirements.txt
```

## Configure

Copy `.env.example` to `.env` and set:
- `SOURCE_WALLET` to the wallet you want to copy
- keep `DRY_RUN=true` for testing
- set `DRY_RUN=false` + `POLY_PRIVATE_KEY` (+ optional `POLY_FUNDER`) for live execution

## Run

```bash
python -m polymarket_bot.bot
```

Dashboard:
- `http://127.0.0.1:8080` (or your `DASHBOARD_HOST`/`DASHBOARD_PORT`)

## Notes

- Source signal detection polls `data-api` wallet trades at high frequency (`SOURCE_POLL_SECONDS`, default `0.75s`) and uses websocket books for fast pricing/execution.
- `market` websocket still runs continuously for top-of-book updates and FOK fill modeling.
- Optional tag filter (`MARKET_TAG_ALLOWLIST`) can restrict the tracked universe (for example sports-only).
- If a source trade arrives for an asset outside the current universe, the bot hydrates book/meta context on demand and adds that asset to websocket subscriptions.
- `SIGNAL_ONLY_BOOK_SUBSCRIPTIONS=true` + `PULL_BOOK_ON_SIGNAL=true` enables wallet-first mode: fewer pretracked assets, and entry book fetched right after source-wallet signal.
- Defaults are set for long-biased copying: `COPY_SELL_TRADES=false` and `CLOSE_ON_SOURCE_SELL=true`.
- Auto-claim requires `POLYGON_RPC_URL` when running live.
- Default `MARKET_REFRESH_SECONDS=20` keeps subscription universe fresh while websocket book updates stay real time.
- `SUBSCRIBE_ALL_ACTIVE_ASSETS_FOR_SIGNALS=true` is optional for broad pre-subscription; keep it `false` in wallet-first mode.
- Dry run now includes a realistic paper-fill model (depth-aware FOK, latency jitter/spikes, and random queue loss).
- The bot emits periodic `Signal/copy stats (last ~60s)` summaries so you can see why signals are skipped.
- `MIN_COPY_PRICE` lets you ignore low-priced entries (for example `0.24` skips source/book prices below 24 cents).
- By default this is conservative and skips stale books, wide spreads, and low-quality markets.
- Keep `DRY_RUN=true` until you confirm logs, sizing, and dashboard behavior.
