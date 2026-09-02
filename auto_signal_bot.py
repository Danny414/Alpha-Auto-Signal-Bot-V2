#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════╗
║        AUTONOMOUS SIGNAL BOT — Full Stack Trading Intelligence   ║
║                                                                  ║
║  Phase 1: Multi-token scanner (Gate.io OHLCV, free API)         ║
║  Phase 2: Onchain — ETH (Etherscan) + BSC (BscScan) +           ║
║            Solana (Helius) — all three chains validated          ║
║  Phase 3: Gemini AI reasoning & quality scoring                  ║
║  Phase 4: Signal queue · 5/day cap · auto-grading system        ║
║                                                                  ║
║  Markets: Crypto only                                            ║
║  Patterns: Engulfing · Morning/Evening Star · 3 Soldiers/Crows  ║
║            Hammer/Shooting Star · Abandoned Baby                 ║
║  Indicators: EMA 20/50/200 · RSI · ATR · Volume · CVD           ║
║  Structure:  S/R · FVG · Order Blocks · MTF (4H/1H)             ║
║  Derivatives: Funding Rate · OI Change                           ║
╚══════════════════════════════════════════════════════════════════╝

SETUP (Replit Secrets):
  GEMINI_API_KEY      → Google Gemini API key
  ETHERSCAN_API_KEY   → Etherscan API key (ETH onchain)
  BSCSCAN_API_KEY     → BscScan API key (BSC onchain)
  HELIUS_API_KEY      → Helius API key (Solana onchain)
  TELEGRAM_BOT_TOKEN  → Telegram bot token (this bot)
  TELEGRAM_CHAT_ID    → Telegram channel / chat ID (this bot)

REQUIREMENTS:
  pip install requests

TELEGRAM COMMANDS:
  /signal_stats  → autonomous signal performance report
  /open_signals  → currently open signals with live P&L
"""

import os, sys, time, json, sqlite3, threading, math, concurrent.futures
from datetime import datetime, timezone
from collections import defaultdict
import requests

# Shared cross-bot signal lock
# Searches same directory first (Railway standalone), then parent (multi-bot Replit setup)
_bot_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _bot_dir)
sys.path.insert(0, os.path.dirname(_bot_dir))
from shared_signal_lock import check_conflict as check_signal_conflict, set_lock as set_signal_lock

# ══════════════════════════════════════════════════════════════════
# SECRETS
# ══════════════════════════════════════════════════════════════════
GEMINI_API_KEY     = os.getenv("GEMINI_API_KEY",     "")
DEEPSEEK_API_KEY   = os.getenv("DEEPSEEK_API_KEY",   "")
ETHERSCAN_API_KEY  = os.getenv("ETHERSCAN_API_KEY",  "")
BSCSCAN_API_KEY    = os.getenv("BSCSCAN_API_KEY",    "")
HELIUS_API_KEY     = os.getenv("HELIUS_KEY") or os.getenv("HELIUS_API_KEY", "")
# Auto Signal Bot uses its OWN dedicated token (AUTO_SIGNAL_BOT_TOKEN).
# This is completely separate from the NDF Bot (TELEGRAM_BOT_TOKEN) to
# prevent message cross-contamination and polling conflicts.
TELEGRAM_BOT_TOKEN  = os.getenv("AUTO_SIGNAL_BOT_TOKEN", "")
TELEGRAM_CHAT_ID    = os.getenv("AUTO_SIGNAL_CHAT_ID", "")
AUTO_SIGNAL_CHAT_ID = "867434065"   # Auto Signal Bot chat — startup & daily recap go here
CMD_BOT_TOKEN       = TELEGRAM_BOT_TOKEN  # same token — handles own commands

HELIUS_API_BASE = "https://api.helius.xyz/v0"

# Free public RPC nodes — no key needed, don't compete with ETH Onchain Bot
ETH_RPC_NODES = [
    "https://ethereum.publicnode.com",
    "https://eth.llamarpc.com",
    "https://1rpc.io/eth",
]
BSC_RPC_NODES = [
    "https://bsc-dataseed1.defibit.io",
    "https://bsc.publicnode.com",
    "https://1rpc.io/bnb",
    "https://bsc-dataseed.binance.org",
]

# ══════════════════════════════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════════════════════════════
SCAN_INTERVAL        = 1800   # 30 min between full scan cycles
PRICE_CHECK_INTERVAL = 120    # 2 min for open signal monitoring
MAX_SIGNALS_PER_DAY  = 3      # hard daily cap
MIN_SIGNAL_GAP_SECS  = 5400   # min 90 min between any two signals
INTERNAL_MIN_SCORE   = 55     # internal filter before AI scoring
MIN_SIGNAL_SCORE     = 60     # V2 minimum confidence; 3/5 conditions maps to 60-74
MIN_24H_VOLUME_USD   = 10_000_000

# Symbols excluded from signals — top-10 market cap + regime trackers
# These remain in WATCHLIST for regime/correlation data but never fire signals.
SIGNAL_EXCLUDED = {
    "BTC", "ETH",          # regime reference chains
    "BNB", "XRP", "SOL",   # top-10 by market cap (futures-tradeable)
    "DOGE", "ADA", "TRX",  # top-10 by market cap
    "TON", "AVAX",         # top-10 by market cap
}
MAX_SIGNAL_AGE_HRS   = 72     # expire open signals after 72H
SL_PCT               = 0.03   # fixed 3% SL on every signal
TP1_MIN_PCT          = 0.06   # minimum TP1 — ATR can push higher
TP2_MIN_PCT          = 0.105  # minimum TP2 — ATR can push higher
ONCHAIN_LOOKBACK_ETH = 1200   # ~4H ETH blocks (12s block time)
ONCHAIN_LOOKBACK_BSC = 1000   # ~50min BSC blocks (3s block) — safely under public RPC 1000-block limit

# ERC-20 / BEP-20 Transfer event topic
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

# ══════════════════════════════════════════════════════════════════
# GATE.IO SYMBOL MAP
# Binance symbol → Gate.io currency_pair  (e.g. BTCUSDT → BTC_USDT)
# Only needed where the auto-conversion differs or symbol is absent
# ══════════════════════════════════════════════════════════════════
GATEIO_OVERRIDE = {
    "SHIBUSDT":    "SHIB_USDT",
    "PEPEUSDT":    "PEPE_USDT",
    "BONKUSDT":    "BONK_USDT",
    "WIFUSDT":     "WIF_USDT",
    "JUPUSDT":     "JUP_USDT",
    "1INCHUSDT":   "1INCH_USDT",
    "BTTCUSDT":    "BTT_USDT",
    "FLOCUSDT":    "FLOKI_USDT",
}

def binance_to_gateio(binance_sym: str) -> str:
    if binance_sym in GATEIO_OVERRIDE:
        return GATEIO_OVERRIDE[binance_sym]
    if binance_sym.endswith("USDT"):
        base = binance_sym[:-4]
        return f"{base}_USDT"
    return binance_sym.replace("BTC", "BTC_").replace("ETH", "ETH_")

GATEIO_INTERVAL_MAP = {
    "1h":  "1h",
    "4h":  "4h",
    "15m": "15m",
    "1d":  "1d",
}

# ══════════════════════════════════════════════════════════════════
# WATCHLIST  (crypto only — stablecoins, commodities & stocks removed)
# ══════════════════════════════════════════════════════════════════
WATCHLIST = {
    # ── BTC & ETH — kept for regime data only, NEVER fire signals ─
    "BTC":    {"binance": "BTCUSDT",  "category": "crypto",
               "eth_contract": "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",
               "bsc_contract": "0x7130d2a12b9bcbfae4f2634d864a1ee1ce3ead9c",
               "sol_mint":     "9n4nbM75f5Ui33ZbPYXn59EwSgE8CGsHtAeTH5YFeJ9E"},
    "ETH":    {"binance": "ETHUSDT",  "category": "crypto",
               "eth_contract": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
               "bsc_contract": "0x2170ed0880ac9a755fd29b2688956bd959f933f8",
               "sol_mint":     "7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs"},
    # ── Large / Mid Caps ──────────────────────────────────────────
    "BNB":    {"binance": "BNBUSDT",  "category": "crypto",
               "eth_contract": "0xb8c77482e45f1f44de1745f52c74426c631bdd52",
               "bsc_contract": None,
               "sol_mint":     None},
    "SOL":    {"binance": "SOLUSDT",  "category": "crypto",
               "eth_contract": None,
               "bsc_contract": None,
               "sol_mint":     "So11111111111111111111111111111111111111112"},
    "XRP":    {"binance": "XRPUSDT",  "category": "crypto",
               "eth_contract": None,
               "bsc_contract": "0x1d2f0da169ceb9fc7b3144628db156f3f6c60dbe",
               "sol_mint":     None},
    "DOGE":   {"binance": "DOGEUSDT", "category": "crypto",
               "eth_contract": None,
               "bsc_contract": "0xba2ae424d960c26247dd6c32edc70b295c744c43",
               "sol_mint":     None},
    "ADA":    {"binance": "ADAUSDT",  "category": "crypto",
               "eth_contract": None,
               "bsc_contract": "0x3ee2200efb3400fabb9aacf31297cbdd1d435d47",
               "sol_mint":     None},
    "AVAX":   {"binance": "AVAXUSDT", "category": "crypto",
               "eth_contract": "0x85f138bfee4ef8e540890cfb48f620571d67eda5",
               "bsc_contract": "0x1ce0c2827e2ef14d5c4f29a091d735a204794041",
               "sol_mint":     None},
    "LINK":   {"binance": "LINKUSDT", "category": "crypto",
               "eth_contract": "0x514910771af9ca656af840dff83e8264ecf986ca",
               "bsc_contract": "0xf8a0bf9cf54bb92f17374d9e9a321e6a111a51bd",
               "sol_mint":     None},
    "UNI":    {"binance": "UNIUSDT",  "category": "crypto",
               "eth_contract": "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984",
               "bsc_contract": "0xbf5140a22578168fd562dccf235e5d43a02ce9b1",
               "sol_mint":     None},
    "AAVE":   {"binance": "AAVEUSDT", "category": "crypto",
               "eth_contract": "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9",
               "bsc_contract": "0xfb6115445bff7b52feb98650c87f44907e58f802",
               "sol_mint":     None},
    "ARB":    {"binance": "ARBUSDT",  "category": "crypto",
               "eth_contract": "0xb50721bcf8d664c30412cfbc6cf7a15145234ad1",
               "bsc_contract": None, "sol_mint": None},
    "OP":     {"binance": "OPUSDT",   "category": "crypto",
               "eth_contract": "0x4200000000000000000000000000000000000042",
               "bsc_contract": None, "sol_mint": None},
    "INJ":    {"binance": "INJUSDT",  "category": "crypto",
               "eth_contract": "0xe28b3b32b6c345a34ff64674606124dd5aceca30",
               "bsc_contract": "0xa2b726b1145a4773f68593cf171187d8ebe4d495",
               "sol_mint":     None},
    "SUI":    {"binance": "SUIUSDT",  "category": "crypto",
               "eth_contract": None,
               "bsc_contract": "0x8f0528ce5ef7b51152a59745befdd91d97091d2f",
               "sol_mint":     None},
    "APT":    {"binance": "APTUSDT",  "category": "crypto",
               "eth_contract": None,
               "bsc_contract": "0xa9c41a46a6b3531d28d5c32f6633dd2ff05dfb90",
               "sol_mint":     None},
    "JUP":    {"binance": "JUPUSDT",  "category": "crypto",
               "eth_contract": None, "bsc_contract": None,
               "sol_mint": "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN"},
    "WIF":    {"binance": "WIFUSDT",  "category": "crypto",
               "eth_contract": None, "bsc_contract": None,
               "sol_mint": "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm"},
    "BONK":   {"binance": "BONKUSDT", "category": "crypto",
               "eth_contract": None, "bsc_contract": None,
               "sol_mint": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"},
    "PEPE":   {"binance": "PEPEUSDT", "category": "crypto",
               "eth_contract": "0x6982508145454ce325ddbe47a25d4ec3d2311933",
               "bsc_contract": "0x25d887ce7a35172c62febfd67a1856f20faebb00",
               "sol_mint":     None},
    "SHIB":   {"binance": "SHIBUSDT", "category": "crypto",
               "eth_contract": "0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce",
               "bsc_contract": "0x2859e4544c4bb03966803b044a93563bd2d0dd4b",
               "sol_mint":     None},
    "APE":    {"binance": "APEUSDT",  "category": "crypto",
               "eth_contract": "0x4d224452801aced8b2f0aebe155379bb5d594381",
               "bsc_contract": None, "sol_mint": None},
    "LDO":    {"binance": "LDOUSDT",  "category": "crypto",
               "eth_contract": "0x5a98fcbea516cf06857215779fd812ca3bef1b32",
               "bsc_contract": None, "sol_mint": None},
    "CRV":    {"binance": "CRVUSDT",  "category": "crypto",
               "eth_contract": "0xd533a949740bb3306d119cc777fa900ba034cd52",
               "bsc_contract": None, "sol_mint": None},
    "MKR":    {"binance": "MKRUSDT",  "category": "crypto",
               "eth_contract": "0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2",
               "bsc_contract": None, "sol_mint": None},
    "PENDLE": {"binance": "PENDLEUSDT","category": "crypto",
               "eth_contract": "0x808507121b80c02388fad14726482e061b8da827",
               "bsc_contract": "0xb3ed0a426155b79b898849803e3b36552f7ed507",
               "sol_mint":     None},
    "WLD":    {"binance": "WLDUSDT",  "category": "crypto",
               "eth_contract": "0x163f8c2467924be0ae7b5347228cabf260318753",
               "bsc_contract": None, "sol_mint": None},
    "TIA":    {"binance": "TIAUSDT",  "category": "crypto",
               "eth_contract": "0x967ef5f9d6f5f8ce48f5d4e3cdc9ccaa78736e91",
               "bsc_contract": None, "sol_mint": None},
    "NEAR":   {"binance": "NEARUSDT", "category": "crypto",
               "eth_contract": "0x85f17cf997934a597031b2e18a9ab6ebd4b9f6a4",
               "bsc_contract": "0x1fa4a73a3f0133f0025378af00236f3abdee5d63",
               "sol_mint":     None},
    "FTM":    {"binance": "FTMUSDT",  "category": "crypto",
               "eth_contract": "0x4e15361fd6b4bb609fa63c81a2be19d873717870",
               "bsc_contract": "0xad29abb318791d579433d831ed122afeaf29dcfe",
               "sol_mint":     None},
    "SAND":   {"binance": "SANDUSDT", "category": "crypto",
               "eth_contract": "0x3845badade8e6dff049820680d1f14bd3903a5d0",
               "bsc_contract": None, "sol_mint": None},
    "MANA":   {"binance": "MANAUSDT", "category": "crypto",
               "eth_contract": "0x0f5d2fb29fb7d3cfee444a200298f468908cc942",
               "bsc_contract": None, "sol_mint": None},
    "AXS":    {"binance": "AXSUSDT",  "category": "crypto",
               "eth_contract": "0xbb0e17ef65f82ab018d8edd776e8dd940327b28b",
               "bsc_contract": "0x715d400f88c167884bbcc41c5fea407ed4d2f8a0",
               "sol_mint":     None},
    "CAKE":   {"binance": "CAKEUSDT", "category": "crypto",
               "eth_contract": None,
               "bsc_contract": "0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82",
               "sol_mint":     None},
    "DOT":    {"binance": "DOTUSDT",  "category": "crypto",
               "eth_contract": None,
               "bsc_contract": "0x7083609fce4d1d8dc0c979aab8c869ea2c873402",
               "sol_mint":     None},
    "LTC":    {"binance": "LTCUSDT",  "category": "crypto",
               "eth_contract": None,
               "bsc_contract": "0x4338665cbb7b2485a8855a139b75d5e34ab0db94",
               "sol_mint":     None},
    "IMX":    {"binance": "IMXUSDT",  "category": "crypto",
               "eth_contract": "0xf57e7e7c23978c3caec3c3548e3d615c346e79ff",
               "bsc_contract": None, "sol_mint": None},
    # ── Expansion batch ───────────────────────────────────────────
    "TRX":    {"binance": "TRXUSDT",  "category": "crypto",
               "eth_contract": None,
               "bsc_contract": "0xce7de646e7208a4ef112cb6ed5038fa6cc6b12e3",
               "sol_mint":     None},
    "TON":    {"binance": "TONUSDT",  "category": "crypto",
               "eth_contract": None,
               "bsc_contract": "0x76a797a59ba2c17726896976b7b3747bfd1d220f",
               "sol_mint":     None},
    "ATOM":   {"binance": "ATOMUSDT", "category": "crypto",
               "eth_contract": None,
               "bsc_contract": "0x0eb3a705fc54725037cc9e008bdede697f62f335",
               "sol_mint":     None},
    "FIL":    {"binance": "FILUSDT",  "category": "crypto",
               "eth_contract": None,
               "bsc_contract": "0x0d8ce2a99bb6e3b7db580ed848240e4a0f9ae153",
               "sol_mint":     None},
    "ALGO":   {"binance": "ALGOUSDT", "category": "crypto",
               "eth_contract": None,
               "bsc_contract": "0x8da443ef8588f5ac809bc40b8e32b66d25af3a90",
               "sol_mint":     None},
    "ENA":    {"binance": "ENAUSDT",  "category": "crypto",
               "eth_contract": "0x57e114b691db790c35207b2e685d4a43181e6061",
               "bsc_contract": None, "sol_mint": None},
    "ZRO":    {"binance": "ZROUSDT",  "category": "crypto",
               "eth_contract": "0x6985884c4392d348587b19cb9eaaf157f13271cd",
               "bsc_contract": None, "sol_mint": None},
    "GALA":   {"binance": "GALAUSDT", "category": "crypto",
               "eth_contract": "0xd1d2eb1b1e90b638588728b4130137d262c87cae",
               "bsc_contract": "0x7ddee176f665cd201f93eede625770e2fd911990",
               "sol_mint":     None},
    "FLOKI":  {"binance": "FLOKIUSDT","category": "crypto",
               "eth_contract": "0xcf0c122c6b73ff809c693db761e7baebe62b6a2e",
               "bsc_contract": "0xfb5b838b6cfeedc2873ab27866079ac55363d37",
               "sol_mint":     None},
    "POL":    {"binance": "POLUSDT",  "category": "crypto",
               "eth_contract": "0x455e53cbb86018ac2b8092fdcd39d8444affc3a6",
               "bsc_contract": "0xcc42724c6683b7e57334c4e856f4c9965ed682bd",
               "sol_mint":     None},
    "W":      {"binance": "WUSDT",    "category": "crypto",
               "eth_contract": None, "bsc_contract": None,
               "sol_mint": "85VBFQZC9TZkfaptBWjvUw7YbZjy52A6mjtPGjstQAmQ"},
    "HYPE":   {"binance": "HYPEUSDT", "category": "crypto",
               "eth_contract": None, "bsc_contract": None, "sol_mint": None},
    "SEI":    {"binance": "SEIUSDT",  "category": "crypto",
               "eth_contract": None, "bsc_contract": None, "sol_mint": None},
    "STX":    {"binance": "STXUSDT",  "category": "crypto",
               "eth_contract": "0x718e417a4e92ffe68bbbfaf8e96c4d99d5e4b0d4",
               "bsc_contract": None, "sol_mint": None},
    "ICP":    {"binance": "ICPUSDT",  "category": "crypto",
               "eth_contract": None, "bsc_contract": None, "sol_mint": None},
    "VET":    {"binance": "VETUSDT",  "category": "crypto",
               "eth_contract": None,
               "bsc_contract": "0x6fdcdfef7c496407ccb0a8c9a3b8f5a7c6c4a40",
               "sol_mint":     None},
    "KAS":    {"binance": "KASUSDT",  "category": "crypto",
               "eth_contract": None, "bsc_contract": None, "sol_mint": None},
    # ── Mid Cap Alts ──────────────────────────────────────────────
    "ONDO":   {"binance": "ONDOUSDT", "category": "crypto",
               "eth_contract": "0xfaba6f8e4a5e8ab82f62fe7c39859fa577269be3",
               "bsc_contract": None, "sol_mint": None},
    "RNDR":   {"binance": "RNDRUSDT", "category": "crypto",
               "eth_contract": "0x6de037ef9ad2725eb40118bb1702ebb27e4aeb24",
               "bsc_contract": None,
               "sol_mint": "rndrizKT3MK1iimdxRdWabcF7Zg7AR5T4nud4EkHBof"},
    "FET":    {"binance": "FETUSDT",  "category": "crypto",
               "eth_contract": "0xaea46a60368a7bd060eec7df8cba43b7ef41ad85",
               "bsc_contract": None, "sol_mint": None},
    "JASMY":  {"binance": "JASMYUSDT","category": "crypto",
               "eth_contract": "0x7420b4b9a0110cdc71fb720908340c03f9bc03ec",
               "bsc_contract": None, "sol_mint": None},
    "ID":     {"binance": "IDUSDT",   "category": "crypto",
               "eth_contract": None, "bsc_contract": None, "sol_mint": None},
    "GRT":    {"binance": "GRTUSDT",  "category": "crypto",
               "eth_contract": "0xc944e90c64b2c07662a292be6244bdf05cda44a7",
               "bsc_contract": None, "sol_mint": None},
    "OCEAN":  {"binance": "OCEANUSDT","category": "crypto",
               "eth_contract": "0x967da4048cd07ab37855c090aaf366e4ce1b9f48",
               "bsc_contract": None, "sol_mint": None},
    "MANTA":  {"binance": "MANTAUSDT","category": "crypto",
               "eth_contract": "0x95cef13441be50d20ca4558cc0a27b601ac544e5",
               "bsc_contract": None, "sol_mint": None},
    "JTO":    {"binance": "JTOUSDT",  "category": "crypto",
               "eth_contract": None, "bsc_contract": None,
               "sol_mint": "jtojtomepa8b81kAjrCF7ymxfieF1JBiDLEKpzBNyGN"},
    "PYTH":   {"binance": "PYTHUSDT", "category": "crypto",
               "eth_contract": None, "bsc_contract": None,
               "sol_mint": "HZ1JovNiVvGqv6W4YmJ5KNcBQN2sBJUkb2dBq8yU8yM"},
    "STRK":   {"binance": "STRKUSDT", "category": "crypto",
               "eth_contract": "0xca14007eff0db1f8135f4c25b34de49ab0d42766",
               "bsc_contract": None, "sol_mint": None},
    "DYM":    {"binance": "DYMUSDT",  "category": "crypto",
               "eth_contract": None, "bsc_contract": None, "sol_mint": None},
    "MEME":   {"binance": "MEMEUSDT", "category": "crypto",
               "eth_contract": "0xb131f4a55907b10d1f0a50d8ab8fa09ec342cd74",
               "bsc_contract": None, "sol_mint": None},
    "ALT":    {"binance": "ALTUSDT",  "category": "crypto",
               "eth_contract": None, "bsc_contract": None, "sol_mint": None},
    "PIXEL":  {"binance": "PIXELUSDT","category": "crypto",
               "eth_contract": None, "bsc_contract": None, "sol_mint": None},
    "PORTAL": {"binance": "PORTALUSDT","category": "crypto",
               "eth_contract": "0x1bbe973bef3a977fc51cbed703e8ffdefe001fed",
               "bsc_contract": None, "sol_mint": None},
    "ETHFI":  {"binance": "ETHFIUSDT","category": "crypto",
               "eth_contract": "0xfe0c30065b384f05761f15d0cc899d4f9f9cc0eb",
               "bsc_contract": None, "sol_mint": None},
    "REZ":    {"binance": "REZUSDT",  "category": "crypto",
               "eth_contract": "0x3b50805453023a91a8bf641e279401a0b23fa6f9",
               "bsc_contract": None, "sol_mint": None},
    "NOT":    {"binance": "NOTUSDT",  "category": "crypto",
               "eth_contract": None, "bsc_contract": None, "sol_mint": None},
    "IO":     {"binance": "IOUSDT",   "category": "crypto",
               "eth_contract": None, "bsc_contract": None, "sol_mint": None},
    "ZK":     {"binance": "ZKUSDT",   "category": "crypto",
               "eth_contract": "0x5a7d6b2f92c77fad6ccabd7ee0624e64907eaf3e",
               "bsc_contract": None, "sol_mint": None},
    "LISTA":  {"binance": "LISTAUSDT","category": "crypto",
               "eth_contract": None,
               "bsc_contract": "0xfceB31A79F71AC9CBDCF853519c1b12D379EdC46",
               "sol_mint": None},
    "G":      {"binance": "GUSDT",    "category": "crypto",
               "eth_contract": None, "bsc_contract": None, "sol_mint": None},
    "NEIRO":  {"binance": "NEIROUSDT","category": "crypto",
               "eth_contract": "0x812ba41e071c7b7fa095a4e7ced526a78b5b575c",
               "bsc_contract": None, "sol_mint": None},
    "CATI":   {"binance": "CATIUSDT", "category": "crypto",
               "eth_contract": None, "bsc_contract": None, "sol_mint": None},
    "HMSTR":  {"binance": "HMSTRUSDT","category": "crypto",
               "eth_contract": None, "bsc_contract": None, "sol_mint": None},
    "EIGEN":  {"binance": "EIGENUSDT","category": "crypto",
               "eth_contract": "0xec53bf9167f50cdeb3ae105f56099aaab9061f83",
               "bsc_contract": None, "sol_mint": None},
    "DOGS":   {"binance": "DOGSUSDT", "category": "crypto",
               "eth_contract": None, "bsc_contract": None, "sol_mint": None},
    # ── Solana Ecosystem ─────────────────────────────────────────
    "POPCAT": {"binance": "POPCATUSDT","category": "crypto",
               "eth_contract": None, "bsc_contract": None,
               "sol_mint": "7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr"},
    "MEW":    {"binance": "MEWUSDT",  "category": "crypto",
               "eth_contract": None, "bsc_contract": None,
               "sol_mint": "MEW1gQWJ3nEXg2qgERiKu7FAFj79PHvQVREQUzScPP5"},
    "BOME":   {"binance": "BOMEUSDT", "category": "crypto",
               "eth_contract": None, "bsc_contract": None,
               "sol_mint": "ukHH6c7mMyiWCf1b9pnWe25TSpkDDt3H5pQZgZ74J82"},
    "SLERF":  {"binance": "SLERFUSDT","category": "crypto",
               "eth_contract": None, "bsc_contract": None,
               "sol_mint": "7BgBvyjrZX1YKz4oh9mjb8ZScatkkwb8DzFx7LoiVkM3"},
    "WEN":    {"binance": "WENUSDT",  "category": "crypto",
               "eth_contract": None, "bsc_contract": None,
               "sol_mint": "WENWENvqqNya429ubCdR81ZmD69brwQaaBYY6p3LCpk"},
    "TRUMP":  {"binance": "TRUMPUSDT","category": "crypto",
               "eth_contract": None, "bsc_contract": None,
               "sol_mint": "6p6xgHyF7AeE6TZkSmFsko444wqoP15icUSqi2jfGiPN"},
    "PENGU":  {"binance": "PENGUUSDT","category": "crypto",
               "eth_contract": None, "bsc_contract": None,
               "sol_mint": "2zMMhcVQEXDtdE6vsFS7S7D5oUodfJHE8vd1gnBouauv"},
    # ── BSC / BNB Chain ecosystem ─────────────────────────────────
    "BAKE":   {"binance": "BAKEUSDT", "category": "crypto",
               "eth_contract": None,
               "bsc_contract": "0xe02df9e3e622debdd69fb838bb799e3f168902c5",
               "sol_mint": None},
    "BTTC":   {"binance": "BTTCUSDT", "category": "crypto",
               "eth_contract": None,
               "bsc_contract": "0x352cb5e19b12fc216548a2677bd0fce83bae434b",
               "sol_mint": None},
    # ── Layer 2 / Infra ──────────────────────────────────────────
    "METIS":  {"binance": "METISUSDT","category": "crypto",
               "eth_contract": "0x9e32b13ce7f2e80a01932b42553652e053d6ed8e",
               "bsc_contract": None, "sol_mint": None},
    "BLUR":   {"binance": "BLURUSDT", "category": "crypto",
               "eth_contract": "0x5283d291dbcf85356a21ba090e6db59121208b44",
               "bsc_contract": None, "sol_mint": None},
    "CFX":    {"binance": "CFXUSDT",  "category": "crypto",
               "eth_contract": None,
               "bsc_contract": "0x045c4324039dA91c52C55DF5D785385Aab073DcF",
               "sol_mint": None},
    "WAXP":   {"binance": "WAXPUSDT", "category": "crypto",
               "eth_contract": "0x39bb259f66e1c59d5abef88375979b4d20d98022",
               "bsc_contract": None, "sol_mint": None},
    # ── Expanded batch 2 (verified Gate.io pairs) ────────────────
    "HBAR":   {"binance": "HBARUSDT",  "category": "crypto",
               "eth_contract": None,
               "bsc_contract": "0x9f9013f86af5def07b2f9b4a62e6c45a76e40fa4",
               "sol_mint": None},
    "DYDX":   {"binance": "DYDXUSDT",  "category": "crypto",
               "eth_contract": "0x92d6c1e31e14520e676a687f0a93788b716beff5",
               "bsc_contract": None, "sol_mint": None},
    "AR":     {"binance": "ARUSDT",    "category": "crypto",
               "eth_contract": None, "bsc_contract": None, "sol_mint": None},
    "ROSE":   {"binance": "ROSEUSDT",  "category": "crypto",
               "eth_contract": None, "bsc_contract": None, "sol_mint": None},
    "KAVA":   {"binance": "KAVAUSDT",  "category": "crypto",
               "eth_contract": None, "bsc_contract": None, "sol_mint": None},
    "CHZ":    {"binance": "CHZUSDT",   "category": "crypto",
               "eth_contract": "0x3506424f91fd33084466f402d5d97f05f8e3b4af",
               "bsc_contract": None, "sol_mint": None},
    "SNX":    {"binance": "SNXUSDT",   "category": "crypto",
               "eth_contract": "0xc011a73ee8576fb46f5e1c5751ca3b9fe0af2a6f",
               "bsc_contract": None, "sol_mint": None},
    "COMP":   {"binance": "COMPUSDT",  "category": "crypto",
               "eth_contract": "0xc00e94cb662c3520282e6f5717214004a7f26888",
               "bsc_contract": None, "sol_mint": None},
    "YFI":    {"binance": "YFIUSDT",   "category": "crypto",
               "eth_contract": "0x0bc529c00c6401aef6d220be8c6ea1667f6ad93e",
               "bsc_contract": None, "sol_mint": None},
    "SUSHI":  {"binance": "SUSHIUSDT", "category": "crypto",
               "eth_contract": "0x6b3595068778dd592e39a122f4f5a5cf09c90fe2",
               "bsc_contract": None, "sol_mint": None},
    "1INCH":  {"binance": "1INCHUSDT", "category": "crypto",
               "eth_contract": "0x111111111117dc0aa78b770fa6a738034120c302",
               "bsc_contract": None, "sol_mint": None},
    "RUNE":   {"binance": "RUNEUSDT",  "category": "crypto",
               "eth_contract": None, "bsc_contract": None, "sol_mint": None},
    "AGIX":   {"binance": "AGIXUSDT",  "category": "crypto",
               "eth_contract": "0x5b7533812759b45c2b44c19e320ba2cd2681b542",
               "bsc_contract": None, "sol_mint": None},
    "MINA":   {"binance": "MINAUSDT",  "category": "crypto",
               "eth_contract": None, "bsc_contract": None, "sol_mint": None},
    "WOO":    {"binance": "WOOUSDT",   "category": "crypto",
               "eth_contract": "0x4691937a7508860f876c9c0a2a617e7d9e945d4b",
               "bsc_contract": "0x4691937a7508860f876c9c0a2a617e7d9e945d4b",
               "sol_mint": None},
    "LRC":    {"binance": "LRCUSDT",   "category": "crypto",
               "eth_contract": "0xbbbbca6a901c926f240b89eacb641d8aec7aeafd",
               "bsc_contract": None, "sol_mint": None},
    "BAT":    {"binance": "BATUSDT",   "category": "crypto",
               "eth_contract": "0x0d8775f648430679a709e98d2b0cb6250d2887ef",
               "bsc_contract": None, "sol_mint": None},
    "FLOW":   {"binance": "FLOWUSDT",  "category": "crypto",
               "eth_contract": None, "bsc_contract": None, "sol_mint": None},
    "AUDIO":  {"binance": "AUDIOUSDT", "category": "crypto",
               "eth_contract": "0x18aaa7115705e8be94bffebde57af9bfc265b998",
               "bsc_contract": None, "sol_mint": None},
    "GMT":    {"binance": "GMTUSDT",   "category": "crypto",
               "eth_contract": None, "bsc_contract": None,
               "sol_mint": "7i5KKsX2weiTkry7jA4ZwSuXGhs5eJBEjY8vVxR4pfRx"},
    "ORDI":   {"binance": "ORDIUSDT",  "category": "crypto",
               "eth_contract": None, "bsc_contract": None, "sol_mint": None},
    "TRB":    {"binance": "TRBUSDT",   "category": "crypto",
               "eth_contract": "0x88df592f8eb5d7bd38bfef7deb0fbc02cf3778a0",
               "bsc_contract": None, "sol_mint": None},
    "MAGIC":  {"binance": "MAGICUSDT", "category": "crypto",
               "eth_contract": "0xb0c7a3ba49c7a6eaba6cd4a96c55a1391070ac9a",
               "bsc_contract": None, "sol_mint": None},
    "GMX":    {"binance": "GMXUSDT",   "category": "crypto",
               "eth_contract": None, "bsc_contract": None, "sol_mint": None},
    "TURBO":  {"binance": "TURBOUSDT", "category": "crypto",
               "eth_contract": "0xa35923162c49cf95e6bf26623385eb431ad920d3",
               "bsc_contract": None, "sol_mint": None},
    "MOODENG":{"binance": "MOODENGUSDT","category": "crypto",
               "eth_contract": None, "bsc_contract": None,
               "sol_mint": "ED5nyyWEzpPPiWimP8vYm7sD7TD3LAt3Q3gRTWHzc8yy"},
    "PNUT":   {"binance": "PNUTUSDT",  "category": "crypto",
               "eth_contract": None, "bsc_contract": None,
               "sol_mint": "2qEHjDLDLbuBgRYvsxhc5D6uDWAivNFZGan56P1tpump"},
    "VIRTUAL":{"binance": "VIRTUALUSDT","category": "crypto",
               "eth_contract": None, "bsc_contract": None, "sol_mint": None},
    "BERA":   {"binance": "BERAUSDT",  "category": "crypto",
               "eth_contract": None, "bsc_contract": None, "sol_mint": None},
    "SONIC":  {"binance": "SONICUSDT", "category": "crypto",
               "eth_contract": None, "bsc_contract": None, "sol_mint": None},
    "MOVE":   {"binance": "MOVEUSDT",  "category": "crypto",
               "eth_contract": None, "bsc_contract": None, "sol_mint": None},
    "USUAL":  {"binance": "USUALUSDT", "category": "crypto",
               "eth_contract": "0xc4441c2be5d8fa8126822b9929ca0b81ea0de38e",
               "bsc_contract": None, "sol_mint": None},
    "LAYER":  {"binance": "LAYERUSDT", "category": "crypto",
               "eth_contract": None, "bsc_contract": None, "sol_mint": None},
    "IP":     {"binance": "IPUSDT",    "category": "crypto",
               "eth_contract": None, "bsc_contract": None, "sol_mint": None},
    "GOAT":   {"binance": "GOATUSDT",  "category": "crypto",
               "eth_contract": None, "bsc_contract": None,
               "sol_mint": "CzLSujWBLFsSjncfkh59rUFqvafWcY5tzedWJSuypump"},
    "BRETT":  {"binance": "BRETTUSDT", "category": "crypto",
               "eth_contract": None, "bsc_contract": None, "sol_mint": None},
    "MOG":    {"binance": "MOGUSDT",   "category": "crypto",
               "eth_contract": "0xaaeE1A9723aaDB7afA2810263653A34bA2C21C7a",
               "bsc_contract": None, "sol_mint": None},
    # ── Expanded universe: mid-cap liquid futures (Binance USDT-perp) ──
    "HBAR":   {"binance": "HBARUSDT",  "category": "crypto",
               "eth_contract": None, "bsc_contract": None, "sol_mint": None},
    "VET":    {"binance": "VETUSDT",   "category": "crypto",
               "eth_contract": None,
               "bsc_contract": "0x6fdcdfef7c496407ccb0a8c9a3b651dce72b6ae1",
               "sol_mint": None},
    "ICP":    {"binance": "ICPUSDT",   "category": "crypto",
               "eth_contract": None, "bsc_contract": None, "sol_mint": None},
    "BCH":    {"binance": "BCHUSDT",   "category": "crypto",
               "eth_contract": None, "bsc_contract": None, "sol_mint": None},
    "EOS":    {"binance": "EOSUSDT",   "category": "crypto",
               "eth_contract": "0x86fa049857e0209aa7d9e616f7eb3b3b78ecfdb0",
               "bsc_contract": None, "sol_mint": None},
    "RENDER": {"binance": "RENDERUSDT","category": "crypto",
               "eth_contract": "0x6de037ef9ad2725eb40118bb1702ebb27e4aeb24",
               "bsc_contract": None, "sol_mint": None},
    "PYTH":   {"binance": "PYTHUSDT",  "category": "crypto",
               "eth_contract": None, "bsc_contract": None,
               "sol_mint": "HZ1JovNiVvGqswkVljg88fkLFwLGbkXa9KBmCKBeDBaj"},
    "W":      {"binance": "WUSDT",     "category": "crypto",
               "eth_contract": "0xb0ffa8000886e57f86dd5264b9582b2cad0b2228",
               "bsc_contract": None, "sol_mint": None},
    "SEI":    {"binance": "SEIUSDT",   "category": "crypto",
               "eth_contract": None, "bsc_contract": None, "sol_mint": None},
    "ZK":     {"binance": "ZKUSDT",    "category": "crypto",
               "eth_contract": "0x5a7d6b2f92c77fad6ccabd7ee0624e64907eaf3e",
               "bsc_contract": None, "sol_mint": None},
    "STRK":   {"binance": "STRKUSDT",  "category": "crypto",
               "eth_contract": "0xca14007eff0db1f8135f4c25b34de49ab0d42766",
               "bsc_contract": None, "sol_mint": None},
    "EIGEN":  {"binance": "EIGENUSDT", "category": "crypto",
               "eth_contract": "0xec53bf9167f50cdeb3ae105f56099aaab9061f83",
               "bsc_contract": None, "sol_mint": None},
    "IO":     {"binance": "IOUSDT",    "category": "crypto",
               "eth_contract": None, "bsc_contract": None, "sol_mint": None},
    "NOT":    {"binance": "NOTUSDT",   "category": "crypto",
               "eth_contract": None, "bsc_contract": None, "sol_mint": None},
    "HYPE":   {"binance": "HYPEUSDT",  "category": "crypto",
               "eth_contract": None, "bsc_contract": None, "sol_mint": None},
    "TRUMP":  {"binance": "TRUMPUSDT", "category": "crypto",
               "eth_contract": "0x576e2bed8f7b46d34016198b52ddf2c2b634e77e",
               "bsc_contract": None, "sol_mint": None},
    "POPCAT": {"binance": "POPCATUSDT","category": "crypto",
               "eth_contract": None, "bsc_contract": None,
               "sol_mint": "7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr"},
    "MEW":    {"binance": "MEWUSDT",   "category": "crypto",
               "eth_contract": None, "bsc_contract": None,
               "sol_mint": "MEW1gQWJ3nEXg2qgERiKu7FAFj79PHvQVREQUzScPP5"},
    "NEIRO":  {"binance": "NEIROUSDT", "category": "crypto",
               "eth_contract": "0x812ba41e071c7b7fa095a0f044f61ab17fa67eff",
               "bsc_contract": None, "sol_mint": None},
    "DOGS":   {"binance": "DOGSUSDT",  "category": "crypto",
               "eth_contract": None, "bsc_contract": None, "sol_mint": None},
    "ROSE":   {"binance": "ROSEUSDT",  "category": "crypto",
               "eth_contract": "0x26b9a637a7f8a7bbce11a5c6e16f0987f89a9ed5",
               "bsc_contract": None, "sol_mint": None},
    "CHZ":    {"binance": "CHZUSDT",   "category": "crypto",
               "eth_contract": "0x3506424f91fd33084466f402d5d97f05f8e3b4af",
               "bsc_contract": None, "sol_mint": None},
    "RSR":    {"binance": "RSRUSDT",   "category": "crypto",
               "eth_contract": "0x320623b8e4ff03373931769a31fc52a4e78b5d70",
               "bsc_contract": None, "sol_mint": None},
    "KAVA":   {"binance": "KAVAUSDT",  "category": "crypto",
               "eth_contract": None, "bsc_contract": None, "sol_mint": None},
    "PORTAL": {"binance": "PORTALUSDT","category": "crypto",
               "eth_contract": None, "bsc_contract": None, "sol_mint": None},
    "PIXEL":  {"binance": "PIXELUSDT", "category": "crypto",
               "eth_contract": None, "bsc_contract": None, "sol_mint": None},
    "BB":     {"binance": "BBUSDT",    "category": "crypto",
               "eth_contract": None, "bsc_contract": None, "sol_mint": None},
    "CATI":   {"binance": "CATIUSDT",  "category": "crypto",
               "eth_contract": None, "bsc_contract": None, "sol_mint": None},
    "HMSTR":  {"binance": "HMSTRUSDT", "category": "crypto",
               "eth_contract": None, "bsc_contract": None, "sol_mint": None},
    "LISTA":  {"binance": "LISTAUSDT", "category": "crypto",
               "eth_contract": None,
               "bsc_contract": "0xfceB31A79F71AC9b749A60CB3C02EFAC0BC4A44",
               "sol_mint": None},
    "ACT":    {"binance": "ACTUSDT",   "category": "crypto",
               "eth_contract": None, "bsc_contract": None, "sol_mint": None},
    "IOTX":   {"binance": "IOTXUSDT",  "category": "crypto",
               "eth_contract": "0x6fb3e0a217407efff7ca062d46c26e5d60a14d69",
               "bsc_contract": None, "sol_mint": None},
    "CTSI":   {"binance": "CTSIUSDT",  "category": "crypto",
               "eth_contract": "0x491604c0fdf08347dd1fa4ee062a822a5dd06b5d",
               "bsc_contract": None, "sol_mint": None},
}

# ══════════════════════════════════════════════════════════════════
# CEX WALLETS — ETHEREUM
# ══════════════════════════════════════════════════════════════════
ETH_CEX_WALLETS = {
    "0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be": "Binance",
    "0xd551234ae421e3bcba99a0da6d736074f22192ff": "Binance",
    "0x564286362092d8e7936f0549571a803b203aaced": "Binance",
    "0xf977814e90da44bfa03b6295a0616a897441acec": "Binance",
    "0x85b931a32a0725be14285b66f1a22178c672d69b": "Binance",
    "0xbe0eb53f46cd790cd13851d5eff43d12404d33e8": "Binance Cold",
    "0x71660c4005ba85c37ccec55d0c4493e66fe775d3": "Coinbase",
    "0x503828976d22510aad0201ac7ec88293211d23da": "Coinbase",
    "0xa090e606e30bd747d4e6245a1517ebe430f0057e": "Coinbase",
    "0x2910543af39aba0cd09dbb2d50200b3e800a63d2": "Kraken",
    "0x0a869d79a7052c7f1b55a8ebabbea3420f0d1e13": "Kraken",
    "0x6cc5f688a315f3dc28a7781717a9a798a59fda7b": "OKX",
    "0x98ec059dc3adfbdd63429454aeb0c990fba4a128": "OKX",
    "0xf89d7b9c864f589bbf53a82105107622b35eaa40": "Bybit",
    "0x1db92e2eebc8e0c075a02bea49a2935bcd2dfcf4": "Bybit",
    "0x742d35cc6634c0532925a3b844bc454e4438f44e": "Bitfinex",
    "0xab5c66752a9e8167967685f1450532fb96d5d24f": "HTX",
    "0xd6216fc19db775df9774a6e33526131da7d19a2c": "KuCoin",
    "0x0d0707963952f2fba59dd06f2b425ace40b492fe": "Gate.io",
    "0x4b16c5de96eb2117bbe5fd171e4d203624b014aa": "MEXC",
    "0xd24400ae8bfebb18ca49be86258a3c749cf46853": "Gemini",
}

# ══════════════════════════════════════════════════════════════════
# CEX WALLETS — BSC
# ══════════════════════════════════════════════════════════════════
BSC_CEX_WALLETS = {
    "0x8894e0a0c962cb723c1976a4421c95949be2d4e3": "Binance",
    "0xf977814e90da44bfa03b6295a0616a897441acec": "Binance",
    "0x0681d8db095565fe8a346fa0277bffde9c0edbbf": "Binance",
    "0x29baf5d9c1cd32cc28b0b74a5072e4a0de82a913": "Binance Hot",
    "0x3c783c21a0383057d128bae431894a5c19f9cf06": "Binance Hot 2",
    "0xe2fc31f816a9b94326492132018c3aecc4a93ae1": "Binance Cold",
    "0x1fbe2acee135d991592f167ac371f3dd893a508b": "Coinbase BSC",
    "0xa180fe01b906a1be37be6c534a3300785b20d947": "Kraken BSC",
    "0x5a52e96bacdabb82fd05763e25335261b270efcb": "Bybit BSC",
    "0xf60c2ea62edbfe808163751dd0d8693dcb30019c": "OKX BSC",
    "0x69a52b4f05bcf9e0bce5d2b4e3e07ff0dd96b71d": "KuCoin BSC",
    "0xd3f4804f9a9dfb35b9ee2e84f95dbacc63e2c66e": "Gate.io BSC",
    "0x4b1a99467a284cc690e3237bc69105956816f762": "MEXC BSC",
    "0x1976e8f24e81b4e0e4d2f2eba684ec0e15584a24": "HTX BSC",
}

# ══════════════════════════════════════════════════════════════════
# CEX WALLETS — SOLANA
# ══════════════════════════════════════════════════════════════════
SOL_CEX_WALLETS = {
    "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM": "Binance",
    "AC5RDfQFmDS1deWZos921JfqscXdByf8BKHs5ACWjtW2": "Binance",
    "5tzFkiKscXHK5uyoRZtIyR7eXmRMoVmrXqJyXBY6iBa": "Binance",
    "H8sMJSCQxfKiFTCfDR3DUMLPwcRbM61LGFJ8N4dK3WjS": "Coinbase",
    "GJRs4FwHtemZ5ZE9x3FNvJ8TMwitKTh21yxdRPqn7npE": "Coinbase",
    "5VCwKtCXgCJ6kit5FybXjvriW3xELsFDhYrPSqtJNmcD": "OKX",
    "FWznbcNXWQuHTawe9RxvQ2LdCENssh12dsznf4RiouN5": "Kraken",
    "2ojv9BAiHUrvsm9gxike3nKnt4G4wajBBwEhMZdmP7Gg": "Bybit",
    "1db92e2eebc8e0c075a02bea49a2935bcd2dfcf4":     "Bybit 2",
    "BmFdpraQhkiDQE6SnfG5omcA1VwzqfXrwtNYBwWTymy6": "HTX",
    "HVh6wHNBAsnt39EbMnNpTFPkPYzFLaG7bMBFvVnbcuuA": "KuCoin",
    "EeRFGvSKxf9yf7fqH7tnBfYJEYDhFkCaFjMkfXbJn5Hy": "Gate.io",
    "3HZaD2j7EKMCAgTDZWQxoHNMVCMBCCKXoRGPLFrBKqFC": "MEXC",
    "4CNBXS3hf3BP2aMXNMVkVpmFBWfxGRnWAoqDWfMEJcJE": "Bitfinex",
}

# ══════════════════════════════════════════════════════════════════
# DATABASE  (stored alongside this script, not in project root)
# ══════════════════════════════════════════════════════════════════
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "signals.db")

PAUSE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pause_until.txt")

def _seed_state_from_db():
    """On startup, restore signals_today and last_signal_time from the DB
    so a bot restart cannot reset or bypass the daily cap / gap limits."""
    global signals_today, last_signal_time, today_date
    today_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        with db_conn() as conn:
            cur = conn.cursor()
            # Count today's signals (fired_at format: '2026-05-08 08:57 UTC')
            cur.execute(
                "SELECT COUNT(*) FROM autonomous_signals WHERE fired_at LIKE ?",
                (today_date + "%",)
            )
            signals_today = cur.fetchone()[0]
            # Find most recent signal time across all days
            cur.execute(
                "SELECT fired_at FROM autonomous_signals ORDER BY id DESC LIMIT 1"
            )
            row = cur.fetchone()
            if row and row[0]:
                try:
                    dt = datetime.strptime(row[0].strip(), "%Y-%m-%d %H:%M UTC")
                    dt = dt.replace(tzinfo=timezone.utc)
                    last_signal_time = dt.timestamp()
                except Exception:
                    last_signal_time = 0
        print(f"[State] Seeded from DB: signals_today={signals_today}, "
              f"last_signal={datetime.fromtimestamp(last_signal_time, tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC') if last_signal_time else 'never'}")
    except Exception as e:
        print(f"[State] Could not seed state from DB: {e}")

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS autonomous_signals (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol          TEXT,
            category        TEXT,
            direction       TEXT,
            timeframe       TEXT,
            entry           REAL,
            sl              REAL,
            tp1             REAL,
            tp2             REAL,
            internal_score  INTEGER,
            gemini_score    INTEGER,
            signal_grade    TEXT,
            confluences     TEXT,
            onchain_eth     TEXT,
            onchain_bsc     TEXT,
            onchain_sol     TEXT,
            onchain_combined TEXT,
            funding_rate    REAL,
            oi_change_pct   REAL,
            cvd_bias        TEXT,
            gemini_summary  TEXT,
            fired_at        TEXT,
            outcome         TEXT DEFAULT 'OPEN',
            outcome_price   REAL,
            outcome_time    TEXT,
            pnl_pct         REAL,
            tg_message_id   INTEGER,
            tp1_hit         INTEGER DEFAULT 0,
            warned_at       TEXT
        )
    """)
    # Migrations: add columns that may be missing in older databases
    for col_def in [
        "ALTER TABLE autonomous_signals ADD COLUMN tg_message_id INTEGER",
        "ALTER TABLE autonomous_signals ADD COLUMN tp1_hit INTEGER DEFAULT 0",
        "ALTER TABLE autonomous_signals ADD COLUMN warned_at TEXT",
    ]:
        try:
            c.execute(col_def)
            conn.commit()
        except sqlite3.OperationalError:
            pass  # Column already exists
    conn.commit()
    conn.close()

def db_conn():
    return sqlite3.connect(DB_PATH)


def get_btc_regime() -> str:
    """Return the BTC macro regime: 'uptrend', 'downtrend', or 'ranging'.

    Uses 4H only — no daily candle. Three complementary lenses:
      1. EMA trend (4H)           — directional structure
      2. ADX on 4H                — trend strength vs chop
      3. Donchian breakout (4H)   — is price sustaining a breakout?
      4. DFT spectral (4H closes) — fraction of power in low frequencies

    ranging  → ADX < 20  AND  Donchian=inside  AND  DFT trend-pct < 0.40
    uptrend  → 4H EMA uptrend
    downtrend→ 4H EMA downtrend
    (No 'neutral' state — avoids blocking signals in transitional markets)
    """
    global _btc_1h_cache
    try:
        candles_4h = fetch_klines("BTCUSDT", "4h", 80)
        candles_1h = fetch_klines("BTCUSDT", "1h", 60)
        _btc_1h_cache = candles_1h   # refresh cache for correlation checks

        h4_trend = detect_trend(candles_4h) if len(candles_4h) >= 50 else "uptrend"

        closes_4h = [c["close"] for c in candles_4h]

        # ADX on 4H
        adx_val, _, _ = calc_adx(candles_4h, period=14)
        adx_trending  = adx_val is not None and adx_val >= 22
        adx_ranging   = adx_val is not None and adx_val < 20

        # Donchian on 4H
        don_state, don_bars = detect_donchian(candles_4h, period=20)
        don_breaking = don_state != "inside"

        # DFT spectral (use 30 most recent 4H closes — fast enough)
        dft_pct      = calc_dft_trend_pct(closes_4h[-30:], num_low_freq=3)
        dft_trending = dft_pct >= 0.50
        dft_ranging  = dft_pct <  0.40

        adx_str = f"{adx_val:.1f}" if adx_val else "n/a"
        print(f"[BTC Regime] 4H={h4_trend} | ADX={adx_str} | "
              f"Donchian={don_state} | DFT={dft_pct:.2f}")

        # ── Regime classification ───────────────────────────────────
        # RANGING: multiple signals agree there is no directional bias
        if adx_ranging and don_state == "inside" and dft_ranging:
            regime = "ranging"

        # DOWNTREND: 4H EMA confirms down
        elif h4_trend == "downtrend":
            regime = "downtrend"

        # UPTREND: 4H EMA confirms up
        elif h4_trend == "uptrend":
            regime = "uptrend"

        # Transitional (neither clearly up nor down) — treat as uptrend to allow signals
        else:
            regime = "uptrend"

        print(f"[BTC Regime] → {regime.upper()} "
              f"(adx_trend={adx_trending}, don_break={don_breaking}, dft_trend={dft_trending})")
        return regime
    except Exception as e:
        print(f"[BTC Regime] {e}")
        return "uptrend"  # permissive fallback — don't block signals on fetch errors


def has_open_signal(symbol: str) -> bool:
    """Return True if an OPEN signal already exists for this symbol.

    Prevents the scanner from re-firing a signal on restart when
    a previous trade is still live in the database.
    """
    try:
        with db_conn() as conn:
            row = conn.execute(
                "SELECT id FROM autonomous_signals WHERE symbol=? AND outcome='OPEN' LIMIT 1",
                (symbol,)
            ).fetchone()
            return row is not None
    except Exception:
        return False


# ══════════════════════════════════════════════════════════════════
# STATE
# ══════════════════════════════════════════════════════════════════
signals_today        = 0
last_signal_time     = 0
today_date           = datetime.now(timezone.utc).strftime("%Y-%m-%d")
last_daily_stats_day = ""   # tracks which UTC date we last auto-posted stats
price_cache          = {}
tg_offset            = 0
_prev_btc_regime     = ""   # last known regime — used to detect transitions

# ══════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════
def fmt_usd(v):
    if abs(v) >= 1e9: return f"${v/1e9:.2f}B"
    if abs(v) >= 1e6: return f"${v/1e6:.2f}M"
    if abs(v) >= 1e3: return f"${v/1e3:.1f}K"
    return f"${v:.4f}"

def now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

def pct(a, b):
    return round((b - a) / a * 100, 2) if a else 0

def sig_round(x, sig=6):
    """Round to N significant figures — handles tiny prices like PEPE/SHIB correctly.
    Unlike round(x, 6) which gives 6 decimal places, this gives 6 significant digits.
    e.g. 0.000004273 → 0.00000427300 (not 0.000004 like round(x,6) would give)."""
    if not x or x == 0:
        return x
    from math import log10, floor
    d = int(floor(log10(abs(x))))
    return round(x, sig - 1 - d)

def reset_daily_if_needed():
    global signals_today, today_date
    d = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if d != today_date:
        signals_today = 0
        today_date = d

# ══════════════════════════════════════════════════════════════════
# TELEGRAM
# ══════════════════════════════════════════════════════════════════
def send_tg(msg, chat_id=None, reply_to_message_id=None):
    cid = chat_id or TELEGRAM_CHAT_ID
    payload = {"chat_id": cid, "text": msg,
                "parse_mode": "HTML", "disable_web_page_preview": True}
    if reply_to_message_id:
        payload["reply_to_message_id"] = reply_to_message_id
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json=payload,
            timeout=10
        )
        if r.ok:
            return r.json().get("result", {}).get("message_id")
        else:
            print(f"[TG] {r.status_code}: {r.text[:80]}")
    except Exception as e:
        print(f"[TG Error] {e}")
    return None

def edit_tg(message_id, new_text, chat_id=None):
    """Edit an existing Telegram message in-place (silent — no member notification)."""
    if not message_id:
        return False
    cid = chat_id or TELEGRAM_CHAT_ID
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageText",
            json={
                "chat_id": cid,
                "message_id": message_id,
                "text": new_text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=10,
        )
        if r.ok:
            return True
        err = r.json().get("description", r.text[:80])
        if "message is not modified" not in err.lower():
            print(f"[TG Edit] {r.status_code}: {err}")
    except Exception as e:
        print(f"[TG Edit Error] {e}")
    return False


def _build_signal_edit_tp1(sig_id, symbol, dire, entry, sl, tp1, tp2,
                             signal_grade, tp1_price, tp1_pnl):
    """Build the replacement message text when TP1 has just been hit."""
    sl_pct  = round(abs(entry - sl)  / entry * 100, 1)
    tp2_pct = round(abs(tp2  - entry) / entry * 100, 1)
    # For SHORTs: SL was above entry (+), TP2 is below entry (-)
    sl_sign  = "+" if dire == "SHORT" else "-"
    tp2_sign = "-" if dire == "SHORT" else "+"
    return (
        f"📡 <b>AUTO SIGNAL #{sig_id} — TP1 HIT ✅</b>\n"
        f"{'─'*32}\n"
        f"🪙 <b>{symbol} — {dire}</b>  |  Grade: <b>{signal_grade}</b>\n"
        f"{'─'*32}\n"
        f"✅ <b>TP1 reached at {tp1_price}  ({tp1_pnl:+.2f}%)</b>\n"
        f"{'─'*32}\n"
        f"📍 Entry:       <b>{entry}</b>\n"
        f"🎯 TP2 (next):  <b>{tp2}</b>  ({tp2_sign}{tp2_pct}%)\n"
        f"🛡️ SL → Breakeven: <b>{entry}</b>  (was {sl}  {sl_sign}{sl_pct}%)\n"
        f"🔒 Stop automatically moved to entry — trade is now risk-free\n"
        f"{'─'*32}\n"
        f"⏰ Updated: {now_utc()}"
    )


def _build_signal_edit_final(sig_id, symbol, dire, entry, outcome,
                               exit_price, final_pnl, signal_grade):
    """Build the replacement message text for a closed signal (TP2 / SL / EXPIRY / MANUAL)."""
    if "WIN_TP2" in outcome:
        outcome_line = "🏆 WIN — TP2 hit ⭐⭐"
        emoji = "✅"
    elif "WIN_TP1" in outcome:
        outcome_line = "✅ WIN — TP1 hit ⭐"
        emoji = "✅"
    elif outcome == "EXPIRED":
        outcome_line = "⏳ EXPIRED — time limit reached"
        emoji = "⏳"
    elif outcome == "MANUAL_CLOSE":
        pnl_sign = final_pnl or 0
        outcome_line = f"🔴 MANUALLY CLOSED — market exit  {'📉' if pnl_sign < 0 else '📈'}"
        emoji = "🔴"
    else:
        outcome_line = "❌ LOSS — stopped out"
        emoji = "❌"
    pnl_str = f"{final_pnl:+.2f}%" if final_pnl is not None else "n/a"
    exit_str = str(exit_price) if exit_price else "n/a"
    return (
        f"{emoji} <b>AUTO SIGNAL #{sig_id} — CLOSED</b>\n"
        f"{'─'*32}\n"
        f"🪙 <b>{symbol} — {dire}</b>  |  Grade: <b>{signal_grade}</b>\n"
        f"{'─'*32}\n"
        f"{outcome_line}\n"
        f"📍 Entry: <b>{entry}</b> → Exit: <b>{exit_str}</b>\n"
        f"💰 P&amp;L: <b>{pnl_str}</b>\n"
        f"{'─'*32}\n"
        f"⏰ Closed: {now_utc()}"
    )


def force_close_signal(sig_id, reason_note=""):
    """Manually close an open signal at the current live price.
    Returns a status string for the command reply."""
    try:
        with db_conn() as conn:
            row = conn.execute(
                """SELECT id, symbol, direction, entry, tp1, tp1_hit,
                          tg_message_id, signal_grade
                   FROM autonomous_signals
                   WHERE id=? AND outcome='OPEN'""",
                (sig_id,)
            ).fetchone()
    except Exception as e:
        return f"❌ DB error: {e}"

    if not row:
        return f"⚠️ Signal #{sig_id} not found or already closed."

    _id, symbol, dire, entry, tp1, tp1_hit, tg_msg_id, sg = row
    token_data = WATCHLIST.get(symbol, {})
    binance_sym = token_data.get("binance", f"{symbol}USDT")
    close_price = fetch_current_price(binance_sym)

    if not close_price:
        return f"⚠️ Could not fetch live price for {symbol} — try again."

    # P&L: if TP1 was already banked, floor loss at 0 (breakeven SL)
    if dire == "LONG":
        raw_pnl = pct(entry, close_price)
    else:
        raw_pnl = pct(close_price, entry)

    if tp1_hit and raw_pnl < 0:
        raw_pnl = 0.0   # SL moved to breakeven — can't lose

    close_time = now_utc()
    try:
        with db_conn() as conn:
            conn.execute(
                """UPDATE autonomous_signals
                   SET outcome='MANUAL_CLOSE', outcome_price=?,
                       outcome_time=?, pnl_pct=?
                   WHERE id=?""",
                (close_price, close_time, round(raw_pnl, 2), sig_id)
            )
            conn.commit()
    except Exception as e:
        return f"❌ DB write error: {e}"

    # Edit the original group message
    if tg_msg_id:
        closed_text = _build_signal_edit_final(
            sig_id, symbol, dire, entry,
            "MANUAL_CLOSE", close_price, raw_pnl, sg or "?"
        )
        try:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageText",
                json={"chat_id": TELEGRAM_CHAT_ID, "message_id": tg_msg_id,
                      "text": closed_text, "parse_mode": "HTML"},
                timeout=10
            )
        except Exception:
            pass

    pnl_tag = f"{raw_pnl:+.2f}%"
    tp1_note = "  (TP1 already banked — no loss)" if (tp1_hit and raw_pnl == 0.0) else ""
    return (
        f"🔴 <b>#{sig_id} {symbol} {dire} — FORCE CLOSED</b>\n"
        f"Entry: {entry}  →  Exit: {close_price}\n"
        f"P&amp;L: <b>{pnl_tag}</b>{tp1_note}"
        + (f"\n📝 {reason_note}" if reason_note else "")
    )

def poll_tg_commands():
    """Poll for commands using the Alpha Bot's dedicated token to avoid conflicting with NDF Bot."""
    global tg_offset, signals_today, last_signal_time
    if not CMD_BOT_TOKEN:
        return   # no dedicated command token — skip polling
    try:
        r = requests.get(
            f"https://api.telegram.org/bot{CMD_BOT_TOKEN}/getUpdates",
            params={"offset": tg_offset, "timeout": 5},
            timeout=10
        )
        for u in r.json().get("result", []):
            tg_offset = u["update_id"] + 1
            msg  = u.get("message", {})
            text = msg.get("text", "").strip()
            cid  = msg.get("chat", {}).get("id")
            cmd  = text.lower().split()[0] if text else ""

            if cmd in ("/start", "/help"):
                send_tg(
                    "👋 <b>Alpha Auto Signals Bot</b>\n\n"
                    "I scan 132 crypto tokens and post up to 5 high-conviction "
                    "trading signals per day — automatically.\n\n"
                    "<b>Commands:</b>\n"
                    "/stats — signal performance record\n"
                    "/daily_recap — today's full performance recap\n"
                    "/open_signals — all currently open signals with live P&amp;L\n"
                    "/close &lt;id&gt; — force-close a signal at live price\n"
                    "/close all — force-close ALL open signals at live price\n"
                    "/pause [hours] — pause new signals (default 24h, e.g. /pause 12)\n"
                    "/resume — lift an active pause early\n"
                    "/test — send a test message to the signal channel\n"
                    "/debug — live snapshot of BTC regime, gap timer, daily cap &amp; every active gate\n"
                    "/jumpstart — clear gap timer, lift pause, resync counters &amp; expire stale signals\n"
                    "/chatid — show this chat's ID\n\n"
                    f"ℹ️ <b>This chat ID:</b> <code>{cid}</code>\n"
                    "To receive signals here, set <code>AUTO_SIGNAL_CHAT_ID</code> "
                    f"to <code>{cid}</code> in Replit Secrets.",
                    chat_id=cid
                )
            elif cmd in ("/signal_stats", "/stats"):
                send_tg(build_stats_report(), chat_id=cid)
            elif cmd in ("/daily_recap", "/recap"):
                today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                send_tg(
                    build_stats_report(
                        header=f"📅 <b>Daily Alpha Bot Recap — {today_str}</b>"
                    ),
                    chat_id=cid
                )
            elif cmd == "/open_signals":
                send_tg(build_open_signals_report(), chat_id=cid)
            elif cmd == "/chatid":
                send_tg(
                    f"ℹ️ <b>Chat ID for this conversation:</b>\n<code>{cid}</code>\n\n"
                    "Set <code>AUTO_SIGNAL_CHAT_ID</code> to this value in Replit Secrets "
                    "so Auto Signals broadcast here.",
                    chat_id=cid
                )
            elif cmd.startswith("/pause"):
                # /pause        → default 24h
                # /pause 12     → 12 hours
                parts = text.split()
                try:
                    hours = float(parts[1]) if len(parts) > 1 else 24.0
                except ValueError:
                    hours = 24.0
                hours = max(0.5, min(hours, 168))  # clamp 0.5h – 7 days
                pause_until_ts = time.time() + hours * 3600
                with open(PAUSE_FILE, "w") as _pf:
                    _pf.write(str(pause_until_ts))
                until_str = datetime.fromtimestamp(pause_until_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
                send_tg(
                    f"⏸ <b>Signals paused for {hours:.0f}h</b>\n"
                    f"Resumes automatically at <b>{until_str}</b>",
                    chat_id=cid
                )
            elif cmd == "/resume":
                if os.path.exists(PAUSE_FILE):
                    os.remove(PAUSE_FILE)
                    send_tg("▶️ <b>Pause lifted.</b> Scanner will resume on next cycle.", chat_id=cid)
                else:
                    send_tg("ℹ️ Bot is not paused.", chat_id=cid)

            elif cmd == "/test":
                # ── Direct Telegram connectivity test — bypasses all scanning logic ──
                token_set  = "✅ Set" if TELEGRAM_BOT_TOKEN else "❌ MISSING"
                chat_set   = "✅ Set" if TELEGRAM_CHAT_ID   else "❌ MISSING"
                # Masked preview: show first 6 and last 4 chars only
                token_hint = (TELEGRAM_BOT_TOKEN[:6] + "…" + TELEGRAM_BOT_TOKEN[-4:]
                              if len(TELEGRAM_BOT_TOKEN) > 10 else "(short/empty)")
                chat_hint  = str(TELEGRAM_CHAT_ID) if TELEGRAM_CHAT_ID else "(empty)"

                # Try sending to the configured channel, not just back to the commander
                channel_ok = False
                if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
                    try:
                        tr = requests.post(
                            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                            json={
                                "chat_id": TELEGRAM_CHAT_ID,
                                "text": (
                                    "🧪 <b>Connectivity test</b>\n\n"
                                    "If you can read this, the bot's token and chat ID "
                                    "are correctly configured and messages are reaching "
                                    "this channel. ✅"
                                ),
                                "parse_mode": "HTML",
                            },
                            timeout=10
                        )
                        channel_ok = tr.ok
                    except Exception as _te:
                        channel_ok = False

                channel_str = "✅ Test message sent to channel!" if channel_ok else "❌ Failed to send to channel"

                send_tg(
                    "🧪 <b>Telegram connectivity test</b>\n\n"
                    f"<b>BOT_TOKEN:</b> {token_set} (<code>{token_hint}</code>)\n"
                    f"<b>CHAT_ID:</b> {chat_set} (<code>{chat_hint}</code>)\n\n"
                    f"<b>Channel delivery:</b> {channel_str}\n\n"
                    "If channel delivery failed, the bot token or chat ID is wrong — "
                    "check your Railway environment variables.",
                    chat_id=cid
                )

            elif cmd == "/debug":
                # ── Live snapshot of every gate that can block signals ──
                try:
                    regime = get_btc_regime()
                except Exception as _re:
                    regime = f"error ({_re})"

                # Pause status
                paused_str = "No"
                if os.path.exists(PAUSE_FILE):
                    try:
                        _pu = float(open(PAUSE_FILE).read().strip())
                        _rem = _pu - time.time()
                        if _rem > 0:
                            paused_str = f"Yes — {_rem/3600:.1f}h remaining"
                        else:
                            paused_str = "Pause file present but already expired"
                    except Exception:
                        paused_str = "Pause file unreadable"

                # Gap timer
                gap_rem = MIN_SIGNAL_GAP_SECS - (time.time() - last_signal_time)
                if last_signal_time == 0:
                    gap_str = "Never fired (no gap active)"
                elif gap_rem > 0:
                    gap_str = f"⏱ {gap_rem/60:.0f} min remaining before next signal allowed"
                else:
                    gap_str = "✅ Gap cleared — ready to fire"

                # Last signal timestamp
                if last_signal_time:
                    ago_min = (time.time() - last_signal_time) / 60
                    last_str = (f"{ago_min:.0f} min ago"
                                if ago_min < 120 else f"{ago_min/60:.1f}h ago")
                else:
                    last_str = "Never (since restart)"

                # Open signals by direction
                try:
                    with db_conn() as _dc:
                        _rows = _dc.execute(
                            "SELECT direction, COUNT(*) FROM autonomous_signals "
                            "WHERE outcome='OPEN' GROUP BY direction"
                        ).fetchall()
                    open_by_dir = {r[0]: r[1] for r in _rows}
                    open_str = (
                        f"LONG: {open_by_dir.get('LONG',0)} | "
                        f"SHORT: {open_by_dir.get('SHORT',0)}"
                    )
                    # Correlation cap warning
                    cap_warn = ""
                    for _d, _n in open_by_dir.items():
                        if _n >= 2:
                            cap_warn += f"\n⚠️ Correlation cap active for {_d}s ({_n} open)"
                except Exception as _de:
                    open_str = f"DB error: {_de}"
                    cap_warn = ""

                send_tg(
                    "🔍 <b>Bot Debug Snapshot</b>\n\n"
                    f"📡 <b>BTC Regime:</b> {regime.upper()}\n"
                    f"📊 <b>Signals today:</b> {signals_today}/{MAX_SIGNALS_PER_DAY}\n"
                    f"⏰ <b>Last signal:</b> {last_str}\n"
                    f"🚦 <b>Gap timer:</b> {gap_str}\n"
                    f"⏸ <b>Paused:</b> {paused_str}\n"
                    f"📂 <b>Open positions:</b> {open_str}{cap_warn}\n\n"
                    "Use /jumpstart to clear the gap timer + pause and force "
                    "the next scan to run immediately.",
                    chat_id=cid
                )

            elif cmd == "/jumpstart":
                # ── Emergency restart: clear every non-DB gate blocking signals ──
                actions = []

                # 1. Lift pause
                if os.path.exists(PAUSE_FILE):
                    try:
                        os.remove(PAUSE_FILE)
                        actions.append("✅ Pause cleared")
                    except Exception as _pe:
                        actions.append(f"⚠️ Could not remove pause file: {_pe}")
                else:
                    actions.append("ℹ️ No active pause")

                # 2. Reset the 3-hour gap timer
                old_gap_rem = max(0, MIN_SIGNAL_GAP_SECS - (time.time() - last_signal_time))
                last_signal_time = 0
                if old_gap_rem > 0:
                    actions.append(f"✅ Gap timer reset ({old_gap_rem/60:.0f} min was remaining)")
                else:
                    actions.append("ℹ️ Gap timer was already clear")

                # 3. Recount signals_today from DB (don't fake-zero it —
                #    that would bypass the daily cap entirely)
                try:
                    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                    with db_conn() as _jc:
                        _cnt = _jc.execute(
                            "SELECT COUNT(*) FROM autonomous_signals "
                            "WHERE DATE(signal_time) = ?", (today_str,)
                        ).fetchone()[0]
                    old_today = signals_today
                    signals_today = _cnt
                    actions.append(
                        f"✅ signals_today resynced from DB: {old_today} → {signals_today}"
                        f" (cap: {MAX_SIGNALS_PER_DAY})"
                    )
                except Exception as _ce:
                    actions.append(f"⚠️ DB resync failed: {_ce}")

                # 4. Mark overdue OPEN signals as EXPIRED so they don't
                #    clog the duplicate-guard or correlation cap
                try:
                    cutoff_ts = time.time() - MAX_SIGNAL_AGE_HRS * 3600
                    cutoff_dt = datetime.fromtimestamp(cutoff_ts, tz=timezone.utc).isoformat()
                    with db_conn() as _ec:
                        _exp = _ec.execute(
                            "SELECT COUNT(*) FROM autonomous_signals "
                            "WHERE outcome='OPEN' AND signal_time < ?", (cutoff_dt,)
                        ).fetchone()[0]
                        if _exp:
                            _ec.execute(
                                "UPDATE autonomous_signals SET outcome='EXPIRED', "
                                "outcome_time=? WHERE outcome='OPEN' AND signal_time < ?",
                                (datetime.now(timezone.utc).isoformat(), cutoff_dt)
                            )
                    if _exp:
                        actions.append(f"✅ {_exp} stale OPEN signal(s) expired (>{MAX_SIGNAL_AGE_HRS}h old)")
                    else:
                        actions.append("ℹ️ No stale OPEN signals found")
                except Exception as _xe:
                    actions.append(f"⚠️ Expiry sweep failed: {_xe}")

                action_lines = "\n".join(actions)
                send_tg(
                    "🚀 <b>Jumpstart complete!</b>\n\n"
                    f"{action_lines}\n\n"
                    f"The next scan cycle runs in ~{SCAN_INTERVAL//60} min. "
                    "If signals are still blocked, run /debug to see what gate is active.",
                    chat_id=cid
                )
                print(f"[Jumpstart] Triggered via Telegram by chat {cid}: {actions}")

            elif cmd == "/close":
                parts = text.split()
                arg   = parts[1].lower() if len(parts) > 1 else ""

                if not arg:
                    # No argument — list open signals so user knows the IDs
                    try:
                        with db_conn() as conn:
                            opens = conn.execute(
                                "SELECT id, symbol, direction FROM autonomous_signals "
                                "WHERE outcome='OPEN' ORDER BY id"
                            ).fetchall()
                    except Exception as e:
                        opens = []
                    if opens:
                        lines = "\n".join(f"  /close {r[0]} — #{r[0]} {r[1]} {r[2]}" for r in opens)
                        send_tg(
                            f"📋 <b>Open signals</b> ({len(opens)} total):\n{lines}\n\n"
                            f"Use <code>/close all</code> to close all at once.",
                            chat_id=cid
                        )
                    else:
                        send_tg("ℹ️ No open signals to close.", chat_id=cid)

                elif arg == "all":
                    # Close every open signal
                    try:
                        with db_conn() as conn:
                            opens = conn.execute(
                                "SELECT id FROM autonomous_signals WHERE outcome='OPEN' ORDER BY id"
                            ).fetchall()
                    except Exception:
                        opens = []

                    if not opens:
                        send_tg("ℹ️ No open signals to close.", chat_id=cid)
                    else:
                        results = []
                        for (sid,) in opens:
                            res = force_close_signal(sid, reason_note="Market exit — closed by operator")
                            results.append(res)
                            time.sleep(0.3)   # avoid rate limit

                        summary = f"🔴 <b>Force-closed {len(opens)} signal(s)</b>\n\n" + "\n\n".join(results)
                        # Telegram has a 4096 char limit — truncate if needed
                        if len(summary) > 4000:
                            summary = summary[:3990] + "\n…(truncated)"
                        send_tg(summary, chat_id=cid)
                        # Also announce to the main group
                        send_tg(
                            f"🔴 <b>Manual close — {len(opens)} positions exited</b>\n"
                            f"Market conditions triggered operator exit. "
                            f"Bot will resume scanning next cycle.",
                            chat_id=TELEGRAM_CHAT_ID
                        )

                else:
                    # Specific signal ID
                    try:
                        sid = int(arg.lstrip("#"))
                    except ValueError:
                        send_tg(f"⚠️ Invalid ID <code>{arg}</code>. Use /close &lt;number&gt; or /close all", chat_id=cid)
                    else:
                        res = force_close_signal(sid, reason_note="Market exit — closed by operator")
                        send_tg(res, chat_id=cid)
                        # If it was a real close (not an error), also post to main group
                        if "FORCE CLOSED" in res:
                            send_tg(res, chat_id=TELEGRAM_CHAT_ID)

            elif text and not cmd.startswith("/"):
                pass   # ignore plain text messages silently
    except Exception as e:
        print(f"[TG Poll] {e}")

# ══════════════════════════════════════════════════════════════════
# GATE.IO  (replaces Binance for OHLCV — geo-block workaround)
# ══════════════════════════════════════════════════════════════════
def fetch_klines(binance_symbol, interval, limit=200):
    """Fetch candles from Gate.io using equivalent symbol and interval."""
    pair = binance_to_gateio(binance_symbol)
    gate_interval = GATEIO_INTERVAL_MAP.get(interval, interval)
    try:
        r = requests.get(
            "https://api.gateio.ws/api/v4/spot/candlesticks",
            params={"currency_pair": pair, "interval": gate_interval, "limit": limit},
            timeout=12
        )
        if r.status_code != 200:
            print(f"[Klines/Gate] {pair} {gate_interval}: HTTP {r.status_code}")
            return []
        # Gate.io candle format: [timestamp, volume, close, high, low, open, ...]
        candles = []
        for k in r.json():
            try:
                candles.append({
                    "timestamp":     int(k[0]) * 1000,
                    "open":          float(k[5]),
                    "high":          float(k[3]),
                    "low":           float(k[4]),
                    "close":         float(k[2]),
                    "volume":        float(k[1]),
                    "taker_buy_vol": float(k[1]) * 0.5,  # approximation
                })
            except (IndexError, ValueError):
                continue
        return candles
    except Exception as e:
        print(f"[Klines/Gate] {binance_symbol}: {e}")
        return []

_gate_oi_cache: dict = {}  # contract → (oi_size, timestamp) for cross-scan OI delta

def _binance_to_gate_futures(binance_symbol: str) -> str:
    """VETUSDT → VET_USDT for Gate.io futures contract names."""
    if binance_symbol.endswith("USDT"):
        return binance_symbol[:-4] + "_USDT"
    return binance_symbol

def fetch_funding_rate(binance_symbol):
    """Funding rate from Gate.io futures (Binance fapi is geo-blocked on Replit)."""
    contract = _binance_to_gate_futures(binance_symbol)
    try:
        r = requests.get(
            "https://api.gateio.ws/api/v4/futures/usdt/tickers",
            params={"contract": contract},
            timeout=8
        )
        if r.status_code == 200 and r.json():
            return float(r.json()[0]["funding_rate"])
    except Exception as e:
        print(f"[Funding/Gate] {contract}: {e}")
    return None

def fetch_oi_change(binance_symbol):
    """OI from Gate.io futures; computes % change vs previous scan snapshot."""
    contract = _binance_to_gate_futures(binance_symbol)
    try:
        r = requests.get(
            "https://api.gateio.ws/api/v4/futures/usdt/tickers",
            params={"contract": contract},
            timeout=8
        )
        if r.status_code == 200 and r.json():
            oi_now = float(r.json()[0].get("total_size", 0) or 0)
            prev   = _gate_oi_cache.get(contract)
            _gate_oi_cache[contract] = (oi_now, time.time())
            if prev and prev[0] > 0 and oi_now > 0:
                return oi_now, (oi_now - prev[0]) / prev[0] * 100
            return oi_now, None
    except Exception as e:
        print(f"[OI/Gate] {contract}: {e}")
    return None, None

def fetch_v2_futures_metrics(binance_symbol, candles_1h):
    """Fetch the five v2 futures inputs from Binance public endpoints.
    Gate.io candles remain the OHLCV fallback when Binance is unavailable.
    """
    base = {"funding": None, "oi_change": None, "taker_ratio": None,
            "ls_ratio": None, "volume_24h": None}
    try:
        ticker = requests.get(
            "https://fapi.binance.com/fapi/v1/ticker/24hr",
            params={"symbol": binance_symbol}, timeout=5)
        if ticker.ok:
            data = ticker.json()
            base["volume_24h"] = float(data.get("quoteVolume", 0) or 0)
    except Exception:
        pass
    try:
        r = requests.get(
            "https://fapi.binance.com/fapi/v1/premiumIndex",
            params={"symbol": binance_symbol}, timeout=5)
        if r.ok:
            base["funding"] = float(r.json().get("lastFundingRate"))
    except Exception:
        pass
    try:
        r = requests.get(
            "https://fapi.binance.com/futures/data/openInterestHist",
            params={"symbol": binance_symbol, "period": "4h", "limit": 2},
            timeout=5)
        if r.ok and len(r.json()) >= 2:
            hist = r.json()
            new = float(hist[-1].get("sumOpenInterestValue") or hist[-1].get("sumOpenInterest"))
            old = float(hist[-2].get("sumOpenInterestValue") or hist[-2].get("sumOpenInterest"))
            if old:
                base["oi_change"] = (new - old) / old * 100
    except Exception:
        pass
    try:
        r = requests.get(
            "https://fapi.binance.com/futures/data/takerlongshortRatio",
            params={"symbol": binance_symbol, "period": "4h", "limit": 1},
            timeout=5)
        if r.ok and r.json():
            base["taker_ratio"] = float(r.json()[-1].get("buySellRatio"))
    except Exception:
        pass
    try:
        r = requests.get(
            "https://fapi.binance.com/futures/data/globalLongShortAccountRatio",
            params={"symbol": binance_symbol, "period": "4h", "limit": 1},
            timeout=5)
        if r.ok and r.json():
            base["ls_ratio"] = float(r.json()[-1].get("longShortRatio"))
    except Exception:
        pass

    # Public Binance futures may be geo-blocked; use Gate funding/OI where possible.
    if base["funding"] is None:
        base["funding"] = fetch_funding_rate(binance_symbol)
    if base["oi_change"] is None:
        _, base["oi_change"] = fetch_oi_change(binance_symbol)
    if base["volume_24h"] is None and candles_1h:
        base["volume_24h"] = sum(c["close"] * c["volume"] for c in candles_1h[-24:])
    return base

def fetch_current_price(binance_symbol):
    """Current price via Gate.io ticker (OKX as fallback)."""
    if binance_symbol in price_cache:
        p, ts = price_cache[binance_symbol]
        if time.time() - ts < 30:
            return p
    pair = binance_to_gateio(binance_symbol)
    # ── Primary: Gate.io ──────────────────────────────────────────
    try:
        r = requests.get(
            "https://api.gateio.ws/api/v4/spot/tickers",
            params={"currency_pair": pair},
            timeout=8
        )
        if r.status_code == 200:
            data = r.json()
            if data:
                price = float(data[0].get("last", 0))
                if price:
                    price_cache[binance_symbol] = (price, time.time())
                    return price
    except Exception as e:
        print(f"[Price/Gate] {binance_symbol}: {e}")
    # ── Fallback: OKX ─────────────────────────────────────────────
    try:
        base = binance_symbol.replace("USDT", "")
        r = requests.get(
            "https://www.okx.com/api/v5/market/ticker",
            params={"instId": f"{base}-USDT"},
            timeout=8
        )
        if r.status_code == 200:
            d = r.json().get("data", [])
            if d:
                price = float(d[0].get("last", 0))
                if price:
                    price_cache[binance_symbol] = (price, time.time())
                    return price
    except Exception as e:
        print(f"[Price/OKX] {binance_symbol}: {e}")
    return None


def fetch_candle_extremes(binance_symbol: str, since_ts: float, limit: int = 48) -> tuple:
    """
    Return (max_high, min_low) from 1H candles since `since_ts` (unix seconds).
    Used by grade_open_signals() so TP/SL touches between scan cycles are never missed.
    Falls back to OKX if Gate.io fails.
    Returns (None, None) if no data is available.
    """
    pair = binance_to_gateio(binance_symbol)
    since_ms = int(since_ts * 1000)

    # ── Gate.io ───────────────────────────────────────────────────
    try:
        r = requests.get(
            "https://api.gateio.ws/api/v4/spot/candlesticks",
            params={"currency_pair": pair, "interval": "1h",
                    "from": int(since_ts), "limit": limit},
            timeout=10
        )
        if r.status_code == 200:
            candles = r.json()  # [ts, vol, close, high, low, open, ...]
            if candles:
                highs = [float(c[3]) for c in candles]
                lows  = [float(c[4]) for c in candles]
                return max(highs), min(lows)
    except Exception as e:
        print(f"[Extremes/Gate] {binance_symbol}: {e}")

    # ── OKX fallback ──────────────────────────────────────────────
    try:
        base = binance_symbol.replace("USDT", "")
        r = requests.get(
            "https://www.okx.com/api/v5/market/history-candles",
            params={"instId": f"{base}-USDT", "bar": "1H",
                    "after": since_ms, "limit": limit},
            timeout=10
        )
        if r.status_code == 200:
            candles = r.json().get("data", [])  # [ts, o, h, l, c, ...]
            if candles:
                highs = [float(c[2]) for c in candles]
                lows  = [float(c[3]) for c in candles]
                return max(highs), min(lows)
    except Exception as e:
        print(f"[Extremes/OKX] {binance_symbol}: {e}")

    return None, None

# ══════════════════════════════════════════════════════════════════
# CVD  (Cumulative Volume Delta — taker_buy_vol approximation)
# ══════════════════════════════════════════════════════════════════
def calc_cvd(candles, lookback=20):
    if len(candles) < lookback:
        return "neutral"
    recent = candles[-lookback:]
    buy_vol  = sum(c["taker_buy_vol"] for c in recent)
    sell_vol = sum(c["volume"] - c["taker_buy_vol"] for c in recent)
    total    = buy_vol + sell_vol
    if total == 0:
        return "neutral"
    ratio = buy_vol / total
    if ratio > 0.55:
        return "bullish"
    if ratio < 0.45:
        return "bearish"
    return "neutral"

# ══════════════════════════════════════════════════════════════════
# INDICATORS
# ══════════════════════════════════════════════════════════════════
def ema(prices, period):
    if len(prices) < period:
        return None
    k   = 2 / (period + 1)
    val = sum(prices[:period]) / period
    for p in prices[period:]:
        val = p * k + val * (1 - k)
    return val

def calc_rsi(candles, period=14):
    if len(candles) < period + 1:
        return None
    closes = [c["close"] for c in candles]
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i-1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100
    rs = avg_gain / avg_loss
    return round(100 - 100 / (1 + rs), 2)

def calc_atr(candles, period=14):
    if len(candles) < period + 1:
        return None
    trs = []
    for i in range(1, len(candles)):
        h, l, pc = candles[i]["high"], candles[i]["low"], candles[i-1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return sum(trs[-period:]) / period

def detect_trend(candles):
    """Classify trend using EMA-20 / EMA-50 alignment + price position.

    e200 is intentionally NOT required — on 1H/4H timeframes 200 candles
    cover only ~8–33 days, so e200 lags far too much and causes nearly
    everything to read 'neutral' even during clear directional moves.

    Logic:
      uptrend   — e20 > e50  AND  price > e20  (momentum up, not a fakeout)
      downtrend — e20 < e50  AND  price < e20  (momentum down, confirmed)
      neutral   — mixed / transitioning
    """
    if len(candles) < 50:
        return "neutral"
    closes = [c["close"] for c in candles]
    e20 = ema(closes, 20)
    e50 = ema(closes, 50)
    if not (e20 and e50):
        return "neutral"
    price = closes[-1]
    if e20 > e50 and price > e20:
        return "uptrend"
    if e20 < e50 and price < e20:
        return "downtrend"
    return "neutral"

def detect_sr_levels(candles, lookback=50):
    if len(candles) < lookback:
        return [], []
    highs  = [c["high"]  for c in candles[-lookback:]]
    lows   = [c["low"]   for c in candles[-lookback:]]
    closes = [c["close"] for c in candles[-lookback:]]
    price  = closes[-1]
    atr    = calc_atr(candles[-lookback-15:]) or (price * 0.01)
    pivots_h = [highs[i] for i in range(1, len(highs)-1)
                if highs[i] >= highs[i-1] and highs[i] >= highs[i+1]]
    pivots_l = [lows[i]  for i in range(1, len(lows)-1)
                if lows[i] <= lows[i-1] and lows[i] <= lows[i+1]]
    res = [h for h in pivots_h if h > price and (h - price) < atr * 5]
    sup = [l for l in pivots_l if l < price and (price - l) < atr * 5]
    return sorted(sup, reverse=True)[:3], sorted(res)[:3]

def detect_fvg(candles):
    fvgs = []
    for i in range(2, len(candles)):
        prev2_h = candles[i-2]["high"]
        curr_l  = candles[i]["low"]
        if curr_l > prev2_h:
            fvgs.append({"type": "bullish", "top": curr_l, "bottom": prev2_h,
                          "idx": i})
        prev2_l = candles[i-2]["low"]
        curr_h  = candles[i]["high"]
        if curr_h < prev2_l:
            fvgs.append({"type": "bearish", "top": prev2_l, "bottom": curr_h,
                          "idx": i})
    return fvgs[-10:] if len(fvgs) > 10 else fvgs

def calc_vwap(candles, anchor_bars=None):
    """
    VWAP — Volume-Weighted Average Price, the primary institutional benchmark.
    anchor_bars: if set, computes Anchored VWAP over the last N bars (e.g. 24 = ~1 trading day on 1H).
    Returns (vwap_price, price_above_vwap, distance_pct) or (None, None, None).
    """
    window = candles[-anchor_bars:] if anchor_bars else candles
    if len(window) < 5:
        return None, None, None
    cum_tpv = 0.0
    cum_vol = 0.0
    for c in window:
        tp = (c["high"] + c["low"] + c["close"]) / 3
        vol = c.get("volume", 0)
        cum_tpv += tp * vol
        cum_vol += vol
    if cum_vol == 0:
        return None, None, None
    vwap = cum_tpv / cum_vol
    current = window[-1]["close"]
    above = current > vwap
    dist_pct = (current - vwap) / vwap * 100
    return round(vwap, 8), above, round(dist_pct, 2)


def calc_rsi_direction(candles, period=14):
    """
    Returns (rsi_rising, rsi_falling) — whether RSI momentum is building
    towards the bullish or bearish side from a meaningful price level.
    rsi_rising:  RSI is climbing and still below 55 (early bullish momentum)
    rsi_falling: RSI is declining and still above 45 (early bearish momentum)
    """
    if len(candles) < period + 5:
        return False, False
    rsi_now  = calc_rsi(candles)
    rsi_prev = calc_rsi(candles[:-2])          # 2 bars ago
    if rsi_now is None or rsi_prev is None:
        return False, False
    rsi_rising  = (rsi_now > rsi_prev and rsi_now < 55)
    rsi_falling = (rsi_now < rsi_prev and rsi_now > 45)
    return rsi_rising, rsi_falling


def detect_rsi_divergence(candles, lookback=30):
    """Detect bullish/bearish RSI divergence in recent candles."""
    if len(candles) < lookback + 20:
        return None
    recent = candles[-lookback:]
    # Bullish divergence: price lower low, RSI higher low
    lows = [i for i in range(1, len(recent)-1)
            if recent[i]["low"] <= recent[i-1]["low"] and recent[i]["low"] <= recent[i+1]["low"]]
    if len(lows) >= 2:
        i1, i2 = lows[-2], lows[-1]
        p1, p2 = recent[i1]["low"], recent[i2]["low"]
        r1 = calc_rsi(candles[-(lookback - i1 + 15):])
        r2 = calc_rsi(candles[-(lookback - i2 + 15):])
        if p2 < p1 and r1 and r2 and r2 > r1 + 3:
            return "bullish_divergence"
    # Bearish divergence: price higher high, RSI lower high
    highs = [i for i in range(1, len(recent)-1)
             if recent[i]["high"] >= recent[i-1]["high"] and recent[i]["high"] >= recent[i+1]["high"]]
    if len(highs) >= 2:
        i1, i2 = highs[-2], highs[-1]
        p1, p2 = recent[i1]["high"], recent[i2]["high"]
        r1 = calc_rsi(candles[-(lookback - i1 + 15):])
        r2 = calc_rsi(candles[-(lookback - i2 + 15):])
        if p2 > p1 and r1 and r2 and r2 < r1 - 3:
            return "bearish_divergence"
    return None


def detect_order_blocks(candles, lookback=30):
    obs = []
    for i in range(1, min(lookback, len(candles)-1)):
        c = candles[-(i+1)]
        n = candles[-i]
        if c["close"] < c["open"] and n["close"] > n["open"] and n["close"] > c["high"]:
            obs.append({"type": "bullish", "high": c["high"], "low": c["low"]})
        if c["close"] > c["open"] and n["close"] < n["open"] and n["close"] < c["low"]:
            obs.append({"type": "bearish", "high": c["high"], "low": c["low"]})
    return obs[:5]

# ══════════════════════════════════════════════════════════════════
# ADX  —  Average Directional Index (trend strength, not direction)
# ══════════════════════════════════════════════════════════════════
def calc_adx(candles, period=14):
    """Return (adx, plus_di, minus_di) or (None, None, None).
    ADX ≥ 25 → trending.  ADX < 20 → ranging/choppy."""
    if len(candles) < period * 2 + 5:
        return None, None, None
    plus_dm_list, minus_dm_list, tr_list = [], [], []
    for i in range(1, len(candles)):
        h_diff = candles[i]["high"]  - candles[i-1]["high"]
        l_diff = candles[i-1]["low"] - candles[i]["low"]
        plus_dm  = max(h_diff, 0) if h_diff > l_diff else 0
        minus_dm = max(l_diff, 0) if l_diff > h_diff else 0
        tr = max(
            candles[i]["high"] - candles[i]["low"],
            abs(candles[i]["high"] - candles[i-1]["close"]),
            abs(candles[i]["low"]  - candles[i-1]["close"])
        )
        plus_dm_list.append(plus_dm)
        minus_dm_list.append(minus_dm)
        tr_list.append(tr)
    def wilder(lst, p):
        s = sum(lst[:p])
        result = [s]
        for v in lst[p:]:
            s = s - s / p + v
            result.append(s)
        return result
    sm_tr  = wilder(tr_list,       period)
    sm_pdm = wilder(plus_dm_list,  period)
    sm_mdm = wilder(minus_dm_list, period)
    dx_vals, pdi_last, mdi_last = [], 0.0, 0.0
    for i in range(len(sm_tr)):
        if sm_tr[i] == 0:
            continue
        pdi = 100 * sm_pdm[i] / sm_tr[i]
        mdi = 100 * sm_mdm[i] / sm_tr[i]
        dsum = pdi + mdi
        if dsum == 0:
            continue
        dx_vals.append(100 * abs(pdi - mdi) / dsum)
        pdi_last, mdi_last = pdi, mdi
    if len(dx_vals) < period:
        return None, None, None
    adx = sum(dx_vals[-period:]) / period
    return round(adx, 1), round(pdi_last, 1), round(mdi_last, 1)


# ══════════════════════════════════════════════════════════════════
# DONCHIAN CHANNEL
# ══════════════════════════════════════════════════════════════════
def detect_donchian(candles, period=20):
    """Return (state, bars_outside) where state is:
    'upper_break'  – price above upper band for ≥2 bars (trending up)
    'lower_break'  – price below lower band for ≥2 bars (trending down)
    'inside'       – price inside channel (ranging / no breakout)"""
    if len(candles) < period + 5:
        return "inside", 0
    channel = candles[-(period + 5):-5]
    upper   = max(c["high"] for c in channel)
    lower   = min(c["low"]  for c in channel)
    recent  = candles[-5:]
    above   = sum(1 for c in recent if c["close"] > upper)
    below   = sum(1 for c in recent if c["close"] < lower)
    if above >= 2:
        return "upper_break", above
    if below >= 2:
        return "lower_break", below
    return "inside", 0


# ══════════════════════════════════════════════════════════════════
# DFT SPECTRAL  —  trend vs noise decomposition (pure Python)
# ══════════════════════════════════════════════════════════════════
def calc_dft_trend_pct(closes, num_low_freq=3):
    """Compute fraction of price-series power in low frequencies.
    High value (>0.55) = clean directional trend.
    Low value (<0.40)  = noisy/cycling/ranging price action."""
    n = len(closes)
    if n < 8:
        return 0.5
    mu = sum(closes) / n
    d  = [c - mu for c in closes]
    powers = []
    for k in range(1, n // 2):
        re = sum(d[j] * math.cos(2 * math.pi * k * j / n) for j in range(n))
        im = sum(d[j] * math.sin(2 * math.pi * k * j / n) for j in range(n))
        powers.append((k, re * re + im * im))
    if not powers:
        return 0.5
    total_p    = sum(p for _, p in powers)
    low_freq_p = sum(p for k, p in powers if k <= num_low_freq)
    return round(low_freq_p / total_p, 3) if total_p > 0 else 0.5


# ══════════════════════════════════════════════════════════════════
# MARKET STRUCTURE  —  HH/HL or LH/LL
# ══════════════════════════════════════════════════════════════════
def detect_market_structure(candles, lookback=60):
    """
    Return 'bullish' (HH + HL), 'bearish' (LH + LL), or 'neutral'.

    Uses the most recent 2 confirmed swing highs and 2 swing lows.
    Only the last sequential pair matters — old structure from 3+ swings
    ago does not override fresh price action.

    Lookback default is 60 bars (60h on 1H / 10 days on 4H) to capture
    medium-term structure without being too slow to flip on breaks.
    """
    lb = min(lookback, len(candles))
    if lb < 10:
        return "neutral"
    recent = candles[-lb:]
    swing_highs, swing_lows = [], []
    for i in range(2, len(recent) - 2):
        # Swing high: local max vs both neighbours (and prior bar for confirmation)
        if (recent[i]["high"] >= recent[i-1]["high"] and
                recent[i]["high"] >= recent[i+1]["high"] and
                recent[i]["high"] >= recent[i-2]["high"]):
            swing_highs.append(recent[i]["high"])
        # Swing low: local min vs both neighbours (and prior bar for confirmation)
        if (recent[i]["low"] <= recent[i-1]["low"] and
                recent[i]["low"] <= recent[i+1]["low"] and
                recent[i]["low"] <= recent[i-2]["low"]):
            swing_lows.append(recent[i]["low"])
    if len(swing_highs) < 2 or len(swing_lows) < 2:
        return "neutral"
    # Use only the last two swing points — fresh structure only
    hh = swing_highs[-1] > swing_highs[-2]   # last high > previous high
    hl = swing_lows[-1]  > swing_lows[-2]    # last low  > previous low
    lh = swing_highs[-1] < swing_highs[-2]   # last high < previous high
    ll = swing_lows[-1]  < swing_lows[-2]    # last low  < previous low
    if hh and hl:  return "bullish"
    if lh and ll:  return "bearish"
    return "neutral"


# ══════════════════════════════════════════════════════════════════
# OVEREXTENSION GATE  —  avoid buying tops or shorting bottoms
# ══════════════════════════════════════════════════════════════════
def is_overextended(rsi_1h, rsi_4h, direction):
    """Return True if the move is exhausted and the entry would chase it.
    LONG: blocked when RSI is overbought (buying near top).
    SHORT: blocked when RSI is oversold  (shorting near bottom).
    Thresholds are deliberately conservative so good pullback entries pass."""
    if direction == "LONG":
        if rsi_1h is not None and rsi_1h > 72:
            return True
        if rsi_4h is not None and rsi_4h > 70:
            return True
    else:  # SHORT
        if rsi_1h is not None and rsi_1h < 28:
            return True
        if rsi_4h is not None and rsi_4h < 30:
            return True
    return False


# ══════════════════════════════════════════════════════════════════
# PULLBACK ENTRY CHECK  —  retest of EMA/FVG/OB only, no chasing
# ══════════════════════════════════════════════════════════════════
def check_pullback_entry(candles_4h, candles_1h, direction, tolerance=0.022):
    """Return True if price is retesting a key level (not breaking out into air).
    Acceptable entries: within tolerance of EMA20/EMA50, inside FVG, at OB, near S/R."""
    if len(candles_1h) < 50 or len(candles_4h) < 50:
        return True   # insufficient data — don't block
    closes_1h = [c["close"] for c in candles_1h]
    closes_4h = [c["close"] for c in candles_4h]
    price     = closes_1h[-1]
    levels    = [l for l in [
        ema(closes_1h, 20), ema(closes_1h, 50),
        ema(closes_4h, 20), ema(closes_4h, 50),
    ] if l]
    near_ema  = any(abs(price - lvl) / lvl <= tolerance for lvl in levels)
    fvgs      = detect_fvg(candles_1h[-30:])
    in_fvg    = any(
        fvg.get("bottom", 0) <= price <= fvg.get("top", 0)
        for fvg in fvgs if isinstance(fvg, dict)
    )
    obs   = detect_order_blocks(candles_1h)
    at_ob = any(ob["low"] <= price <= ob["high"] for ob in obs)
    sups, ress = detect_sr_levels(candles_1h)
    near_sup = any(abs(price - s) / price < 0.025 for s in sups)
    near_res = any(abs(r - price) / price < 0.025 for r in ress)
    if direction == "LONG":
        return near_ema or in_fvg or at_ob or near_sup
    else:
        return near_ema or in_fvg or at_ob or near_res


# ══════════════════════════════════════════════════════════════════
# VOLUME THRESHOLD  —  minimum liquidity gate
# ══════════════════════════════════════════════════════════════════
def check_volume_threshold(candles, min_vol_usd=300_000):
    """Return True if the token's average 5-bar volume exceeds minimum.
    Prevents signals on illiquid or near-dead tokens."""
    if len(candles) < 5:
        return True
    avg_vol_usd = sum(c["volume"] * c["close"] for c in candles[-5:]) / 5
    return avg_vol_usd >= min_vol_usd


# ══════════════════════════════════════════════════════════════════
# VOLATILITY FILTER  —  skip news spikes and ultra-low-vol sessions
# ══════════════════════════════════════════════════════════════════
def check_volatility_ok(candles, period=14):
    """Return True if current ATR is within an acceptable range.
    Rejects: sudden 3× ATR spike (news/manipulation) or ATR < 0.1% (dead market)."""
    if len(candles) < period + 2:
        return True
    atr_now  = calc_atr(candles,          period)
    atr_prev = calc_atr(candles[:-1],     period)
    if atr_now is None or atr_prev is None:
        return True
    price = candles[-1]["close"]
    if price <= 0:
        return True
    atr_pct = atr_now / price
    if atr_pct < 0.001:          # < 0.1% ATR — completely dead/illiquid
        return False
    if atr_prev > 0 and atr_now > atr_prev * 3.0:  # sudden 3× ATR spike
        return False
    return True


# ══════════════════════════════════════════════════════════════════
# MANIPULATION DETECTOR  —  stop hunts, wash trading, fake breakouts
# ══════════════════════════════════════════════════════════════════
def detect_manipulation(candles, lookback=5):
    """Return list of detected manipulation flags in recent candles.
    Signals that fire on flagged candles are automatically down-scored."""
    flags = []
    if len(candles) < lookback + 5:
        return flags
    recent   = candles[-lookback:]
    avg_vol  = sum(c["volume"] for c in candles[-(lookback+15):-lookback]) / 15 \
               if len(candles) > lookback + 15 else 0
    for c in recent:
        body       = abs(c["close"] - c["open"]) or (c["high"] - c["low"]) * 0.01
        upper_wick = c["high"]  - max(c["close"], c["open"])
        lower_wick = min(c["close"], c["open"]) - c["low"]
        # Stop hunt: massive wick (>4× body) snapping back
        if lower_wick > body * 4 and c["close"] > c["open"]:
            flags.append("bullish_stop_hunt")
        if upper_wick > body * 4 and c["close"] < c["open"]:
            flags.append("bearish_stop_hunt")
        # Wash trading: huge volume spike with tiny price movement
        move = abs(c["close"] - c["open"]) / c["open"] if c["open"] > 0 else 0
        if avg_vol > 0 and c["volume"] > avg_vol * 3.5 and move < 0.003:
            flags.append("wash_trading_suspected")
    # Fake breakout: new high/low immediately reversed by close
    if len(candles) >= 10:
        lb_high = max(c["high"] for c in candles[-10:-2])
        lb_low  = min(c["low"]  for c in candles[-10:-2])
        if candles[-2]["high"] > lb_high and candles[-1]["close"] < lb_high:
            flags.append("bearish_fake_breakout")
        if candles[-2]["low"] < lb_low and candles[-1]["close"] > lb_low:
            flags.append("bullish_fake_breakout")
    return list(set(flags))


# ══════════════════════════════════════════════════════════════════
# ORDERBOOK IMBALANCE  (Gate.io REST — no key required)
# ══════════════════════════════════════════════════════════════════
def fetch_orderbook_imbalance(binance_symbol, direction, levels=20):
    """Check if the entry side has ≥1.5× depth of the opposing side.
    Returns True (favourable), False (unfavourable), or None (unavailable)."""
    pair = binance_to_gateio(binance_symbol)
    try:
        r = requests.get(
            "https://api.gateio.ws/api/v4/spot/order_book",
            params={"currency_pair": pair, "limit": levels},
            timeout=8
        )
        if r.status_code != 200:
            return None
        data = r.json()
        bids = [(float(p), float(s)) for p, s in data.get("bids", [])]
        asks = [(float(p), float(s)) for p, s in data.get("asks", [])]
        if not bids or not asks:
            return None
        bid_depth = sum(price * size for price, size in bids)
        ask_depth = sum(price * size for price, size in asks)
        if direction == "LONG":
            return bid_depth >= ask_depth * 1.2
        else:
            return ask_depth >= bid_depth * 1.2
    except Exception as e:
        print(f"[OrderBook] {binance_symbol}: {e}")
        return None


# ══════════════════════════════════════════════════════════════════
# BTC CORRELATION  —  Pearson correlation of price changes
# ══════════════════════════════════════════════════════════════════
def calc_btc_correlation(token_closes, btc_closes, period=20):
    """Pearson correlation between token and BTC 1H price changes.
    High positive correlation (> 0.85) = token just follows BTC (not independent).
    Returns float –1…1, or None if insufficient data."""
    n = min(len(token_closes), len(btc_closes))
    if n < period + 2:
        return None
    tok = token_closes[-period:]
    btc = btc_closes[-period:]
    tok_ret = [tok[i] / tok[i-1] - 1 for i in range(1, len(tok))]
    btc_ret = [btc[i] / btc[i-1] - 1 for i in range(1, len(btc))]
    n2 = min(len(tok_ret), len(btc_ret))
    if n2 < 5:
        return None
    tok_ret, btc_ret = tok_ret[-n2:], btc_ret[-n2:]
    mu_t = sum(tok_ret) / n2
    mu_b = sum(btc_ret) / n2
    cov   = sum((tok_ret[i] - mu_t) * (btc_ret[i] - mu_b) for i in range(n2))
    var_t = sum((v - mu_t) ** 2 for v in tok_ret)
    var_b = sum((v - mu_b) ** 2 for v in btc_ret)
    denom = (var_t * var_b) ** 0.5
    return round(cov / denom, 3) if denom > 0 else None


# BTC 1H candle cache — refreshed once per scan cycle
_btc_1h_cache: list = []


# ══════════════════════════════════════════════════════════════════
# CANDLESTICK PATTERNS
# ══════════════════════════════════════════════════════════════════
def detect_patterns(candles):
    patterns = []
    if len(candles) < 3:
        return patterns
    p1, p2, p3 = candles[-3], candles[-2], candles[-1]

    def body(c):    return abs(c["close"] - c["open"])
    def is_bull(c): return c["close"] > c["open"]
    def is_bear(c): return c["close"] < c["open"]

    # Bullish engulfing
    if is_bear(p2) and is_bull(p3) and p3["open"] < p2["close"] and p3["close"] > p2["open"]:
        patterns.append("bullish_engulfing")
    # Bearish engulfing
    if is_bull(p2) and is_bear(p3) and p3["open"] > p2["close"] and p3["close"] < p2["open"]:
        patterns.append("bearish_engulfing")
    # Morning star
    if is_bear(p1) and body(p2) < body(p1)*0.3 and is_bull(p3) and p3["close"] > (p1["open"]+p1["close"])/2:
        patterns.append("morning_star")
    # Evening star
    if is_bull(p1) and body(p2) < body(p1)*0.3 and is_bear(p3) and p3["close"] < (p1["open"]+p1["close"])/2:
        patterns.append("evening_star")
    # Three white soldiers
    if all(is_bull(c) for c in [p1, p2, p3]) and p2["open"] > p1["open"] and p3["open"] > p2["open"]:
        patterns.append("three_white_soldiers")
    # Three black crows
    if all(is_bear(c) for c in [p1, p2, p3]) and p2["open"] < p1["open"] and p3["open"] < p2["open"]:
        patterns.append("three_black_crows")
    # Hammer
    c = p3
    lower_wick = min(c["open"], c["close"]) - c["low"]
    upper_wick = c["high"] - max(c["open"], c["close"])
    if lower_wick > body(c) * 2 and upper_wick < body(c) * 0.5:
        patterns.append("hammer")
    # Shooting star
    if upper_wick > body(c) * 2 and lower_wick < body(c) * 0.5:
        patterns.append("shooting_star")
    # Abandoned baby
    if (is_bear(p1) and body(p2) < body(p1)*0.1 and is_bull(p3) and
            p2["high"] < p1["low"] and p2["high"] < p3["low"]):
        patterns.append("abandoned_baby_bull")
    if (is_bull(p1) and body(p2) < body(p1)*0.1 and is_bear(p3) and
            p2["low"] > p1["high"] and p2["low"] > p3["high"]):
        patterns.append("abandoned_baby_bear")

    return patterns

# ══════════════════════════════════════════════════════════════════
# V2 EXTREME FUTURES SIGNAL LOGIC
# ══════════════════════════════════════════════════════════════════
def analyse_v2_token(symbol, token_data):
    """New regime-free signal engine: at least 3 of 5 futures conditions."""
    bsym = token_data["binance"]
    candles_4h = fetch_klines(bsym, "4h", 100)
    candles_1h = fetch_klines(bsym, "1h", 100)
    if len(candles_4h) < 50 or len(candles_1h) < 50:
        return None

    closes = [c["close"] for c in candles_1h]
    current = closes[-1]
    rsi = calc_rsi(candles_1h)
    rsi_4h = calc_rsi(candles_4h)
    atr = calc_atr(candles_1h)
    if rsi is None or atr is None:
        return None

    metrics = fetch_v2_futures_metrics(bsym, candles_1h)
    volume_24h = metrics["volume_24h"] or 0
    if volume_24h <= MIN_24H_VOLUME_USD:
        return None

    funding = metrics["funding"]
    oi_change = metrics["oi_change"]
    taker_ratio = metrics["taker_ratio"]
    ls_ratio = metrics["ls_ratio"]
    long_conditions = [
        funding is not None and funding < -0.0005,
        oi_change is not None and oi_change > 10,
        rsi < 40,
        taker_ratio is not None and taker_ratio > 1.1,
        ls_ratio is not None and ls_ratio < 0.8,
    ]
    short_conditions = [
        funding is not None and funding > 0.0005,
        oi_change is not None and oi_change < -10,
        rsi > 60,
        taker_ratio is not None and taker_ratio < 0.9,
        ls_ratio is not None and ls_ratio > 1.2,
    ]
    long_count = sum(long_conditions)
    short_count = sum(short_conditions)
    if long_count < 3 and short_count < 3:
        return None
    if long_count == short_count:
        return None
    direction = "LONG" if long_count > short_count else "SHORT"
    conditions = long_conditions if direction == "LONG" else short_conditions
    count = sum(conditions)
    confidence = {3: 67, 4: 82, 5: 95}[count]

    fvgs = detect_fvg(candles_1h[-30:])
    in_fvg = any(f["bottom"] <= current <= f["top"] for f in fvgs if isinstance(f, dict))
    obs = detect_order_blocks(candles_1h)
    at_ob = any(o["low"] <= current <= o["high"] for o in obs)
    supports, resistances = detect_sr_levels(candles_1h)
    vwap, vwap_above, vwap_dist = calc_vwap(candles_1h)
    avwap, avwap_above, _ = calc_vwap(candles_1h, anchor_bars=24)
    risk = current * SL_PCT
    sl = current - risk if direction == "LONG" else current + risk
    tp1 = current + risk if direction == "LONG" else current - risk
    tp2 = current + risk * 2 if direction == "LONG" else current - risk * 2
    labels = ["Funding", "OI 4H", "RSI", "Taker Flow", "L/S Ratio"]
    confluences = [f"{labels[i]} condition confirmed" for i, ok in enumerate(conditions) if ok]
    confluences.append(f"24H futures volume ${volume_24h:,.0f}")

    return {
        "symbol": symbol, "category": token_data.get("category", "crypto"),
        "direction": direction, "timeframe": "4H",
        "entry": sig_round(current, 6), "sl": sig_round(sl, 6),
        "tp1": sig_round(tp1, 6), "tp2": sig_round(tp2, 6),
        "trend_4h": detect_trend(candles_4h), "trend_1h": detect_trend(candles_1h),
        "rsi": rsi, "rsi_4h": rsi_4h, "cvd_bias": "neutral",
        "vol_spike": False, "has_volume": True, "vol_ok": True,
        "near_support": any(abs(current-s)/current < .015 for s in supports),
        "near_resistance": any(abs(r-current)/current < .015 for r in resistances),
        "in_fvg": in_fvg, "at_ob": at_ob, "patterns": [],
        "rsi_rising": False, "rsi_falling": False, "rsi_div": None,
        "mkt_structure": detect_market_structure(candles_1h),
        "mkt_structure_4h": detect_market_structure(candles_4h, lookback=20),
        "adx": None, "is_pullback": in_fvg or at_ob, "manip_flags": [],
        "btc_corr": None, "confluences": confluences,
        "internal_score": confidence, "sl_pct": SL_PCT * 100,
        "price_below_4h_e20": False, "price_above_4h_e20_ext": False,
        "vwap": vwap, "vwap_above": vwap_above, "vwap_dist_pct": vwap_dist,
        "avwap_24h": avwap, "avwap_above": avwap_above,
        "funding_rate": funding, "oi_change_pct": oi_change,
        "taker_ratio": taker_ratio, "ls_ratio": ls_ratio,
        "volume_24h": volume_24h, "conditions_met": count,
        "signal_logic_v2": True,
    }

# ══════════════════════════════════════════════════════════════════
# LEGACY ANALYSE TOKEN (kept below for rollback/reference)
# ══════════════════════════════════════════════════════════════════
def analyse_token(symbol, token_data):
    bsym = token_data["binance"]

    candles_4h = fetch_klines(bsym, "4h", 200)
    time.sleep(0.3)
    candles_1h = fetch_klines(bsym, "1h", 200)
    time.sleep(0.3)

    if len(candles_4h) < 50 or len(candles_1h) < 50:
        return None

    trend_4h = detect_trend(candles_4h)
    trend_1h = detect_trend(candles_1h)

    closes_4h_raw  = [c["close"] for c in candles_4h]
    e20_4h_val     = ema(closes_4h_raw, 20)
    price_below_4h_e20    = bool(e20_4h_val and closes_4h_raw[-1] < e20_4h_val)
    price_above_4h_e20_ext = bool(e20_4h_val and closes_4h_raw[-1] > e20_4h_val * 1.08)

    rsi_4h   = calc_rsi(candles_4h)
    rsi      = calc_rsi(candles_1h)
    atr_1h   = calc_atr(candles_1h)
    cvd_bias = calc_cvd(candles_1h)

    if rsi is None or atr_1h is None:
        return None

    closes_1h = [c["close"] for c in candles_1h]
    current   = closes_1h[-1]

    # Volume spike + minimum volume gate
    avg_vol   = sum(c["volume"] for c in candles_1h[-20:-1]) / 19
    vol_spike = candles_1h[-1]["volume"] > avg_vol * 1.5
    has_volume = check_volume_threshold(candles_1h, min_vol_usd=200_000)

    # Volatility gate — reject news spikes and dead markets
    vol_ok = check_volatility_ok(candles_1h)

    # S/R
    supports, resistances = detect_sr_levels(candles_1h)
    near_support    = any(abs(current - s) / current < 0.015 for s in supports)
    near_resistance = any(abs(r - current) / current < 0.015 for r in resistances)

    # FVG
    fvgs   = detect_fvg(candles_1h[-30:])
    in_fvg = any(
        (fvg["bottom"] <= current <= fvg["top"])
        for fvg in fvgs
        if isinstance(fvg, dict) and "bottom" in fvg and "top" in fvg
    )

    # Order blocks
    obs   = detect_order_blocks(candles_1h)
    at_ob = any(ob["low"] <= current <= ob["high"] for ob in obs)

    # Patterns (1H)
    patterns = detect_patterns(candles_1h)

    # RSI direction (1H) — momentum confirmation (replaces MACD)
    rsi_rising, rsi_falling = calc_rsi_direction(candles_1h)

    # RSI divergence (1H)
    rsi_div = detect_rsi_divergence(candles_1h)

    # EMA200
    e200_1h    = ema(closes_1h, 200)
    above_e200 = current > e200_1h if e200_1h else None

    # Market structure (HH/HL or LH/LL) — 1H and 4H
    mkt_structure    = detect_market_structure(candles_1h)          # 60-bar default
    mkt_structure_4h = detect_market_structure(candles_4h, lookback=20)  # 20 × 4H ≈ 80h

    # ADX (token-level trend strength)
    adx_tok, pdi_tok, mdi_tok = calc_adx(candles_1h, period=14)
    adx_trending = adx_tok is not None and adx_tok >= 22
    adx_ranging  = adx_tok is not None and adx_tok < 18

    # Manipulation detection
    manip_flags = detect_manipulation(candles_1h, lookback=5)

    # BTC correlation — prefer tokens showing independence from BTC
    btc_corr = None
    if _btc_1h_cache and len(_btc_1h_cache) >= 22:
        btc_closes = [c["close"] for c in _btc_1h_cache]
        btc_corr   = calc_btc_correlation(closes_1h, btc_closes, period=20)

    # ── VWAP — primary institutional benchmark ───────────────────────
    # Session VWAP (full series) and 24-bar Anchored VWAP (~1 trading day)
    vwap_1h,  vwap_above,  vwap_dist_pct  = calc_vwap(candles_1h)
    avwap_1h, avwap_above, avwap_dist_pct = calc_vwap(candles_1h, anchor_bars=24)
    near_vwap = (vwap_1h is not None and vwap_dist_pct is not None
                 and abs(vwap_dist_pct) < 0.8)

    # ── Direction bias — institutional weighted vote ──────────────────
    # Primary drivers: trend + market structure + VWAP position
    # Secondary:  CVD, RSI divergence, candlestick patterns, EMA200
    # RSI extremes & MACD removed from vote — used in scoring only
    bull_score = bear_score = 0

    if trend_4h == "uptrend":   bull_score += 4
    if trend_4h == "downtrend": bear_score += 4
    if trend_1h == "uptrend":   bull_score += 3
    if trend_1h == "downtrend": bear_score += 3

    # Market structure (HH/HL = bullish, LH/LL = bearish)
    if mkt_structure == "bullish":  bull_score += 3
    if mkt_structure == "bearish":  bear_score += 3

    # VWAP position — institutional price benchmark
    if vwap_above is True:    bull_score += 2
    if vwap_above is False:   bear_score += 2
    if avwap_above is True:   bull_score += 1
    if avwap_above is False:  bear_score += 1

    # FVG / OB structural confluence
    if in_fvg and near_support:    bull_score += 1
    if in_fvg and near_resistance: bear_score += 1

    # CVD (cumulative volume delta)
    if cvd_bias == "bullish":  bull_score += 1
    if cvd_bias == "bearish":  bear_score += 1

    # RSI divergence (leading signal — kept in vote)
    if rsi_div == "bullish_divergence":  bull_score += 3
    if rsi_div == "bearish_divergence":  bear_score += 3

    # EMA 200 macro structure
    if above_e200 is True:   bull_score += 1
    if above_e200 is False:  bear_score += 1

    # Overextension from 4H EMA20 — mean-reversion pressure
    if price_above_4h_e20_ext:  bear_score += 2

    # Candlestick patterns
    bull_pats = [p for p in patterns if "bull" in p or p in
                 ("morning_star","hammer","three_white_soldiers","abandoned_baby_bull")]
    bear_pats = [p for p in patterns if "bear" in p or p in
                 ("evening_star","shooting_star","three_black_crows","abandoned_baby_bear")]
    bull_score += len(bull_pats) * 2
    bear_score += len(bear_pats) * 2

    if bull_score > bear_score:
        direction = "LONG"
    elif bear_score > bull_score:
        direction = "SHORT"
    else:
        return None

    # ══════════════════════════════════════════════════════════════════════
    # HARD GATE 1 — MARKET STRUCTURE MUST NOT CONTRADICT DIRECTION
    # ══════════════════════════════════════════════════════════════════════
    # Never short an asset that is printing HH/HL on 1H or 4H.
    # Never long an asset that is printing LH/LL on 1H or 4H.
    # A confirmed structural break (mkt_structure flipping) is the only
    # valid exception — if that has happened, mkt_structure will already
    # reflect the new structure, so no exception handling is needed here.
    #
    # This gate directly fixes the class of error where the direction vote
    # leans SHORT based on RSI/MACD while price is in clear HH/HL structure.
    bullish_struct_confirmed = (mkt_structure    == "bullish" or
                                mkt_structure_4h == "bullish")
    bearish_struct_confirmed = (mkt_structure    == "bearish" or
                                mkt_structure_4h == "bearish")

    if direction == "SHORT" and bullish_struct_confirmed and not bearish_struct_confirmed:
        return None  # NO TRADE — shorting into confirmed HH/HL structure

    if direction == "LONG" and bearish_struct_confirmed and not bullish_struct_confirmed:
        return None  # NO TRADE — longing into confirmed LH/LL structure

    # ══════════════════════════════════════════════════════════════════════
    # HARD GATE 2 — ENTRY MUST BE AT A MAJOR LIQUIDITY ZONE
    # ══════════════════════════════════════════════════════════════════════
    # Entries in the middle of a move (no OB, no FVG, no S/R proximity)
    # are rejected regardless of direction vote or score.
    # VWAP proximity alone is NOT a liquidity zone — it is a momentum gauge.
    at_major_zone = in_fvg or at_ob or near_support or near_resistance
    if not at_major_zone:
        return None  # NO TRADE — no major liquidity zone at current price

    # ══════════════════════════════════════════════════════════════════════
    # HARD GATE 3 — ORDER FLOW MUST CONFIRM DIRECTION
    # ══════════════════════════════════════════════════════════════════════
    # CVD (Cumulative Volume Delta) captures net aggressive order flow.
    # If the dominant buy/sell pressure contradicts the signal direction,
    # institutional positioning is working against the trade — reject it.
    if direction == "LONG"  and cvd_bias == "bearish":
        return None  # NO TRADE — bearish order flow opposes LONG
    if direction == "SHORT" and cvd_bias == "bullish":
        return None  # NO TRADE — bullish order flow opposes SHORT

    # ── Overextension gate (hard block before scoring) ───────────────
    # Don't short already-oversold coins; don't long already-overbought coins.
    if is_overextended(rsi, rsi_4h, direction):
        return None   # signal will not be shown, no print (scanner prints skip reason)

    # ── Pullback entry check ─────────────────────────────────────────
    is_pullback = check_pullback_entry(candles_4h, candles_1h, direction)

    # ── Entry / SL / TP ─────────────────────────────────────────────
    entry    = current
    risk     = entry * SL_PCT
    tp1_dist = max(risk * 2.0, atr_1h * 3.0)
    tp2_dist = max(risk * 3.5, atr_1h * 5.0)
    tp1_dist = min(tp1_dist, entry * 0.30)
    tp2_dist = min(tp2_dist, entry * 0.50)
    if direction == "LONG":
        sl  = round(entry * (1 - SL_PCT), 8)
        tp1 = round(entry + tp1_dist, 8)
        tp2 = round(entry + tp2_dist, 8)
    else:
        sl  = round(entry * (1 + SL_PCT), 8)
        tp1 = round(entry - tp1_dist, 8)
        tp2 = round(entry - tp2_dist, 8)

    # ══════════════════════════════════════════════════════════════════
    # Confluence score (0-100)
    # ── Priority tier ──────────────────────────────────────────────
    #  1. PRIMARY   — VWAP, market structure, FVGs, OBs, pullback entry
    #  2. SECONDARY — Trend alignment, S/R, CVD, RSI divergence, volume
    #  3. TERTIARY  — RSI extremes (confirmation), MACD (reduced weight)
    # ══════════════════════════════════════════════════════════════════
    confluences = []
    score = 0

    # ── PRIMARY: Trend alignment ──────────────────────────────────────
    if trend_4h != "neutral":
        confluences.append(f"4H Trend: {trend_4h}")
        score += 15
    if trend_1h != "neutral":
        confluences.append(f"1H Trend: {trend_1h}")
        score += 10

    # ── PRIMARY: VWAP position (institutional benchmark) ─────────────
    if vwap_1h is not None:
        if (direction == "LONG" and vwap_above is True):
            confluences.append(f"Price above VWAP {vwap_1h:.4f} — bullish institutional")
            score += 15
        elif (direction == "SHORT" and vwap_above is False):
            confluences.append(f"Price below VWAP {vwap_1h:.4f} — bearish institutional")
            score += 15
        elif near_vwap:
            confluences.append(f"Near VWAP {vwap_1h:.4f} — potential inflection")
            score += 8
        else:
            # Price on wrong side of VWAP for setup — reduce conviction
            score -= 6
        # Anchored VWAP (24H) secondary confirmation
        if avwap_1h is not None:
            if (direction == "LONG" and avwap_above is True) or \
               (direction == "SHORT" and avwap_above is False):
                confluences.append("Anchored VWAP aligned (24H) ✦")
                score += 8

    # ── PRIMARY: Market structure (HH/HL or LH/LL) ───────────────────
    if (direction == "LONG" and mkt_structure == "bullish") or \
       (direction == "SHORT" and mkt_structure == "bearish"):
        confluences.append(f"Market structure: {mkt_structure} (HH/HL or LH/LL) ✦")
        score += 18

    # ── PRIMARY: Fair Value Gap ───────────────────────────────────────
    if in_fvg:
        confluences.append("Inside Fair Value Gap ✦")
        score += 15

    # ── PRIMARY: Order Block ──────────────────────────────────────────
    if at_ob:
        confluences.append("At Order Block ✦")
        score += 15

    # ── PRIMARY: Pullback entry (high-probability retest) ─────────────
    if is_pullback:
        confluences.append("Pullback entry (EMA/FVG/OB retest) ✦")
        score += 15
    else:
        confluences.append("Chasing price — no pullback to structure")
        score -= 12

    # ── SECONDARY: Support / Resistance proximity ─────────────────────
    if near_support and direction == "LONG":
        confluences.append("Near key support")
        score += 12
    if near_resistance and direction == "SHORT":
        confluences.append("Near key resistance")
        score += 12

    # ── SECONDARY: RSI Divergence (leading signal — high weight kept) ──
    if rsi_div == "bullish_divergence" and direction == "LONG":
        confluences.append("RSI Bullish Divergence ✦")
        score += 20
    elif rsi_div == "bearish_divergence" and direction == "SHORT":
        confluences.append("RSI Bearish Divergence ✦")
        score += 20

    # ── SECONDARY: CVD (cumulative volume delta) ──────────────────────
    if (direction == "LONG" and cvd_bias == "bullish") or \
       (direction == "SHORT" and cvd_bias == "bearish"):
        confluences.append(f"CVD aligned: {cvd_bias}")
        score += 10

    # ── SECONDARY: Volume spike ───────────────────────────────────────
    if vol_spike:
        confluences.append("Volume spike (breakout confirmation)")
        score += 10
    else:
        score -= 4

    # ── SECONDARY: EMA 200 macro structure ───────────────────────────
    if above_e200 is True and direction == "LONG":
        confluences.append("Price above EMA 200 (bull structure)")
        score += 8
    elif above_e200 is False and direction == "SHORT":
        confluences.append("Price below EMA 200 (bear structure)")
        score += 8

    # ── SECONDARY: ADX trend strength ────────────────────────────────
    if adx_trending:
        confluences.append(f"ADX trending ({adx_tok:.1f})")
        score += 8
    if adx_ranging:
        confluences.append(f"ADX ranging ({adx_tok:.1f}) — weak trend")
        score -= 8

    # ── TERTIARY: RSI extremes (confirmation only, reduced weight) ────
    if (direction == "LONG" and rsi < 35) or (direction == "SHORT" and rsi > 65):
        confluences.append(f"RSI extreme: {rsi:.1f}")
        score += 8
    if (direction == "LONG" and rsi < 25) or (direction == "SHORT" and rsi > 75):
        confluences.append(f"RSI extreme+: {rsi:.1f}")
        score += 6

    # ── TERTIARY: RSI direction (momentum confirmation) ───────────────
    if (direction == "LONG" and rsi_rising) or (direction == "SHORT" and rsi_falling):
        confluences.append(f"RSI momentum aligned ({'↑' if direction=='LONG' else '↓'})")
        score += 6
    elif (direction == "LONG" and rsi_falling) or (direction == "SHORT" and rsi_rising):
        confluences.append("RSI momentum opposing — conviction reduced")
        score -= 4

    # ── Volume adequacy gates ─────────────────────────────────────────
    if not has_volume:
        confluences.append("⚠️ Low liquidity — below volume threshold")
        score -= 15
    if not vol_ok:
        confluences.append("⚠️ Abnormal volatility — spike or dead market")
        score -= 12

    # ── BTC correlation — independent mover bonus ─────────────────────
    if btc_corr is not None:
        if btc_corr < 0.60:
            confluences.append(f"Low BTC correlation ({btc_corr:.2f}) — independent mover ✦")
            score += 10
        elif btc_corr > 0.90:
            confluences.append(f"High BTC correlation ({btc_corr:.2f}) — follows BTC closely")
            score -= 5

    # ── Manipulation penalty ──────────────────────────────────────────
    if manip_flags:
        flag_str = ", ".join(f.replace("_", " ") for f in manip_flags)
        confluences.append(f"⚠️ Manipulation flag: {flag_str}")
        score -= 12 * len(manip_flags)

    # ── Candlestick patterns ──────────────────────────────────────────
    for p in patterns:
        confluences.append(f"Pattern: {p.replace('_',' ').title()}")
        score += 10

    return {
        "symbol":                  symbol,
        "category":                token_data.get("category", "crypto"),
        "direction":               direction,
        "timeframe":               "1H",
        "entry":                   sig_round(entry, 6),
        "sl":                      sig_round(sl, 6),
        "tp1":                     sig_round(tp1, 6),
        "tp2":                     sig_round(tp2, 6),
        "trend_4h":                trend_4h,
        "trend_1h":                trend_1h,
        "rsi":                     rsi,
        "rsi_4h":                  rsi_4h,
        "cvd_bias":                cvd_bias,
        "vol_spike":               vol_spike,
        "has_volume":              has_volume,
        "vol_ok":                  vol_ok,
        "near_support":            near_support,
        "near_resistance":         near_resistance,
        "in_fvg":                  in_fvg,
        "at_ob":                   at_ob,
        "patterns":                patterns,
        "rsi_rising":              rsi_rising,
        "rsi_falling":             rsi_falling,
        "rsi_div":                 rsi_div,
        "mkt_structure":           mkt_structure,
        "mkt_structure_4h":        mkt_structure_4h,
        "adx":                     adx_tok,
        "is_pullback":             is_pullback,
        "manip_flags":             manip_flags,
        "btc_corr":                btc_corr,
        "confluences":             confluences,
        "internal_score":          min(max(score, 0), 100),
        "sl_pct":                  round(SL_PCT * 100, 2),
        "price_below_4h_e20":      price_below_4h_e20,
        "price_above_4h_e20_ext":  price_above_4h_e20_ext,
        # VWAP — institutional benchmark data (included in every signal)
        "vwap":                    vwap_1h,
        "vwap_above":              vwap_above,
        "vwap_dist_pct":           vwap_dist_pct,
        "avwap_24h":               avwap_1h,
        "avwap_above":             avwap_above,
    }

# ══════════════════════════════════════════════════════════════════
# ONCHAIN — SHARED PUBLIC RPC HELPERS
# Uses free public nodes — no API key, no competition with ETH Onchain Bot
# ══════════════════════════════════════════════════════════════════
def _rpc_block_number(rpc_url):
    """Get current block number via JSON-RPC. Returns int or None."""
    try:
        r = requests.post(
            rpc_url,
            json={"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]},
            headers={"Content-Type":"application/json"},
            timeout=8
        )
        result = r.json().get("result")
        if result and isinstance(result, str) and result.startswith("0x"):
            return int(result, 16)
    except Exception as e:
        print(f"[RPC blockNumber] {rpc_url}: {e}")
    return None


def _rpc_eth_logs(rpc_url, contract, from_block, to_block):
    """Fetch ERC-20 Transfer logs via eth_getLogs JSON-RPC. Returns list of log dicts."""
    try:
        r = requests.post(
            rpc_url,
            json={"jsonrpc":"2.0","id":1,"method":"eth_getLogs","params":[{
                "address":   contract,
                "topics":    [TRANSFER_TOPIC],
                "fromBlock": hex(from_block),
                "toBlock":   hex(to_block),
            }]},
            headers={"Content-Type":"application/json"},
            timeout=15
        )
        result = r.json()
        if "error" in result:
            print(f"[RPC getLogs] error: {result['error']}")
            return []
        return result.get("result", [])
    except Exception as e:
        print(f"[RPC getLogs] {rpc_url}: {e}")
    return []


def _analyse_evm_logs(logs, cex_wallets):
    """Classify CEX inflow vs outflow from ERC-20 Transfer logs."""
    if not logs:
        return "no_data"
    cex_inflow = cex_outflow = 0
    for log in logs:
        try:
            val  = int(log.get("data", "0x0"), 16)
            tops = log.get("topics", [])
            if len(tops) < 3:
                continue
            frm_addr = "0x" + tops[1][-40:]
            to_addr  = "0x" + tops[2][-40:]
            if frm_addr in cex_wallets: cex_outflow += val
            if to_addr  in cex_wallets: cex_inflow  += val
        except:
            continue
    if cex_inflow == 0 and cex_outflow == 0:
        return "neutral"
    ratio = cex_inflow / (cex_inflow + cex_outflow)
    if ratio > 0.6:  return "distribution"
    if ratio < 0.4:  return "accumulation"
    return "mixed"


def _rpc_pick(rpc_list):
    """Try each RPC in order; return (block_number, rpc_url) for the first that responds."""
    for rpc in rpc_list:
        cur = _rpc_block_number(rpc)
        if cur:
            return cur, rpc
    return None, None


# ══════════════════════════════════════════════════════════════════
# ONCHAIN — ETHEREUM  (public RPC, no Etherscan key needed)
# ══════════════════════════════════════════════════════════════════
def check_onchain_eth(token_data):
    contract = token_data.get("eth_contract")
    if not contract:
        return "no_data"
    try:
        cur, rpc = _rpc_pick(ETH_RPC_NODES)
        if not cur:
            return "no_data"
        from_block = cur - ONCHAIN_LOOKBACK_ETH
        logs = _rpc_eth_logs(rpc, contract, from_block, cur)
        result = _analyse_evm_logs(logs, ETH_CEX_WALLETS)
        print(f"[ETH/RPC] {contract[:10]}… logs={len(logs)} → {result}")
        return result
    except Exception as e:
        print(f"[ETH Onchain] {e}")
        return "no_data"


# ══════════════════════════════════════════════════════════════════
# ONCHAIN — BSC  (public RPC, no BscScan key needed)
# ══════════════════════════════════════════════════════════════════
def check_onchain_bsc(token_data):
    contract = token_data.get("bsc_contract")
    if not contract:
        return "no_data"
    try:
        cur, rpc = _rpc_pick(BSC_RPC_NODES)
        if not cur:
            return "no_data"
        from_block = cur - ONCHAIN_LOOKBACK_BSC
        logs = _rpc_eth_logs(rpc, contract, from_block, cur)
        result = _analyse_evm_logs(logs, BSC_CEX_WALLETS)
        print(f"[BSC/RPC] {contract[:10]}… logs={len(logs)} → {result}")
        return result
    except Exception as e:
        print(f"[BSC Onchain] {e}")
        return "no_data"


# ══════════════════════════════════════════════════════════════════
# ONCHAIN — SOLANA  (DexScreener buy/sell ratio — no key needed)
# For Solana-native tokens DexScreener 1H buy/sell ratio is the most
# reliable real-time signal available without a paid Helius subscription.
# ══════════════════════════════════════════════════════════════════
def check_onchain_sol(token_data):
    mint = token_data.get("sol_mint")
    if not mint:
        return "no_data"
    try:
        r = requests.get(
            f"https://api.dexscreener.com/latest/dex/tokens/{mint}",
            timeout=10
        )
        if r.status_code != 200:
            return "no_data"
        pairs = [p for p in r.json().get("pairs", [])
                 if p.get("chainId") == "solana"]
        if not pairs:
            return "no_data"
        # Use highest-liquidity Solana pair
        best = max(pairs, key=lambda p: float(
            (p.get("liquidity") or {}).get("usd", 0) or 0))
        txns_1h = (best.get("txns") or {}).get("h1", {})
        buys  = int(txns_1h.get("buys",  0) or 0)
        sells = int(txns_1h.get("sells", 0) or 0)
        if buys + sells < 10:   # too few transactions to read
            return "neutral"
        ratio = buys / (buys + sells)
        if ratio > 0.60:  return "accumulation"
        if ratio < 0.40:  return "distribution"
        return "mixed"
    except Exception as e:
        print(f"[SOL/DexScreener] {e}")
        return "no_data"

# ══════════════════════════════════════════════════════════════════
# COMBINED ONCHAIN
# ══════════════════════════════════════════════════════════════════
def check_onchain_all(token_data):
    # Run all three chain checks in parallel with a 12-second combined cap.
    # Sequential execution was hanging the scan for 30-60s per token when
    # multiple RPC nodes timed out one-by-one (3 ETH + 4 BSC RPCs × 8-15s each).
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as ex:
        fut_eth = ex.submit(check_onchain_eth, token_data)
        fut_bsc = ex.submit(check_onchain_bsc, token_data)
        fut_sol = ex.submit(check_onchain_sol, token_data)
        def _get(fut, label):
            try:
                return fut.result(timeout=12)
            except concurrent.futures.TimeoutError:
                print(f"[Onchain/{label}] Timeout — no_data")
                return "no_data"
            except Exception as e:
                print(f"[Onchain/{label}] Error: {e}")
                return "no_data"
        eth_result = _get(fut_eth, "ETH")
        bsc_result = _get(fut_bsc, "BSC")
        sol_result = _get(fut_sol, "SOL")

    votes   = [v for v in [eth_result, bsc_result, sol_result]
               if v not in ("no_data", "neutral")]
    accum   = votes.count("accumulation")
    distrib = votes.count("distribution")

    if not votes:
        combined = "neutral"
    elif accum > distrib:
        combined = "accumulation"
    elif distrib > accum:
        combined = "distribution"
    elif accum == distrib and accum > 0:
        combined = "mixed"
    else:
        combined = "neutral"

    return eth_result, bsc_result, sol_result, combined

# ══════════════════════════════════════════════════════════════════
# DERIVATIVES CHECK
# ══════════════════════════════════════════════════════════════════
def check_derivatives(binance_symbol):
    funding   = fetch_funding_rate(binance_symbol)
    _, oi_chg = fetch_oi_change(binance_symbol)
    time.sleep(0.2)

    bias = "neutral"
    if funding is not None and oi_chg is not None:
        if   funding >  0.001:               bias = "warning_long_crowded"
        elif funding < -0.001:               bias = "warning_short_crowded"
        elif funding > 0 and oi_chg >  2:    bias = "bullish"
        elif funding < 0 and oi_chg < -2:    bias = "bearish"
        else:                                bias = "neutral"
    elif funding is None and oi_chg is None:
        bias = "no_perp_data"

    return funding, oi_chg, bias

# ══════════════════════════════════════════════════════════════════
# SIGNAL GRADE
# ══════════════════════════════════════════════════════════════════
def assign_signal_grade(gemini_score, onchain_combined, deriv_bias,
                        mtf_aligned, patterns_count):
    pts  = min(gemini_score, 100)
    pts += 20 if onchain_combined in ("accumulation", "distribution") else 0
    pts += 10 if onchain_combined == "mixed" else 0
    pts += 15 if mtf_aligned else 0
    pts += 10 * min(patterns_count, 2)
    pts += 10 if deriv_bias in ("bullish", "bearish") else 0
    pts -= 15 if "warning" in deriv_bias else 0

    if pts >= 145: return "S"
    if pts >= 120: return "A"
    if pts >= 90:  return "B"
    return "C"


# Grade → estimated historical win rate and avg P&L at TP1/TP2
GRADE_STATS = {
    "S": {"win_pct": 78, "avg_pnl_tp1": "+7.0%", "avg_pnl_tp2": "+12.0%", "label": "S ⭐⭐⭐ ELITE"},
    "A": {"win_pct": 68, "avg_pnl_tp1": "+6.5%", "avg_pnl_tp2": "+11.0%", "label": "A ⭐⭐ STRONG"},
    "B": {"win_pct": 57, "avg_pnl_tp1": "+6.0%", "avg_pnl_tp2": "+10.5%", "label": "B ⭐ DECENT"},
    "C": {"win_pct": 45, "avg_pnl_tp1": "+6.0%", "avg_pnl_tp2": "+10.5%", "label": "C SPECULATIVE"},
}

# ══════════════════════════════════════════════════════════════════
# DEEPSEEK REASONING  (primary scorer — preserves Gemini quota)
# ══════════════════════════════════════════════════════════════════
def deepseek_score_and_summarise(analysis, eth_res, bsc_res, sol_res,
                                  onchain_combined, funding, oi_change, deriv_bias):
    if not DEEPSEEK_API_KEY:
        return None, None

    confl_str   = "\n".join(analysis["confluences"])
    pats_str    = ", ".join(p.replace("_"," ").title() for p in analysis["patterns"]) or "None"
    funding_str = f"{funding*100:.4f}%" if funding is not None else "N/A"
    oi_str      = f"{oi_change:+.2f}%" if oi_change is not None else "N/A"

    system_msg = (
        "You are a senior quantitative crypto trader. Score the setup and return ONLY "
        "a JSON object with exactly two keys: \"score\" (integer 0-100) and \"summary\" "
        "(2-3 sentences of sharp professional reasoning, plain text, no markdown). "
        "Scoring: 85-100=exceptional, 70-84=strong, 50-69=decent but do not fire, 0-49=reject."
    )
    rsi_div_str  = analysis.get("rsi_div") or "none"
    rsi_dir_str  = ("rising" if analysis.get("rsi_rising") else
                    "falling" if analysis.get("rsi_falling") else "flat")
    adx_str      = f"{analysis.get('adx')}" if analysis.get("adx") is not None else "n/a"
    corr_str     = f"{analysis.get('btc_corr'):.2f}" if analysis.get("btc_corr") is not None else "n/a"
    manip_str    = ", ".join(analysis.get("manip_flags") or []) or "none"
    vwap_str     = (f"{analysis['vwap']:.6f} ({'above' if analysis.get('vwap_above') else 'below'}, "
                    f"dist {analysis.get('vwap_dist_pct',0):+.2f}%)")  \
                   if analysis.get("vwap") else "n/a"
    avwap_str    = (f"{analysis['avwap_24h']:.6f} ({'above' if analysis.get('avwap_above') else 'below'})") \
                   if analysis.get("avwap_24h") else "n/a"
    user_msg = f"""
Asset: {analysis['symbol']}  Direction: {analysis['direction']}  TF: {analysis['timeframe']}
Entry: {analysis['entry']}  SL: {analysis['sl']} ({round(abs(analysis['entry']-analysis['sl'])/analysis['entry']*100,2)}% risk)
TP1: {analysis['tp1']}  TP2: {analysis['tp2']}

Institutional Price Framework (PRIMARY INDICATORS):
VWAP (session): {vwap_str}
Anchored VWAP (24H): {avwap_str}
Market Structure (1H): {analysis.get('mkt_structure','n/a')}
FVG: {analysis['in_fvg']}  Order Block: {analysis['at_ob']}
Pullback Entry: {analysis.get('is_pullback','n/a')}
Support: {analysis['near_support']}  Resistance: {analysis['near_resistance']}

Technical (secondary/tertiary):
V2 Futures: Funding {funding_str} | OI 4H {oi_str} | Taker {analysis.get('taker_ratio', 'N/A')} | L/S {analysis.get('ls_ratio', 'N/A')}
24H Futures Volume: ${analysis.get('volume_24h', 0):,.0f} | Conditions: {analysis.get('conditions_met', 'N/A')}/5
4H Trend: {analysis['trend_4h']}  1H Trend: {analysis['trend_1h']}
RSI(1H): {analysis['rsi']}  RSI(4H): {analysis.get('rsi_4h','n/a')}  CVD: {analysis['cvd_bias']}
RSI Direction(1H): {rsi_dir_str}  RSI Divergence: {rsi_div_str}
ADX(1H): {adx_str}  Vol Spike: {analysis['vol_spike']}
Patterns: {pats_str}
BTC Correlation (20-bar Pearson): {corr_str}
Manipulation Flags: {manip_str}
Internal Confluence: {analysis['internal_score']}

Onchain Intelligence (last 4H whale flow — ETH/BSC=CEX wallet net flow, SOL=DEX buy/sell ratio):
ETH: {eth_res}  BSC: {bsc_res}  SOL: {sol_res}
Combined: {onchain_combined}

Derivatives:
Funding: {funding_str}  OI Change(4H): {oi_str}  Bias: {deriv_bias}

Confluences:
{confl_str}
"""
    try:
        r = requests.post(
            "https://api.deepseek.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {DEEPSEEK_API_KEY}",
                     "Content-Type": "application/json"},
            json={"model": "deepseek-chat",
                  "messages": [{"role": "system", "content": system_msg},
                                {"role": "user",   "content": user_msg}],
                  "response_format": {"type": "json_object"},
                  "temperature": 0.2,
                  "max_tokens": 300},
            timeout=30
        )
        if r.status_code == 200:
            text   = r.json()["choices"][0]["message"]["content"].strip()
            parsed = json.loads(text)
            return int(parsed["score"]), str(parsed["summary"])
        else:
            print(f"[DeepSeek] {r.status_code}: {r.text[:120]}")
            return None, None
    except Exception as e:
        print(f"[DeepSeek Error] {e}")
        return None, None


# ══════════════════════════════════════════════════════════════════
# GEMINI REASONING  (fallback when DeepSeek unavailable)
# ══════════════════════════════════════════════════════════════════
def gemini_score_and_summarise(analysis, eth_res, bsc_res, sol_res,
                                onchain_combined, funding, oi_change, deriv_bias):
    if not GEMINI_API_KEY:
        return min(analysis["internal_score"], 100), "Gemini not configured — using internal score."

    confl_str   = "\n".join(analysis["confluences"])
    pats_str    = ", ".join(p.replace("_"," ").title() for p in analysis["patterns"]) or "None"
    funding_str = f"{funding*100:.4f}%" if funding is not None else "N/A"
    oi_str      = f"{oi_change:+.2f}%" if oi_change is not None else "N/A"

    rsi_div_str = analysis.get("rsi_div") or "none"
    rsi_dir_str_g = ("rising" if analysis.get("rsi_rising") else
                     "falling" if analysis.get("rsi_falling") else "flat")
    adx_str_g   = f"{analysis.get('adx')}" if analysis.get("adx") is not None else "n/a"
    corr_str_g  = f"{analysis.get('btc_corr'):.2f}" if analysis.get("btc_corr") is not None else "n/a"
    manip_str_g = ", ".join(analysis.get("manip_flags") or []) or "none"
    vwap_str_g  = (f"{analysis['vwap']:.6f} ({'above' if analysis.get('vwap_above') else 'below'}, "
                   f"dist {analysis.get('vwap_dist_pct',0):+.2f}%)") \
                  if analysis.get("vwap") else "n/a"
    avwap_str_g = (f"{analysis['avwap_24h']:.6f} ({'above' if analysis.get('avwap_above') else 'below'})") \
                  if analysis.get("avwap_24h") else "n/a"
    prompt = f"""
You are a professional crypto trading analyst specialising in institutional price action.

Analyse this trading setup and return ONLY a JSON object with two fields:
- "score": integer 0-100
- "summary": 2-3 sentence professional trade brief in plain English

Asset Details:
Symbol: {analysis['symbol']}
Direction: {analysis['direction']} | Timeframe: {analysis['timeframe']}
Entry: {analysis['entry']} | SL: {analysis['sl']} ({round(abs(analysis['entry']-analysis['sl'])/analysis['entry']*100,2)}% risk)
TP1: {analysis['tp1']} | TP2: {analysis['tp2']}

Institutional Price Framework (PRIMARY — weight these most heavily):
VWAP (session): {vwap_str_g}
Anchored VWAP (24H): {avwap_str_g}
Market Structure (1H): {analysis.get('mkt_structure','n/a')}
Inside FVG: {analysis['in_fvg']} | At Order Block: {analysis['at_ob']}
Pullback Entry (at EMA/FVG/OB): {analysis.get('is_pullback','n/a')}
At Support: {analysis['near_support']} | At Resistance: {analysis['near_resistance']}

Technical Analysis (secondary):
V2 Futures: Funding {funding_str} | OI 4H {oi_str} | Taker {analysis.get('taker_ratio', 'N/A')} | L/S {analysis.get('ls_ratio', 'N/A')}
24H Futures Volume: ${analysis.get('volume_24h', 0):,.0f} | Conditions: {analysis.get('conditions_met', 'N/A')}/5
4H Trend: {analysis['trend_4h']} | 1H Trend: {analysis['trend_1h']}
RSI (1H): {analysis['rsi']} | RSI (4H): {analysis.get('rsi_4h','n/a')} | CVD: {analysis['cvd_bias']}
RSI Direction (1H): {rsi_dir_str_g} | RSI Divergence: {rsi_div_str}
ADX (1H): {adx_str_g} | Vol Spike: {analysis['vol_spike']}
Patterns: {pats_str}
BTC Correlation (20-bar Pearson): {corr_str_g}
Manipulation Flags: {manip_str_g}
Internal Confluence Score: {analysis['internal_score']}

Onchain Intelligence (ETH/BSC=CEX net wallet flow, SOL=DEX buy/sell ratio — last 4H):
ETH Chain: {eth_res}
BSC Chain: {bsc_res}
Solana Chain: {sol_res}
Combined Onchain Verdict: {onchain_combined}

Derivatives:
Funding Rate: {funding_str}
OI Change (4H): {oi_str}
Derivatives Bias: {deriv_bias}

Confluence Factors:
{confl_str}

Scoring rules:
90-100: Exceptional. VWAP aligned, market structure confirmed, FVG/OB entry, onchain agrees, derivatives supportive.
75-89: Strong. Most institutional factors align, 1-2 minor contradictions acceptable.
60-74: Decent but missing key confirmations — DO NOT FIRE.
0-59: Major contradictions — reject.

- VWAP aligned with direction = +10 pts. Price on wrong side = -8 pts.
- FVG or Order Block entry = +10 pts.
- "mixed" onchain = chains disagree — reduce score by 5-10 pts.
- RSI divergence confirming direction = add 10-15 pts.
- If 4H and 1H trends align, add confidence. If they conflict, penalise heavily.
- MACD is a tertiary confirmation only — do not heavily penalise if opposing.

Return ONLY valid JSON: {{"score": 78, "summary": "..."}}
"""

    # Try models in order — fallback on 429/503
    _gemini_models = ["gemini-2.5-flash", "gemini-2.0-flash", "gemini-1.5-flash-latest"]
    for model in _gemini_models:
        try:
            r = requests.post(
                f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={GEMINI_API_KEY}",
                json={"contents": [{"parts": [{"text": prompt}]}],
                      "generationConfig": {"temperature": 0.2, "maxOutputTokens": 350}},
                timeout=25
            )
            if r.status_code == 200:
                raw    = r.json()["candidates"][0]["content"]["parts"][0]["text"]
                raw    = raw.strip().replace("```json","").replace("```","").strip()
                parsed = json.loads(raw)
                return int(parsed["score"]), str(parsed["summary"])
            elif r.status_code in (429, 503):
                print(f"[Gemini] {r.status_code} on {model}, trying next…")
                continue
            else:
                print(f"[Gemini] {r.status_code}: {r.text[:100]}")
                return None, None
        except Exception as e:
            print(f"[Gemini Error] {model}: {e}")
    return None, None

# ══════════════════════════════════════════════════════════════════
# FIRE SIGNAL TO TELEGRAM
# ══════════════════════════════════════════════════════════════════
def fire_signal(analysis, eth_res, bsc_res, sol_res, onchain_combined,
                funding, oi_change, deriv_bias,
                gemini_score, gemini_summary, signal_grade, signal_id,
                onchain_flip=False, onchain_flip_str=""):

    sym   = analysis["symbol"]
    dire  = analysis["direction"]
    emoji = "🟢" if dire == "LONG" else "🔴"

    # ── Onchain formatting helpers ──────────────────────────────────
    def oc_label(v):
        return {
            "accumulation": "🟢 ACCUMULATION",
            "distribution": "🔴 DISTRIBUTION",
            "neutral":      "⚪️ Neutral",
            "mixed":        "🟡 Mixed",
            "no_data":      "— No data",
        }.get(v, "—")

    def oc_short(v):
        return {
            "accumulation": "🟢 Accum",
            "distribution": "🔴 Distrib",
            "neutral":      "⚪️ Neutral",
            "mixed":        "🟡 Mixed",
            "no_data":      "—",
        }.get(v, "—")

    def oc_meaning(v, chain):
        """Explain what the data means in plain language."""
        if chain == "sol":
            if v == "accumulation":  return "(DEX buy pressure dominant)"
            if v == "distribution":  return "(DEX sell pressure dominant)"
            if v == "mixed":         return "(DEX buys/sells balanced)"
            return ""
        else:
            if v == "accumulation":  return "(whales withdrawing from CEX)"
            if v == "distribution":  return "(whales depositing to CEX)"
            if v == "mixed":         return "(mixed CEX flow)"
            return ""

    funding_str  = f"{funding*100:.4f}%" if funding else "N/A"
    oi_str       = f"{oi_change:+.2f}%" if oi_change else "N/A"
    deriv_labels = {
        "bullish":               "🟢 Bullish",
        "bearish":               "🔴 Bearish",
        "neutral":               "⚪️ Neutral",
        "warning_long_crowded":  "⚠️ Longs Overcrowded",
        "warning_short_crowded": "⚠️ Shorts Overcrowded",
        "no_perp_data":          "— (no perp data)",
    }
    pats_str = " · ".join(p.replace("_"," ").title() for p in analysis["patterns"]) or "None"

    # Flip banner (shown at top when onchain overrode TA direction)
    flip_banner = (
        f"\n⚡ <b>ONCHAIN OVERRIDE</b> — TA was {onchain_flip_str.split('→')[0]} "
        f"but whale flow flipped to <b>{dire}</b>\n"
    ) if onchain_flip else ""

    # Onchain detail block — the differentiating section of this bot
    has_any_onchain = any(v != "no_data" for v in [eth_res, bsc_res, sol_res])
    eth_line = (f"  ETH (CEX whale flow): {oc_short(eth_res)} {oc_meaning(eth_res,'eth')}"
                if eth_res != "no_data" else "  ETH: — (no contract)")
    bsc_line = (f"  BSC (CEX whale flow): {oc_short(bsc_res)} {oc_meaning(bsc_res,'bsc')}"
                if bsc_res != "no_data" else "  BSC: — (no contract)")
    sol_line = (f"  SOL (DEX buy/sell):   {oc_short(sol_res)} {oc_meaning(sol_res,'sol')}"
                if sol_res != "no_data" else "  SOL: — (no token data)")

    onchain_verdict = (
        f"  📡 <b>Verdict: {oc_label(onchain_combined)}</b>"
        + (" ← drove signal direction" if onchain_flip else "")
    )
    onchain_block = (
        f"🔗 <b>Onchain Intelligence (4H window)</b>\n"
        f"{eth_line}\n"
        f"{bsc_line}\n"
        f"{sol_line}\n"
        f"{onchain_verdict}"
    ) if has_any_onchain else (
        f"🔗 <b>Onchain Intelligence</b>\n"
        f"  No on-chain contract data for {sym} on ETH/BSC/SOL"
    )

    gs       = GRADE_STATS.get(signal_grade, GRADE_STATS["C"])
    sl_pct   = round(abs(analysis['entry'] - analysis['sl']) / analysis['entry'] * 100, 1)
    tp1_pct  = round(abs(analysis['tp1'] - analysis['entry']) / analysis['entry'] * 100, 1)
    tp2_pct  = round(abs(analysis['tp2'] - analysis['entry']) / analysis['entry'] * 100, 1)

    msg = (
        f"{emoji} <b>{dire} | {sym}/USDT</b>{flip_banner}\n"
        f"{'─'*32}\n"
        f"📍 Entry:     <b>{analysis['entry']}</b>\n"
        f"🎯 TP1:       <b>{analysis['tp1']}</b>\n"
        f"🎯 TP2:       <b>{analysis['tp2']}</b>\n"
        f"🛑 SL:        <b>{analysis['sl']}</b>\n"
        f"📊 Confidence: <b>{gemini_score}/100</b>\n"
        f"{'─'*32}\n"
        f"<b>Reason — {analysis.get('conditions_met', '—')}/5 conditions</b>\n"
        f"Funding: <b>{funding_str}</b> | OI: <b>{oi_str}</b>\n"
        f"Taker Flow: <b>{analysis.get('taker_ratio', 'N/A')}</b> | "
        f"L/S Ratio: <b>{analysis.get('ls_ratio', 'N/A')}</b>\n"
        f"RSI: <b>{analysis['rsi']:.1f}</b> | "
        f"Volume: <b>${analysis.get('volume_24h', 0):,.0f}</b>\n"
        f"{'─'*32}\n"
        f"📊 <b>Institutional Framework ({analysis['timeframe']})</b>\n"
        f"VWAP: <b>{analysis['vwap']:.4f}</b> "
        f"{'🟢 Price Above' if analysis.get('vwap_above') else '🔴 Price Below'} "
        f"({analysis.get('vwap_dist_pct',0):+.2f}%)\n"
        + (f"Anchored VWAP (24H): <b>{analysis['avwap_24h']:.4f}</b> "
           f"{'🟢 Above' if analysis.get('avwap_above') else '🔴 Below'}\n"
           if analysis.get("avwap_24h") else "")
        + f"Mkt Structure: <b>{analysis.get('mkt_structure','—').title()}</b> | "
        f"FVG: {'✅' if analysis['in_fvg'] else '—'} | "
        f"OB: {'✅' if analysis['at_ob'] else '—'}\n"
        f"{'─'*32}\n"
        f"📈 <b>Technical ({analysis['timeframe']})</b>\n"
        f"Trend: 4H <b>{analysis['trend_4h'].upper()}</b> | "
        f"1H <b>{analysis['trend_1h'].upper()}</b>\n"
        f"RSI: <b>{analysis['rsi']:.1f}</b> | CVD: <b>{analysis['cvd_bias'].title()}</b> | "
        f"Vol: {'🔊 Spike' if analysis['vol_spike'] else 'Normal'}\n"
        f"RSI Dir: {'🟢 Rising' if analysis.get('rsi_rising') else ('🔴 Falling' if analysis.get('rsi_falling') else '⚪️ Flat')} | "
        f"RSI Div: <b>{analysis.get('rsi_div','—') or '—'}</b>\n"
        f"Patterns: <b>{pats_str}</b>\n"
        f"S/R: {'✅' if analysis['near_support'] or analysis['near_resistance'] else '—'} | "
        f"Pullback: {'✅' if analysis.get('is_pullback') else '—'}\n"
        f"{'─'*32}\n"
        f"{onchain_block}\n"
        f"{'─'*32}\n"
        f"💰 <b>Derivatives</b>\n"
        f"Funding: <b>{funding_str}</b> | OI Change: <b>{oi_str}</b>\n"
        f"Bias: {deriv_labels.get(deriv_bias,'—')}\n"
        f"{'─'*32}\n"
        f"🤖 <b>AI Research (DeepSeek/Gemini):</b>\n{gemini_summary}\n"
        f"{'─'*32}\n"
        f"⏰ {now_utc()}"
    )
    return send_tg(msg, chat_id=AUTO_SIGNAL_CHAT_ID)

# ══════════════════════════════════════════════════════════════════
# LOG TO DATABASE
# ══════════════════════════════════════════════════════════════════
def log_signal(analysis, eth_res, bsc_res, sol_res, onchain_combined,
               funding, oi_change, deriv_bias,
               gemini_score, gemini_summary, signal_grade):
    conn = db_conn()
    c    = conn.cursor()
    c.execute("""
        INSERT INTO autonomous_signals
        (symbol,category,direction,timeframe,entry,sl,tp1,tp2,
         internal_score,gemini_score,signal_grade,confluences,
         onchain_eth,onchain_bsc,onchain_sol,onchain_combined,
         funding_rate,oi_change_pct,cvd_bias,gemini_summary,fired_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        analysis["symbol"], analysis.get("category","crypto"),
        analysis["direction"], analysis["timeframe"],
        analysis["entry"], analysis["sl"], analysis["tp1"], analysis["tp2"],
        analysis["internal_score"], gemini_score, signal_grade,
        " | ".join(analysis["confluences"]),
        eth_res, bsc_res, sol_res, onchain_combined,
        funding, oi_change, analysis["cvd_bias"],
        gemini_summary, now_utc()
    ))
    sig_id = c.lastrowid
    conn.commit()
    conn.close()
    return sig_id

# ══════════════════════════════════════════════════════════════════
# PRICE MONITOR & AUTO-GRADING  (background thread)
# ══════════════════════════════════════════════════════════════════
def grade_open_signals():
    conn = db_conn()
    c    = conn.cursor()
    c.execute("""SELECT id,symbol,direction,entry,sl,tp1,tp2,fired_at,
                        tg_message_id,tp1_hit,signal_grade,gemini_score,warned_at
                 FROM autonomous_signals WHERE outcome='OPEN'""")
    rows = c.fetchall()

    for (sig_id, symbol, dire, entry, sl, tp1, tp2, fired_at,
         tg_message_id, tp1_hit, signal_grade, gemini_score, warned_at) in rows:

        # ── Expiry check ──────────────────────────────────────────
        try:
            fired_dt = datetime.strptime(fired_at, "%Y-%m-%d %H:%M UTC").replace(tzinfo=timezone.utc)
            age_hrs = (datetime.now(timezone.utc) - fired_dt).total_seconds() / 3600
            if age_hrs > MAX_SIGNAL_AGE_HRS:
                exp_token = WATCHLIST.get(symbol)
                exp_price = fetch_current_price(exp_token["binance"]) if exp_token else None
                price_str = f"{exp_price}" if exp_price else "n/a"
                age_str   = f"{age_hrs:.0f}h" if age_hrs < 48 else f"{age_hrs/24:.1f}d"

                # Capture current P&L at expiry so it counts in stats
                exp_pnl = None
                if entry and exp_price:
                    exp_pnl = pct(entry, exp_price) if dire == "LONG" else pct(exp_price, entry)

                c.execute("""UPDATE autonomous_signals
                             SET outcome='EXPIRED', outcome_time=?,
                                 outcome_price=?, pnl_pct=?
                             WHERE id=?""",
                          (now_utc(), exp_price, exp_pnl, sig_id))
                conn.commit()

                pnl_str = f"{exp_pnl:+.2f}%" if exp_pnl is not None else "n/a"

                # Edit original message with expired status + final P&L
                if tg_message_id:
                    edit_tg(tg_message_id,
                            _build_signal_edit_final(sig_id, symbol, dire, entry,
                                                     "EXPIRED", exp_price, exp_pnl, signal_grade))

                # Reply so members get a notification
                reply_msg = (
                    f"⏳ <b>{symbol} {dire} expired</b> {pnl_str}\n"
                    f"Age: {age_str} | Last price: {price_str}\n"
                    f"Entry: {entry} | TP1: {tp1} | TP2: {tp2} | SL: {sl}"
                )
                send_tg(reply_msg, reply_to_message_id=tg_message_id or None)
                print(f"[Grade] #{sig_id} {symbol} EXPIRED ({age_str}, last={price_str}, pnl={pnl_str})")
                continue
        except Exception:
            pass

        # ── Skip signals with no price data (manually inserted, ungradeable) ──
        if entry is None or sl is None or tp1 is None or tp2 is None:
            print(f"[Grade] #{sig_id} {symbol} SKIP — no price data in DB")
            continue

        token = WATCHLIST.get(symbol)
        if not token: continue

        price = fetch_current_price(token["binance"])
        if not price: continue

        # ── Candle extremes since entry (catches TP/SL wicks between scans) ──
        # The spot price at scan time can be BELOW TP1 even if it was touched
        # on a wick hours ago. Fetching the max high / min low since entry
        # ensures we never miss a TP1 or SL that happened between cycles.
        try:
            fired_dt_ts = datetime.strptime(fired_at, "%Y-%m-%d %H:%M UTC").replace(
                tzinfo=timezone.utc).timestamp()
        except Exception:
            fired_dt_ts = time.time() - 86400  # fallback: 24h ago
        candle_high, candle_low = fetch_candle_extremes(token["binance"], fired_dt_ts)

        # Effective high/low = best of spot price OR candle extreme
        eff_high = max(price, candle_high) if candle_high else price
        eff_low  = min(price, candle_low)  if candle_low  else price

        # ── Price vs levels ───────────────────────────────────────
        outcome      = None
        pnl          = None
        tp1_just_hit = False   # True only when TP1 is crossed for the first time

        if tp1_hit:
            # TP1 already banked — SL is now breakeven (entry price)
            if dire == "LONG":
                if   eff_high >= tp2:   outcome, pnl = "WIN_TP2 ⭐⭐", pct(entry, tp2)
                elif eff_low  <= entry: outcome, pnl = "WIN_TP1 ⭐",  pct(entry, tp1)
            else:
                if   eff_low  <= tp2:   outcome, pnl = "WIN_TP2 ⭐⭐", pct(tp2, entry)
                elif eff_high >= entry: outcome, pnl = "WIN_TP1 ⭐",  pct(tp1, entry)
        else:
            # TP1 not yet hit — original SL still live
            if dire == "LONG":
                if   eff_low  <= sl:    outcome, pnl = "LOSS",        pct(entry, eff_low)
                elif eff_high >= tp2:   outcome, pnl = "WIN_TP2 ⭐⭐", pct(entry, tp2)
                elif eff_high >= tp1:   tp1_just_hit = True
            else:
                if   eff_high >= sl:    outcome, pnl = "LOSS",        pct(eff_high, entry)
                elif eff_low  <= tp2:   outcome, pnl = "WIN_TP2 ⭐⭐", pct(tp2, entry)
                elif eff_low  <= tp1:   tp1_just_hit = True

        # ── TP1 first touch: notify, keep signal OPEN, move SL to breakeven ──
        if tp1_just_hit:
            tp1_pnl = pct(entry, tp1) if dire == "LONG" else pct(tp1, entry)
            c.execute("UPDATE autonomous_signals SET tp1_hit=1 WHERE id=?", (sig_id,))
            conn.commit()
            if tg_message_id:
                edit_tg(tg_message_id,
                        _build_signal_edit_tp1(sig_id, symbol, dire, entry, sl,
                                               tp1, tp2, signal_grade, tp1, tp1_pnl))
            send_tg(
                f"✅ <b>{symbol} {dire} — TP1 HIT!</b>  {tp1_pnl:+.1f}%\n"
                f"Entry: {entry} → TP1: {tp1}\n"
                f"🔒 SL moved to breakeven ({entry}) — trade is now risk-free\n"
                f"🎯 Still running for TP2: {tp2}",
                reply_to_message_id=tg_message_id or None
            )
            print(f"[Grade] #{sig_id} {symbol} TP1 HIT {tp1_pnl:+.2f}% — keeping open for TP2")

        # ── Final close (TP2 / SL / Breakeven after TP1) ─────────────
        elif outcome:
            # Use the exact target level as exit price, not the live tick
            if "WIN_TP2" in outcome:
                exit_price = tp2
            elif "WIN_TP1" in outcome:
                exit_price = tp1
            else:
                exit_price = price   # SL: use actual live price

            c.execute("""UPDATE autonomous_signals
                         SET outcome=?,outcome_price=?,outcome_time=?,pnl_pct=?
                         WHERE id=?""",
                      (outcome, exit_price, now_utc(), pnl, sig_id))
            conn.commit()

            # Edit the original signal message with closed status (silent update)
            if tg_message_id:
                edit_tg(tg_message_id,
                        _build_signal_edit_final(sig_id, symbol, dire, entry,
                                                 outcome, exit_price, pnl, signal_grade))

            # Build mini running stats for the reply notification
            c.execute("""SELECT outcome, pnl_pct, tp1_hit, entry, tp1 FROM autonomous_signals""")
            all_sig_rows  = c.fetchall()
            closed_rows   = [r for r in all_sig_rows if r[0] not in ('OPEN', None)]
            open_banked_r = [r for r in all_sig_rows if r[0] == 'OPEN' and r[2] == 1]
            wins_cnt      = sum(1 for r in closed_rows if "WIN" in (r[0] or "")) + len(open_banked_r)
            losses_cnt    = sum(1 for r in closed_rows if r[0] == "LOSS")
            expiry_cnt    = sum(1 for r in closed_rows if r[0] == "EXPIRED")
            total_decided = wins_cnt + losses_cnt
            win_rate      = round(wins_cnt / max(total_decided, 1) * 100, 1)
            banked_pnl_r  = sum(round((r[4]-r[3])/r[3]*100,2) for r in open_banked_r if r[3] and r[4])
            closed_pnl_r  = sum(r[1] for r in closed_rows if r[1] and ("WIN" in (r[0] or "") or r[0]=="LOSS"))
            total_pnl     = round(closed_pnl_r + banked_pnl_r, 2)
            _sep = "\u2500" * 28
            stats_line = (f"\n{_sep}\n\U0001f4ca Bot Record: <b>{wins_cnt}W \u00b7 {losses_cnt}L \u00b7 "
                          f"{expiry_cnt} exp \u00b7 {win_rate}% WR \u00b7 {total_pnl:+.2f}% net</b>")

            if "WIN_TP2" in outcome:
                emoji, outcome_label = "✅✅", "TP2 HIT"
            elif "WIN_TP1" in outcome:
                emoji, outcome_label = "✅", "TP1 HIT (BE exit)"
            else:
                emoji, outcome_label = "❌", "STOPPED OUT"

            # Reply notification so members get an alert
            send_tg(
                f"{emoji} <b>{symbol} {dire} — {outcome_label}</b> {pnl:+.1f}%\n"
                f"Entry: {entry} → Exit: {exit_price} | {now_utc()}"
                f"{stats_line}",
                reply_to_message_id=tg_message_id or None
            )
            print(f"[Grade] #{sig_id} {symbol} {outcome} {pnl:+.2f}%")
        else:
            # ── Halfway-point warning (signal still OPEN, no TP/SL hit) ──────
            try:
                fired_dt = datetime.strptime(fired_at, "%Y-%m-%d %H:%M UTC").replace(tzinfo=timezone.utc)
                age_hrs  = (datetime.now(timezone.utc) - fired_dt).total_seconds() / 3600
                halfway  = MAX_SIGNAL_AGE_HRS / 2
                if age_hrs >= halfway and not warned_at:
                    remaining_hrs = MAX_SIGNAL_AGE_HRS - age_hrs
                    age_str       = f"{age_hrs:.0f}h"
                    rem_str       = f"{remaining_hrs:.0f}h"
                    warn_msg = (
                        f"⚠️ <b>{symbol} {dire} — halfway warning</b>\n"
                        f"Signal has been open for <b>{age_str}</b> — "
                        f"expires in <b>{rem_str}</b>\n"
                        f"Entry: {entry} | TP1: {tp1} | TP2: {tp2} | SL: {sl}"
                    )
                    sent_id = send_tg(warn_msg, reply_to_message_id=tg_message_id or None)
                    if sent_id:
                        c.execute("UPDATE autonomous_signals SET warned_at=? WHERE id=?",
                                  (now_utc(), sig_id))
                        conn.commit()
                        print(f"[Grade] #{sig_id} {symbol} halfway warning sent (age={age_str}, rem={rem_str})")
                    else:
                        print(f"[Grade] #{sig_id} {symbol} halfway warning send failed — will retry next cycle")
            except Exception:
                pass
        time.sleep(0.3)

    conn.close()

def price_monitor_loop():
    while True:
        try:
            grade_open_signals()
        except Exception as e:
            print(f"[Monitor] {e}")
        time.sleep(PRICE_CHECK_INTERVAL)


# ══════════════════════════════════════════════════════════════════
# DAILY AUTO-RECAP  (fires once per day at midnight UTC)
# ══════════════════════════════════════════════════════════════════
_last_recap_date: str = ""

def daily_recap_loop():
    global _last_recap_date
    while True:
        try:
            now_utc_dt = datetime.now(timezone.utc)
            today_str  = now_utc_dt.strftime("%Y-%m-%d")
            # Fire once at midnight UTC (00:00–00:02 window to avoid double-fire)
            if now_utc_dt.hour == 0 and now_utc_dt.minute < 2 and today_str != _last_recap_date:
                _last_recap_date = today_str
                recap = build_stats_report(
                    header=f"📅 <b>Daily Alpha Bot Recap — {today_str}</b>"
                )
                # Daily recap → Auto Signal Bot chat
                send_tg(recap, chat_id=AUTO_SIGNAL_CHAT_ID)
                print(f"[DailyRecap] Sent recap for {today_str}")
        except Exception as e:
            print(f"[DailyRecap] {e}")
        time.sleep(60)  # check every minute

# ══════════════════════════════════════════════════════════════════
# STATS REPORTS
# ══════════════════════════════════════════════════════════════════
def build_stats_report(header="📊 <b>AUTO SIGNAL STATS</b>"):
    conn = db_conn()
    c    = conn.cursor()
    c.execute("""SELECT outcome, pnl_pct, signal_grade, symbol, direction, tp1_hit, entry, tp1
                 FROM autonomous_signals""")
    all_rows = c.fetchall()
    c.execute("""SELECT id, symbol, direction, signal_grade, outcome, pnl_pct, tp1_hit
                 FROM autonomous_signals WHERE outcome='OPEN' ORDER BY id""")
    recent_rows = c.fetchall()
    conn.close()

    if not all_rows:
        return "📊 No signals recorded yet."

    closed_rows = [r for r in all_rows if r[0] != "OPEN"]
    # Open signals where TP1 was already banked — count as wins at TP1 P&L
    open_banked = [r for r in all_rows if r[0] == "OPEN" and r[5] == 1]
    open_ct     = sum(1 for r in all_rows if r[0] == "OPEN")

    if not closed_rows and not open_banked:
        return f"📊 No completed signals yet. ({open_ct} open)"

    losses  = [r for r in closed_rows if r[0] == "LOSS"]
    expired = [r for r in closed_rows if r[0] == "EXPIRED"]
    wins_closed = [r for r in closed_rows if "WIN" in (r[0] or "")]
    tp1_w   = [r for r in wins_closed if "TP1" in (r[0] or "")] + open_banked
    tp2_w   = [r for r in wins_closed if "TP2" in (r[0] or "")]

    total_wins   = len(wins_closed) + len(open_banked)
    total_decided = total_wins + len(losses)
    win_rate  = round(total_wins / max(total_decided, 1) * 100, 1)

    # P&L: closed wins/losses + banked TP1 P&L for still-open signals
    banked_pnl = sum(
        round((r[7] - r[6]) / r[6] * 100, 2)
        for r in open_banked if r[6] and r[7]
    )
    closed_pnl = sum(r[1] for r in closed_rows if r[1] and ("WIN" in (r[0] or "") or r[0] == "LOSS"))
    total_pnl  = round(closed_pnl + banked_pnl, 2)

    all_wins = wins_closed + open_banked
    avg_win  = round(
        (sum(r[1] for r in wins_closed if r[1]) + banked_pnl) / max(total_wins, 1), 2
    )
    avg_loss = round(sum(r[1] for r in losses if r[1]) / max(len(losses), 1), 2)

    pnl_rows = closed_rows + [
        (r[0], round((r[7]-r[6])/r[6]*100,2) if r[6] and r[7] else 0,
         r[2], r[3], r[4], r[5], r[6], r[7])
        for r in open_banked
    ]
    best  = max(pnl_rows, key=lambda r: r[1] or -999)
    worst = min(pnl_rows, key=lambda r: r[1] or  999)

    # Per-grade win-rate breakdown
    by_grade: dict = defaultdict(lambda: {"w": 0, "l": 0, "e": 0})
    for r in closed_rows:
        g = (r[2] or "?")[0]
        if "WIN"  in (r[0] or ""):  by_grade[g]["w"] += 1
        elif r[0] == "LOSS":        by_grade[g]["l"] += 1
        elif r[0] == "EXPIRED":     by_grade[g]["e"] += 1
    for r in open_banked:
        g = (r[2] or "?")[0]
        by_grade[g]["w"] += 1
    grade_lines = []
    for g in ["S", "A", "B", "C", "?"]:
        v = by_grade.get(g)
        if not v:
            continue
        cl = v["w"] + v["l"]
        wr = round(v["w"] / cl * 100) if cl else 0
        exp_part = f" · {v['e']} exp" if v["e"] else ""
        grade_lines.append(f"  Grade {g}: {v['w']}W · {v['l']}L{exp_part} → <b>{wr}% WR</b>")
    grade_block = "\n".join(grade_lines) or "  —"

    # Open signals list
    rec_lines = []
    for row in recent_rows:
        sig_id, sym, dire, grade, out, pnl, tp1h = row
        if tp1h:
            emoji   = "🔒"
            out_str = "TP1 BANKED — running for TP2"
            pnl_str = "—"
        else:
            out_str = out or "OPEN"
            pnl_str = f"{pnl:+.2f}%" if pnl else "—"
            emoji   = "⏳"
        rec_lines.append(f"  {emoji} {sym} {dire} ({grade}) → {out_str} {pnl_str}")
    recent_block = "\n".join(rec_lines) or "  —"

    return (
        f"{header}\n"
        f"{'─'*30}\n"
        f"<b>{total_wins}W · {len(losses)}L · {len(expired)} exp · {win_rate}% WR</b>\n"
        f"Wins: <b>{total_wins}</b> (TP1: {len(tp1_w)} · TP2: {len(tp2_w)}) | "
        f"Losses: <b>{len(losses)}</b> | Expired: <b>{len(expired)}</b>\n"
        f"🏆 Win Rate: <b>{win_rate}%</b> (W+L only) | ✅ Avg Win: <b>+{avg_win}%</b> | "
        f"🔴 Avg Loss: <b>{avg_loss}%</b>\n"
        f"📈 Total P&L: <b>{total_pnl:+.2f}%</b>\n"
        f"🥇 Best: <b>{best[3]} {best[1]:+.2f}%</b> | "
        f"🪦 Worst: <b>{worst[3]} {worst[1]:+.2f}%</b>\n"
        f"{'─'*30}\n"
        f"📈 Grade Accuracy:\n{grade_block}\n"
        f"{'─'*30}\n"
        f"📡 Open Signals ({open_ct}):\n{recent_block}\n"
        f"⏰ {now_utc()}"
    )

def build_open_signals_report():
    conn = db_conn()
    c    = conn.cursor()
    c.execute("""SELECT id,symbol,direction,entry,sl,tp1,tp2,signal_grade,fired_at,category
                 FROM autonomous_signals WHERE outcome='OPEN'""")
    rows = c.fetchall()
    conn.close()

    if not rows: return "📭 No open signals."

    lines = [f"📡 <b>OPEN SIGNALS ({len(rows)})</b>\n"]
    for r in rows:
        sig_id, sym, dire, entry, sl, tp1, tp2, grade, fired, cat = r
        emoji = "🟢" if dire == "LONG" else "🔴"
        cur   = fetch_current_price(WATCHLIST.get(sym, {}).get("binance","")) or 0
        live_pnl = pct(entry, cur) if dire == "LONG" else pct(cur, entry)
        lines.append(
            f"{emoji} <b>#{sig_id} {sym} {dire}</b> | Grade: {grade}\n"
            f"  Entry: {entry} | Now: {cur} | P&L: {live_pnl:+.2f}%\n"
            f"  SL: {sl} | TP1: {tp1} | TP2: {tp2}\n"
            f"  Fired: {fired}\n"
        )
    return "\n".join(lines)

# ══════════════════════════════════════════════════════════════════
# MAIN SCANNER
# ══════════════════════════════════════════════════════════════════
def run_scanner():
    global signals_today, last_signal_time

    # ── Pause file check ─────────────────────────────────────────────
    try:
        if os.path.exists(PAUSE_FILE):
            with open(PAUSE_FILE) as _pf:
                pause_until_ts = float(_pf.read().strip())
            remaining = pause_until_ts - time.time()
            if remaining > 0:
                hrs = remaining / 3600
                print(f"[Scanner] PAUSED — {hrs:.1f}h remaining (until "
                      f"{datetime.fromtimestamp(pause_until_ts, tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')})")
                return
            else:
                os.remove(PAUSE_FILE)
                print("[Scanner] Pause expired — resuming normal operation.")
    except Exception as _pe:
        print(f"[Scanner] Pause file error (ignored): {_pe}")

    reset_daily_if_needed()

    if signals_today >= MAX_SIGNALS_PER_DAY:
        print(f"[Scanner] Daily cap reached ({MAX_SIGNALS_PER_DAY}). Skipping.")
        return

    # ── BTC macro regime — one fetch per scan cycle ─────────────────
    global _prev_btc_regime
    btc_regime = get_btc_regime()
    print(f"\n[Scanner] BTC regime: {btc_regime.upper()} | "
          f"Signals today: {signals_today}/{MAX_SIGNALS_PER_DAY}")

    # ── Regime transition alert ───────────────────────────────────────
    # Fire a Telegram notification when BTC shifts between regimes.
    _was_ranging  = _prev_btc_regime in ("ranging", "")
    _now_ranging  = btc_regime == "ranging"
    _was_trending = _prev_btc_regime in ("uptrend", "downtrend")
    _now_trending = btc_regime in ("uptrend", "downtrend")

    if _was_ranging and _now_trending and _prev_btc_regime != "":
        # Unlocked: was ranging, now has a clear direction
        emoji = "🟢" if btc_regime == "uptrend" else "🔴"
        label = "UPTREND — LONGs active" if btc_regime == "uptrend" else "DOWNTREND — SHORTs active"
        send_tg(
            f"{emoji} <b>BTC regime change: {label}</b>\n\n"
            f"BTC has broken out of a ranging phase → <b>{btc_regime.upper()}</b>.\n\n"
            f"🤖 Bot is now <b>ACTIVE</b> — scanning for high-conviction signals.\n"
            f"Next scan in ~{SCAN_INTERVAL // 60} min.",
            chat_id=TELEGRAM_CHAT_ID
        )
        print(f"[Regime Alert] Unlocked: {_prev_btc_regime} → {btc_regime}")

    elif _was_trending and _now_ranging:
        # Locked: was trending, now gone choppy
        send_tg(
            f"⚪ <b>BTC regime change: RANGING</b>\n\n"
            f"BTC has shifted from <b>{_prev_btc_regime.upper()}</b> → <b>RANGING</b>.\n\n"
            f"🤖 Bot is <b>STANDING ASIDE</b> — BTC EMAs are tightly coiled. "
            f"No signals until a clear direction develops.",
            chat_id=TELEGRAM_CHAT_ID
        )
        print(f"[Regime Alert] Locked: {_prev_btc_regime} → {btc_regime}")

    elif _was_trending and _now_trending and _prev_btc_regime != btc_regime:
        # Flip: uptrend → downtrend or vice versa
        send_tg(
            f"🔄 <b>BTC regime flip: {_prev_btc_regime.upper()} → {btc_regime.upper()}</b>\n\n"
            f"Macro direction has reversed. Bot now scans for "
            f"{'LONGs' if btc_regime == 'uptrend' else 'SHORTs'} only.\n"
            f"Open signals in the opposite direction will be monitored for early exit.",
            chat_id=TELEGRAM_CHAT_ID
        )
        print(f"[Regime Alert] Flip: {_prev_btc_regime} → {btc_regime}")

    _prev_btc_regime = btc_regime

    if btc_regime == "ranging":
        print(f"[Scanner] BTC is RANGING (EMAs coiled) — standing aside.")
        return

    candidates = []

    for symbol, token_data in WATCHLIST.items():
        try:
            # BTC & ETH are in WATCHLIST for onchain data only — never fire signals
            if symbol in SIGNAL_EXCLUDED:
                continue

            print(f"  [{symbol}] Analysing...", end=" ", flush=True)
            # Per-token 45-second hard timeout — any stalled API call inside
            # analyse_token (Gate.io klines, onchain RPC, etc.) cannot freeze
            # the entire scan. Use shutdown(wait=False) so the executor does
            # NOT block waiting for the hung thread when a TimeoutError fires.
            # Using `with ThreadPoolExecutor()` would call shutdown(wait=True)
            # on exit even after TimeoutError, defeating the purpose entirely.
            _tex = concurrent.futures.ThreadPoolExecutor(max_workers=1)
            try:
                _fut = _tex.submit(analyse_v2_token, symbol, token_data)
                result = _fut.result(timeout=45)
            except concurrent.futures.TimeoutError:
                print(f"timeout — skipping")
                _tex.shutdown(wait=False)
                continue
            except Exception as _te:
                print(f"error ({_te}) — skipping")
                _tex.shutdown(wait=False)
                continue
            _tex.shutdown(wait=False)

            if result is None:
                # Covers: structure gate, liquidity gate, order flow gate,
                # overextension block, equal conviction, thin data
                print("filtered (structure/liquidity/orderflow/overextended)")
                continue

            score = result["internal_score"]
            rsi_v = result.get("rsi", 0)
            s1h   = result.get("mkt_structure",    "?")
            s4h   = result.get("mkt_structure_4h", "?")
            print(f"{result['direction']} score={score} "
                  f"RSI={rsi_v:.0f} ADX={result.get('adx','?')} "
                  f"struct=1H:{s1h}/4H:{s4h} "
                  f"fvg={result.get('in_fvg','?')} ob={result.get('at_ob','?')} "
                  f"cvd={result.get('cvd_bias','?')} "
                  f"pullback={result.get('is_pullback','?')}")

            if score < INTERNAL_MIN_SCORE:
                continue

            # V2 already applies the complete five-condition futures gate.
            # Do not apply legacy trend/regime/order-flow gates to V2 setups.
            if result.get("signal_logic_v2"):
                candidates.append(result)
                continue

            t4, t1, dire = result["trend_4h"], result["trend_1h"], result["direction"]

            # ── Rule 1: 4H must not oppose the direction ─────────────
            if (dire == "LONG"  and t4 == "downtrend") or \
               (dire == "SHORT" and t4 == "uptrend"):
                print(f"  [{symbol}] Skipped — 4H conflicts direction")
                continue

            # ── Rule 1.5: Block token-level correction LONGs ─────────────
            # Even in a BTC uptrend, if the token's own 4H price has dropped
            # below its EMA-20 (pullback/distribution zone), a LONG entry here
            # is buying into weakness. Require BTC to be clearly bullish first.
            # Note: ranging/neutral/downtrend regimes already block LONGs via
            # Rule 3, so this only fires in the uptrend case.
            if dire == "LONG" and result.get("price_below_4h_e20"):
                print(f"  [{symbol}] Skipped — token 4H price below E20 (correction zone)")
                continue

            # ── Rule 2: 1H must not oppose the direction ─────────────
            if dire == "LONG" and t1 == "downtrend":
                print(f"  [{symbol}] Skipped — 1H downtrend blocks LONG")
                continue

            if dire == "SHORT" and t1 == "uptrend":
                # Exception: overbought extension SHORT is valid even when
                # 1H EMAs are still bullishly stacked — if RSI is already
                # elevated AND RSI direction is falling (momentum rolling over),
                # price is extended above the mean and losing steam.
                # This is a high-probability mean-reversion SHORT setup.
                rsi_1h   = result["rsi"]
                rsi_fall = result.get("rsi_falling", False)
                ext_4h   = result.get("price_above_4h_e20_ext", False)
                overbought_ext = (rsi_1h > 68 and rsi_fall) or (rsi_1h > 72 and ext_4h)
                if not overbought_ext:
                    print(f"  [{symbol}] Skipped — 1H uptrend blocks SHORT "
                          f"(RSI={rsi_1h:.0f}, rsi_falling={rsi_fall}, ext_4h={ext_4h})")
                    continue
                print(f"  [{symbol}] SHORT allowed in 1H uptrend — "
                      f"overbought extension (RSI={rsi_1h:.0f}, ext_4h={ext_4h})")

            # ── Rule 3: Regime-directional gate (bidirectional) ──────────
            #
            # The bot trades WITH the macro trend by default.
            # Exception: tokens showing INDEPENDENT price action may trade
            # against the regime when they have confirmed opposing structure.
            #
            # "Independent" = BTC correlation < 0.55 (token is NOT just
            # following BTC), AND the token's own market structure is
            # clearly moving opposite to the regime.
            #
            # This allows genuine alpha setups in both directions while
            # avoiding low-conviction counter-trend trades.
            #
            btc_corr_rt   = result.get("btc_corr")
            mkt_struct_rt = result.get("mkt_structure", "neutral")
            is_independent_rt = (btc_corr_rt is not None and btc_corr_rt < 0.55)

            if btc_regime == "uptrend" and dire == "SHORT":
                if is_independent_rt and mkt_struct_rt == "bearish":
                    print(f"  [{symbol}] SHORT in uptrend ALLOWED — independent mover "
                          f"(corr={btc_corr_rt:.2f}) with bearish structure")
                else:
                    print(f"  [{symbol}] Skipped — BTC bullish: SHORT needs independent "
                          f"token (corr<0.55) + bearish structure "
                          f"(corr={btc_corr_rt}, struct={mkt_struct_rt})")
                    continue

            if btc_regime == "downtrend" and dire == "LONG":
                if is_independent_rt and mkt_struct_rt == "bullish":
                    print(f"  [{symbol}] LONG in downtrend ALLOWED — independent mover "
                          f"(corr={btc_corr_rt:.2f}) with bullish structure")
                else:
                    print(f"  [{symbol}] Skipped — BTC bearish: LONG needs independent "
                          f"token (corr<0.55) + bullish structure "
                          f"(corr={btc_corr_rt}, struct={mkt_struct_rt})")
                    continue

            candidates.append(result)
            time.sleep(0.3)

        except Exception as e:
            print(f"  [{symbol}] Error: {e}")
            continue

    if not candidates:
        print("[Scanner] No candidates passed internal filter.")
        return

    candidates.sort(key=lambda x: x["internal_score"], reverse=True)
    print(f"\n[Scanner] {len(candidates)} candidate(s) above threshold.")

    # ── Correlation cap: max 2 open LONG or SHORT positions ─────────────
    # If there are already ≥2 open signals in the same direction, block new
    # signals in that direction unless the token shows low BTC correlation
    # (independent mover). This prevents over-concentration in one direction.
    def count_open_by_direction():
        try:
            with db_conn() as _c:
                rows = _c.execute(
                    "SELECT direction, COUNT(*) FROM autonomous_signals "
                    "WHERE outcome='OPEN' GROUP BY direction"
                ).fetchall()
            return {r[0]: r[1] for r in rows}
        except Exception:
            return {}

    open_counts = count_open_by_direction()

    for result in candidates:
        if signals_today >= MAX_SIGNALS_PER_DAY:
            break

        if time.time() - last_signal_time < MIN_SIGNAL_GAP_SECS:
            mins = (MIN_SIGNAL_GAP_SECS - (time.time() - last_signal_time)) / 60
            print(f"  [{result['symbol']}] Signal gap: {mins:.0f} min remaining")
            continue

        sym        = result["symbol"]
        token_data = WATCHLIST[sym]
        dire       = result["direction"]

        # ── Duplicate guard: never re-fire a signal that is still OPEN ──
        if has_open_signal(sym):
            print(f"  [{sym}] SKIPPED — open signal already exists (restart guard)")
            continue

        # ── Correlation cap: max 2 open in same direction ────────────────
        # Exception: independent movers (btc_corr < 0.60) bypass the cap
        btc_corr_val = result.get("btc_corr")
        is_independent = btc_corr_val is not None and btc_corr_val < 0.60
        same_dir_open  = open_counts.get(dire, 0)
        if same_dir_open >= 2 and not is_independent:
            print(f"  [{sym}] SKIPPED — correlation cap: {same_dir_open} open {dire}s, "
                  f"token correlates with BTC ({btc_corr_val})")
            continue

        # ── Orderbook imbalance check (advisory, not a hard gate) ───────────
        # 20-level depth almost never reaches 1.2× imbalance on liquid markets
        # because market makers actively balance both sides. Using it as a hard
        # block killed every candidate. It now logs a warning but never rejects.
        ob_ok = fetch_orderbook_imbalance(token_data["binance"], dire, levels=20)
        if ob_ok is False:
            print(f"  [{sym}] ⚠️  Orderbook depth thin on {dire} side — proceeding (advisory)")
        elif ob_ok is None:
            print(f"  [{sym}] Orderbook unavailable — proceeding")
        else:
            print(f"  [{sym}] Orderbook depth ✓")

        print(f"  [{sym}] Checking onchain (ETH + BSC + SOL)...")
        eth_res, bsc_res, sol_res, onchain_combined = check_onchain_all(token_data)
        print(f"  [{sym}] ETH:{eth_res} BSC:{bsc_res} SOL:{sol_res} → {onchain_combined}")

        # ── Onchain data (advisory, not a hard gate) ──────────────────────
        # Many Gate.io tokens have no ETH/BSC/SOL DexScreener coverage.
        # Blocking on "no chain data" was killing valid TA setups.
        # Onchain still flips direction when ≥2 chains agree — see below.
        all_no_data = all(v == "no_data" for v in [eth_res, bsc_res, sol_res])
        if all_no_data:
            print(f"  [{sym}] No onchain data — proceeding on TA only")

        onchain_flip     = False
        onchain_flip_str = ""
        if (dire == "LONG"  and onchain_combined == "distribution") or \
           (dire == "SHORT" and onchain_combined == "accumulation"):
            # ── Conservative override rule ────────────────────────────────────
            # TA remains primary. Onchain flips direction ONLY when:
            #   1. At least 2 chains independently agree on the flow direction
            #      (prevents a single stale or noisy source from overriding TA)
            #   2. The combined signal is unambiguous (accumulation/distribution,
            #      not "mixed" or "neutral")
            #   3. The flip is bidirectional: works LONG→SHORT and SHORT→LONG
            # If data is insufficient or only 1 chain agrees, skip the signal
            # entirely rather than trade against a weak onchain signal.
            # ─────────────────────────────────────────────────────────────────
            flip_target = "accumulation" if onchain_combined == "accumulation" else "distribution"
            chains_agreeing = [v for v in [eth_res, bsc_res, sol_res]
                               if v == flip_target]
            if len(chains_agreeing) >= 2:
                old_dir = dire
                new_dir = "LONG" if onchain_combined == "accumulation" else "SHORT"
                result["direction"] = new_dir
                dire                = new_dir
                onchain_flip        = True
                entry  = result["entry"]
                if new_dir == "LONG":
                    result["sl"]  = sig_round(entry * (1 - SL_PCT),      6)
                    result["tp1"] = sig_round(entry * (1 + TP1_MIN_PCT), 6)
                    result["tp2"] = sig_round(entry * (1 + TP2_MIN_PCT), 6)
                else:
                    result["sl"]  = sig_round(entry * (1 + SL_PCT),      6)
                    result["tp1"] = sig_round(entry * (1 - TP1_MIN_PCT), 6)
                    result["tp2"] = sig_round(entry * (1 - TP2_MIN_PCT), 6)
                onchain_flip_str = f"{old_dir}→{new_dir}"
                result["confluences"].insert(
                    0, f"⚡ Onchain override: {onchain_combined} ({len(chains_agreeing)}/3 chains) flipped {onchain_flip_str}"
                )
                print(f"  [{sym}] ⚡ ONCHAIN FLIP {onchain_flip_str} "
                      f"({len(chains_agreeing)}/3 chains agree, onchain={onchain_combined})")
            else:
                # Only 0 or 1 chain agrees — data too weak/stale to override TA.
                # Follow the original technical setup direction unchanged.
                print(f"  [{sym}] Onchain contradicts TA but only {len(chains_agreeing)}/3 "
                      f"chains agree — following TA direction (no stale override)")

        print(f"  [{sym}] Checking derivatives...")
        funding, oi_change, deriv_bias = check_derivatives(token_data["binance"])

        if deriv_bias == "warning_long_crowded"  and dire == "LONG":
            print(f"  [{sym}] Rejected — longs overcrowded")
            continue
        if deriv_bias == "warning_short_crowded" and dire == "SHORT":
            print(f"  [{sym}] Rejected — shorts overcrowded")
            continue

        # DeepSeek primary → Gemini fallback
        gemini_score, gemini_summary = None, None
        if DEEPSEEK_API_KEY:
            print(f"  [{sym}] DeepSeek scoring...")
            gemini_score, gemini_summary = deepseek_score_and_summarise(
                result, eth_res, bsc_res, sol_res,
                onchain_combined, funding, oi_change, deriv_bias
            )
            if gemini_score is None:
                print(f"  [{sym}] DeepSeek failed, trying Gemini...")
        if gemini_score is None:
            print(f"  [{sym}] Gemini scoring...")
            gemini_score, gemini_summary = gemini_score_and_summarise(
                result, eth_res, bsc_res, sol_res,
                onchain_combined, funding, oi_change, deriv_bias
            )

        if gemini_score is None:
            # Both AI services failed — fall back to internal score rather than
            # dropping a signal that already passed every technical filter.
            raw_int = result["internal_score"]
            # Map internal score (0-100) → AI-equivalent scale with a small haircut
            # so signals that rely on the fallback are slightly more conservative.
            gemini_score   = max(0, min(100, int(raw_int * 0.92)))
            gemini_summary = (
                f"AI scoring unavailable (API error) — internal technical score: "
                f"{raw_int}/100. Setup passed all filters autonomously."
            )
            print(f"  [{sym}] AI unavailable — using internal score fallback: {gemini_score}")

        if gemini_score < MIN_SIGNAL_SCORE:
            print(f"  [{sym}] skipped — AI score too low ({gemini_score} < {MIN_SIGNAL_SCORE})")
            continue

        mtf_aligned  = result["trend_4h"] == result["trend_1h"] != "neutral"
        signal_grade = assign_signal_grade(
            gemini_score, onchain_combined, deriv_bias,
            mtf_aligned, len(result["patterns"])
        )

        # Cross-bot conflict check — block if NDF Bot has an opposite open signal
        if check_signal_conflict(sym, dire):
            print(f"  [{sym}] BLOCKED — NDF Bot has conflicting open signal")
            continue

        sig_id = log_signal(
            result, eth_res, bsc_res, sol_res, onchain_combined,
            funding, oi_change, deriv_bias,
            gemini_score, gemini_summary, signal_grade
        )

        tg_msg_id = fire_signal(
            result, eth_res, bsc_res, sol_res, onchain_combined,
            funding, oi_change, deriv_bias,
            gemini_score, gemini_summary, signal_grade, sig_id,
            onchain_flip=onchain_flip, onchain_flip_str=onchain_flip_str
        )

        # ── Gate: only keep signals that were actually delivered ──────────
        # If Telegram did not return a message_id the post failed — roll back
        # the DB row so the signal never enters the grading/tracking system.
        if not tg_msg_id:
            try:
                conn_tmp = db_conn()
                conn_tmp.execute("DELETE FROM autonomous_signals WHERE id=?", (sig_id,))
                conn_tmp.commit()
                conn_tmp.close()
            except Exception as e:
                print(f"[DB] Rollback failed for #{sig_id}: {e}")
            print(f"  [{sym}] ⚠️ Signal #{sig_id} discarded — "
                  f"Telegram post failed (no message_id returned)")
            continue   # do NOT increment counter or write lock

        # Telegram confirmed — persist the message_id
        try:
            conn_tmp = db_conn()
            conn_tmp.execute(
                "UPDATE autonomous_signals SET tg_message_id=? WHERE id=?",
                (tg_msg_id, sig_id)
            )
            conn_tmp.commit()
            conn_tmp.close()
        except Exception as e:
            print(f"[DB] Failed to store tg_message_id: {e}")

        # Write to shared lock — includes rich data so NDF Bot can align its response
        set_signal_lock(
            sym, dire, "alpha",
            entry   = result.get("entry",  result.get("close", 0)),
            tp1     = result.get("tp1",    0),
            tp2     = result.get("tp2",    0),
            sl      = result.get("sl",     0),
            grade   = signal_grade,
            score   = gemini_score,
            summary = gemini_summary or "",
        )

        signals_today   += 1
        last_signal_time = time.time()
        # Keep open_counts fresh so the correlation cap is accurate for the
        # next candidate in the same scan cycle (was stale before this fix).
        open_counts[dire] = open_counts.get(dire, 0) + 1

        print(f"  ✅ [{sym}] Signal #{sig_id} fired | Grade:{signal_grade} | "
              f"Gemini:{gemini_score} | {dire}")

        if signals_today < MAX_SIGNALS_PER_DAY:
            time.sleep(2)

# ══════════════════════════════════════════════════════════════════
# COMMAND POLLING LOOP  (background thread)
# ══════════════════════════════════════════════════════════════════
def command_loop():
    while True:
        try:
            poll_tg_commands()
        except Exception as e:
            print(f"[CmdLoop] {e}")
        time.sleep(10)

# ══════════════════════════════════════════════════════════════════
# AUTO DAILY STATS
# ══════════════════════════════════════════════════════════════════
def _post_daily_stats_if_needed():
    """Post daily stats at the first scan after UTC midnight."""
    global last_daily_stats_day
    utc_today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if utc_today == last_daily_stats_day:
        return                  # already posted today
    last_daily_stats_day = utc_today
    try:
        report = build_stats_report(
            header=f"📅 <b>Daily Alpha Bot Recap — {utc_today}</b>"
        )
        # Daily stats → Auto Signal Bot chat
        send_tg(report, chat_id=AUTO_SIGNAL_CHAT_ID)
        print(f"[Stats] Daily auto-post sent for {utc_today}")
    except Exception as e:
        print(f"[Stats] Auto-post failed: {e}")


# ══════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════
def main():
    print("=" * 60)
    print("  AUTONOMOUS SIGNAL BOT — starting up")
    print(f"  Watching {len(WATCHLIST)} crypto tokens")
    print(f"  Scan interval: {SCAN_INTERVAL//60} min  |  DB: {DB_PATH}")
    print("  Logic: V2 futures extremes — funding · OI · RSI · taker flow · L/S ratio")
    print("         3 of 5 conditions minimum · $10M+ volume · confidence 60+")
    print("=" * 60)

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[WARN] AUTO_SIGNAL_BOT_TOKEN or AUTO_SIGNAL_CHAT_ID not set — "
              "signals will not be sent.")
    else:
        # Startup message → Auto Signal Bot chat
        send_tg(
            "🤖 <b>Autonomous Signal Bot online</b>\n"
            f"📊 Watching <b>{len(WATCHLIST)} tokens</b> · Max {MAX_SIGNALS_PER_DAY} signals/day\n"
            f"🔒 Min score: <b>{MIN_SIGNAL_SCORE}</b> · Gap: <b>{MIN_SIGNAL_GAP_SECS//3600}h</b> · "
            f"AI: <b>DeepSeek → Gemini</b>\n"
            f"⏰ {now_utc()}",
            chat_id=AUTO_SIGNAL_CHAT_ID
        )

    init_db()
    _seed_state_from_db()

    monitor_thread = threading.Thread(target=price_monitor_loop, daemon=True)
    monitor_thread.start()

    recap_thread = threading.Thread(target=daily_recap_loop, daemon=True)
    recap_thread.start()

    cmd_thread = threading.Thread(target=command_loop, daemon=True)
    cmd_thread.start()

    # Run first scan immediately
    run_scanner()

    while True:
        time.sleep(SCAN_INTERVAL)
        try:
            _post_daily_stats_if_needed()
            run_scanner()
        except Exception as e:
            print(f"[Main] Scanner error: {e}")

if __name__ == "__main__":
    main()