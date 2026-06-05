#!/usr/bin/env python3
"""
Morning Briefing Generator v2 — Lightweight RSS + LLM Architecture
Fetches RSS feeds from central banks, financial media, and Asia-Pacific sources.
No browser scraping. All intelligence delegated to the LLM.
"""

import os
import sys
import time
import socket
import ssl
import re
import signal
import json
import multiprocessing as mp
import tempfile
import traceback
import smtplib
from email.message import EmailMessage
from html import escape
from difflib import SequenceMatcher
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutureTimeoutError
from datetime import datetime, timedelta, timezone
from pathlib import Path

import certifi
ssl._create_default_https_context = lambda: ssl.create_default_context(cafile=certifi.where())
SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

# Bypass macOS system proxy (not needed in HK; avoids Connection refused when Clash is off)
os.environ['no_proxy'] = '*'

import urllib.request
import feedparser
from anthropic import Anthropic

from briefing_runtime import env_int, write_json_artifact, write_text_artifact

# launchd captures stdout/stderr to files; line buffering keeps progress visible while a run is active.
try:
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)
except AttributeError:
    pass


def _load_env_from_claude_config():
    """Populate missing env vars from ~/.claude/settings.json.
    Ensures the script works when run from cron (which lacks Claude Code's env injection)."""
    config_path = Path.home() / ".claude" / "settings.json"
    if not config_path.exists():
        return
    with open(config_path) as f:
        import json
        config = json.load(f)
    env_overrides = config.get("env", {})
    for key, value in env_overrides.items():
        if key not in os.environ:
            os.environ[key] = value


_load_env_from_claude_config()

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# HTML-scraped sources (no RSS — we parse the page directly)
HTML_SOURCES = {
    "PBOC_News": "http://www.pbc.gov.cn/goutongjiaoliu/113456/113469/index.html",
    "HKMA_Press": "https://www.hkma.gov.hk/eng/key-information/press-releases/",
    "ChnFund_Macro": "https://www.chnfund.com/article/list?category=macro",
    "Xinhua_Fortune": "https://www.news.cn/fortune/index.htm",
    "NBSC_Releases": "https://www.stats.gov.cn/",
    "ChinaFinance_CN": "https://finance.china.com.cn/",
    "Caixin_Homepage": "https://www.caixin.com/",
}

TE_CALENDAR_URL = "https://tradingeconomics.com/calendar"

# Timezone map for Economic Calendar: country code -> IANA timezone
TE_TIMEZONES = {
    "US": "America/New_York",
    "GB": "Europe/London",
    "EZ": "Europe/Berlin", "DE": "Europe/Berlin", "FR": "Europe/Paris",
    "IT": "Europe/Rome", "ES": "Europe/Madrid",
    "CN": "Asia/Shanghai", "HK": "Asia/Hong_Kong",
    "JP": "Asia/Tokyo", "KR": "Asia/Seoul",
    "TW": "Asia/Taipei", "SG": "Asia/Singapore",
    "AU": "Australia/Sydney",
}


def _build_hkt_conversion_table():
    """Build a table of timezone offset to HKT for the current date (handles DST)."""
    from datetime import datetime as dt
    today = dt.now(LOCAL_TZ)
    lines = ["Timezone offsets to HKT (HKT = UTC+8):"]
    seen = set()
    for cc, tz_name in sorted(TE_TIMEZONES.items()):
        if tz_name in seen:
            continue
        seen.add(tz_name)
        try:
            from zoneinfo import ZoneInfo
            tz = ZoneInfo(tz_name)
            local_now = today.astimezone(tz)
            offset_hours = local_now.utcoffset().total_seconds() / 3600
            diff_to_hkt = 8 - offset_hours
            sign = "+" if diff_to_hkt >= 0 else ""
            utc_sign = "+" if offset_hours >= 0 else ""
            lines.append(f"  {tz_name} (UTC{utc_sign}{offset_hours:.0f}): add {sign}{diff_to_hkt:.0f}h → HKT")
        except Exception:
            pass
    return "\n".join(lines)

RSS_FEEDS = {
    # Official central bank & government sources
    "Fed_Press":            "https://www.federalreserve.gov/feeds/press_all.xml",
    "ECB_Press":            "https://www.ecb.europa.eu/rss/press.html",
    "SF_Fed":               "https://www.frbsf.org/feed/",
    "BOE_News":             "https://www.bankofengland.co.uk/rss/news",
    "BOE_Publications":     "https://www.bankofengland.co.uk/rss/publications",
    # BOJ/BOK/RBA/CBC have no direct RSS — Google News fallback
    "BOJ_GN":               "https://news.google.com/rss/search?q=site:boj.or.jp&hl=en-US&gl=US&ceid=US:en&num=20",
    "BOK_GN":               "https://news.google.com/rss/search?q=site:bok.or.kr&hl=en-US&gl=US&ceid=US:en&num=20",
    "RBA_GN":               "https://news.google.com/rss/search?q=site:rba.gov.au&hl=en-US&gl=US&ceid=US:en&num=20",
    "CBC_GN":               "https://news.google.com/rss/search?q=site:cbc.gov.tw&hl=en-US&gl=US&ceid=US:en&num=20",

    # Top-tier global financial media
    "BBG_Econ":       "https://feeds.bloomberg.com/economics/news.rss",
    "BBG_Markets":    "https://feeds.bloomberg.com/markets/news.rss",
    "CNBC_Markets":         "https://www.cnbc.com/id/10000664/device/rss/rss.html",
    "CNBC_World":           "https://www.cnbc.com/id/100727362/device/rss/rss.html",
    "CNBC_Economy":         "https://www.cnbc.com/id/20910258/device/rss/rss.html",
    "FT_Global_Econ":       "https://www.ft.com/global-economy?format=rss",
    "FT_Markets":           "https://www.ft.com/markets?format=rss",
    "FT_Asia":              "https://www.ft.com/asia-pacific?format=rss",
    "FT_Currencies":        "https://www.ft.com/currencies?format=rss",
    "BBC_Business":         "https://feeds.bbci.co.uk/news/business/rss.xml",
    "Economist":            "https://www.economist.com/finance-and-economics/rss.xml",

    # Asia-Pacific core media
    "SCMP_Econ":            "https://www.scmp.com/rss/91/feed",
    "SCMP_China_Econ":      "https://www.scmp.com/rss/4/feed",
    "SCMP_Asia":            "https://www.scmp.com/rss/3/feed",
    "KED_Global_KR":        "https://www.kedglobal.com/rss",

    # Singapore — MAS (no direct RSS), CNA, Business Times via Google News
    "MAS_GN":               "https://news.google.com/rss/search?q=site:mas.gov.sg&hl=en-US&gl=US&ceid=US:en&num=20",
    "CNA_GN":               "https://news.google.com/rss/search?q=site:channelnewsasia.com+business+OR+economy+OR+MAS+OR+SGD+OR+monetary&hl=en-US&gl=US&ceid=US:en&num=20",
    "BusinessTimes_GN":     "https://news.google.com/rss/search?q=site:businesstimes.com.sg&hl=en-US&gl=US&ceid=US:en&num=20",

    # Greater China macro (Chinese-language primary sources — via Google News)
    "WallstreetCN_GN":     "https://news.google.com/rss/search?q=site:wallstreetcn.com+macro+OR+央行+OR+汇率+OR+利率+OR+流动性&hl=zh-CN&gl=CN&ceid=CN:zh-Hans&num=50",
    "Caixin_GN":           "https://news.google.com/rss/search?q=site:caixin.com+economy+OR+%E5%AE%8F%E8%A7%82+OR+%E8%B4%A7%E5%B8%81+OR+%E8%B4%A2%E6%94%BF&hl=zh-CN&gl=CN&ceid=CN:zh-Hans&num=50",
    "CCTV_GN":             "https://news.google.com/rss/search?q=site:news.cctv.com+经济+OR+宏观+OR+央行+OR+财政+OR+贸易&hl=zh-CN&gl=CN&ceid=CN:zh-Hans&num=30",
    "HKEJ_GN":             "https://news.google.com/rss/search?q=site:hkej.com+金融+OR+经济+OR+汇率+OR+港股&hl=zh-HK&gl=HK&ceid=HK:zh-Hant&num=30",

    # Mainstream sentiment & geopolitics (CNN RSS dead since 2023 — use Google News)
    "CNN_GN":              "https://news.google.com/rss/search?q=site:cnn.com+business+OR+markets+OR+economy+OR+inflation+OR+recession+OR+tariff+OR+china+OR+oil+OR+jobs+OR+stock+market&hl=en-US&gl=US&ceid=US:en&num=50",

    # Macro aggregators & news wires
    "Yahoo_Finance":        "https://finance.yahoo.com/news/rssindex",
    "Investing_Forex":      "https://www.investing.com/rss/news_1.rss",
    "Reuters_GN":           "https://news.google.com/rss/search?q=site:reuters.com&hl=en-US&gl=US&ceid=US:en&num=100",
    "WSJ_GN":               "https://news.google.com/rss/search?q=site:wsj.com&hl=en-US&gl=US&ceid=US:en&num=100",

    # Geopolitics & War Monitor — hard military/intel sources for Iran/Middle East conflict
    # Note: CENTCOM blocks direct RSS bots; ISW has no stable RSS. Using Google News as fallback.
    "US_CENTCOM":           "https://news.google.com/rss/search?q=site:centcom.mil&hl=en-US&gl=US&ceid=US:en&num=20",
    "ISW_Assessments":      "https://news.google.com/rss/search?q=site:understandingwar.org&hl=en-US&gl=US&ceid=US:en&num=20",
    "AlJazeera_ME":         "https://www.aljazeera.com/xml/rss/all.xml",

    # Energy — official/authoritative sources for commodity supply shocks & shipping chokepoints
    # Note: IEA Cloudflare-protected, SP Global/Lloyd's List no public RSS. Using Google News fallback.
    "EIA_Press":            "https://www.eia.gov/rss/todayinenergy.xml",
    "IEA_News":             "https://news.google.com/rss/search?q=site:iea.org&hl=en-US&gl=US&ceid=US:en&num=20",
    "SP_Global_Commodities": "https://news.google.com/rss/search?q=site:spglobal.com+commodity+OR+oil+OR+shipping&hl=en-US&gl=US&ceid=US:en&num=20",
    "Lloyds_List_Shipping": "https://news.google.com/rss/search?q=site:lloydslist.com&hl=en-US&gl=US&ceid=US:en&num=20",
}

HIGH_PRIORITY_KEYWORDS = [
    "RMB", "yuan", "renminbi", "CNY", "CNH",
    "HKD", "Hong Kong dollar",
    "PBOC", "People's Bank of China", "PBoC",
    "KRW", "won", "Bank of Korea", "BOK",
    "TWD", "Taiwan dollar", "CBC", "Taiwan central bank",
    "SGD", "Singapore dollar", "MAS", "Monetary Authority of Singapore", "S$NEER",
]

MEDIUM_PRIORITY_KEYWORDS = [
    "Fed", "Federal Reserve", "ECB", "BOE", "BOJ",
    "emerging market", "Asia FX", "Asia currency",
    "trade war", "tariff", "sanction",
    "Korea", "Taiwan", "TSMC",
]

OUTPUT_DIR = Path(os.environ.get(
    "OBSIDIAN_OUTPUT_DIR",
    "/Users/sharonxu/Library/Mobile Documents/iCloud~md~obsidian/Documents/Macro Flux/02_Morning_Reports/News",
))
MODEL = os.environ.get("ANTHROPIC_MODEL", "deepseek-v4-pro")
if MODEL.lower().startswith("deepseek-v4-pro"):
    MODEL = "deepseek-v4-pro"
MAX_OUTPUT_TOKENS = 24576
FEED_TIMEOUT = 10  # seconds per feed
MAX_AGE_HOURS = 48
LOCAL_TZ = timezone(timedelta(hours=8))  # HKT
MAX_ENHANCE_ARTICLES = 30       # max articles to fetch full text for
ENHANCE_DELAY = 0.8             # seconds between full-text requests
FETCH_ROUND_TIMEOUT_SECONDS = env_int("FETCH_ROUND_TIMEOUT_SECONDS", 180, min_value=30)
ENHANCE_TIMEOUT_SECONDS = env_int("ENHANCE_TIMEOUT_SECONDS", 90, min_value=10)
LLM_TIMEOUT_SECONDS = env_int("LLM_TIMEOUT_SECONDS", 240, min_value=60)
RUN_TIMEOUT_SECONDS = env_int("RUN_TIMEOUT_SECONDS", 2400, min_value=300)
MAX_PROMPT_ARTICLES = env_int("MAX_PROMPT_ARTICLES", 240, min_value=80)
MAX_GENERAL_ARTICLES = env_int("MAX_GENERAL_ARTICLES", 180, min_value=40)
MAX_ARTICLES_PER_SOURCE = env_int("MAX_ARTICLES_PER_SOURCE", 25, min_value=5)
MAX_PRIORITY_BODY_CHARS = env_int("MAX_PRIORITY_BODY_CHARS", 1600, min_value=400)
MAX_CNN_BODY_CHARS = env_int("MAX_CNN_BODY_CHARS", 900, min_value=300)
MAX_GENERAL_BODY_CHARS = env_int("MAX_GENERAL_BODY_CHARS", 500, min_value=150)
MAX_MORNING_CONTEXT_CHARS = env_int("MAX_MORNING_CONTEXT_CHARS", 3000, min_value=1000)
MIN_ARTICLES_TO_PUBLISH = env_int("MIN_ARTICLES_TO_PUBLISH", 20, min_value=1)
HTTP_HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36"}

# Sources where simple HTTP GET can extract article text (no paywall / anti-bot)
OPEN_SOURCES = {"BBC_Business", "SCMP_Econ", "SCMP_China_Econ", "SCMP_Asia", "Yahoo_Finance", "Investing_Forex"}

CNN_MACRO_KEYWORDS = [
    "fed ", "rate hike", "rate cut", "interest rate", "inflation", "recession",
    "economy", "market", "stock", "bond", "treasury", "yield",
    "tariff", "trade war", "sanction", "oil price", "crude", "energy crisis",
    "housing market", "jobs report", "payroll", "layoff", "unemployment",
    "wage growth", "congress", "white house", "dollar", "currency",
    "china", "russia", "iran", "war", "military", "nato", "putin", "xi",
    "central bank", "Wall Street", "stimulus", "debt ceiling", "gdp",
    "consumer spending", "retail sales", "cost of living",
    "bailout", "supply chain", "shortage", "crisis",
]

SOURCE_PROMPT_ORDER = [
    "BBG", "Reuters", "WSJ", "FT", "CNBC", "SCMP", "BBC", "CNN",
    "ECB", "Fed", "BOE", "BOJ", "BOK", "RBA", "MAS", "HKMA",
    "WallstreetCN", "Caixin", "CCTV", "HKEJ",
]

# For China-related stories, prioritize official / Chinese-language primary sources
# before global English media so China narratives are not crowded out by BBG/Reuters.
CHINA_SOURCE_PROMPT_ORDER = [
    "PBOC", "HKMA", "NBSC", "Xinhua", "ChinaFinance", "ChnFund",
    "Caixin", "WallstreetCN", "CCTV", "HKEJ", "SCMP",
    "Reuters", "BBG", "WSJ", "FT", "CNBC", "BBC", "CNN",
    "Fed", "ECB", "BOE", "BOJ", "BOK", "RBA", "MAS",
]

CHINA_TOPIC_KEYWORDS = [
    "china", "chinese", "beijing", "shanghai", "hong kong", "hkma",
    "pboc", "people's bank of china", "yuan", "renminbi",
    "rmb", "cny", "cnh", "hkd", "xi jinping",
    "中国", "北京", "上海", "香港", "中国人民银行", "央行", "人民币",
    "离岸人民币", "在岸人民币", "汇率", "关税", "贸易", "财政", "宏观",
]

SYSTEM_PROMPT = """[Stage 1: Persona & Objective]

ROLE: Senior macro strategist at a top-tier global macro hedge fund.
AUDIENCE: Portfolio managers. 90-second read.
FOCUS: China (RMB/CNH/HKD/PBOC), Korea (KRW/BOK), Taiwan (TWD/CBC), Singapore (SGD/MAS), major global macro (Fed/ECB/BOJ/trade/commodities/geopolitics). Cover dominant global stories beyond Asia.

AFTERNOON RULES (when MORNING BRIEFING block is present):
- Skip any story already in the morning report. Only include if there is a material intraday update.
- Lead with intraday price action. Overview reflects what moved today, not overnight.

LANGUAGE:
- Chinese sources (Caixin, Xinhua, KED, etc.) → output ORIGINAL Chinese
- Chinese proper nouns: Chinese name alongside abbreviation (e.g. 中国人民银行 PBOC)
- All other analysis: English

PBOC RULES:
- MLF/LPR/RRR/OMO appear in Central Banks ONLY when there is news
- MLF: always report rate, amount maturing, rolled/new, NET injection/drain (投放/回笼)
- No news → omit PBOC section entirely

[Stage 2: Extraction & Rules]

SOURCE MAPPING — Map feed labels to citation abbreviations:

| Feed Labels | Citation |
|---|---|
| BBG_Markets, BBG_Econ | BBG |
| FT_Global_Econ, FT_Markets, FT_Asia, FT_Currencies | FT |
| WSJ_GN | WSJ |
| Reuters_GN | Reuters |
| CNBC_Markets, CNBC_World, CNBC_Economy | CNBC |
| SCMP_Econ, SCMP_China_Econ, SCMP_Asia | SCMP |
| BBC_Business | BBC |
| CNN_GN | CNN |
| AlJazeera_ME | Al Jazeera |
| WallstreetCN_GN | WSJCN |
| Caixin_GN | Caixin |
| CNA_GN | CNA |
| BusinessTimes_GN | BT |
| MAS_GN, HKEJ_GN, CCTV_GN, US_CENTCOM, ISW_Assessments, EIA_Press, IEA_News, SP_Global_Commodities, Lloyds_List_Shipping | keep as-is |

SOURCE SEPARATION:
- Western topics (Fed/ECB/US data/Middle East): ONLY English primary sources (FT, BBG, WSJ, Reuters, CNBC, BBC, CNN)
- Western event with DIRECT China impact via Chinese sources → merge both, flag synthesis
- Each event appears ONCE, from the highest-tier source that covers it
- Same number in different currencies → use Western source's figure

CNN EXTRACTION — Surface 1-2 signals per report:
- EXTRACT: (1) geopolitical breaking events, (2) US policy with macro consequences, (3) economic anxiety signals (cost of living, layoffs, recession fears)
- SKIP: crime, celebrity, sports
- Cross-reference with FT/BBG/WSJ; flag CNN firsts

GEOPOLITICS & WAR MONITOR EXTRACTION:
Sources: US_CENTCOM (operational reports), ISW_Assessments (battlefield assessments), AlJazeera_ME (regional/diplomatic).

| Category | Triggers — Extract ONLY when ≥1 article in today's feed contains: |
|---|---|
| 1. MILITARY ESCALATION | New front openings; strikes on oil fields/ports/refineries; unconventional weapons warnings; major troop movements or mobilizations; no-fly zone declarations; military-enforced blockades |
| 2. DE-ESCALATION & CEASEFIRE | Official ceasefire proposals from recognized state actors; substantive negotiation progress (beyond 'willingness to talk'); confirmed mutual restraint; verified force withdrawal; humanitarian corridors with ceasefire implications |
| 3. CHOKEPOINTS & PROXIES | Proxy force attacks shifting regional balance; shipping blockade enforcement; Hormuz / Bab el-Mandeb / Suez transit disruptions; attacks on commercial vessels or energy infrastructure by non-state actors |

EXTRACTION CONSTRAINT: Write a war monitor entry ONLY when ≥1 article in today's feed matches a category above. For ISW: factual battlefield updates only — skip policy editorials. Omit section if no trigger matches.

ENERGY & COMMODITY SUPPLY SHOCK EXTRACTION:
Sources: EIA_Press (inventory/production data), IEA_News (global outlooks, emergency stockpiles), SP_Global_Commodities (commodity markets), Lloyds_List_Shipping (maritime shipping).

| Category | Triggers — Extract ONLY when ≥1 article in today's feed contains: |
|---|---|
| 1. CHOKEPOINT DISRUPTIONS | Oil tanker or LNG carrier rerouting, loitering, or attack at Strait of Hormuz, Bab el-Mandeb, Red Sea, Suez, or Strait of Malacca. Signal threshold: multiple vessels OR official routing advisory (single vessel = noise) |
| 2. FREIGHT & WAR RISK SPIKES | Abnormal jumps in Baltic Dirty Tanker Index, LNG spot rates, or war risk insurance premiums. Report magnitude and direction of move |
| 3. INVENTORY DRAWS & CAPACITY | EIA or IEA sharp crude/product inventory declines; SPR releases/refills; explicit insufficient spare capacity warnings. Track trend across successive reports |

CROSS-REFERENCING: Energy/shipping sources → physical supply picture. FT/BBG/WSJ → financial market transmission (crude futures, energy equities, inflation breakevens, petrocurrency FX). Physical flow data leads financial pricing. Military sources (US_CENTCOM, ISW, AlJazeera) → operational WHAT. Financial sources (FT/BBG/WSJ/Reuters) → market SO WHAT. Cite both when both relevant.

NARRATIVE WATCH SELECTION — Rank candidate stories by market usefulness, not keyword volume:

| Criterion | Priority |
|---|---|
| Direct transmission to rates, FX, commodities, broad equity indices, credit, or cross-border capital flows | Highest |
| Clear surprise versus Macro State or the prior briefing | Highest |
| Policy reaction function, official response, or next 24-72h catalyst | High |
| Cross-source confirmation from FT/BBG/WSJ/Reuters/CNBC plus relevant primary/local source | High |
| China/Korea/Taiwan/Singapore local-market relevance | High |
| Single-stock, corporate earnings, personality, or process-only politics without macro transmission | Low |

SELECTION CONSTRAINT: Narrative Watch uses this table. HIGH-PRIORITY FEED is an input bucket, not an automatic output order.

ANTI-FABRICATION — Every factual claim MUST be traceable to a specific article in today's feed:
- Numbers, names, dates, events, price levels, titles, roles, biographical claims → all from today's feed
- Use article's exact framing: 'nominated' stays 'nominated', 'incoming' stays 'incoming'
- Economic Calendar entries: ONLY from today's TradingEconomics feed
- Feed lacks data → omit section or state 'Not available in this window'

[Stage 3: Reasoning Constraints]

<scratchpad>
Before writing the final output, plan your analysis in this order:
1. Scan all feeds — identify top 3-5 stories ranked by market usefulness, not headline volume
2. Check war monitor feeds for hard signal triggers (categories 1-3)
3. Check energy feeds for supply shock triggers (categories 1-3)
4. Cross-reference military WHAT with financial SO WHAT (FT/BBG/WSJ)
5. Check Macro State to classify each candidate as New / Acceleration / Reversal / Confirmation / Noise
6. Map each chosen narrative through one transmission chain: growth, inflation, policy, liquidity, risk premium, or capital flows
7. Rank Overview: macro impact > headline volume.
</scratchpad>

FACTS vs AI ANALYSIS:
- Narrative Watch Fact paragraphs: EXCERPT mode — copy verbatim from article text. Do NOT rephrase, summarize, or paraphrase. Stitch selected excerpts into a SINGLE continuous paragraph (no line breaks, no `>` prefixes). Inline citations wrapped in parentheses: `([Source](URL))` immediately after each claim. NEVER enrich with training data or Macro State.
- Citation validity: Fact and Global Radar citations MUST be clickable markdown links from today's feed. If a claim is inferred, extrapolated, unsupported, or lacks a feed URL, omit the claim. Never output bracketed source notes such as `([SCMP — Note: ...])`, `extrapolated`, `inferred`, or `no source`.
- Forecast revision rule: If a Fact or Global Radar item says a bank, official, or forecaster raised, lowered, revised, delayed, upgraded, or downgraded a forecast, the excerpt MUST include the concrete forecast values and, when available, prior values and time horizons. If the feed text lacks those values, omit the forecast-revision claim.
- Economic indicator naming rule: Use the official indicator name from the article/calendar. Do NOT rename indicators into media shorthand or explanatory labels (e.g., write "US PPI" or "US Producer Price Index", not "wholesale inflation"; write "CPI", "Core CPI", "PCE Price Index", "Nonfarm Payrolls", "Initial Jobless Claims" as named by the source).
- Global Radar: EXCERPT mode — one direct excerpt per bullet. Do NOT rephrase, summarize, or paraphrase.
- AI Reasoning: Output `> [!info] [AI Reasoning]` with EXACTLY three bullets. Keep excerpts in Fact; put judgment ONLY in these bullets.
  1. Narrative change: classify as New / Acceleration / Deceleration / Reversal / Confirmation / Noise versus Macro State or the prior briefing.
  2. Transmission / Market Read: name the macro transmission chain, then give directional asset implications.
  3. Watchpoint / Confidence: concrete 24-72h confirmation/invalidation trigger plus Confidence: High / Medium / Low.
  REQUIRED bullet labels: `Narrative change`, `Transmission / Market Read`, `Watchpoint / Confidence`.
  FORMAT INVALID if any AI Reasoning block uses legacy or redundant labels: `What happened`, `Base Case`, or `Tactical Trade`.
  NO new facts, no uncited numbers, no price targets unless explicitly cited by FT/BBG/WSJ/Reuters today. Example:
> [!info] [AI Reasoning]
> * **Narrative change**: Acceleration because military protection is now attached to commercial transit rather than only diplomatic messaging.
> * **Transmission / Market Read**: Risk-premium channel favors **long Brent** and **short KRW** until transit normalizes.
> * **Watchpoint / Confidence**: Pivot to neutral if first convoy clears without kinetic contact; Confidence: Medium due to cross-source confirmation but volatile official messaging.

MACRO STATE USAGE:
- State provides continuity for AI Reasoning and narrative-change classification — it is NOT a source for fact sections
- Use State to decide whether today's feed creates New / Acceleration / Reversal / Confirmation / Noise
- If prior state uses an older format, translate it mentally into active narratives, watchpoints, open questions, and key levels before writing `<state_update>`
- Cross-day cumulative counts ('third hike since Hormuz closure', 'Day 16 of the blockade', 'fourth consecutive week') are BANNED in Fact sections UNLESS a source article in TODAY's feed explicitly states that exact number
- State says 'third hike' + today's articles only say 'OPEC+ agreed a 188k bpd increase' → write ONLY what the article says. Reserve cumulative context for [AI Reasoning] sections

TIMELINE CONSISTENCY:
- Before naming a person in an event: verify they were present at THAT specific meeting
- Future expected dissent ≠ actual dissent that already happened
- List all names in ONE sentence

RANKING: Overview ordered by macro impact, not headline volume. Corporate bankruptcy never leads over central bank pivot or geopolitical escalation.

[Stage 4: Formatting & Output]

OUTPUT STRUCTURE (in order):
0. YAML Frontmatter — valid YAML block at VERY TOP, enclosed by `---`. Generate a `tags` array (max 3-5 tags) from three categories ONLY: (a) Macro/Theme: `Macro/Hormuz_Blockade`, `Macro/Tariffs`, `Macro/PBOC_Easing` (b) Asset/Ticker: `Asset/Brent`, `Asset/CNH`, `Asset/UST` (c) Trade/Bias: `Trade/Long_USD`, `Trade/Short_KRW`. No generic noise tags (`News`, `Economy`, `Update`). Example:
---
tags:
  - Macro/Hormuz_Blockade
  - Asset/Brent
  - Asset/KRW
  - Trade/Long_Brent
  - Trade/Short_KRW
---
1. Header (date range)
2. Overview (ranked by impact; single paragraph; no AI Reasoning; cross-market thesis in last sentence)
3. Narrative Watch (Fact excerpts → AI Reasoning: Narrative change / Transmission & Market Read / Watchpoint & Confidence)
4. Global Radar (Economic Indicators → Central Banks → Geopolitics → Commodities → Equities)
5. Economic Calendar — Output as a Markdown table. Each event is one table row with these 5 columns:
   | Time (HKT) | Rgn | Event | Est. | Prior |
   |---|---|---|---|---|
   | 09:30 AM | AU | Household Spending YoY | — | 4.6% |
   | 12:30 PM | AU | RBA Interest Rate Decision | 4.35% | 4.1% |
   Rules: Strip "Consensus:" and "Prior:" prefixes (raw numbers only). Use `—` for unavailable. Only include events from today's TradingEconomics feed.
6. Full Reading List — 2-level nested Markdown bullets.
   Level 1: bold source name `- **SourceName**`.
   Level 2: 2-space-indented clickable link `  - [Headline](URL)`.
   Structure each source block as:
   - **BBG**
     - [Top Bank of Korea Official Says It's Time to Consider Rate Hike](URL)
     - [Asian Stocks Outside of Japan Hit Record High](URL)
   - **Reuters**
     - [Gold dips as inflation concerns linger](URL)
   Sort: BBG → Reuters → WSJ → FT → CNBC → SCMP → BBC → CNN. Omit sources with zero articles.

SOURCE ATTRIBUTION:
Every factual claim ends with linked citations using article URLs from today's feed. Format: `([SourceAbbrev](URL))` — standard Markdown link wrapped in parentheses. Same source multiple articles: number them without repeating source name — `([BBG 1](URL1), [2](URL2))`. Multiple sources: `([BBG 1](URL1), [2](URL2), [Reuters](URL3))`. Example in flowing text:
  Oil slipped on the "Project Freedom" announcement ([BBG](URL)). A bulk carrier was attacked by multiple small craft 11 nautical miles west of Sirik, Iran ([Al Jazeera](URL)). Minneapolis Fed President Kashkari said the Iran war limits the Fed's ability to provide rate guidance ([Reuters](URL)).
Every URL from today's feed — no substitutions, no homepage links. No colons, no "Source:" labels. Preference order: FT > BBG > WSJ > Reuters > CNBC > SCMP > BBC > CNN > others.

BOLD:
- Permitted locations: (a) headings, (b) hard war signals from GEOPOLITICS & WAR MONITOR (categories 1-2), (c) severe energy chokepoint disruptions (Energy category 1), (d) directional asset views inside AI Reasoning
- Bold ONLY the key signal, not surrounding context
- All other body text: no bold

NUMBER FORMATTING: Backticks — `4.25%`, `$125/bbl`

TONE:
- Investment views: calm, probabilistic. Vocabulary: 'favors,' 'supports long/short,' 'tilts risks toward,' 'bears watching'
- Trade views: directional only (e.g. 'favors long USD/Asia FX')
- Price levels, entry points, targets: ONLY if the exact number appears in today's feed

POTENTIAL MARKET IMPACT FORMATTING:
- Asset class, specific ticker, and directional bias MUST be **bold**
- Example: "Favors **short KRW** and **long USD/Asia FX** on risk-off"
- Example: "Tilts risks toward **higher USD/CNH**"

TIGHTNESS CONSTRAINTS:
- AI Reasoning: exactly three bullets; each bullet max 28 words
- Fact paragraphs: max 250 words each. Be selective — not every article needs to be cited.
- Global Radar: max 4 items per category. Skip categories entirely if nothing high-impact.
- Narrative Watch: max 3 stories. Use a 4th only if it changes cross-asset pricing or a major Asia FX narrative.
- Full Reading List: only articles actually cited. Skip sources with zero citations.
- Morning reports have a 24K output token ceiling — if you're running long, cut low-priority stories before cutting the calendar or reading list.
- No preamble, no academic hedging, no defining basic concepts
- No em dashes. Cut all redundancies.

<state_update>
After the Full Reading List, you MUST output updated state in this XML block. This is REQUIRED on every run — never skip it. The state is stripped before publishing so it won't appear in the final report:
## Active Narratives (max 4 — each line: **Name** — Status: New/Acceleration/Reversal/Confirmation/Fading/Resolved; why it matters; next trigger)
## Watchpoints (max 5 — concrete 24-72h triggers; remove resolved)
## Open Questions (max 3 — only unresolved questions that affect positioning)
## Key Levels (max 5 — only with today's source citations; include asset and why the level matters)
</state_update>"""



# ---------------------------------------------------------------------------
# RSS Fetching
# ---------------------------------------------------------------------------

def _html_to_text(html, source):
    """Extract visible text from HTML. Basic but fast — no browser needed."""
    # Remove non-content elements
    for tag in ("script", "style", "nav", "header", "footer", "noscript", "iframe"):
        html = re.sub(rf"<{tag}[^>]*>.*?</{tag}>", "", html, flags=re.DOTALL | re.IGNORECASE)
    # Strip all tags, collapse whitespace
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"\s+", " ", text).strip()
    # Remove common boilerplate prefixes
    text = re.sub(r"^.*?(Advertisement\s*)", "", text, count=1, flags=re.IGNORECASE)
    # Truncate to reasonable length
    return text[:8000] if len(text) > 8000 else text


def fetch_full_text(url, source):
    """Try to get full article text. Never raises, returns None on failure.
    Strategy: direct HTTP for open sources → Jina AI proxy for paywalled → give up."""
    # Strategy 1: Direct HTTP GET for sources known to serve article text
    if source in OPEN_SOURCES:
        try:
            req = urllib.request.Request(url, headers=HTTP_HEADERS)
            resp = urllib.request.urlopen(req, timeout=10)
            html = resp.read().decode("utf-8", errors="replace")
            text = _html_to_text(html, source)
            if text and len(text) > 300:
                return text
        except Exception:
            pass

    # Strategy 2: Jina AI reader proxy (handles Bloomberg, FT, CNBC, etc.)
    try:
        cleaned = url.replace("https://", "").replace("http://", "")
        proxy_url = f"https://r.jina.ai/http://{cleaned}"
        req = urllib.request.Request(proxy_url, headers={**HTTP_HEADERS, "Accept": "text/plain"})
        resp = urllib.request.urlopen(req, timeout=15)
        text = resp.read().decode("utf-8", errors="replace")
        if text and len(text) > 500:
            return text[:8000]  # cap at 8K chars per article
    except Exception:
        pass

    return None


def _fetch_pboc_html(name, url, window_start, window_end):
    """Parse PBOC Chinese news page HTML. Returns list of article dicts."""
    articles = []
    try:
        req = urllib.request.Request(url, headers=HTTP_HEADERS)
        resp = urllib.request.urlopen(req, timeout=FEED_TIMEOUT)
        html = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  [error] {name}: {e}", file=sys.stderr)
        return articles

    # PBOC news items: <a title="FULL TITLE" href="...">... <span class="hui12">DATE</span>
    pattern = re.compile(
        r'<a[^>]*href="(/goutongjiaoliu[^"]*?/(\d{14})/index\.html)"[^>]*'
        r'title="([^"]+)"[^>]*>'
        r'.*?<span class="hui12">(\d{4}-\d{2}-\d{2})</span>',
        re.DOTALL,
    )
    in_window = 0
    for match in pattern.finditer(html):
        relative_url, date_digits, title, date_str = match.groups()
        link = f"http://www.pbc.gov.cn{relative_url}"
        try:
            pub_dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=LOCAL_TZ)
        except ValueError:
            pub_dt = None

        if pub_dt and (pub_dt < window_start or pub_dt > window_end):
            continue
        in_window += 1

        articles.append({
            "source": name,
            "title": title.strip(),
            "link": link,
            "summary": f"中国人民银行公告 — {date_str}",
            "published": pub_dt.isoformat() if pub_dt else "",
            "priority": priority_score(title, ""),
        })

    if in_window > 0:
        print(f"  {name}: {len(articles)} articles ({in_window} in window) [HTML]")
    return articles


def _fetch_hkma_html(name, url, window_start, window_end):
    """Parse HKMA press release page HTML. Returns list of article dicts."""
    articles = []
    try:
        req = urllib.request.Request(url, headers=HTTP_HEADERS)
        resp = urllib.request.urlopen(req, timeout=FEED_TIMEOUT)
        html = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  [error] {name}: {e}", file=sys.stderr)
        return articles

    # HKMA press releases: date ID + title pairs: (\d{8}-\d+) ... title="TITLE"
    pattern = re.compile(
        r'(\d{4})(\d{2})(\d{2})-(\d+)[^}]*?title=\"([^\"]+)\"',
        re.DOTALL,
    )
    in_window = 0
    for match in pattern.finditer(html):
        year, month, day, seq, title = match.groups()
        try:
            pub_dt = datetime(int(year), int(month), int(day)).replace(tzinfo=LOCAL_TZ)
        except ValueError:
            continue

        if pub_dt < window_start or pub_dt > window_end:
            continue
        in_window += 1

        date_id = f"{year}{month}{day}-{seq}"
        link = f"https://www.hkma.gov.hk/eng/news-and-media/press-releases/{year}/{month}/{date_id}/"

        articles.append({
            "source": name,
            "title": title.strip(),
            "link": link,
            "summary": f"HKMA Press Release — {year}-{month}-{day}",
            "published": pub_dt.isoformat(),
            "priority": priority_score(title, ""),
        })

    if in_window > 0:
        print(f"  {name}: {len(articles)} articles ({in_window} in window) [HTML]")
    return articles


def _fetch_chnfund_json(name, url, window_start, window_end):
    """Parse chnfund.com JSON API for macro articles. Returns list of article dicts."""
    import json
    articles = []
    try:
        req = urllib.request.Request(url, headers=HTTP_HEADERS)
        resp = urllib.request.urlopen(req, timeout=FEED_TIMEOUT)
        raw = resp.read().decode("utf-8", errors="replace")
        data = json.loads(raw)
    except Exception as e:
        print(f"  [error] {name}: {e}", file=sys.stderr)
        return articles

    items = data.get("data", [])
    in_window = 0
    for item in items:
        pub_str = item.get("publishedTime", "")
        try:
            pub_dt = datetime.strptime(pub_str, "%Y-%m-%d").replace(tzinfo=LOCAL_TZ)
        except ValueError:
            continue

        if pub_dt < window_start or pub_dt > window_end:
            continue
        in_window += 1

        title = item.get("title", "").strip()
        summary = item.get("summary", "").strip()
        detail = item.get("detailUrl", "")
        link = f"https://www.chnfund.com{detail}" if detail else ""

        articles.append({
            "source": name,
            "title": title,
            "link": link,
            "summary": summary[:1500],
            "published": pub_dt.isoformat(),
            "priority": priority_score(title, summary),
        })

    if in_window > 0:
        print(f"  {name}: {len(articles)} articles ({in_window} in window) [JSON]")
    return articles


def _fetch_xinhua_html(name, url, window_start, window_end):
    """Parse Xinhua fortune page HTML. Returns list of article dicts."""
    articles = []
    try:
        req = urllib.request.Request(url, headers=HTTP_HEADERS)
        resp = urllib.request.urlopen(req, timeout=FEED_TIMEOUT)
        html = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  [error] {name}: {e}", file=sys.stderr)
        return articles

    # Xinhua article links: /fortune/YYYYMMDD/hash/c.html
    # We'll also grab the title from nearby text
    pattern = re.compile(
        r'href="(/fortune/(\d{4})(\d{2})(\d{2})/[^"]+\.html)"[^>]*>([^<]+)</a>',
        re.DOTALL,
    )
    in_window = 0
    seen = set()
    for match in pattern.finditer(html):
        link_path, year, month, day, title = match.groups()
        try:
            pub_dt = datetime(int(year), int(month), int(day)).replace(tzinfo=LOCAL_TZ)
        except ValueError:
            continue

        if pub_dt < window_start or pub_dt > window_end:
            continue

        link = f"https://www.news.cn{link_path}"
        if link in seen:
            continue
        seen.add(link)
        in_window += 1

        articles.append({
            "source": name,
            "title": title.strip(),
            "link": link,
            "summary": f"新华网财经 — {year}-{month}-{day}",
            "published": pub_dt.isoformat(),
            "priority": priority_score(title, ""),
        })

    if in_window > 0:
        print(f"  {name}: {len(articles)} articles ({in_window} in window) [HTML]")
    return articles


def _fetch_nbsc_html(name, url, window_start, window_end):
    """Parse 国家统计局 (NBSC) homepage for data releases. Returns list of article dicts."""
    articles = []
    try:
        req = urllib.request.Request(url, headers=HTTP_HEADERS)
        resp = urllib.request.urlopen(req, timeout=FEED_TIMEOUT)
        html = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  [error] {name}: {e}", file=sys.stderr)
        return articles

    # NBSC links: /sj/zxfbhjd/YYYYMM/tYYYYMMDD_hash.html
    pattern = re.compile(
        r'href="(/[^"]*?/(\d{4})(\d{2})/t(\d{4})(\d{2})(\d{2})_\d+\.html)"[^>]*>([^<]+)</a>',
        re.DOTALL,
    )
    in_window = 0
    seen = set()
    for match in pattern.finditer(html):
        link_path, yr1, mo1, yr2, mo2, day, title = match.groups()
        try:
            pub_dt = datetime(int(yr2), int(mo2), int(day)).replace(tzinfo=LOCAL_TZ)
        except ValueError:
            continue

        if pub_dt < window_start or pub_dt > window_end:
            continue

        link = f"https://www.stats.gov.cn{link_path}"
        if link in seen:
            continue
        seen.add(link)
        in_window += 1

        articles.append({
            "source": name,
            "title": title.strip(),
            "link": link,
            "summary": f"国家统计局数据发布 — {yr2}-{mo2}-{day}",
            "published": pub_dt.isoformat(),
            "priority": priority_score(title, ""),
        })

    if in_window > 0:
        print(f"  {name}: {len(articles)} articles ({in_window} in window) [HTML]")
    return articles


def _fetch_chinafinance_html(name, url, window_start, window_end):
    """Parse china.com.cn finance page HTML. Returns list of article dicts."""
    articles = []
    try:
        req = urllib.request.Request(url, headers=HTTP_HEADERS)
        resp = urllib.request.urlopen(req, timeout=FEED_TIMEOUT)
        html = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  [error] {name}: {e}", file=sys.stderr)
        return articles

    # Links: /roll/photo/YYYYMMDD/NNNNN.shtml or /news/YYYYMMDD/...
    pattern = re.compile(
        r'href="(/[^"]*?/(\d{4})(\d{2})(\d{2})/\d+\.s?html)"[^>]*>([^<]+)</a>',
        re.DOTALL,
    )
    in_window = 0
    seen = set()
    for match in pattern.finditer(html):
        link_path, year, month, day, title = match.groups()
        try:
            pub_dt = datetime(int(year), int(month), int(day)).replace(tzinfo=LOCAL_TZ)
        except ValueError:
            continue

        if pub_dt < window_start or pub_dt > window_end:
            continue

        link = f"https://finance.china.com.cn{link_path}"
        if link in seen:
            continue
        seen.add(link)
        in_window += 1

        articles.append({
            "source": name,
            "title": title.strip(),
            "link": link,
            "summary": f"中国财经 — {year}-{month}-{day}",
            "published": pub_dt.isoformat(),
            "priority": priority_score(title, ""),
        })

    if in_window > 0:
        print(f"  {name}: {len(articles)} articles ({in_window} in window) [HTML]")
    return articles


def _fetch_caixin_homepage(name, url, window_start, window_end):
    """Parse Caixin homepage for featured articles with real URLs."""
    articles = []
    try:
        req = urllib.request.Request(url, headers=HTTP_HEADERS)
        resp = urllib.request.urlopen(req, timeout=FEED_TIMEOUT)
        html = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  [error] {name}: {e}", file=sys.stderr)
        return articles

    pattern = re.compile(
        r'href="(https?://www\.caixin\.com/(\d{4})-(\d{2})-(\d{2})/\d+\.html)"[^>]*>([^<]+)</a>',
        re.DOTALL,
    )
    in_window = 0
    seen = set()
    for match in pattern.finditer(html):
        link, year, month, day, title = match.groups()
        try:
            pub_dt = datetime(int(year), int(month), int(day)).replace(tzinfo=LOCAL_TZ)
        except ValueError:
            continue

        if pub_dt < window_start or pub_dt > window_end:
            continue
        if link in seen:
            continue
        seen.add(link)
        in_window += 1

        articles.append({
            "source": name,
            "title": title.strip(),
            "link": link,
            "summary": f"财新网 — {year}-{month}-{day}",
            "published": pub_dt.isoformat(),
            "priority": priority_score(title, ""),
        })

    if in_window > 0:
        print(f"  {name}: {len(articles)} articles ({in_window} in window) [HTML]")
    return articles


def _fetch_te_calendar(window_end):
    """Scrape TradingEconomics calendar, pre-process entirely in Python.
    Filters countries, converts times to HKT, filters to 24h window, sorts by HKT.
    Returns clean list ready for the LLM."""
    from zoneinfo import ZoneInfo
    import urllib.request as ur

    TE_COUNTRIES = {"US", "CN", "HK", "EZ", "DE", "FR", "IT", "ES", "GB", "KR", "JP", "TW", "SG", "AU"}
    CB_MEETING_KW = ["FOMC", "ECB", "BOE", "BOJ", "PBOC", "BOK", "CBC", "RBA",
                     "Interest Rate", "Rate Decision", "Monetary Policy", "Meeting Minutes"]

    events = []
    html = None
    for attempt in (1, 2):
        try:
            req = ur.Request(TE_CALENDAR_URL, headers=HTTP_HEADERS)
            resp = ur.urlopen(req, timeout=20, context=SSL_CTX)
            html = resp.read().decode("utf-8", errors="replace")
            break
        except Exception as e:
            print(f"  [warn] TE calendar attempt {attempt}: {e}", file=sys.stderr)
            if attempt == 1:
                time.sleep(5)
    if not html:
        print(f"  [warn] TE calendar: fetch failed after 2 attempts", file=sys.stderr)
        return events

    # Parse rows
    row_start_pattern = re.compile(
        r"<tr\s+data-url=\"([^\"]+)\"[^>]*data-country=\"([^\"]+)\"[^>]*data-category=\"([^\"]+)\"[^>]*data-event=\"([^\"]+)\"[^>]*data-symbol='([^']+)'>",
        re.DOTALL,
    )

    raw_count = 0
    for match in row_start_pattern.finditer(html):
        raw_count += 1
        url, country, category, event_name, symbol = match.groups()
        row_start = match.start()
        depth = 1
        pos = match.end()
        while depth > 0 and pos < len(html):
            next_open = html.find("<tr", pos)
            next_close = html.find("</tr>", pos)
            if next_close == -1:
                break
            if next_open != -1 and next_open < next_close:
                depth += 1
                pos = next_open + 3
            else:
                depth -= 1
                if depth == 0:
                    row_html = html[match.end():next_close]
                    break
                pos = next_close + 5
        else:
            continue

        date_m = re.search(r"<td[^>]*class='[^']*?(\d{4}-\d{2}-\d{2})", row_html)
        if not date_m:
            continue
        event_date = date_m.group(1)

        time_m = re.search(r"calendar-date-\d+\">\s*([^<]+)</span>", row_html)
        time_str = time_m.group(1).strip() if time_m else ""

        cc_m = re.search(r'class="calendar-iso">(\w+)</td>', row_html)
        country_code = cc_m.group(1) if cc_m else country.upper()

        # --- Country filter: keep only target countries or CB meetings ---
        event_text_upper = (event_name + category).upper()
        is_cb_meeting = any(kw.upper() in event_text_upper for kw in CB_MEETING_KW)
        if country_code not in TE_COUNTRIES and not is_cb_meeting:
            continue

        evt_m = re.search(r"<a\s+class='calendar-event'[^>]*>([^<]+)</a>", row_html)
        display_event = evt_m.group(1).strip() if evt_m else event_name

        period_m = re.search(r'<span\s+class="calendar-reference">([^<]+)</span>', row_html)
        period = period_m.group(1).strip() if period_m else ""

        prev_m = re.search(r"<span\s+id='previous'>([^<]*)</span>", row_html)
        previous = prev_m.group(1).strip() if prev_m and prev_m.group(1).strip() else ""

        cons_m = re.search(r"id='consensus'[^>]*>([^<]*)</(?:a|span)>", row_html)
        consensus = cons_m.group(1).strip() if cons_m and cons_m.group(1).strip() else ""

        # --- Timezone conversion: raw HTML times are UTC → add 8h to get HKT ---
        try:
            # Parse time like "01:00 PM" or "12:30 AM"
            local_dt_str = f"{event_date} {time_str}"
            utc_dt = datetime.strptime(local_dt_str, "%Y-%m-%d %I:%M %p")
            utc_dt = utc_dt.replace(tzinfo=timezone.utc)
            hkt_dt = (utc_dt + timedelta(hours=8)).replace(tzinfo=LOCAL_TZ)
        except (ValueError, AttributeError):
            continue

        # --- Time window filter: only events within 24h of window_end ---
        if hkt_dt < window_end or hkt_dt > window_end + timedelta(hours=26):
            continue

        events.append({
            "hkt_time": hkt_dt.strftime("%b %d %I:%M %p"),
            "hkt_dt": hkt_dt,
            "country": country_code,
            "event": display_event,
            "period": period,
            "consensus": consensus or "-",
            "previous": previous or "-",
        })

    # Sort by HKT time
    events.sort(key=lambda e: e["hkt_dt"])

    # Strip sort key before returning
    for e in events:
        del e["hkt_dt"]

    print(f"  TE Calendar: {raw_count} rows parsed → {len(events)} after country/window filter")
    return events


def _parse_published(entry):
    """Extract a timezone-aware datetime from a feedparser entry. Returns None if unparseable."""
    # Standard RSS/Atom date fields (feedparser parses these into struct_time)
    for attr in ("published_parsed", "updated_parsed"):
        tp = getattr(entry, attr, None)
        if tp is not None:
            try:
                dt = datetime(*tp[:6])
                return dt.replace(tzinfo=timezone.utc)
            except Exception:
                continue

    # Dublin Core date fields (used by KED, some Asian sources) — raw strings
    for attr in ("dc_publishdate", "dc_modifydate", "dc_date"):
        raw = getattr(entry, attr, None) or entry.get(attr, "")
        if raw:
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
                try:
                    dt = datetime.strptime(raw.strip(), fmt)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt
                except ValueError:
                    continue
    return None


def priority_score(title, summary):
    """Score an article by keyword matches. Higher = more relevant."""
    text = f"{title} {summary}".lower()
    score = 0
    for kw in HIGH_PRIORITY_KEYWORDS:
        if kw.lower() in text:
            score += 10
    for kw in MEDIUM_PRIORITY_KEYWORDS:
        if kw.lower() in text:
            score += 3
    return score


def _fetch_one_feed(name, url, window_start, window_end):
    """Fetch a single RSS feed. Returns list of article dicts. Never raises."""
    articles = []
    try:
        feed = feedparser.parse(url)
    except Exception as e:
        print(f"  [error] {name}: parse exception ({e})", file=sys.stderr)
        return articles

    if feed.bozo and len(feed.entries) == 0:
        err = str(feed.bozo_exception)[:100] if feed.bozo_exception else "unknown"
        print(f"  [warn] {name}: bozo + 0 entries ({err})", file=sys.stderr)
        return articles

    in_window = 0
    for entry in feed.entries:
        link = entry.get("link", "").split("?")[0].rstrip("/")
        if not link:
            continue

        title = (entry.get("title") or "").strip()
        summary = (entry.get("summary") or entry.get("description") or "").strip()
        summary = re.sub(r"<[^>]+>", "", summary)

        pub_dt = _parse_published(entry)

        # Keep if within [window_start, window_end]; always keep if date unparseable
        if pub_dt is not None and (pub_dt < window_start or pub_dt > window_end):
            continue
        in_window += 1

        articles.append({
            "source": name,
            "title": title,
            "link": link,
            "summary": summary,
            "published": pub_dt.isoformat() if pub_dt else "",
            "priority": priority_score(title, summary),
        })

    total_with_link = sum(1 for e in feed.entries if e.get("link"))

    if total_with_link > 0 and in_window == 0:
        print(f"  [warn] {name}: appears frozen or outdated, skipped.", file=sys.stderr)
    elif feed.bozo:
        print(f"  [warn] {name}: bozo but got {len(articles)} articles ({in_window} in window)", file=sys.stderr)
    else:
        print(f"  {name}: {len(articles)} articles ({in_window} in window)")
    return articles


def fetch_all_feeds(window_start, window_end):
    """Fetch all RSS feeds in parallel with isolated error handling. Returns deduplicated, sorted list."""
    socket.setdefaulttimeout(FEED_TIMEOUT)

    seen_urls = set()
    all_articles = []
    all_tasks = []
    html_fetchers = {
        "PBOC_News": _fetch_pboc_html,
        "HKMA_Press": _fetch_hkma_html,
        "ChnFund_Macro": _fetch_chnfund_json,
        "Xinhua_Fortune": _fetch_xinhua_html,
        "NBSC_Releases": _fetch_nbsc_html,
        "ChinaFinance_CN": _fetch_chinafinance_html,
        "Caixin_Homepage": _fetch_caixin_homepage,
    }

    # HTML-scraped sources + RSS feeds combined into a single task list
    for name, url in HTML_SOURCES.items():
        all_tasks.append(("html", name, url))
    for name, url in RSS_FEEDS.items():
        all_tasks.append(("rss", name, url))

    def _run_fetch_round():
        """Fetch all configured sources once and return raw articles plus per-feed counts."""
        round_articles = []
        feed_articles = {name: 0 for _, name, _ in all_tasks}
        executor = ThreadPoolExecutor(max_workers=8)
        futures = {}
        try:
            for task_type, name, url in all_tasks:
                if task_type == "html":
                    fetcher = html_fetchers[name]
                else:
                    fetcher = _fetch_one_feed
                futures[executor.submit(fetcher, name, url, window_start, window_end)] = name

            # Track per-feed article count, but never let one stuck source hold the whole report.
            try:
                completed = 0
                for fut in as_completed(futures, timeout=FETCH_ROUND_TIMEOUT_SECONDS):
                    completed += 1
                    name = futures[fut]
                    try:
                        articles = fut.result()
                    except Exception as e:
                        print(f"  [error] {name}: fetch task failed ({e})", file=sys.stderr)
                        articles = []
                    feed_articles[name] = len(articles)
                    round_articles.extend(articles)
            except FutureTimeoutError:
                pending = [name for fut, name in futures.items() if not fut.done()]
                print(
                    f"  [warn] Feed fetch round timed out after {FETCH_ROUND_TIMEOUT_SECONDS}s; "
                    f"continuing without {len(pending)} pending source(s): {', '.join(pending[:8])}"
                    f"{'...' if len(pending) > 8 else ''}",
                    file=sys.stderr,
                )
                write_json_artifact("fetch_timeout_sources.json", {
                    "timeout_seconds": FETCH_ROUND_TIMEOUT_SECONDS,
                    "pending_sources": pending,
                    "completed_sources": completed,
                })
                for fut in futures:
                    fut.cancel()
        finally:
            executor.shutdown(wait=False, cancel_futures=True)
        return round_articles, feed_articles

    def _merge_articles(articles):
        for a in articles:
            link = a["link"]
            if link not in seen_urls:
                seen_urls.add(link)
                all_articles.append(a)

    round_articles, feed_articles = _run_fetch_round()
    _merge_articles(round_articles)

    # Log feeds with zero articles
    zeros = sorted([n for n, c in feed_articles.items() if c == 0])
    if zeros:
        print(f"  {len(zeros)}/{len(all_tasks)} feeds returned 0 articles: {', '.join(zeros[:10])}{'...' if len(zeros) > 10 else ''}")

    if not all_articles:
        print(f"[error] No articles fetched from any feed ({len(all_tasks)} feeds).", file=sys.stderr)
        print("[error] Retrying once in 30s...", file=sys.stderr)
        time.sleep(30)
        round_articles, feed_articles = _run_fetch_round()
        _merge_articles(round_articles)
        if not all_articles:
            zeros = sorted([n for n, c in feed_articles.items() if c == 0])
            if zeros:
                print(f"  retry zero feeds: {', '.join(zeros[:10])}{'...' if len(zeros) > 10 else ''}")
            print("[error] Still no articles after retry. Exiting.", file=sys.stderr)
            sys.exit(1)

    write_json_artifact("source_counts.json", {
        "total_sources": len(all_tasks),
        "total_articles_before_dedup": sum(feed_articles.values()),
        "total_articles_after_dedup": len(all_articles),
        "zero_feeds": sorted([n for n, c in feed_articles.items() if c == 0]),
        "feed_article_counts": dict(sorted(feed_articles.items())),
    })

    all_articles.sort(key=lambda a: (-a["priority"], a["source"], a["title"]))

    # Enhance top articles with full text — also parallelized
    enhance_count = min(MAX_ENHANCE_ARTICLES, len(all_articles))
    enhanced = 0

    def _enhance_one(article):
        if article.get("full_text"):
            return article["source"], True  # already enhanced
        if article["source"].endswith("_GN"):
            return article["source"], False  # Google News links unusable
        full = fetch_full_text(article["link"], article["source"])
        if full:
            article["full_text"] = full
            return article["source"], True
        return article["source"], False

    executor = ThreadPoolExecutor(max_workers=6)
    try:
        fut_to_article = {
            executor.submit(_enhance_one, all_articles[i]): all_articles[i]
            for i in range(enhance_count)
        }
        try:
            for fut in as_completed(fut_to_article, timeout=ENHANCE_TIMEOUT_SECONDS):
                src, ok = fut.result()
                if ok:
                    enhanced += 1
        except FutureTimeoutError:
            pending = sum(1 for fut in fut_to_article if not fut.done())
            print(f"  [warn] Full-text enhancement timed out with {pending} pending article(s); continuing with RSS summaries", file=sys.stderr)
            for fut in fut_to_article:
                fut.cancel()
    finally:
        executor.shutdown(wait=False, cancel_futures=True)

    if enhanced:
        print(f"  Enhanced: {enhanced}/{enhance_count} articles with full text")

    return all_articles


# ---------------------------------------------------------------------------
# Prompt building
# ---------------------------------------------------------------------------

def _is_cnn_macro_article(article):
    """Return True for CNN items with enough macro signal to enter the prompt."""
    if not article["source"].startswith("CNN_"):
        return False
    text = f"{article['title']} {article.get('summary','')}".lower()
    return any(kw in text for kw in CNN_MACRO_KEYWORDS)


def _source_prompt_rank(source, order=SOURCE_PROMPT_ORDER):
    for rank, prefix in enumerate(order):
        if source.startswith(prefix):
            return rank
    return len(order)


def _is_china_related_article(article):
    source = article.get("source", "")
    if _source_prompt_rank(source, CHINA_SOURCE_PROMPT_ORDER) < CHINA_SOURCE_PROMPT_ORDER.index("Reuters"):
        return True
    text = f"{source} {article.get('title', '')} {article.get('summary', '')}".lower()
    return any(kw.lower() in text for kw in CHINA_TOPIC_KEYWORDS)


def _article_prompt_sort_key(article):
    if _is_china_related_article(article):
        return (0, _source_prompt_rank(article["source"], CHINA_SOURCE_PROMPT_ORDER), -article["priority"], article["title"])
    return (1, -article["priority"], _source_prompt_rank(article["source"]), article["title"])


def _article_key(article):
    return article.get("link") or f"{article.get('source')}::{article.get('title')}"


def _select_prompt_articles(articles):
    """Select a bounded, high-signal article set for the LLM prompt."""
    from collections import Counter

    selected = []
    seen = set()
    source_counts = Counter()
    general_count = 0

    def add(article, force=False, counts_as_general=False):
        nonlocal general_count
        key = _article_key(article)
        if key in seen:
            return False
        if not force:
            if len(selected) >= MAX_PROMPT_ARTICLES:
                return False
            if source_counts[article["source"]] >= MAX_ARTICLES_PER_SOURCE:
                return False
            if counts_as_general and general_count >= MAX_GENERAL_ARTICLES:
                return False
        seen.add(key)
        selected.append(article)
        source_counts[article["source"]] += 1
        if counts_as_general:
            general_count += 1
        return True

    priority = [a for a in articles if a["priority"] >= 10]
    cnn_signal = [a for a in articles if _is_cnn_macro_article(a)]
    remaining = [
        a for a in articles
        if a["priority"] < 10 and not _is_cnn_macro_article(a)
    ]
    priority.sort(key=_article_prompt_sort_key)
    remaining.sort(key=_article_prompt_sort_key)

    for article in priority:
        add(article, force=True)
    for article in cnn_signal:
        add(article)
    for article in remaining:
        add(article, counts_as_general=True)

    meta = {
        "articles_fetched": len(articles),
        "articles_in_prompt": len(selected),
        "articles_dropped": max(0, len(articles) - len(selected)),
        "priority_articles_available": len(priority),
        "cnn_signal_articles_available": len(cnn_signal),
        "general_articles_in_prompt": general_count,
        "max_prompt_articles": MAX_PROMPT_ARTICLES,
        "max_general_articles": MAX_GENERAL_ARTICLES,
        "max_articles_per_source": MAX_ARTICLES_PER_SOURCE,
        "source_counts_in_prompt": dict(sorted(source_counts.items())),
    }
    return selected, meta


def build_prompt(articles, window_start_str, window_end_str, window_start, window_end, te_events=None, briefing_type="morning"):
    """Build a Bloomberg-terminal-style dense feed for the LLM."""
    original_article_count = len(articles)
    articles, selection_meta = _select_prompt_articles(articles)
    if len(articles) != original_article_count:
        print(
            f"  Prompt articles: {len(articles)}/{original_article_count} "
            f"(source cap {MAX_ARTICLES_PER_SOURCE}, general cap {MAX_GENERAL_ARTICLES})"
        )

    # Natural language date range for display
    def _fmt_dt(dt):
        return dt.strftime("%b %-d %I%p").replace(" 0", " ")
    display_range = f"{_fmt_dt(window_start)} to {_fmt_dt(window_end)} HKT"
    greeting = "Good morning." if briefing_type == "morning" else "Good afternoon."
    briefing_label = "morning" if briefing_type == "morning" else "afternoon"
    lines = [
        f"News feed covering {window_start_str} to {window_end_str} (HKT).",
        "Source count and priority breakdown at the top, followed by every article.",
        f"Distill this into a structured {briefing_label} briefing.",
        "",
        "---",
        "",
    ]

    # For afternoon briefings, inject the morning report so the LLM knows what was already covered
    if briefing_type == "afternoon":
        morning_report = _load_morning_report(window_end)
        if morning_report:
            lines.append("## MORNING BRIEFING (already published — do NOT repeat)")
            lines.append("Skip any story already covered below unless there is a MATERIAL intraday update")
            lines.append("(new data, official statement, >1% price move, or escalation/de-escalation).")
            lines.append("")
            lines.append(morning_report)
            lines.append("")
            lines.append("---")
            lines.append("")

    # Source statistics
    from collections import Counter
    source_counts = Counter(a["source"] for a in articles)
    lines.append("## FEED STATISTICS")
    lines.append(f"Total articles fetched: {original_article_count}")
    lines.append(f"Articles included in prompt: {len(articles)}")
    lines.append(f"Sources hit: {len(source_counts)}")
    lines.append(f"Priority (CNY/KRW/TWD keywords >= 10): {sum(1 for a in articles if a['priority'] >= 10)}")
    lines.append(f"Medium (Fed/ECB/trade etc >= 3): {sum(1 for a in articles if 3 <= a['priority'] < 10)}")
    lines.append("")
    for src, cnt in source_counts.most_common():
        lines.append(f"  {src}: {cnt}")
    lines.append("")
    lines.append("---")
    lines.append("")

    # Inject persistent macro state (continuity across daily runs)
    macro_state = _load_macro_state()
    if macro_state:
        lines.append("## MACRO STATE (Persistent PM's Notebook — from prior briefings)")
        lines.append("This is YOUR prior analysis for continuity awareness. Use it to classify")
        lines.append("today's narratives as New / Acceleration / Reversal / Confirmation / Noise")
        lines.append("inside [AI Reasoning]. Do NOT use it as a factual source in Fact sections")
        lines.append("or Global Radar — only today's feed articles provide reportable facts.")
        lines.append("HARD BAN: no cumulative counts like 'third hike since Hormuz closure' or")
        lines.append("'Day 16 of blockade' in Fact sections unless today's articles explicitly")
        lines.append("state that number. If this state uses an older schema, convert it into")
        lines.append("Active Narratives / Watchpoints / Open Questions / Key Levels in the")
        lines.append("final <state_update> block.")
        lines.append("")
        lines.append(macro_state)
        lines.append("")
        lines.append("---")
        lines.append("")

    # Priority articles first
    priority = [a for a in articles if a["priority"] >= 10]
    if priority:
        lines.append("## HIGH-PRIORITY FEED (China / Korea / Taiwan FX)")
        for a in priority:
            lines.append(f"[{a['source']}] {a['title']}")
            body = a.get("full_text") or a.get("summary", "")
            if body:
                lines.append(f"  {body[:MAX_PRIORITY_BODY_CHARS]}")
            if a["link"]:
                lines.append(f"  URL: {a['link']}")
            lines.append("")
        lines.append("---")

    # CNN signal extraction — pre-filter: keep only articles with macro-relevant keywords
    cnn_articles = [a for a in articles if _is_cnn_macro_article(a)]
    if cnn_articles:
        lines.append("## CNN SIGNAL FEED (Sentiment & Speed — Must surface at least 1-2 in report)")
        for a in cnn_articles:
            lines.append(f"[{a['source']}] {a['title']}")
            body = a.get("full_text") or a.get("summary", "")
            if body:
                lines.append(f"  {body[:MAX_CNN_BODY_CHARS]}")
            if a["link"]:
                lines.append(f"  URL: {a['link']}")
            lines.append("")
        lines.append("---")

    # Remaining articles (non-CNN)
    others = [a for a in articles if a["priority"] < 10 and not a["source"].startswith("CNN_")]
    if others:
        lines.append("## GENERAL FEED")
        for a in others:
            lines.append(f"[{a['source']}] {a['title']}")
            body = a.get("full_text") or a.get("summary", "")
            if body:
                lines.append(f"  {body[:MAX_GENERAL_BODY_CHARS]}")
            if a["link"]:
                lines.append(f"  URL: {a['link']}")
            lines.append("")

    # TradingEconomics Economic Calendar — pre-processed in Python (filtered, HKT-converted, sorted)
    if te_events:
        lines.append("---")
        lines.append("")
        lines.append("## TRADINGECONOMICS ECONOMIC CALENDAR (Pre-processed — HKT times, next 24h only)")
        lines.append("Use this data directly for the Economic Calendar section. Times already in HKT. Events already filtered and sorted.")
        lines.append("Calendar values are scheduled releases/consensus/prior only, not published data. Do not cite TradingEconomics or these future events in Overview, Narrative Watch, or Global Radar.")
        lines.append("")
        lines.append("| Time (HKT) | Country | Event | Consensus | Prior |")
        lines.append("|------------|---------|-------|-----------|-------|")
        for e in te_events[:40]:
            lines.append(
                f"| {e['hkt_time']} | **{e['country']}** | {e['event']} | {e.get('consensus', '-')} | {e.get('previous', '-')} |"
            )
        lines.append("")

    # Output format spec
    lines.append("---")
    lines.append(f"""
Produce a markdown report for Obsidian following EXACTLY this structure and ordering. Window: {window_start_str} to {window_end_str} (HKT).

**SECTION ORDER (DO NOT REARRANGE):**
0. YAML Frontmatter (see below)
1. Header
2. Overview
3. Narrative Watch
4. Global Radar (includes Central Banks subsection)
5. Economic Calendar — Next 24 Hours
6. Full Reading List

---

[YAML FRONTMATTER: Start EVERY briefing with a valid YAML block enclosed by `---`. Generate a `tags` array — max 3-5 tags across: Macro/Theme (`Macro/Hormuz_Blockade`, `Macro/Tariffs`), Asset/Ticker (`Asset/Brent`, `Asset/CNH`), Trade/Bias (`Trade/Long_USD`, `Trade/Short_KRW`). No generic tags (`News`, `Economy`). Example:
---
tags:
  - Macro/Hormuz_Blockade
  - Asset/Brent
  - Asset/KRW
  - Trade/Long_Brent
  - Trade/Short_KRW
---
Then continue with the header below.]

---

<div align="center">

# 🌊 Macro Flux

<small>{display_range}</small>

<small>{greeting}</small>

</div>

---

> [!abstract] Overview
> [A single paragraph of 3-5 sentences. No bullet points, no lists. HARD BAN: NO source names in parentheses — no `(FT)`, `(BBG)`, `(Reuters)`, no source abbreviations of any kind. NO markdown citation links. NO AI Reasoning. This is pure synthesis in your own words — citations and analysis go in Narrative Watch, never here. RANK by macro market impact, NOT by headline volume. Lead with the event that has the largest transmission to rates, FX, commodities, or broad equity indices. If a PM reads only the first sentence, it must capture the dominant macro driver. Final sentence: the ONE cross-market thread connecting the day.]

OVERVIEW OUTPUT RULE: Output only the Overview paragraph inside the `> [!abstract] Overview` callout. Do not output template notes, examples, bracketed instructions, or markdown-formatting explanations. Every Overview content line must start with `> `.

---

## 🔬 Narrative Watch

[Choose Narrative Watch stories by market usefulness: cross-asset transmission, surprise versus Macro State, policy reaction function, next 24-72h catalyst, source quality, and Asia FX relevance. Feed priority is input metadata, not output order. Group stories ONLY when they share a DIRECT causal or thematic link — do NOT force unrelated stories under a broad heading just because they involve the same country or region. If two stories are only tangentially related, give each its own ### 📌 block. When in doubt, separate.]

**HEADLINE ANTI-HALLUCINATION**: Every claim in the `### 📌` headline MUST appear in at least one Fact citation below. No unsourced directional moves (e.g. "CNH fall" requires a cited CNH article), no unsourced country names, no synthesized narratives. If only KRW and JPY moved, write "KRW, JPY weaken" — do NOT add CNH or TWD.

**FACT — Direct excerpt only**: Copy key sentences verbatim from source articles. No rephrasing, no summarizing, no connecting commentary. Stitch selected excerpts into one continuous paragraph. Group excerpts from the same story under one `### 📌` block. If two articles don't share a direct factual thread, they belong in separate Narrative Watch blocks. Inline citations are sufficient — do NOT add a separate citation line at the end of the block.]

**CITATION VALIDITY CHECK**: Every Fact citation must be a clickable markdown link from today's feed: `([Source](URL))`. If a claim is inferred, extrapolated, unsupported, or lacks a feed URL, omit that claim. Never output bracketed source notes such as `([SCMP — Note: ...])`, `extrapolated`, `inferred`, or `no source`.

**FORECAST REVISION CHECK**: If an excerpt says a bank, official, or forecaster raised, lowered, revised, delayed, upgraded, or downgraded a forecast, it must include the specific forecast values and, when available, prior values and time horizons. Example: "USD/CNY to 6.80 in three months, 6.70 in six months, and 6.50 in 12 months, compared with previous forecasts of 6.85, 6.80, and 6.70." If the available excerpt only says "raised forecasts" without the values, omit that claim.

**ECONOMIC INDICATOR NAMING**: Use the official indicator name used by the source or calendar. Do NOT translate indicators into media shorthand or explanatory labels. Examples: use "US PPI" or "US Producer Price Index", not "wholesale inflation"; use "CPI", "Core CPI", "PCE Price Index", "Nonfarm Payrolls", "Initial Jobless Claims" exactly as the source names them.

**SOURCE URL RULES FOR GN FEEDS**: Articles from Google News RSS feeds (Reuters_GN, WSJ_GN, CNN_GN, Caixin_GN) have URLs starting with `https://news.google.com/rss/articles/`. These are Google News article pages that display the full article text. They are FUNCTIONAL links that readers can click to read the article. Use them as citation links when no direct source URL is available. NEVER fabricate a generic homepage URL like `https://www.reuters.com/` — this is worse than a Google News link. If an article has both a GN URL and a direct source URL, prefer the direct source URL. If only a GN URL is available, use it.]

### 📌 [Theme / Headline]

**Fact**
[Direct excerpts from source articles — copy verbatim, no rephrasing. Stitch all extracted sentences into a SINGLE continuous paragraph — no line breaks, no `>` prefixes, no hard returns between excerpts. Inline citations wrapped in parentheses: `([Source 1](URL1))` immediately after each excerpted claim. Same source multiple articles: number them — `([Source 1](URL1), [2](URL2))`. Example:
The US military will support the launch of "Project Freedom" beginning Monday to guide ships through the Strait of Hormuz ([US_CENTCOM](URL)). A bulk carrier was attacked by multiple small craft 11 nautical miles west of Sirik, Iran, according to UKMTO ([Al Jazeera 1](URL1)). The crew is safe and no environmental impact has been reported ([Al Jazeera 2](URL2)). Iran denounced the mission as a ceasefire violation ([Al Jazeera 3](URL3)).
Use `backticks` for ALL numeric values and tickers: `$125/bbl`, `3.2%`, `$34.5B`.]

> [!info] [AI Reasoning]
> * **Narrative change**: [New / Acceleration / Reversal / Confirmation / Noise versus Macro State or the prior briefing; explain why.]
> * **Transmission / Market Read**: [Growth / inflation / policy / liquidity / risk-premium / capital-flow channel, then directional asset view with bold assets.]
> * **Watchpoint / Confidence**: [24-72h confirmation or invalidation trigger; Confidence: High / Medium / Low based on source quality and cross-source confirmation.]

FORMAT CHECK: Every `> [!info] [AI Reasoning]` block must use exactly these three labels. Do not output redundant or legacy labels `What happened`, `Base Case`, or `Tactical Trade`, even if they appear in prior reports or morning context.

[Repeat for each priority theme.]

---

## 🌍 Global Radar

**EXCERPT mode — direct quotes from articles, no rephrasing. Each excerpt on its OWN bullet line starting with `- `, inline citation wrapped in parentheses `([Source](URL))` at the end. One excerpt per bullet. Use ONLY categories below that have content, in this EXACT order. No overlap with Narrative Watch. If a category has no valid bullets, OMIT that category heading entirely. Never output empty Global Radar category headings.**

**DEDUP RULE:** Before writing Global Radar, list the concrete events already covered in Narrative Watch. Omit any Global Radar bullet about the same event, datapoint, policy call, market move, or source article. Example: if Narrative Watch covers China exports/imports or Goldman delaying Fed cuts, Global Radar must NOT repeat those items in Economic Indicators or Central Banks. Global Radar is for additional high-impact items only.

**CALENDAR EXCLUSION RULE:** TradingEconomics calendar rows are scheduled events with Est./Prior values, not news excerpts or released data. Never cite TradingEconomics in Global Radar. Do not write future CPI/PPI/GDP/PMI consensus as if the data has printed. Put scheduled releases only in Economic Calendar.

### 📊 Economic Indicators
[Each bullet: one continuous excerpt line with `[Source](URL)` at end. Hard data ONLY after publication: inflation, employment, PMIs, GDP. Soft signals: recession warnings, central banker growth assessments, private-sector credit data, structural trade shifts. Calendar consensus/estimates for unreleased events are NOT hard data and must be omitted here. Country filter: US, CN, HK, EZ/EU, GB, JP, KR, TW, SG, AU.]

### 🏦 Central Banks
[Each bullet: `**Bank Name** — excerpt text [Source](URL)`. Only banks with new announcements. ORDER: PBOC → Fed → BOJ → BOK → CBC → RBA → ECB → BOE → others.]

### 🌐 Geopolitics & Policy
[Each bullet: one continuous excerpt line with `[Source](URL)` at end. Trade, sanctions, military, FX regime, sovereign/fiscal, capital controls, diplomacy. Every item must have clear market/macro transmission. NO corporate M&A, NO single-stock stories, NO earnings — those go in Equities or skip.]

### 🛢️ Commodities
[Each bullet: one continuous excerpt line with `[Source](URL)` at end. Oil, gas, metals, ags — supply/demand, inventory, price moves with macro driver.]

### 📈 Equities
[Each bullet: one continuous excerpt line with `[Source](URL)` at end. Index futures moves, sector rotation, vol regime, investor sentiment (e.g. Fear & Greed, put/call), index options flow. Sector heavyweight stocks only if macro theme (e.g. TSMC for AI capex). NO individual corporate stories, NO M&A, NO single-stock earnings.]

---

## 📅 Economic Calendar — Next 24 Hours

[Output the pre-filtered TRADINGECONOMICS ECONOMIC CALENDAR as a Markdown table. Use the column headers shown below. Each calendar event = one table row: Time (HKT) | Rgn | Event | Est. | Prior. Copy values directly from the TradingEconomics data above. Times are already in HKT.]

| Time (HKT) | Rgn | Event | Est. | Prior |

If the TRADINGECONOMICS table is empty or has no events, write: "No high-impact releases in the next 24 hours." Do NOT fill in events from memory or training data — ONLY use the provided table.

---

## 📚 Full Reading List
[2-level nested bullets. Level 1: `- **SourceName**`. Level 2: 2-space-indented `  - [Headline](URL)`. Sort: BBG → Reuters → WSJ → FT → CNBC → SCMP → BBC → CNN → others. Omit sources with zero articles.]

After the Full Reading List, output `<state_update>` block with updated macro state. REQUIRED — never skip.

<state_update>
## Active Narratives (max 4 — each line: **Name** — Status: New/Acceleration/Reversal/Confirmation/Fading/Resolved; why it matters; next trigger)
## Watchpoints (max 5 — concrete 24-72h triggers; remove resolved)
## Open Questions (max 3 — only unresolved questions that affect positioning)
## Key Levels (max 5 — only with today's source citations; include asset and why the level matters)
</state_update>

---
""")
    prompt = "\n".join(lines)
    write_json_artifact("prompt_meta.json", {
        **selection_meta,
        "briefing_type": briefing_type,
        "window_start_hkt": window_start_str,
        "window_end_hkt": window_end_str,
        "prompt_chars": len(prompt),
        "estimated_prompt_tokens": len(prompt) // 4,
        "max_priority_body_chars": MAX_PRIORITY_BODY_CHARS,
        "max_cnn_body_chars": MAX_CNN_BODY_CHARS,
        "max_general_body_chars": MAX_GENERAL_BODY_CHARS,
    })
    write_text_artifact("prompt_preview.txt", prompt[:12000])
    return prompt


# ---------------------------------------------------------------------------
# Claude API
# ---------------------------------------------------------------------------

def _has_invalid_ai_reasoning_labels(report):
    return bool(re.search(r"\*\s+\*\*(What happened|Base Case|Tactical Trade)\*\*:", report))


def _normalize_ai_reasoning_format(report):
    """Deterministically convert legacy AI Reasoning blocks to the current 3-bullet schema."""
    lines = report.splitlines()
    output = []
    i = 0

    def _bullet_value(block_lines, label):
        pattern = re.compile(rf"^>\s*\*\s+\*\*{re.escape(label)}\*\*:\s*(.*)$")
        for block_line in block_lines:
            match = pattern.match(block_line)
            if match:
                return match.group(1).strip()
        return ""

    while i < len(lines):
        line = lines[i]
        if not line.startswith("> [!info] [AI Reasoning]"):
            output.append(line)
            i += 1
            continue

        block = [line]
        i += 1
        while i < len(lines) and (lines[i].startswith(">") or not lines[i].strip()):
            block.append(lines[i])
            i += 1

        base_case = _bullet_value(block, "Base Case")
        tactical_trade = _bullet_value(block, "Tactical Trade")
        narrative_change = _bullet_value(block, "Narrative change")
        transmission = _bullet_value(block, "Transmission / Market Read")
        watchpoint = _bullet_value(block, "Watchpoint / Confidence")

        if base_case or tactical_trade:
            pivot_match = re.search(r"\bPIVOT\s+to\s+(.+)$", tactical_trade, flags=re.IGNORECASE)
            if pivot_match:
                transmission_text = tactical_trade[:pivot_match.start()].strip(" ;.")
                transmission_text = re.sub(
                    r"\s*;\s*(however|but)\s*,?\s*$",
                    "",
                    transmission_text,
                    flags=re.IGNORECASE,
                ).strip()
                watchpoint_text = f"Pivot to {pivot_match.group(1).replace('**', '').strip()}"
            else:
                transmission_text = tactical_trade.strip()
                watchpoint_text = "Monitor the next 24-72h catalyst cited in the Fact section."

            normalized = [
                "> [!info] [AI Reasoning]",
                f"> * **Narrative change**: Confirmation: {base_case}",
                f"> * **Transmission / Market Read**: {transmission_text or base_case}",
                f"> * **Watchpoint / Confidence**: {watchpoint_text}; Confidence: Medium.",
            ]
            output.extend(normalized)
            output.append("")
            continue

        normalized = ["> [!info] [AI Reasoning]"]
        if narrative_change:
            normalized.append(f"> * **Narrative change**: {narrative_change}")
        if transmission:
            normalized.append(f"> * **Transmission / Market Read**: {transmission}")
        if watchpoint:
            normalized.append(f"> * **Watchpoint / Confidence**: {watchpoint}")

        if len(normalized) == 4:
            output.extend(normalized)
            output.append("")
        else:
            output.extend([b for b in block if "**What happened**" not in b])

    return "\n".join(output)


def _repair_ai_reasoning_format(report):
    repair_prompt = f"""Convert ONLY the AI Reasoning blocks in this markdown report to the required three-bullet format.

Rules:
- Preserve all non-AI-Reasoning content exactly.
- Do not add facts, citations, sections, or sources.
- Each `> [!info] [AI Reasoning]` block must contain exactly these labels:
  1. `Narrative change`
  2. `Transmission / Market Read`
  3. `Watchpoint / Confidence`
- Do not output redundant or legacy labels `What happened`, `Base Case`, or `Tactical Trade`.
- Return the full markdown report only.

<report>
{report}
</report>
"""
    return call_claude(repair_prompt)


def _remove_invalid_citation_notes(report):
    """Drop sentences where the model emitted a non-source note as if it were a citation."""
    invalid_citation = re.compile(
        r"\(\[[^\]\n]*(?:Note:|extrapolat|inferred|unsupported|uncited|no source)[^\]\n]*\]\)",
        flags=re.IGNORECASE,
    )
    removed = 0
    cleaned_lines = []
    for line in report.splitlines():
        if not invalid_citation.search(line):
            cleaned_lines.append(line)
            continue

        sentences = re.split(r"(?<=[.!?])\s+", line)
        kept = []
        for sentence in sentences:
            if invalid_citation.search(sentence):
                removed += 1
                continue
            kept.append(sentence)
        cleaned_line = " ".join(kept).strip()
        if cleaned_line:
            cleaned_lines.append(cleaned_line)

    return "\n".join(cleaned_lines), removed


def _remove_template_instruction_leaks(report):
    """Remove prompt/template instructions that occasionally leak into the markdown report."""
    leak_patterns = [
        "Each line of the Overview content MUST start",
        "keep the callout box intact in MkDocs",
        "OVERVIEW OUTPUT RULE:",
        "Do not output template notes",
    ]
    removed = 0
    cleaned_lines = []
    for line in report.splitlines():
        if any(pattern in line for pattern in leak_patterns):
            removed += 1
            continue
        cleaned_lines.append(line)
    return "\n".join(cleaned_lines), removed


def _remove_tradingeconomics_global_radar_leaks(report):
    """TradingEconomics calendar rows belong only in the Economic Calendar table."""
    removed = 0
    cleaned_lines = []
    for line in report.splitlines():
        if line.startswith("- ") and "TradingEconomics" in line:
            removed += 1
            continue
        cleaned_lines.append(line)
    return "\n".join(cleaned_lines), removed


def _normalize_support_text(text):
    text = re.sub(r"\[[^\]]+\]\([^)]+\)", " ", text)
    text = re.sub(r"https?://\S+", " ", text)
    text = re.sub(r"[*_`>#|]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip().lower()


def _article_support_index(articles):
    index = {}
    for article in articles or []:
        link = article.get("link")
        if not link:
            continue
        body = " ".join(
            str(article.get(key, ""))
            for key in ("title", "summary", "full_text")
            if article.get(key)
        )
        index[link] = _normalize_support_text(body)
    return index


def _claim_supported_by_source(claim, source_text):
    claim = _normalize_support_text(claim)
    if not claim or not source_text:
        return False

    # Very short claims are too easy to match accidentally; require them to be title-level exact-ish.
    claim_words = claim.split()
    if len(claim_words) < 7:
        return claim in source_text

    if claim in source_text:
        return True

    window_size = max(len(claim), 140)
    best = SequenceMatcher(None, claim, source_text[:window_size]).ratio()
    step = max(40, window_size // 3)
    for start in range(0, max(1, len(source_text) - window_size + 1), step):
        chunk = source_text[start:start + window_size]
        best = max(best, SequenceMatcher(None, claim, chunk).ratio())
        if best >= 0.72:
            return True

    claim_tokens = {w for w in claim_words if len(w) > 3}
    source_tokens = set(source_text.split())
    if len(claim_tokens) >= 8:
        overlap = len(claim_tokens & source_tokens) / len(claim_tokens)
        return overlap >= 0.78
    return False


def _split_cited_segments(line):
    citation = re.compile(r"\(\[[^\]]+\]\((https?://[^)]+)\)(?:,\s*\[[^\]]+\]\((https?://[^)]+)\))*\)")
    segments = []
    pos = 0
    for match in citation.finditer(line):
        segment = line[pos:match.end()].strip()
        urls = re.findall(r"\]\((https?://[^)]+)\)", match.group(0))
        if segment:
            segments.append((segment, urls))
        pos = match.end()
    trailing = line[pos:].strip()
    if trailing and not re.fullmatch(r"[.。;；,，:：!?！？\s]+", trailing):
        segments.append((trailing, []))
    return segments


def _remove_unsupported_global_radar_segments(report, articles):
    """Keep Global Radar in excerpt mode by dropping cited segments not found in source text."""
    if not articles:
        return report, 0

    support_index = _article_support_index(articles)
    lines = report.splitlines()
    cleaned = []
    in_global_radar = False
    removed = 0

    for line in lines:
        if line.startswith("## "):
            in_global_radar = line.startswith("## 🌍 Global Radar")
            cleaned.append(line)
            continue

        if not in_global_radar or not line.startswith("- "):
            cleaned.append(line)
            continue

        kept_segments = []
        for segment, urls in _split_cited_segments(line):
            if not urls:
                removed += 1
                continue
            claim = re.sub(r"^[-\s.。;；,，:：!?！？]*(?:\*\*[^*]+\*\*\s*[—-]\s*)?", "", segment)
            if any(_claim_supported_by_source(claim, support_index.get(url, "")) for url in urls):
                kept_segments.append(segment)
            else:
                removed += 1

        if kept_segments:
            rebuilt = " ".join(kept_segments)
            if not rebuilt.startswith("- "):
                rebuilt = "- " + rebuilt.lstrip("- ").strip()
            if not re.search(r"[.!?。！？]\s*$", rebuilt):
                rebuilt += "."
            cleaned.append(rebuilt)

    return "\n".join(cleaned), removed


def _remove_empty_global_radar_sections(report):
    """Drop Global Radar subsection headings that have no bullet content."""
    lines = report.splitlines()
    cleaned = []
    in_global_radar = False
    pending_heading = None
    pending_body = []
    removed = 0

    def flush_pending():
        nonlocal pending_heading, pending_body, removed
        if pending_heading is None:
            return
        has_bullet = any(line.startswith("- ") for line in pending_body)
        if has_bullet:
            cleaned.append(pending_heading)
            cleaned.extend(pending_body)
        else:
            # Preserve section separators that belong to the following top-level section.
            for body_line in pending_body:
                if body_line.strip() == "---":
                    cleaned.append(body_line)
            removed += 1
        pending_heading = None
        pending_body = []

    for line in lines:
        if line.startswith("## "):
            if in_global_radar:
                flush_pending()
            in_global_radar = line.startswith("## 🌍 Global Radar")
            cleaned.append(line)
            continue

        if not in_global_radar:
            cleaned.append(line)
            continue

        if line.startswith("### "):
            flush_pending()
            pending_heading = line
            pending_body = []
            continue

        if pending_heading is not None:
            pending_body.append(line)
        else:
            cleaned.append(line)

    if in_global_radar:
        flush_pending()

    # Avoid stacks of blank lines left by removed empty categories, while keeping
    # the standard blank line after horizontal rules before top-level headings.
    result = re.sub(r"\n{3,}", "\n\n", "\n".join(cleaned))
    result = re.sub(r"\n---\n(?=## )", "\n---\n\n", result)
    return result, removed


def _normalize_header_greeting(report, briefing_type):
    """Force the header greeting to match the requested briefing type."""
    expected = "Good morning." if briefing_type == "morning" else "Good afternoon."
    return re.sub(
        r"<small>Good (morning|afternoon)\.</small>",
        f"<small>{expected}</small>",
        report,
        count=1,
    )


def _validate_markdown(report, articles=None):
    """Post-process LLM output to fix common markdown syntax errors."""
    import re
    fixes = 0

    report, invalid_citation_notes = _remove_invalid_citation_notes(report)
    if invalid_citation_notes:
        print(f"  [validate] Removed {invalid_citation_notes} invalid citation note sentence(s)")

    report, template_instruction_leaks = _remove_template_instruction_leaks(report)
    if template_instruction_leaks:
        print(f"  [validate] Removed {template_instruction_leaks} template instruction leak line(s)")

    if _has_invalid_ai_reasoning_labels(report):
        report = _normalize_ai_reasoning_format(report)
        print("  [validate] Normalized invalid AI Reasoning labels")

    report, te_leaks = _remove_tradingeconomics_global_radar_leaks(report)
    if te_leaks:
        print(f"  [validate] Removed {te_leaks} TradingEconomics Global Radar leak line(s)")

    report, unsupported_radar_segments = _remove_unsupported_global_radar_segments(report, articles)
    if unsupported_radar_segments:
        print(f"  [validate] Removed {unsupported_radar_segments} unsupported Global Radar segment(s)")

    report, empty_radar_sections = _remove_empty_global_radar_sections(report)
    if empty_radar_sections:
        print(f"  [validate] Removed {empty_radar_sections} empty Global Radar section(s)")

    # 1. Fix unbalanced brackets in markdown links: count [ and ]( per line
    lines = report.split("\n")
    for i, line in enumerate(lines):
        opens = line.count("[")
        link_defs = line.count("](")
        closes = line.count("]") - link_defs  # ] that are not part of link
        if opens > closes + link_defs:
            # Missing closing bracket somewhere
            pass  # Too complex to auto-fix safely

    # 2. Fix unclosed parenthesized citation groups: ([Source](URL), [2](URL). → missing )
    # Pattern: ([Text](URL).  where the outer ( is not closed before next sentence
    _fix_count = [0]
    def _fix_unclosed_parens(text):
        # Find patterns like: , [N](URL). NextSentence — missing ) after URL
        # Matches: ([Source 1](URL), [2](URL).  → should be ([Source 1](URL), [2](URL)).
        pattern = r'(\(\[[^\]]+\]\([^)]+\),\s*\[(\d+)\]\(([^)]+)\))\.(\s+[A-Z])'
        while re.search(pattern, text):
            text = re.sub(pattern, r'\1).\4', text)
            _fix_count[0] += 1
        # Also: ([Source 1](URL), [2](URL) NextWord
        pattern2 = r'(\(\[[^\]]+\]\([^)]+\),\s*\[(\d+)\]\(([^)]+)\))(\s+[A-Z])'
        while re.search(pattern2, text):
            text = re.sub(pattern2, r'\1)\4', text)
            _fix_count[0] += 1
        return text

    report = _fix_unclosed_parens(report)

    # 3. Check for unbalanced backticks
    backtick_count = report.count("`")
    if backtick_count % 2 != 0:
        print(f"  [validate] ⚠️ Odd number of backticks ({backtick_count}) — possible unclosed code span")

    if _fix_count[0] > 0:
        print(f"  [validate] Fixed {_fix_count[0]} markdown syntax issue(s)")

    return report


def _call_claude_once(user_message):
    """Call Claude API via Anthropic SDK."""
    base_url = os.environ.get("ANTHROPIC_BASE_URL", "https://api.deepseek.com/anthropic").rstrip("/")
    api_key = os.environ["ANTHROPIC_AUTH_TOKEN"]
    client = Anthropic(
        base_url=base_url,
        api_key=api_key,
        timeout=LLM_TIMEOUT_SECONDS,
    )
    response = client.messages.create(
        model=MODEL,
        max_tokens=MAX_OUTPUT_TOKENS,
        temperature=0.7,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_message}],
        thinking={"type": "enabled", "budget_tokens": 8192},
    )
    # Extended thinking returns multiple content blocks; extract just the text
    for block in response.content:
        if getattr(block, "type", None) == "text":
            return block.text
    return response.content[0].text


def _call_claude_worker(user_message, result_path):
    try:
        text = _call_claude_once(user_message)
        payload = {"ok": True, "text": text}
    except BaseException as exc:
        payload = {
            "ok": False,
            "error": repr(exc),
            "traceback": traceback.format_exc(),
        }
    Path(result_path).write_text(json.dumps(payload), encoding="utf-8")


def call_claude(user_message):
    """Call Claude in a child process so launchd cannot hang forever on a stuck SDK call."""
    with tempfile.TemporaryDirectory(prefix="macro_flux_llm_") as tmpdir:
        result_path = Path(tmpdir) / "result.json"
        proc = mp.Process(target=_call_claude_worker, args=(user_message, str(result_path)))
        proc.start()
        proc.join(LLM_TIMEOUT_SECONDS)

        if proc.is_alive():
            proc.terminate()
            proc.join(10)
            if proc.is_alive() and hasattr(proc, "kill"):
                proc.kill()
                proc.join(5)
            msg = f"Claude API call exceeded hard timeout of {LLM_TIMEOUT_SECONDS}s"
            write_text_artifact("llm_timeout.txt", msg + "\n")
            raise TimeoutError(msg)

        if not result_path.exists():
            raise RuntimeError(f"Claude worker exited without writing result (exit code {proc.exitcode})")

        payload = json.loads(result_path.read_text(encoding="utf-8"))
        if payload.get("ok"):
            return payload.get("text", "")

        write_text_artifact("llm_worker_error.txt", payload.get("traceback", payload.get("error", "")))
        raise RuntimeError(payload.get("error", "Claude worker failed"))


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

GITHUB_PAGES_REPO = Path(os.environ.get("BRIEFING_REPO_PATH", Path.home() / "daily-briefing"))
GITHUB_PAGES_URL = "https://sharonxu16.github.io/macro-flux/"


def _generate_archive_md(docs_dir):
    """Generate docs/archive.md listing all past briefings."""
    past_dir = docs_dir / "past"
    if not past_dir.exists():
        return
    files = sorted(past_dir.glob("20*.md"), reverse=True)
    if not files:
        return
    # Sort: newest date first; within same date, afternoon (chronologically later) before morning
    def _archive_sort_key(f):
        stem = f.stem  # e.g. "2026-05-04-morning"
        parts = stem.rsplit("-", 1)
        date_str = parts[0]
        is_afternoon = 1 if len(parts) > 1 and parts[1] == "afternoon" else 0
        return (date_str, is_afternoon)
    files.sort(key=_archive_sort_key, reverse=True)
    lines = ["# Past Macro Flux", ""]
    for f in files:
        stem = f.stem  # e.g. "2026-05-04-morning"
        lines.append(f"- [{stem}](past/{stem}.md)")
    (docs_dir / "archive.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _strip_ai_reasoning_blocks(markdown):
    """Remove prior report AI Reasoning blocks used only for afternoon de-duplication."""
    lines = markdown.splitlines()
    cleaned = []
    skipping = False
    for line in lines:
        if line.startswith("> [!info] [AI Reasoning]"):
            skipping = True
            continue
        if skipping:
            if line.startswith(">") or not line.strip():
                continue
            skipping = False
        cleaned.append(line)
    return "\n".join(cleaned).strip()


def _load_morning_report(window_end):
    """Load today's morning report for afternoon briefing context.
    Returns the markdown text (stripped of Full Reading List to save tokens), or None."""
    date_str = window_end.strftime("%Y-%m-%d")
    morning_path = GITHUB_PAGES_REPO / "docs" / "past" / f"{date_str}-morning.md"
    if not morning_path.exists():
        return None
    text = morning_path.read_text(encoding="utf-8")
    # Strip Full Reading List section (long, not needed for context)
    frl_marker = "## 📚 Full Reading List"
    if frl_marker in text:
        text = text[:text.index(frl_marker)].strip()
    text = _strip_ai_reasoning_blocks(text)
    # Truncate if still too long; morning context is only for de-duplication.
    if len(text) > MAX_MORNING_CONTEXT_CHARS:
        text = text[:MAX_MORNING_CONTEXT_CHARS] + "\n\n[...truncated for context window...]"
    return text


def _load_macro_state():
    """Read the persistent macro state notebook."""
    state_path = GITHUB_PAGES_REPO / "docs" / "macro_state.md"
    if state_path.exists():
        return state_path.read_text(encoding="utf-8").strip()
    return ""


def _save_macro_state(state_content, window_end):
    """Save updated macro state with timestamp."""
    state_path = GITHUB_PAGES_REPO / "docs" / "macro_state.md"
    date_str = window_end.strftime("%Y-%m-%d")
    header = f"# Macro State — Last updated: {date_str}\n\n"
    state_text = header + state_content.strip() + "\n"
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(state_text, encoding="utf-8")
    # Also save to Obsidian vault for monitoring
    vault_state = OUTPUT_DIR.parent / "macro_state.md"
    try:
        vault_state.write_text(state_text, encoding="utf-8")
    except OSError:
        pass  # GitHub Actions won't have the vault path
    print(f"  [state] Macro state saved ({len(state_content)} chars)")


def _write_docs_to_repo(repo, report_md, window_end, briefing_type="morning"):
    """Write report files into a repo checkout without touching git state."""
    date_str = window_end.strftime("%Y-%m-%d")
    docs_dir = repo / "docs"
    docs_dir.mkdir(parents=True, exist_ok=True)
    # Only update index.md if this is today's briefing (not a re-run of past dates)
    if window_end.strftime("%Y-%m-%d") == datetime.now(LOCAL_TZ).strftime("%Y-%m-%d"):
        (docs_dir / "index.md").write_text(report_md, encoding="utf-8")
    past_dir = docs_dir / "past"
    past_dir.mkdir(parents=True, exist_ok=True)
    (past_dir / f"{date_str}-{briefing_type}.md").write_text(report_md, encoding="utf-8")
    _generate_archive_md(docs_dir)


def _save_docs(report_md, window_end, briefing_type="morning"):
    """Save markdown to docs/ for MkDocs build. Runs synchronously."""
    repo = GITHUB_PAGES_REPO
    if not (repo / ".git").exists():
        return
    _write_docs_to_repo(repo, report_md, window_end, briefing_type)


def deploy_to_github_pages(report_md, window_end, briefing_type="morning"):
    """Commit and push using a clean origin/main worktree, avoiding local branch divergence."""
    import shutil
    import subprocess
    import tempfile

    repo = GITHUB_PAGES_REPO
    if not (repo / ".git").exists():
        print(f"  [deploy] Repo not found at {repo}, skipping")
        return

    date_str = window_end.strftime("%Y-%m-%d")
    commit_msg = f"Briefing {date_str}-{briefing_type} {datetime.now(LOCAL_TZ).strftime('%H:%M')} (HKT)"
    local_state = repo / "docs" / "macro_state.md"

    for attempt in (1, 2):
        tmp_dir = None
        try:
            subprocess.run(["git", "-C", str(repo), "fetch", "origin", "main"],
                           capture_output=True, timeout=60, check=True)
            tmp_dir = Path(tempfile.mkdtemp(prefix="macro-flux-deploy-", dir="/private/tmp"))
            worktree = tmp_dir / "repo"
            subprocess.run(["git", "-C", str(repo), "worktree", "add", "--detach", str(worktree), "origin/main"],
                           capture_output=True, timeout=60, check=True)

            _write_docs_to_repo(worktree, report_md, window_end, briefing_type)
            if local_state.exists():
                target_state = worktree / "docs" / "macro_state.md"
                target_state.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(local_state, target_state)

            subprocess.run(["git", "-C", str(worktree), "config", "user.name", "sharonxu16"],
                           capture_output=True, timeout=10)
            subprocess.run(["git", "-C", str(worktree), "config", "user.email", "sharonxu16@users.noreply.github.com"],
                           capture_output=True, timeout=10)
            subprocess.run(["git", "-C", str(worktree), "add", "-A"],
                           capture_output=True, timeout=10, check=True)
            result = subprocess.run(["git", "-C", str(worktree), "commit", "-m", commit_msg],
                                    capture_output=True, timeout=20)
            if result.returncode == 1:
                print("  [deploy] No changes to commit")
                return
            if result.returncode != 0:
                print(f"  [deploy] ⚠️ Commit failed: {result.stderr.decode()[:200]}")
                return

            push = subprocess.run(["git", "-C", str(worktree), "push", "origin", "HEAD:main"],
                                  capture_output=True, timeout=300)
            if push.returncode == 0:
                print(f"  [deploy] ✅ {GITHUB_PAGES_URL}")
                return
            print(f"  [deploy] ⚠️ Push attempt {attempt} failed: {push.stderr.decode()[:200]}")
        except Exception as e:
            print(f"  [deploy] ⚠️ Deploy attempt {attempt} failed: {e}")
        finally:
            if tmp_dir is not None:
                try:
                    worktree = tmp_dir / "repo"
                    if worktree.exists():
                        subprocess.run(["git", "-C", str(repo), "worktree", "remove", "--force", str(worktree)],
                                       capture_output=True, timeout=30)
                    shutil.rmtree(tmp_dir, ignore_errors=True)
                except Exception:
                    pass
        if attempt == 1:
            time.sleep(2)

    print("  [deploy] ⚠️ Deploy failed after retry")


def save_report(markdown, window_start, window_end, briefing_type="morning"):
    """Save briefing to markdown file. Naming matches website: YYYY-MM-DD-{morning,afternoon}.md
    On GitHub Actions where the local Obsidian path doesn't exist, saves to the repo docs/ instead."""
    date_str = window_end.strftime("%Y-%m-%d")
    filename = f"{date_str}-{briefing_type}.md"
    try:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        path = OUTPUT_DIR / filename
    except OSError:
        # Running on GitHub Actions (no /Users/ on Linux) — save to repo docs/ as fallback
        path = GITHUB_PAGES_REPO / "docs" / filename
        path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(markdown, encoding="utf-8")
    print(f"\n[saved] {path}")
    return path


def _env_flag(name, default=False):
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _email_recipients():
    raw = os.environ.get("BRIEFING_EMAIL_RECIPIENTS", "")
    for sep in (";", "\n"):
        raw = raw.replace(sep, ",")
    return [item.strip() for item in raw.split(",") if item.strip()]


def _extract_overview(markdown):
    lines = markdown.splitlines()
    overview = []
    in_overview = False
    for line in lines:
        if line.startswith("> [!abstract] Overview"):
            in_overview = True
            continue
        if in_overview:
            if line.startswith("---") or line.startswith("## "):
                break
            if line.startswith(">"):
                overview.append(line.lstrip("> ").strip())
            elif line.strip():
                overview.append(line.strip())
    return "\n".join(part for part in overview if part).strip()


def _markdown_email_html(markdown_text, title):
    email_markdown = markdown_text
    try:
        import markdown as markdown_lib
        rendered = markdown_lib.markdown(
            email_markdown,
            extensions=["extra", "sane_lists", "nl2br"],
            output_format="html5",
        )
    except Exception:
        rendered = "<pre>" + escape(email_markdown) + "</pre>"

    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <style>
    body {{ font-family: Arial, Helvetica, sans-serif; color: #1f2933; line-height: 1.55; font-size: 15px; }}
    .container {{ max-width: 860px; margin: 0 auto; padding: 20px; }}
    .meta {{ color: #667085; font-size: 13px; margin-bottom: 20px; }}
    h1, h2, h3 {{ color: #111827; line-height: 1.25; margin: 24px 0 10px; }}
    h1 {{ font-size: 24px; }}
    h2 {{ font-size: 20px; border-bottom: 1px solid #e5e7eb; padding-bottom: 6px; }}
    h3 {{ font-size: 17px; }}
    p {{ margin: 10px 0; }}
    ul, ol {{ padding-left: 24px; margin: 10px 0; }}
    li {{ margin: 5px 0; }}
    blockquote {{ border-left: 4px solid #9ca3af; margin: 14px 0; padding: 8px 14px; background: #f8fafc; color: #374151; }}
    table {{ border-collapse: collapse; width: 100%; margin: 14px 0; }}
    th, td {{ border: 1px solid #d0d5dd; padding: 7px 9px; vertical-align: top; }}
    th {{ background: #f3f4f6; text-align: left; }}
    code {{ font-family: Consolas, Menlo, monospace; background: #f3f4f6; padding: 1px 4px; border-radius: 3px; }}
    pre {{ white-space: pre-wrap; word-wrap: break-word; background: #f8fafc; border: 1px solid #e5e7eb; padding: 12px; }}
    a {{ color: #175cd3; }}
  </style>
</head>
<body>
  <div class="container">
    <div class="meta">{escape(title)}</div>
    {rendered}
  </div>
</body>
</html>"""

def send_briefing_email(report_md, report_name, briefing_type, report_path=None, website_url=GITHUB_PAGES_URL):
    """Send a completion email when SMTP settings are configured."""
    recipients = _email_recipients()
    smtp_host = os.environ.get("SMTP_HOST", "").strip()
    smtp_port = env_int("SMTP_PORT", 587, min_value=1)
    smtp_user = os.environ.get("SMTP_USER", "").strip()
    smtp_password = os.environ.get("SMTP_PASSWORD", "")
    smtp_from = os.environ.get("SMTP_FROM", smtp_user).strip()

    if not recipients:
        print("  [email] No BRIEFING_EMAIL_RECIPIENTS configured; skipping")
        return False
    if not smtp_host or not smtp_from:
        print("  [email] SMTP_HOST/SMTP_FROM not configured; skipping")
        return False

    label = briefing_type.capitalize()
    subject = f"Macro Flux {label} Briefing - {report_name.replace('.md', '')}"
    body_parts = [
        f"Macro Flux {label} briefing finished.",
        f"Report: {report_name}",
        "",
        report_md,
    ]

    msg = EmailMessage()
    msg["From"] = smtp_from
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject
    body_text = "\n".join(body_parts)
    msg.set_content(body_text)
    msg.add_alternative(_markdown_email_html(report_md, "Macro Flux " + label + " Briefing - " + report_name), subtype="html")

    try:
        use_ssl = _env_flag("SMTP_USE_SSL", smtp_port == 465)
        use_starttls = _env_flag("SMTP_STARTTLS", not use_ssl)
        smtp_cls = smtplib.SMTP_SSL if use_ssl else smtplib.SMTP
        with smtp_cls(smtp_host, smtp_port, timeout=30) as smtp:
            if not use_ssl:
                smtp.ehlo()
                if use_starttls:
                    smtp.starttls()
                    smtp.ehlo()
            if smtp_user and smtp_password:
                smtp.login(smtp_user, smtp_password)
            smtp.send_message(msg)
        print(f"  [email] Sent briefing email to {len(recipients)} recipient(s)")
        return True
    except Exception as exc:
        print(f"  [email] ⚠️ Failed to send email: {exc}", file=sys.stderr)
        write_text_artifact("email_error.txt", repr(exc) + "\n")
        return False


def fallback_report(articles, window_start, window_end):
    """Produce a headline-only report when Claude API is unavailable."""
    start_str = window_start.strftime("%Y-%m-%d %H:%M")
    end_str = window_end.strftime("%Y-%m-%d %H:%M")
    lines = [
        f"# Macro Flux — {start_str} to {end_str}",
        "",
        "> Claude API unavailable. RSS headlines only.",
        "",
    ]
    priority = [a for a in articles if a["priority"] >= 10]
    others = [a for a in articles if a["priority"] < 10]

    if priority:
        lines.append("## Priority: China / Korea / Taiwan")
        for a in priority:
            lines.append(f"- [{a['title']}]({a['link']}) — {a['source']}")
            if a.get("summary"):
                lines.append(f"  {a['summary'][:300]}")
        lines.append("")

    lines.append("## All Articles")
    prev_source = None
    for a in articles:
        if a["source"] != prev_source:
            lines.append(f"### {a['source']}")
            prev_source = a["source"]
        lines.append(f"- [{a['title']}]({a['link']})")
        if a.get("summary"):
            lines.append(f"  {a['summary'][:200]}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _parse_time_arg(arg, name):
    """Parse 'YYYY-MM-DD HH:MM' string in HKT. Exit on failure."""
    try:
        dt = datetime.strptime(arg, "%Y-%m-%d %H:%M")
        return dt.replace(tzinfo=LOCAL_TZ)
    except ValueError:
        print(f"[error] --{name} must be 'YYYY-MM-DD HH:MM' (HKT), got: {arg}", file=sys.stderr)
        sys.exit(1)


def _handle_run_timeout(signum, frame):
    msg = f"Run exceeded hard timeout of {RUN_TIMEOUT_SECONDS}s; aborting so backup schedules can retry."
    print(f"  [error] {msg}", file=sys.stderr)
    write_text_artifact("run_timeout.txt", msg + "\n")
    sys.exit(124)


def _arm_run_timeout():
    if RUN_TIMEOUT_SECONDS and hasattr(signal, "SIGALRM"):
        signal.signal(signal.SIGALRM, _handle_run_timeout)
        signal.alarm(RUN_TIMEOUT_SECONDS)


def _clear_run_timeout():
    if hasattr(signal, "SIGALRM"):
        signal.alarm(0)


def main():
    _load_env_from_claude_config()
    _arm_run_timeout()

    # Parse --from / --to / --morning / --afternoon / --overnight (deprecated alias)
    from_arg = None
    to_arg = None
    morning = False
    afternoon = False
    args = sys.argv[1:]
    i = 0
    while i < len(args):
        if args[i] == "--from" and i + 1 < len(args):
            from_arg = args[i + 1]; i += 2
        elif args[i] == "--to" and i + 1 < len(args):
            to_arg = args[i + 1]; i += 2
        elif args[i] in ("--morning", "--overnight"):
            morning = True; i += 1
        elif args[i] == "--afternoon":
            afternoon = True; i += 1
        else:
            i += 1

    now = datetime.now(LOCAL_TZ)
    today_8am = now.replace(hour=8, minute=0, second=0, microsecond=0)

    if from_arg and to_arg:
        window_start = _parse_time_arg(from_arg, "from")
        window_end = _parse_time_arg(to_arg, "to")
        if window_start >= window_end:
            print("[error] --from must be before --to", file=sys.stderr)
            sys.exit(1)
        # Determine type from window: if window_end is in the morning (<=10:00), it's morning
        if afternoon:
            briefing_type = "afternoon"
        elif morning:
            briefing_type = "morning"
        else:
            briefing_type = "morning"
    elif from_arg or to_arg:
        print("[error] Both --from and --to must be provided together.", file=sys.stderr)
        sys.exit(1)
    elif afternoon:
        # Afternoon window: today 08:00 → 18:00
        window_start = today_8am
        window_end = today_8am + timedelta(hours=10)
        briefing_type = "afternoon"
    else:
        # Morning (default): yesterday 18:00 → today 08:00
        window_end = today_8am
        window_start = today_8am - timedelta(hours=14)
        briefing_type = "morning"

    window_start_str = window_start.strftime("%Y-%m-%d %H:%M")
    window_end_str = window_end.strftime("%Y-%m-%d %H:%M")

    explicit_window = bool(from_arg and to_arg)
    report_date = window_end.date() if briefing_type == "morning" else window_start.date()
    report_name = f"{report_date:%Y-%m-%d}-{briefing_type}.md"
    existing_report_paths = [
        OUTPUT_DIR / report_name,
        GITHUB_PAGES_REPO / "docs" / "past" / report_name,
    ]
    if not explicit_window and any(path.exists() and path.stat().st_size > 0 for path in existing_report_paths):
        print(f"[macro-flux {briefing_type}] {report_name} already exists; skipping automatic backup run.")
        _clear_run_timeout()
        return

    print(f"[macro-flux {briefing_type}] {window_start_str} → {window_end_str} (HKT)")
    print("=" * 60)
    write_json_artifact("run_context.json", {
        "briefing_type": briefing_type,
        "window_start_hkt": window_start_str,
        "window_end_hkt": window_end_str,
        "model": MODEL,
        "max_output_tokens": MAX_OUTPUT_TOKENS,
        "fetch_round_timeout_seconds": FETCH_ROUND_TIMEOUT_SECONDS,
        "enhance_timeout_seconds": ENHANCE_TIMEOUT_SECONDS,
        "llm_timeout_seconds": LLM_TIMEOUT_SECONDS,
        "run_timeout_seconds": RUN_TIMEOUT_SECONDS,
    })

    # Stage 1: Fetch all RSS feeds
    total_sources = len(RSS_FEEDS) + len(HTML_SOURCES)
    print(f"\n[1/3] Fetching {total_sources} sources ({len(RSS_FEEDS)} RSS + {len(HTML_SOURCES)} HTML) ({FEED_TIMEOUT}s timeout each)...")
    articles = fetch_all_feeds(window_start, window_end)
    priority_count = sum(1 for a in articles if a["priority"] >= 10)
    medium_count = sum(1 for a in articles if 3 <= a["priority"] < 10)
    print(f"  Total: {len(articles)} articles ({priority_count} high-priority, {medium_count} medium)")
    if len(articles) < MIN_ARTICLES_TO_PUBLISH:
        msg = (
            f"Only {len(articles)} articles fetched; below publish threshold "
            f"{MIN_ARTICLES_TO_PUBLISH}. Aborting to avoid publishing a hollow report."
        )
        print(f"  [error] {msg}", file=sys.stderr)
        write_text_artifact("fetch_quality_gate.txt", msg + "\n")
        sys.exit(2)

    # Fetch TradingEconomics economic calendar (next 24h events with consensus/prior)
    te_events = _fetch_te_calendar(window_end)
    if te_events:
        print(f"  TE Calendar: {len(te_events)} events scraped")

    # Stage 2: Claude API
    print("\n[2/3] Calling Claude API...")
    report = None
    for attempt in (1, 2):
        try:
            user_message = build_prompt(articles, window_start_str, window_end_str, window_start, window_end, te_events, briefing_type)
            print(f"  Prompt: {len(user_message)} chars (~{len(user_message)//4} tokens)")
            report = call_claude(user_message)
            break
        except Exception as e:
            print(f"  [error] Attempt {attempt}: {e}", file=sys.stderr)
            write_text_artifact("llm_error.txt", f"Attempt {attempt}: {e}\n")
            if attempt == 1:
                time.sleep(5)

    if not report:
        print("  Falling back to RSS-only report...")
        report = fallback_report(articles, window_start, window_end)

    # Parse macro state update from LLM output (strip before publishing)
    if report and "<state_update>" in report:
        report_body, rest = report.split("<state_update>", 1)
        state_update = rest.split("</state_update>")[0] if "</state_update>" in rest else rest
        _save_macro_state(state_update.strip(), window_end)
        # Remove the entire state_update block from report
        if "</state_update>" in rest:
            after_state = rest.split("</state_update>", 1)[1]
            report = (report_body + after_state).strip()
        else:
            report = report_body.strip()
    elif report:
        print("  [state] No <state_update> found in LLM output — state not updated")

    if _has_invalid_ai_reasoning_labels(report):
        print("  [format] Invalid AI Reasoning labels detected; applying deterministic normalization")
        write_text_artifact("invalid_ai_reasoning_report.txt", report)
        report = _normalize_ai_reasoning_format(report)

    if _has_invalid_ai_reasoning_labels(report):
        print("  [format] Deterministic normalization incomplete; requesting format-only repair")
        try:
            repaired = _repair_ai_reasoning_format(report)
            if repaired and not _has_invalid_ai_reasoning_labels(repaired):
                report = repaired
                print("  [format] AI Reasoning format repaired")
            else:
                print("  [format] Repair did not fully remove invalid labels; keeping original output")
        except Exception as e:
            print(f"  [format] Repair failed: {e}", file=sys.stderr)

    # Post-processing: validate and fix common markdown issues
    report = _validate_markdown(report, articles)
    report = _normalize_header_greeting(report, briefing_type)

    # Stage 3: Save + Deploy
    print("\n[3/3] Saving report...")
    path = save_report(report, window_start, window_end, briefing_type)

    # Save docs to local repo (sync — must complete before exit)
    _save_docs(report, window_end, briefing_type)

    report_name = path.name
    if os.environ.get("GITHUB_ACTIONS") == "true":
        print("  [deploy] GitHub Actions will commit and deploy this report")
    else:
        deploy_to_github_pages(report, window_end, briefing_type)
        send_briefing_email(report, report_name, briefing_type, path)

    _clear_run_timeout()
    print("\nDone.")


if __name__ == "__main__":
    main()
