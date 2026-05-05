# 🌊 Macro Flux

Your best macro news synthesizer. 50+ sources distilled & cross-referenced. Get informed in 90 seconds by Asia open.

[sharonxu16.github.io/macro-flux](https://sharonxu16.github.io/macro-flux/)

## Structure

- **Overview** — LLM-synthesized narrative, cross-market thesis
- **Narrative Watch** — Key macro stories: Fact (source excerpts) → [AI Reasoning] (macro lens + tactical trade)
- **Global Radar** — Source excerpts by theme: Economic Indicators, Central Banks, Geopolitics, Commodities, Equities
- **Economic Calendar** — Next 24h high-impact releases, consensus/prior from TradingEconomics
- **Full Reading List** — All referenced articles with source attribution

## Data Sources

50 sources scanned every window: 43 RSS feeds + 7 scraped pages.

| Tier | Sources |
|---|---|
| Primary | Bloomberg, FT, WSJ, Reuters |
| Supplementary | CNBC, SCMP, BBC Business, Economist |
| China Local | 华尔街见闻, 财新, 信报, CCTV |
| Singapore | MAS, CNA, Business Times |
| Sentiment | CNN (via Google News) |
| Calendar | TradingEconomics (scraped) |
| Official | Federal Reserve, ECB, BOE, BOJ, BOK, RBA, CBC, PBOC, HKMA |
| Geopolitics | US CENTCOM, ISW, Al Jazeera |
| Energy | EIA, IEA, S&P Global Commodities, Lloyd's List |

## Delivery

- **Morning**: 08:05 HKT (18:00–08:00 overnight)
- **Afternoon**: 18:05 HKT (08:00–18:00 intraday)
- **Tone**: Central bank research note — measured, probabilistic, no hyperbole
- **AI**: Only [AI Reasoning] blocks are model-generated analysis. Overview is LLM synthesis. Everything else is source excerpts.
- **Tech**: Python → 50 RSS/HTML sources → LLM (DeepSeek) → MkDocs Material → GitHub Pages
