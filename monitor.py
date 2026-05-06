#!/usr/bin/env python3
"""
PMVC Monitor — daily intelligence collector for Public Market Venture Capital.

Sources (all free):
- SEC EDGAR JSON API (new filings)
- 10 RSS news feeds (TechCrunch, SpaceNews, Defense News, etc.)
- 7 X/Twitter feeds via nitter.net (Justus Parmar + 6 PMVC experts)
- 9 YouTube podcast channels (All-In, Bg2, Acquired, Crux Investor, etc.)
- ollama qwen2.5:7b for summaries

Output sections:
1. Что произошло в новостях
2. Что произошло на бирже (SEC EDGAR)
3. Что говорят лидеры мнений (YouTube + opinion leader X)
4. Что говорят смарт-имена (Thiel, Khosla, etc. в новостях)
5. На что обратить внимание (priority signals)
6. Что происходит в наших секторах
7. Что происходит в ментал хелс нише

Usage:
  ./monitor.py daily        # full run + TG send
  ./monitor.py test         # full run, NO TG send
  ./monitor.py weekly       # synthesize last 7 days
"""

import json
import os
import re
import sys
import time
import subprocess
import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from datetime import datetime, timedelta, date
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "config.yaml"
STATE_PATH = ROOT / "state.json"
LOG_DIR = ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/605.1.15 PMVC-Monitor"


# ---------- State ----------
def load_state():
    if STATE_PATH.exists():
        return json.loads(STATE_PATH.read_text())
    return {"last_run": None, "seen_edgar": [], "seen_news": [], "seen_tweets": [], "seen_youtube": []}


def save_state(state):
    for k in ("seen_edgar", "seen_news", "seen_tweets", "seen_youtube"):
        state[k] = state.get(k, [])[-5000:]
    STATE_PATH.write_text(json.dumps(state, indent=2))


def load_config():
    return yaml.safe_load(CONFIG_PATH.read_text())


# ---------- HTTP ----------
def http_get(url, headers=None, timeout=30):
    h = {"User-Agent": UA, "Accept": "application/xml,application/rss+xml,application/json,text/xml,*/*"}
    if headers:
        h.update(headers)
    req = urllib.request.Request(url, headers=h)
    return urllib.request.urlopen(req, timeout=timeout).read()


# ---------- SEC EDGAR ----------
def fetch_edgar_filings(forms, since_date, until_date):
    """Pull EDGAR filings — one query per form (multi-form syntax has bugs server-side)."""
    results = []
    seen_ids = set()
    for form in forms:
        page_from = 0
        while True:
            params = {
                "q": "", "forms": form, "dateRange": "custom",
                "startdt": since_date, "enddt": until_date, "from": page_from,
            }
            url = "https://efts.sec.gov/LATEST/search-index?" + urllib.parse.urlencode(params) + "&size=100"
            try:
                data = json.loads(http_get(url))
            except Exception as e:
                print(f"  EDGAR err {form}: {e}", file=sys.stderr)
                break
            hits = data.get("hits", {}).get("hits", [])
            if not hits:
                break
            for h in hits:
                fid = h.get("_id", "")
                if fid in seen_ids:
                    continue
                seen_ids.add(fid)
                s = h.get("_source", {})
                results.append({
                    "id": fid,
                    "date": s.get("file_date"),
                    "form": s.get("form"),
                    "ciks": s.get("ciks", []),
                    "names": s.get("display_names", []),
                    "sic": (s.get("sics") or ["?"])[0],
                    "inc_state": (s.get("inc_states") or ["?"])[0],
                    "biz_state": (s.get("biz_states") or ["?"])[0],
                    "biz_loc": (s.get("biz_locations") or ["?"])[0],
                    "accession": s.get("adsh", ""),
                })
            if len(hits) < 100:
                break
            page_from += 100
            time.sleep(0.15)
        time.sleep(0.2)
    return results


def parse_company_ticker(names):
    if not names:
        return None, None
    n = names[0]
    m = re.match(r"^(.+?)\s*(?:\(([A-Z0-9.\-]{1,8})\))?\s*\(CIK", n)
    if m:
        return m.group(1).strip(), m.group(2)
    return n, None


# ---------- RSS / Atom ----------
def fetch_rss(url):
    try:
        body = http_get(url)
    except Exception as e:
        print(f"  RSS err {url[:60]}: {e}", file=sys.stderr)
        return []
    items = []
    try:
        root = ET.fromstring(body)
    except ET.ParseError as e:
        print(f"  parse err {url[:60]}: {e}", file=sys.stderr)
        return []

    # RSS 2.0
    for item in root.iter("item"):
        items.append({
            "title": (item.findtext("title") or "").strip(),
            "link": (item.findtext("link") or "").strip(),
            "summary": (item.findtext("description") or "").strip(),
            "published": (item.findtext("pubDate") or "").strip(),
        })

    # Atom (YouTube uses Atom)
    if not items:
        ns_a = "{http://www.w3.org/2005/Atom}"
        ns_media = "{http://search.yahoo.com/mrss/}"
        for entry in root.iter(f"{ns_a}entry"):
            title_el = entry.find(f"{ns_a}title")
            link_el = entry.find(f"{ns_a}link")
            summary_el = entry.find(f"{ns_media}group/{ns_media}description")
            if summary_el is None:
                summary_el = entry.find(f"{ns_a}summary")
            pub_el = entry.find(f"{ns_a}published")
            if pub_el is None:
                pub_el = entry.find(f"{ns_a}updated")
            def _t(el):
                if el is None: return ""
                return (el.text or "").strip()
            items.append({
                "title": _t(title_el),
                "link": link_el.get("href", "") if link_el is not None else "",
                "summary": _t(summary_el)[:500],
                "published": _t(pub_el),
            })
    return items


def strip_html(s):
    s = re.sub(r"<img[^>]*>", "", s)
    s = re.sub(r"<[^>]+>", "", s)
    s = re.sub(r"&\w+;", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


# ---------- Classification ----------
# Weak keywords — match too broadly; only count if combined with stronger signal
WEAK_KEYWORDS = {
    "ai", "neural", "gpu", "machine learning", "inference", "data center",
    "networking", "robot", "robotic", "satellite", "rocket", "launch",
    "military", "defense", "drone", "uav", "autonomous"
}
# Strong keywords — direct hit on PMVC mandate, no ambiguity
STRONG_KEYWORDS = {
    "critical mineral", "critical minerals", "rare earth", "tungsten", "uranium",
    "lithium mining", "molybdenum", "vanadium", "cobalt mining", "nickel mining",
    "antimony", "gallium", "germanium", "haleu", "smr", "small modular reactor",
    "fusion reactor", "hypersonic", "counter-uas", "drone swarm", "swarm drone",
    "humanoid robot", "quantum computing", "quantum supremacy", "neural implant",
    "brain-computer", "bci", "data center in space", "orbital data center",
    "dpa title iii", "defense production act", "pentagon contract",
    "office of strategic capital", "osc loan", "dod contract",
    "darpa contract", "fda fast track", "darpa sbir",
}
FUNDING_PATTERNS = [
    r"\braised?\s+\$\d+(?:\.\d+)?\s*[mb]illion", r"\braises\s+\$\d+",
    r"\bseries\s+[A-G]\b", r"\bipo\b", r"\bs-1\b", r"\bf-1\b",
    r"\bvaluation\s+of\s+\$\d+", r"\bpost-money\b", r"\bpre-ipo\b",
]
GOV_PATTERNS = ["pentagon", "department of defense", "department of war",
                "white house", "trump administration", "doe loan",
                "dpa", "osc", "darpa", "dod", "dhs"]


_WORD_BOUNDARY_CACHE = {}

def _word_match(keyword, text_lower):
    """Word-boundary keyword matcher.

    Replaces naive `kw in text` substring matching, which caused false positives:
    short acronyms (REE, LEO, GPU) and short names (Trump, Musk) matched
    inside unrelated words (free→ree, leopard→leo, trumpcard→trump).

    Uses \\b word boundaries — matches whole words/multi-word phrases only.
    Cached compiled regex per keyword for speed across thousands of items.
    """
    kw_lower = keyword.lower()
    if kw_lower not in _WORD_BOUNDARY_CACHE:
        _WORD_BOUNDARY_CACHE[kw_lower] = re.compile(
            rf'\b{re.escape(kw_lower)}\b', re.IGNORECASE
        )
    return bool(_WORD_BOUNDARY_CACHE[kw_lower].search(text_lower))


def classify(text, config):
    text_lower = text.lower()
    matches = {
        "sectors": [], "smart_money": [], "ipo_candidates": [],
        "avoided": [], "mental_health": False,
        "strong_kw": [], "weak_kw": [], "funding_signal": False, "gov_signal": False,
        "score": 0, "reasons": [],
    }

    # Sector match (word-boundary, no substring false positives)
    for sector_id, sector_def in config["sectors"].items():
        for kw in sector_def["keywords"]:
            kw_lower = kw.lower()
            if _word_match(kw_lower, text_lower):
                matches["sectors"].append(sector_id)
                if kw_lower in STRONG_KEYWORDS:
                    matches["strong_kw"].append(kw_lower)
                elif kw_lower in WEAK_KEYWORDS:
                    matches["weak_kw"].append(kw_lower)
                else:
                    matches["strong_kw"].append(kw_lower)  # default = strong if listed in sector
                break

    for kw in config.get("avoided_keywords", []):
        if _word_match(kw, text_lower):
            matches["avoided"].append(kw)

    for name in config["smart_money"]["investors"] + config["smart_money"]["operators"]:
        if _word_match(name, text_lower):
            matches["smart_money"].append(name)

    for cand in config["ipo_candidates"]:
        if _word_match(cand, text_lower):
            matches["ipo_candidates"].append(cand)

    for kw in config.get("mental_health", {}).get("keywords", []):
        if _word_match(kw, text_lower):
            matches["mental_health"] = True
            break

    # Funding signal (raised $XXM, Series A, IPO, etc.) — already regex
    for pat in FUNDING_PATTERNS:
        if re.search(pat, text_lower):
            matches["funding_signal"] = True
            break

    # Government / Pentagon signal — also word-boundary
    for kw in GOV_PATTERNS:
        if _word_match(kw, text_lower):
            matches["gov_signal"] = True
            break

    # Compute relevance score + reasons
    score = 0
    if matches["smart_money"]:
        score += 5
        matches["reasons"].append(f"smart-money: {','.join(matches['smart_money'][:2])}")
    if matches["ipo_candidates"]:
        score += 5
        matches["reasons"].append(f"IPO-cand: {','.join(matches['ipo_candidates'][:2])}")
    if matches["gov_signal"]:
        score += 4
        matches["reasons"].append("gov/Pentagon")
    if matches["funding_signal"]:
        score += 3
        matches["reasons"].append("funding")
    if matches["strong_kw"]:
        score += 3
        matches["reasons"].append(f"strong: {matches['strong_kw'][0]}")
    elif matches["weak_kw"]:
        score += 1
        matches["reasons"].append(f"weak: {matches['weak_kw'][0]}")

    if len(set(matches["sectors"])) >= 2:
        score += 2

    matches["score"] = score
    return matches


def is_relevant(matches, edgar_record=None, config=None, min_score=3):
    """Strict filter: must score >= 3 AND not be in avoided sector (unless overridden by smart-money)."""
    if matches["avoided"] and not (matches["smart_money"] or matches["ipo_candidates"]):
        return False

    # EDGAR records by SIC bypass score check
    if edgar_record and config:
        sic = edgar_record.get("sic", "")
        for sector_def in config["sectors"].values():
            if sic in sector_def.get("sic_codes", []):
                return True

    return matches["score"] >= min_score


# ---------- LLM ----------
def llm_summarize(text, llm_config, prompt_prefix="Сделай ОЧЕНЬ краткое (1 предложение, до 25 слов) описание новости на русском. Только факт, без вводных."):
    if not llm_config.get("enabled"):
        return None
    try:
        prompt = f"{prompt_prefix}\n\nТекст:\n{text[:1500]}\n\nКраткое описание:"
        result = subprocess.run(
            ["ollama", "run", llm_config["model"]],
            input=prompt, capture_output=True, text=True,
            timeout=llm_config.get("timeout_sec", 60),
        )
        out = result.stdout.strip()
        # Strip ANSI escape codes
        out = re.sub(r"\x1b\[[0-9;]*[a-zA-Z]", "", out)
        out = re.sub(r"\[\d+[A-Z]", "", out)
        return out
    except Exception:
        return None


# ---------- Telegram ----------
def _smart_chunk(text, max_size=3800):
    """Split on blank lines so chunks stay self-contained for Markdown parsing."""
    chunks = []
    current = ""
    for paragraph in text.split("\n\n"):
        block = paragraph + "\n\n"
        if len(current) + len(block) > max_size and current:
            chunks.append(current.rstrip())
            current = block
        else:
            current += block
    if current.strip():
        chunks.append(current.rstrip())
    return chunks


def _send_chunk(token, chat_id, chunk, parse_mode):
    cmd = ["curl", "-s", "-X", "POST",
           f"https://api.telegram.org/bot{token}/sendMessage",
           "-d", f"chat_id={chat_id}",
           "--data-urlencode", f"text={chunk}"]
    if parse_mode:
        cmd += ["-d", f"parse_mode={parse_mode}"]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    return '"ok":true' in r.stdout, r.stdout


def _parse_migrate_id(resp_text):
    """Detect Telegram supergroup-migration error and return new chat_id, else None.
    Telegram returns: {"ok":false,"error_code":400,"description":"...upgraded to a supergroup...",
                       "parameters":{"migrate_to_chat_id":-1003528188247}}"""
    try:
        import json as _json
        data = _json.loads(resp_text)
        if data.get("ok") is False and data.get("error_code") == 400:
            params = data.get("parameters") or {}
            new_id = params.get("migrate_to_chat_id")
            if new_id is not None:
                return str(new_id)
    except Exception:
        pass
    return None


def _persist_chat_id(env_file, chat_var, new_chat_id):
    """Atomically rewrite .env so chat_var=<new_chat_id>. Adds an audit comment."""
    p = Path(env_file)
    lines = p.read_text().splitlines()
    out = []
    found = False
    today_iso = date.today().isoformat()
    for line in lines:
        stripped = line.strip()
        if stripped.startswith(f"{chat_var}=") or stripped.startswith(f"{chat_var} ="):
            out.append(f"# {today_iso}: auto-migrated by monitor.py (supergroup upgrade)")
            out.append(f"{chat_var}={new_chat_id}")
            found = True
        else:
            out.append(line)
    if not found:
        out.append(f"# {today_iso}: auto-added by monitor.py (supergroup migration)")
        out.append(f"{chat_var}={new_chat_id}")
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text("\n".join(out) + "\n")
    tmp.replace(p)
    print(f"  TG: chat_id auto-migrated → {new_chat_id} (persisted to {env_file})", file=sys.stderr)


def send_telegram(text, env_file, parse_mode="HTML", document=None, caption=None, chat_var="IPO_NEWS_CHAT_ID"):
    """USER POLICY (2026-05-01, hard rule):
       Only the approved HTML-digest format is allowed in IPO-NEWS chat.
       NO ad-hoc fallbacks (no .md attachments, no plain dumps, no test pings)
       without explicit user confirmation in chat. If the approved format fails
       to send (permissions, parse error, etc.) — log the error and EXIT silently.
       Do NOT improvise an alternative payload.

       Cloud-mode: env_file is optional/None. Credentials come from os.environ
       (BOT_TOKEN, IPO_NEWS_CHAT_ID set as GitHub Secrets in workflow)."""
    import os
    env = {}
    if env_file and Path(env_file).exists():
        for line in Path(env_file).read_text().splitlines():
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip().strip('"').strip("'")
    # Env vars override .env (cloud mode uses only env vars)
    for k in ("BOT_TOKEN", "IPO_NEWS_CHAT_ID", "IDEAS_CHAT_ID"):
        v = os.environ.get(k)
        if v:
            env[k] = v
    token = env.get("BOT_TOKEN")
    chat_id = env.get(chat_var) or env.get("IDEAS_CHAT_ID")
    if not (token and chat_id):
        print(f"  TG: missing BOT_TOKEN or chat_id ({chat_var})", file=sys.stderr)
        return False

    if document:
        def _send_doc(cid):
            cmd = ["curl", "-s", "-X", "POST",
                   f"https://api.telegram.org/bot{token}/sendDocument",
                   "-F", f"chat_id={cid}",
                   "-F", f"document=@{document}"]
            if caption:
                cmd += ["-F", f"caption={caption}"]
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            return '"ok":true' in r.stdout, r.stdout
        sent, resp = _send_doc(chat_id)
        if not sent:
            new_id = _parse_migrate_id(resp)
            if new_id:
                _persist_chat_id(env_file, chat_var, new_id)
                sent, _ = _send_doc(new_id)
        return sent

    chunks = _smart_chunk(text, 3800)
    ok = True
    for chunk in chunks:
        sent, resp = _send_chunk(token, chat_id, chunk, parse_mode)
        if not sent:
            # 1) Supergroup migration: persist new id and retry transparently
            new_id = _parse_migrate_id(resp)
            if new_id:
                _persist_chat_id(env_file, chat_var, new_id)
                chat_id = new_id
                sent, resp = _send_chunk(token, chat_id, chunk, parse_mode)
            if not sent:
                # 2) Markup parse error: strip HTML and retry as plain
                print(f"  TG parse error, retrying as plain: {resp[:200]}", file=sys.stderr)
                plain = re.sub(r"<[^>]+>", "", chunk)
                sent, resp = _send_chunk(token, chat_id, plain, None)
                if not sent:
                    # 3) Maybe migration AFTER a plain retry (defense in depth)
                    new_id2 = _parse_migrate_id(resp)
                    if new_id2 and new_id2 != chat_id:
                        _persist_chat_id(env_file, chat_var, new_id2)
                        chat_id = new_id2
                        sent, resp = _send_chunk(token, chat_id, plain, None)
                if not sent:
                    print(f"  TG plain retry FAILED: {resp[:200]}", file=sys.stderr)
                    ok = False
        time.sleep(0.5)
    return ok


# ---------- Daily ----------
def run_daily(test_mode=False):
    import os as _os
    config = load_config()
    state = load_state()

    today = date.today()
    yesterday = today - timedelta(days=1)

    # IDEMPOTENCY: skip TG send if today's digest already delivered.
    # Multiple cron triggers per morning fire to compensate for GitHub Actions
    # scheduled-workflow lag (can be 20min - 6h on free tier). First successful
    # send marks state["last_daily_sent_date"] = today; subsequent triggers see
    # this and exit early before any data fetching, saving compute and avoiding
    # duplicate sends.
    # Override: DRY_RUN=1 still runs full pipeline (testing). FORCE_RESEND=1 bypasses idempotency.
    if (not test_mode
            and _os.environ.get("DRY_RUN") != "1"
            and _os.environ.get("FORCE_RESEND") != "1"
            and state.get("last_daily_sent_date") == today.isoformat()):
        print(f"[daily] IDEMPOTENT SKIP — already sent today ({today.isoformat()})", flush=True)
        return

    since = state.get("last_run_date") or yesterday.isoformat()
    until = today.isoformat()

    print(f"[daily] window: {since} → {until}", flush=True)

    # 1. EDGAR — pull TODAY (yesterday-today) + WEEK (last 7 days) for activity context
    print("[daily] fetching EDGAR (today + week)...", flush=True)
    edgar_today = fetch_edgar_filings(config["edgar_forms"], since, until)
    week_start = (today - timedelta(days=7)).isoformat()
    edgar_week = fetch_edgar_filings(config["edgar_forms"], week_start, until)
    edgar_new = [f for f in edgar_today if f["id"] not in set(state["seen_edgar"])]
    edgar_relevant = []
    for f in edgar_new:
        company, ticker = parse_company_ticker(f["names"])
        f["company"] = company
        f["ticker"] = ticker
        text = f"{company or ''} {f['form']} SIC{f['sic']}"
        f["matches"] = classify(text, config)
        if is_relevant(f["matches"], edgar_record=f, config=config):
            edgar_relevant.append(f)
        state["seen_edgar"].append(f["id"])
    # Process week filings (without dedup — for context)
    for f in edgar_week:
        company, ticker = parse_company_ticker(f["names"])
        f["company"] = company
        f["ticker"] = ticker
        text = f"{company or ''} {f['form']} SIC{f['sic']}"
        f["matches"] = classify(text, config)
    print(f"[daily] EDGAR today: {len(edgar_today)} ({len(edgar_relevant)} relevant) · week total: {len(edgar_week)}", flush=True)

    # 2. RSS news
    print("[daily] fetching RSS news...", flush=True)
    news_relevant = []
    seen_news_set = set(state["seen_news"])
    for src in config["rss_sources"]:
        items = fetch_rss(src["url"])
        for it in items:
            uid = it["link"] or it["title"]
            if uid in seen_news_set:
                continue
            seen_news_set.add(uid)
            state["seen_news"].append(uid)
            text = f"{it['title']} {strip_html(it['summary'])}"
            it["matches"] = classify(text, config)
            if is_relevant(it["matches"], config=config):
                it["source"] = src["name"]
                news_relevant.append(it)
        time.sleep(0.3)
    print(f"[daily] News relevant: {len(news_relevant)}", flush=True)

    # 3. Twitter via Nitter — STRICT FILTER: keep only if matches sector / smart-money / IPO-candidate
    print("[daily] fetching opinion leader X feeds (strict filter)...", flush=True)
    tweets_relevant = []
    seen_tweets_set = set(state["seen_tweets"])
    nitter = config.get("nitter_base", "https://nitter.net")
    for ol in config["opinion_leaders"]["twitter_handles"]:
        url = f"{nitter}/{ol['handle']}/rss"
        items = fetch_rss(url)
        for it in items:
            uid = it["link"] or it["title"]
            if uid in seen_tweets_set:
                continue
            seen_tweets_set.add(uid)
            state["seen_tweets"].append(uid)
            text = f"{it['title']} {strip_html(it['summary'])}"
            it["matches"] = classify(text, config)
            it["author"] = ol["name"]
            it["author_handle"] = ol["handle"]
            # STRICT: keep only if matches our universe (was: keep all)
            if is_relevant(it["matches"], config=config):
                tweets_relevant.append(it)
        time.sleep(0.3)
    print(f"[daily] X tweets (filtered): {len(tweets_relevant)}", flush=True)

    # 3b. Google News per smart-money name — replaces broken LinkedIn tracking
    print("[daily] fetching Google News smart-money mentions...", flush=True)
    smart_money_news = []
    seen_smn_set = set(state.get("seen_smartmoney_news", []))
    if "seen_smartmoney_news" not in state:
        state["seen_smartmoney_news"] = []
    for entry in config.get("google_news_smart_money", []):
        items = fetch_rss(entry["url"])
        for it in items[:10]:  # cap at 10 newest per person
            uid = it["link"] or it["title"]
            if uid in seen_smn_set:
                continue
            seen_smn_set.add(uid)
            state["seen_smartmoney_news"].append(uid)
            text = f"{it['title']} {strip_html(it['summary'])}"
            it["matches"] = classify(text, config)
            it["tracked_person"] = entry["name"]
            # Filter — only keep if also intersects with our sectors / IPO-candidates
            # (otherwise drowns in generic celebrity news)
            if it["matches"]["sectors"] or it["matches"]["ipo_candidates"] or it["matches"].get("smart_money"):
                smart_money_news.append(it)
        time.sleep(0.25)
    print(f"[daily] Smart-money news (filtered): {len(smart_money_news)}", flush=True)

    # 4. YouTube podcasts
    print("[daily] fetching YouTube podcasts...", flush=True)
    youtube_relevant = []
    seen_yt_set = set(state["seen_youtube"])
    for ch in config["opinion_leaders"]["youtube_channels"]:
        url = f"https://www.youtube.com/feeds/videos.xml?channel_id={ch['channel_id']}"
        items = fetch_rss(url)
        for it in items:
            uid = it["link"] or it["title"]
            if uid in seen_yt_set:
                continue
            seen_yt_set.add(uid)
            state["seen_youtube"].append(uid)
            text = f"{it['title']} {strip_html(it['summary'])}"
            it["matches"] = classify(text, config)
            it["channel"] = ch["name"]
            youtube_relevant.append(it)
        time.sleep(0.3)
    print(f"[daily] YouTube videos: {len(youtube_relevant)}", flush=True)

    # 5. LLM summaries (top items) — gracefully skipped if LLM not configured (cloud mode)
    if config.get("llm", {}).get("enabled"):
        for it in news_relevant[:20]:
            text = f"{it['title']}. {strip_html(it['summary'])}"
            s = llm_summarize(text, config["llm"])
            if s:
                it["llm_summary"] = s

    # 5b. Aggregate week-level IPO activity (priced/upcoming/pipeline/withdrawn)
    # Default since_date in aggregate = today-1 (only yesterday/today RWs)
    market_activity = aggregate_market_activity(edgar_week, config)

    # 5c. Detect listing model (classic / direct / uplist) for priced + imminent
    print("[daily] detecting listing models...", flush=True)
    models = detect_listing_models(week_start, until)
    for bucket in ("priced", "imminent", "pipeline", "withdrawn"):
        for f in market_activity[bucket]:
            cik = f["ciks"][0] if f["ciks"] else ""
            f["listing_model"] = models.get(cik, "classic")
    # Compute counts per model for "unusual" detection
    model_counts = Counter(f["listing_model"] for f in market_activity["priced"])
    market_activity["model_counts"] = dict(model_counts)
    market_activity["unusual_flag"] = None
    total_priced = sum(model_counts.values())
    if total_priced >= 5:
        direct_share = model_counts.get("direct", 0) / total_priced
        if direct_share > 0.4:
            market_activity["unusual_flag"] = (
                f"⚠️ Unusual: {model_counts.get('direct',0)} of {total_priced} priced this week "
                f"are DIRECT LISTINGS (no cash to company). High share ({int(direct_share*100)}%) "
                f"= signal weak market or shell-route uptick."
            )
    if len(market_activity["withdrawn"]) >= 3:
        flag = (
            f"⚠️ Unusual: {len(market_activity['withdrawn'])} IPO withdrawn this week "
            f"= sign of poor demand / window closing."
        )
        if market_activity["unusual_flag"]:
            market_activity["unusual_flag"] += "\n" + flag
        else:
            market_activity["unusual_flag"] = flag
    print(f"[daily] models: {model_counts}", flush=True)

    # 6. Build report — Obsidian write only if obsidian.daily_dir is configured (cloud skips)
    obsidian_cfg = config.get("obsidian") or {}
    daily_dir = obsidian_cfg.get("daily_dir")
    if daily_dir:
        report = build_daily_report(today, edgar_relevant, news_relevant, tweets_relevant, youtube_relevant, market_activity, config, smart_money_news=smart_money_news)
        try:
            report_path = Path(daily_dir) / f"{today.isoformat()}.md"
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(report)
            print(f"[daily] report: {report_path}", flush=True)
        except Exception as e:
            print(f"[daily] obsidian write skipped: {e}", flush=True)
    else:
        print("[daily] obsidian write skipped (cloud mode)", flush=True)

    # 7. TG digest — NEW SPLIT FORMAT (2 messages: compact + Telegraph link card)
    # User policy 2026-05-06: split into msg1 (top 7 news + NASDAQ activity) and
    # msg2 (Telegraph link → top 20 news, sector counts, opinion, smart money).
    if not test_mode and config["telegram"]["enabled"]:
        import os as _os
        msg1, msg2 = build_telegram_digest_split(
            today, edgar_relevant, news_relevant, tweets_relevant, youtube_relevant,
            market_activity, config, state=state, smart_money_news=smart_money_news,
        )
        total_len = len(msg1) + len(msg2)
        if _os.environ.get("DRY_RUN") == "1":
            print(f"[daily] DRY_RUN=1 — split digest built (msg1={len(msg1)}, msg2={len(msg2)}). First 300 of msg1:", flush=True)
            print(msg1[:300], flush=True)
            sent_ok = "skipped"
        else:
            env_file_cfg = config["telegram"].get("env_file")
            # TEST_RECIPIENT_CHAT_ID overrides production chat — used to send to user's DM
            # for format approval before going live to IPO-NEWS supergroup.
            test_chat = _os.environ.get("TEST_RECIPIENT_CHAT_ID")
            chat_var_override = None
            if test_chat:
                # Inject into env so send_telegram picks it up via override env vars
                _os.environ["IPO_NEWS_CHAT_ID"] = test_chat
                print(f"[daily] TEST MODE — sending to chat {test_chat} (not production)", flush=True)
            ok1 = send_telegram(msg1, env_file_cfg)
            time.sleep(1)  # let TG rate-limit settle between messages
            ok2 = send_telegram(msg2, env_file_cfg)
            sent_ok = bool(ok1 and ok2)
        print(f"[daily] TG sent split (len={total_len} = {len(msg1)}+{len(msg2)}, ok={sent_ok})", flush=True)
        # Mark idempotency only on REAL successful send of BOTH messages
        if sent_ok is True and not _os.environ.get("TEST_RECIPIENT_CHAT_ID"):
            state["last_daily_sent_date"] = today.isoformat()
            state["last_daily_sent_at"] = datetime.now().isoformat()

    state["last_run_date"] = today.isoformat()
    state["last_run"] = datetime.now().isoformat()
    save_state(state)


# ---------- Listing model detection (classic vs direct vs uplist) ----------
def detect_listing_models(since_date, until_date):
    """
    Use EDGAR full-text search to identify direct listings and uplists.
    Returns dict: { cik (str) -> 'direct' | 'uplist' }.
    Filings priced (424B4) NOT in this dict are assumed classic underwritten IPO.
    """
    direct_ciks = set()
    uplist_ciks = set()

    queries = [
        ('"direct listing"', "424B4", direct_ciks),
        ('"selling stockholders"', "424B4", direct_ciks),
        ('"direct listing"', "S-1", direct_ciks),
        ('uplisting', "424B4", uplist_ciks),
        ('"uplist"', "424B4", uplist_ciks),
    ]
    for q, form, bucket in queries:
        params = {
            "q": q, "forms": form,
            "dateRange": "custom", "startdt": since_date, "enddt": until_date,
        }
        url = "https://efts.sec.gov/LATEST/search-index?" + urllib.parse.urlencode(params) + "&size=100"
        try:
            data = json.loads(http_get(url))
            for h in data.get("hits", {}).get("hits", []):
                for cik in h.get("_source", {}).get("ciks", []):
                    bucket.add(cik)
        except Exception as e:
            print(f"  listing_models err ({q}/{form}): {e}", file=sys.stderr)
        time.sleep(0.25)

    models = {}
    for c in direct_ciks:
        models[c] = "direct"
    for c in uplist_ciks:
        if c not in models:
            models[c] = "uplist"
    return models


def listing_model_emoji(model):
    return {"direct": "🆓", "uplist": "🔄", "classic": "💵"}.get(model, "💵")


# ---------- Telegraph publishing ----------
from html.parser import HTMLParser


class _TelegraphBuilder(HTMLParser):
    """Convert a small subset of HTML (b, i, a, br) to Telegraph DOM nodes."""
    def __init__(self):
        super().__init__()
        self.root = []
        self.stack = [self.root]

    def handle_starttag(self, tag, attrs):
        if tag == "br":
            self.stack[-1].append({"tag": "br"})
            return
        node = {"tag": tag}
        if attrs:
            node["attrs"] = {k: v for k, v in attrs if v is not None}
        node["children"] = []
        self.stack[-1].append(node)
        self.stack.append(node["children"])

    def handle_endtag(self, tag):
        if tag == "br":
            return
        if len(self.stack) > 1:
            self.stack.pop()

    def handle_data(self, data):
        if data:
            self.stack[-1].append(data)


def html_to_telegraph_nodes(html_text):
    """Each non-empty line becomes a <p>. Inline HTML is parsed."""
    paragraphs = []
    for line in html_text.split("\n"):
        if not line.strip():
            continue
        b = _TelegraphBuilder()
        b.feed(line)
        if b.root:
            paragraphs.append({"tag": "p", "children": b.root})
    return paragraphs


def telegraph_create_account():
    params = {"short_name": "PMVC", "author_name": "PMVC Daily"}
    url = "https://api.telegra.ph/createAccount?" + urllib.parse.urlencode(params)
    try:
        resp = json.loads(http_get(url))
        return resp.get("result", {}).get("access_token")
    except Exception as e:
        print(f"  TG-account err: {e}", file=sys.stderr)
        return None


def telegraph_publish(access_token, title, html_text):
    nodes = html_to_telegraph_nodes(html_text)
    data = urllib.parse.urlencode({
        "access_token": access_token,
        "title": title[:256],
        "author_name": "PMVC Daily",
        "content": json.dumps(nodes, ensure_ascii=False),
    }).encode()
    try:
        req = urllib.request.Request("https://api.telegra.ph/createPage", data=data,
                                      headers={"User-Agent": UA})
        resp = json.loads(urllib.request.urlopen(req, timeout=30).read())
        return resp.get("result", {}).get("url")
    except Exception as e:
        print(f"  TG-publish err: {e}", file=sys.stderr)
        return None


# ---------- Market activity aggregation ----------
_company_listed_cache = {}
_submissions_cache = {}  # CIK → full submissions.json data (cached per run)


def _fetch_submissions(cik):
    """Fetch + cache SEC submissions.json for a company. Returns dict or None."""
    cik_padded = str(cik).zfill(10)
    if cik_padded in _submissions_cache:
        return _submissions_cache[cik_padded]
    try:
        url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
        data = json.loads(http_get(url))
        _submissions_cache[cik_padded] = data
        return data
    except Exception as e:
        print(f"  submissions fetch failed CIK {cik_padded}: {e}", file=sys.stderr)
        _submissions_cache[cik_padded] = None
        return None


def is_first_time_us_ipo(cik, current_filing_date):
    """USER POLICY (2026-05-06): 'first-time IPO' check.

    Definition: the company's CURRENT filing (424B4 / 8-A12B / S-1 / F-1) is the
    EARLIEST 424* prospectus filing in their SEC history. Any prior 424-series
    (B3/B4/B5/B7) means they already had an IPO (424B3/B5 are post-IPO shelf
    supplements; 424B4 is final IPO prospectus). If any prior 424* exists →
    this is a follow-on, not first-time IPO.

    Catches:
      - GMTL Guardian Metal — uplist from AIM, no prior 424* → first-time ✓
      - FEAM 5E Advanced — public since 2022 with 12 prior 424B3/B5 supplements
        → drops correctly as follow-on ✓
      - APEX Global / EUPEC / CCIS — pre-IPO, no prior 424* → first-time ✓

    Returns:
        True  — current filing is the earliest 424* (real first-time IPO)
        False — company has prior 424* filing (this is follow-on, drop)
        None  — fetch failed (caller treats as keep-with-caution)
    """
    sub = _fetch_submissions(cik)
    if sub is None:
        return None
    recent = sub.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    dates = recent.get("filingDate", [])
    # ALL 424-series prospectus filings (424B1/B3/B4/B5/B7/B8 etc.) — any prior
    # one means company already has been public.
    all_424_dates = [dates[i] for i, f in enumerate(forms) if f.startswith("424")]
    # Also check older filings via "files" pagination (older >1000 entries)
    for older in sub.get("filings", {}).get("files", []) or []:
        try:
            older_url = f"https://data.sec.gov/submissions/{older.get('name')}"
            older_data = json.loads(http_get(older_url))
            of = older_data.get("form", [])
            od = older_data.get("filingDate", [])
            all_424_dates.extend(od[i] for i, f in enumerate(of) if f.startswith("424"))
        except Exception:
            pass
    if not all_424_dates:
        # Company has no 424* at all — could be DRS-only / pending registration,
        # or new pipeline filing. For 8-A12B / S-1 / F-1 in our buckets this means
        # "first-time" (no IPO yet). Default True.
        return True
    # First-time IFF current filing's date <= earliest 424* in history
    return current_filing_date <= min(all_424_dates)


def is_spac(sic, company_name=""):
    """Detect SPAC (Special Purpose Acquisition Company / blank-check entity).

    Primary signal: SIC code 6770 = Blank Checks (the standard SPAC classification).
    Fallback signal: company name contains 'Acquisition Corp' or 'Capital Corp' or
    'Merger Corp' — handles cases where SIC is misclassified.
    """
    if str(sic) == "6770":
        return True
    name_lower = (company_name or "").lower()
    spac_patterns = ("acquisition corp", "merger corp", "blank check")
    return any(p in name_lower for p in spac_patterns)


def is_company_publicly_listed(cik):
    """Returns True if company is already trading on a US exchange.

    Used to filter the 'Withdrawn IPOs' bucket — RW forms can be filed for
    follow-on offerings (S-3 shelf, S-4 merger, S-8 employee plans, F-3 resale,
    10-12G class registration) by ALREADY-PUBLIC companies. Those are NOT
    'pulled IPOs'. Real pulled IPO = pre-IPO company (no exchange) withdraws
    initial S-1/F-1/DRS registration.

    Per SEC submissions.json: pre-IPO companies have empty/None exchanges array;
    public companies have at least one non-None exchange (e.g. ['Nasdaq']).

    Cached per run to avoid hammering SEC API. Returns None on fetch failure.
    """
    cik_padded = str(cik).zfill(10)
    if cik_padded in _company_listed_cache:
        return _company_listed_cache[cik_padded]
    try:
        url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
        data = json.loads(http_get(url))
        exchanges = data.get("exchanges") or []
        # Has at least one non-None exchange entry → trading publicly
        is_public = any(e for e in exchanges if e)
        _company_listed_cache[cik_padded] = is_public
        return is_public
    except Exception as e:
        print(f"  submissions fetch failed CIK {cik_padded}: {e}", file=sys.stderr)
        _company_listed_cache[cik_padded] = None
        return None


def aggregate_market_activity(edgar_week, config, since_date=None):
    """
    Group filings by CIK to derive IPO lifecycle stage:
    - PRICED:    has 424B4 in window → IPO closed
    - IMMINENT:  has 8-A12B but no 424B4 → about to price (1-3 days)
    - PIPELINE:  has S-1 / F-1 / S-1/A / F-1/A → in registration
    - WITHDRAWN: has RW form FOR an initial offering (S-1/F-1/DRS),
                 AND company is NOT already publicly listed,
                 AND RW filed since `since_date` (default = last 2 days).

    Returns dict: {priced: [...], imminent: [...], pipeline: [...], withdrawn: [...]}
    """
    by_cik = defaultdict(list)
    for f in edgar_week:
        if not f["ciks"]:
            continue
        by_cik[f["ciks"][0]].append(f)

    # Default cutoff: show only RWs filed YESTERDAY or TODAY.
    # Old behavior used 7-day window → same company shown 5+ days in row (user complaint
    # 2026-05-06: BMNR/CDIX/WTO/CCIS/APEX repeated 5 days). Daily digest should show
    # only WHAT'S NEW since previous digest.
    if since_date is None:
        cutoff_date = (date.today() - timedelta(days=1)).isoformat()
    else:
        cutoff_date = since_date

    priced = []
    imminent = []
    pipeline = []
    withdrawn = []

    PRICED_FORMS = {"424B4"}  # 424B4 = final IPO prospectus (priced); 424B3/B5 = follow-on supplements (excluded)
    PRELISTING_FORMS = {"8-A12B"}
    PIPELINE_FORMS = {"S-1", "F-1"}  # S-1/A and F-1/A are amendments — only count first-time S-1/F-1
    INITIAL_REG_FORMS = {"S-1", "F-1", "S-1/A", "F-1/A", "DRS", "DRS/A"}  # forms that signal an INITIAL IPO registration

    for cik, filings in by_cik.items():
        forms_filed = {f["form"] for f in filings}
        # latest filing for company info
        latest = max(filings, key=lambda x: x["date"])

        # WITHDRAWN — strict filter: real pulled IPO only.
        # Companies file initial S-1/F-1 months before RW, so checking forms in 7-day
        # window doesn't help. Instead: query SEC submissions.json — if company is
        # already trading on a US exchange → RW is for follow-on/shelf/employee-plan,
        # NOT a pulled IPO. Pre-IPO companies (no exchange listing) → real pulled IPO.
        # User feedback 2026-05-06: this drops BMNR/CDIX/WTO/RLYB/AMST/MountLogan/etc
        # (already-public companies) and keeps APEX/EUPEC/CCIS (pre-IPO companies).
        if "RW" in forms_filed:
            rw_filing = max((f for f in filings if f["form"] == "RW"), key=lambda x: x["date"])
            # Filter 1: RW must be RECENT — avoid showing same company 5+ days in row
            if rw_filing["date"] < cutoff_date:
                continue
            # Filter 2: company must NOT be already publicly listed
            # (public co with RW = follow-on / shelf withdrawal, not pulled IPO)
            already_listed = is_company_publicly_listed(cik)
            if already_listed is True:  # explicitly True; None (fetch fail) keeps the entry as-is
                continue
            withdrawn.append({
                **latest,
                "withdraw_date": rw_filing["date"],
            })
            continue

        # PRICED — 424B4 in window. USER POLICY 2026-05-06 (Definition C):
        # only count if this is the company's FIRST 424B4 ever (initial IPO, not follow-on).
        if PRICED_FORMS & forms_filed:
            price_filing = max((f for f in filings if f["form"] in PRICED_FORMS), key=lambda x: x["date"])
            first_time = is_first_time_us_ipo(cik, price_filing["date"])
            if first_time is False:  # explicitly False; None (fetch fail) keeps to avoid drops
                continue
            company_name = latest.get("company") or (latest["names"][0] if latest["names"] else "")
            priced.append({
                **latest,
                "form": price_filing["form"],
                "price_date": price_filing["date"],
                "is_spac": is_spac(latest.get("sic", ""), company_name),
            })
            continue

        # IMMINENT — 8-A12B but not yet priced. Definition C: drop if company has prior 424B4.
        if PRELISTING_FORMS & forms_filed:
            pre_filing = next(f for f in filings if f["form"] in PRELISTING_FORMS)
            first_time = is_first_time_us_ipo(cik, pre_filing["date"])
            if first_time is False:
                continue
            company_name = latest.get("company") or (latest["names"][0] if latest["names"] else "")
            imminent.append({
                **latest,
                "form": "8-A12B",
                "pre_date": pre_filing["date"],
                "is_spac": is_spac(latest.get("sic", ""), company_name),
            })
            continue

        # PIPELINE — S-1 / F-1 (initial filings). Definition C: drop if company has prior 424B4.
        if PIPELINE_FORMS & forms_filed:
            init_filing = min((f for f in filings if f["form"] in PIPELINE_FORMS), key=lambda x: x["date"])
            first_time = is_first_time_us_ipo(cik, init_filing["date"])
            if first_time is False:
                continue
            company_name = latest.get("company") or (latest["names"][0] if latest["names"] else "")
            pipeline.append({
                **latest,
                "form": init_filing["form"],
                "first_filed": init_filing["date"],
                "is_spac": is_spac(latest.get("sic", ""), company_name),
            })

    return {
        "priced": sorted(priced, key=lambda x: x.get("price_date", ""), reverse=True),
        "imminent": sorted(imminent, key=lambda x: x.get("pre_date", ""), reverse=True),
        "pipeline": sorted(pipeline, key=lambda x: x.get("first_filed", ""), reverse=True),
        "withdrawn": sorted(withdrawn, key=lambda x: x.get("withdraw_date", ""), reverse=True),
    }


# ---------- Report builders ----------
def build_daily_report(today, edgar, news, tweets, youtube, market_activity, config, smart_money_news=None):
    """Markdown long-form report for Obsidian."""
    lines = [
        "---",
        "tags: [pmvc, daily-report]",
        f"date: {today.isoformat()}",
        "---",
        "",
        f"# PMVC Daily — {today.strftime('%A, %B %d, %Y')}",
        "",
        f"> Auto-generated {datetime.now().strftime('%H:%M')}.",
        "",
        f"**Volume:** {len(news)} news · {len(edgar)} EDGAR filings · {len(tweets)} tweets · {len(youtube)} YouTube videos",
        "",
    ]

    # Section 1: News
    if news:
        lines += ["## 📰 1. Что произошло в новостях", ""]
        for it in news[:25]:
            summary = it.get("llm_summary") or strip_html(it["summary"])[:200]
            sectors = ", ".join(it["matches"]["sectors"]) or "—"
            lines.append(f"- *{it['source']}* · _{sectors}_: [{it['title']}]({it['link']})")
            if summary:
                lines.append(f"  > {summary}")
        lines.append("")

    # Section 2: NASDAQ/NYSE Activity (последние 7 дней — кто прайснулся / выходит / в pipeline / отозвался)
    lines += ["## 🏛 2. Активность по NASDAQ / NYSE (последние 7 дней)", ""]
    priced = market_activity["priced"]
    imminent = market_activity["imminent"]
    pipeline = market_activity["pipeline"]
    withdrawn = market_activity["withdrawn"]

    lines.append(f"**Pulse**: 💵 {len(priced)} priced · ⏰ {len(imminent)} imminent · 📋 {len(pipeline)} pipeline · ❌ {len(withdrawn)} withdrawn")
    lines.append("")

    if priced:
        lines.append(f"### 💵 Priced this week ({len(priced)})")
        lines.append("_Тикер · Компания · Дата · SIC · Юрисдикция · Наш сектор?_")
        for f in priced[:30]:
            t = f.get("ticker") or "—"
            c = f.get("company") or "?"
            d = f.get("price_date", "")
            sectors = ", ".join(f["matches"]["sectors"]) or "—"
            cik_int = int(f["ciks"][0]) if f["ciks"] else 0
            url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik_int}" if cik_int else ""
            sector_marker = "🎯" if f["matches"]["sectors"] else "  "
            lines.append(f"- {sector_marker} **{t}** · {c} · {d} · SIC {f['sic']} · {f['inc_state']} · _{sectors}_  [link]({url})")
        lines.append("")

    if imminent:
        lines.append(f"### ⏰ Imminent — выходят на этой неделе или следующей ({len(imminent)})")
        lines.append("_8-A12B подан = регистрация класса акций за 1-3 дня до прайсинга_")
        for f in imminent[:30]:
            t = f.get("ticker") or "—"
            c = f.get("company") or "?"
            d = f.get("pre_date", "")
            sectors = ", ".join(f["matches"]["sectors"]) or "—"
            cik_int = int(f["ciks"][0]) if f["ciks"] else 0
            url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik_int}" if cik_int else ""
            sector_marker = "🎯" if f["matches"]["sectors"] else "  "
            lines.append(f"- {sector_marker} **{t}** · {c} · 8-A12B {d} · SIC {f['sic']} · {f['inc_state']} · _{sectors}_  [link]({url})")
        lines.append("")

    if pipeline:
        lines.append(f"### 📋 New in pipeline — подались на регистрацию ({len(pipeline)})")
        lines.append("_S-1 / F-1 = первичная подача, ещё не прайснулись_")
        for f in pipeline[:30]:
            t = f.get("ticker") or "—"
            c = f.get("company") or "?"
            d = f.get("first_filed", "")
            sectors = ", ".join(f["matches"]["sectors"]) or "—"
            cik_int = int(f["ciks"][0]) if f["ciks"] else 0
            url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik_int}" if cik_int else ""
            sector_marker = "🎯" if f["matches"]["sectors"] else "  "
            lines.append(f"- {sector_marker} **{t}** · {c} · {f['form']} {d} · SIC {f['sic']} · {f['inc_state']} · _{sectors}_  [link]({url})")
        lines.append("")

    if withdrawn:
        lines.append(f"### ❌ Withdrawn / отозвали ({len(withdrawn)})")
        lines.append("_RW = Registration Withdrawal — компания отказалась от IPO_")
        for f in withdrawn[:30]:
            t = f.get("ticker") or "—"
            c = f.get("company") or "?"
            d = f.get("withdraw_date", "")
            cik_int = int(f["ciks"][0]) if f["ciks"] else 0
            url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik_int}" if cik_int else ""
            lines.append(f"- **{t}** · {c} · withdrawn {d} · SIC {f['sic']} · {f['inc_state']}  [link]({url})")
        lines.append("")

    # Today's relevant filings (for our sector mandate)
    if edgar:
        lines += ["### 🎯 Сегодня в наших секторах", ""]
        for f in edgar[:20]:
            t = f.get("ticker") or "—"
            c = f.get("company") or "?"
            sectors = ", ".join(f["matches"]["sectors"]) or "—"
            cik_int = int(f["ciks"][0]) if f["ciks"] else 0
            url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik_int}" if cik_int else ""
            lines.append(f"- {f['form']} · **{t}** · {c} · SIC {f['sic']} · _{sectors}_  [link]({url})")
        lines.append("")

    # Section 3: Лидеры мнений (YouTube + opinion leader X)
    if youtube or tweets:
        lines += ["## 🎙 3. Что говорят лидеры мнений", ""]
        if youtube:
            lines.append("### YouTube podcasts")
            for v in youtube[:15]:
                sectors = ", ".join(v["matches"]["sectors"]) or "—"
                lines.append(f"- *{v['channel']}*: [{v['title']}]({v['link']}) · _{sectors}_")
            lines.append("")
        if tweets:
            lines.append("### X / Twitter (opinion leaders)")
            by_author = defaultdict(list)
            for t in tweets:
                by_author[t["author"]].append(t)
            for author, ts in by_author.items():
                lines.append(f"#### {author}")
                for t in ts[:5]:
                    text = strip_html(t["summary"])[:200] or t["title"][:200]
                    lines.append(f"- {text} [→]({t['link']})")
                lines.append("")

    # Section 4: Smart money mentions
    sm_news = [it for it in news if it["matches"]["smart_money"]]
    sm_tweets = [t for t in tweets if t["matches"]["smart_money"]]
    if sm_news or sm_tweets:
        lines += ["## 💰 4. Что говорят / делают смарт-имена", ""]
        for it in sm_news[:15]:
            names = ", ".join(it["matches"]["smart_money"])
            summary = it.get("llm_summary") or strip_html(it["summary"])[:200]
            lines.append(f"- **{names}** · *{it['source']}*: [{it['title']}]({it['link']})")
            if summary:
                lines.append(f"  > {summary}")
        for t in sm_tweets[:10]:
            names = ", ".join(t["matches"]["smart_money"])
            text = strip_html(t["summary"])[:200] or t["title"][:200]
            lines.append(f"- **{names}** in @{t['author_handle']}: {text} [→]({t['link']})")
        lines.append("")

    # Section 5: Priority signals
    priority = []
    for it in news:
        if it["matches"]["smart_money"] and (it["matches"]["ipo_candidates"] or it["matches"]["sectors"]):
            priority.append(("news", it))
    for f in edgar:
        if f["form"] in ("S-1", "F-1", "8-A12B") and f["matches"]["sectors"]:
            priority.append(("filing", f))
    for t in tweets:
        if t["matches"]["ipo_candidates"]:
            priority.append(("tweet", t))
    if priority:
        lines += ["## ⚡ 5. На что обратить внимание (priority signals)", ""]
        for kind, item in priority[:15]:
            if kind == "news":
                lines.append(f"- 📰 **{', '.join(item['matches']['smart_money'])}** + sectors: [{item['title']}]({item['link']})")
            elif kind == "filing":
                t = item.get("ticker") or "—"
                lines.append(f"- 🏛 New **{item['form']}** · {t} · {item.get('company','?')} · sectors: {', '.join(item['matches']['sectors'])}")
            elif kind == "tweet":
                text = strip_html(item['summary'])[:200] or item['title'][:200]
                lines.append(f"- 🐦 @{item['author_handle']} mentions {', '.join(item['matches']['ipo_candidates'])}: {text} [→]({item['link']})")
        lines.append("")

    # Section 6: Sector breakdown
    by_sector = defaultdict(list)
    for it in news:
        for s in it["matches"]["sectors"]:
            by_sector[s].append(it)
    if by_sector:
        lines += ["## 🏷 6. Новостей за день по сектору", ""]
        for sid, items in sorted(by_sector.items(), key=lambda x: -len(x[1])):
            label = config["sectors"].get(sid, {}).get("label", sid)
            lines.append(f"### {label} ({len(items)})")
            for it in items[:10]:
                summary = it.get("llm_summary") or strip_html(it["summary"])[:200]
                lines.append(f"- *{it['source']}*: [{it['title']}]({it['link']})")
                if summary:
                    lines.append(f"  > {summary}")
            lines.append("")

    return "\n".join(lines)


def truncate(s, n=100):
    s = s or ""
    return s if len(s) <= n else s[:n-1] + "…"


def _h(s):
    """HTML escape — strips chars that break Telegram HTML parsing."""
    if s is None:
        return ""
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _build_section_news(news):
    L = []
    # Sort by relevance score, descending
    sorted_news = sorted(news, key=lambda x: -x.get("matches", {}).get("score", 0))
    top_news = sorted_news[:7]
    if top_news:
        L.append("📰 <b>1. News (ranked by relevance)</b>")
        for it in top_news:
            score = it.get("matches", {}).get("score", 0)
            reasons = it.get("matches", {}).get("reasons", [])
            why = " · ".join(reasons[:3]) if reasons else "?"
            L.append(f"• <a href=\"{_h(it['link'])}\">{_h(truncate(it['title'], 100))}</a>")
            L.append(f"  <i>{_h(it['source'])}</i> · score <b>{score}</b> · <i>{_h(why)}</i>")
        L.append("")
    return L


def _build_section_market(market_activity, compact=False, include_withdrawn=True):
    """Build NASDAQ/NYSE Activity section.

    USER POLICY 2026-05-06:
    - Daily digest: include_withdrawn=False (withdrawn moved to weekly only)
    - Weekly digest: include_withdrawn=True with full list

    SPAC marker (🪙) added when entry has is_spac=True (SIC 6770 or name match).
    """
    L = []
    priced = market_activity["priced"]
    imminent = market_activity["imminent"]
    pipeline = market_activity["pipeline"]
    withdrawn = market_activity["withdrawn"]
    mc = market_activity.get("model_counts", {})

    L.append("🏛 <b>2. NASDAQ / NYSE Activity (last 7 days)</b>")
    model_breakdown = ""
    if mc:
        parts = []
        if mc.get("classic"): parts.append(f"💵 {mc['classic']} classic")
        if mc.get("direct"):  parts.append(f"🆓 {mc['direct']} direct")
        if mc.get("uplist"):  parts.append(f"🔄 {mc['uplist']} uplist")
        if parts:
            model_breakdown = f" ({' · '.join(parts)})"
    pulse = f"💵 Priced: {len(priced)}{model_breakdown} · ⏰ Imminent: {len(imminent)} · 📋 Pipeline: {len(pipeline)}"
    if include_withdrawn:
        pulse += f" · ❌ Withdrawn: {len(withdrawn)}"
    L.append(pulse)
    if market_activity.get("unusual_flag"):
        L.append(f"<b>{_h(market_activity['unusual_flag'])}</b>")
    L.append("")
    L.append("<i>Legend: 💵 classic · 🆓 direct listing · 🔄 uplist · 🪙 SPAC · 🎯 our sectors</i>")
    L.append("")

    cap = 6 if compact else 10

    def _markers(f):
        """Build leading emoji markers for a filing entry."""
        emoji = listing_model_emoji(f.get("listing_model", "classic"))
        sectors = ", ".join(f["matches"]["sectors"])
        niche = "🎯 " if sectors else ""
        spac = "🪙 " if f.get("is_spac") else ""
        return emoji, niche, spac, sectors

    if priced:
        L.append(f"💵 <b>Priced this week ({len(priced)}):</b>")
        for f in priced[:cap]:
            t = f.get("ticker") or "—"
            c = truncate(f.get("company") or "?", 45)
            emoji, niche, spac, sectors = _markers(f)
            sec_str = f" · <i>{_h(sectors)}</i>" if sectors else ""
            L.append(f"• {emoji} {spac}{niche}<b>{_h(t)}</b> · {_h(c)}{sec_str}")
        L.append("")

    if imminent:
        L.append(f"⏰ <b>Going public this week ({len(imminent)}):</b>")
        L.append("<i>8-A12B = securities-class registration filed 1-3 days before pricing</i>")
        for f in imminent[:cap]:
            t = f.get("ticker") or "—"
            c = truncate(f.get("company") or "?", 45)
            _, niche, spac, sectors = _markers(f)
            sec_str = f" · <i>{_h(sectors)}</i>" if sectors else ""
            L.append(f"• {spac}{niche}<b>{_h(t)}</b> · {_h(c)}{sec_str}")
        L.append("")

    if pipeline:
        L.append(f"📋 <b>New in pipeline ({len(pipeline)}):</b>")
        for f in pipeline[:cap]:
            t = f.get("ticker") or "—"
            c = truncate(f.get("company") or "?", 45)
            emoji, niche, spac, sectors = _markers(f)
            sec_str = f" · <i>{_h(sectors)}</i>" if sectors else ""
            L.append(f"• {emoji} {spac}{niche}{_h(f['form'])} · <b>{_h(t)}</b> · {_h(c)}{sec_str}")
        L.append("")

    if include_withdrawn and withdrawn:
        L.append(f"❌ <b>Withdrawn / pulled IPOs ({len(withdrawn)}):</b>")
        for f in withdrawn[:cap]:
            t = f.get("ticker") or "—"
            c = truncate(f.get("company") or "?", 50)
            spac_mark = "🪙 " if f.get("is_spac") else ""
            L.append(f"• {spac_mark}<b>{_h(t)}</b> · {_h(c)}")
        L.append("")
    return L


def _build_section_opinion(youtube, tweets):
    L = []
    if youtube or tweets:
        L.append("🎙 <b>3. Opinion Leaders</b>")
        for v in youtube[:4]:
            L.append(f"• YT <i>{_h(truncate(v['channel'], 30))}</i>: {_h(truncate(v['title'], 80))}")
        ol_top = [t for t in tweets if t["matches"]["sectors"] or t["matches"]["ipo_candidates"] or t["matches"]["smart_money"]][:6]
        if not ol_top:
            ol_top = tweets[:4]
        for t in ol_top:
            text = strip_html(t["summary"]) or t["title"]
            L.append(f"• @{_h(t['author_handle'])}: {_h(truncate(text, 100))}")
        L.append("")
    return L


def _build_section_smart_money(news, tweets):
    L = []
    sm_news = [it for it in news if it["matches"]["smart_money"]]
    sm_tweets = [t for t in tweets if t["matches"]["smart_money"]]
    if sm_news or sm_tweets:
        L.append("💰 <b>4. Smart Money</b>")
        for it in sm_news[:5]:
            names = ", ".join(it["matches"]["smart_money"][:3])
            L.append(f"• <b>{_h(names)}</b>: {_h(truncate(it['title'], 80))}")
        for t in sm_tweets[:3]:
            names = ", ".join(t["matches"]["smart_money"][:2])
            text = strip_html(t["summary"]) or t["title"]
            L.append(f"• 🐦 <b>{_h(names)}</b> via @{_h(t['author_handle'])}: {_h(truncate(text, 80))}")
        L.append("")
    return L


def _build_section_priority(news, edgar, tweets):
    L = []
    priority = []
    for it in news:
        if it["matches"]["smart_money"] and (it["matches"]["ipo_candidates"] or it["matches"]["sectors"]):
            priority.append(("📰", it["title"]))
    for f in edgar:
        if f["form"] in ("S-1", "F-1", "8-A12B") and f["matches"]["sectors"]:
            priority.append(("🏛", f"{f['form']}: {f.get('ticker') or '—'} {f.get('company','?')}"))
    for t in tweets:
        if t["matches"]["ipo_candidates"]:
            text = strip_html(t["summary"]) or t["title"]
            priority.append(("🐦", f"@{t['author_handle']} → {', '.join(t['matches']['ipo_candidates'])}: {text[:80]}"))
    if priority:
        L.append("⚡ <b>5. What to Watch</b>")
        for icon, text in priority[:8]:
            L.append(f"• {icon} {_h(truncate(text, 130))}")
        L.append("")
    return L


def _build_section_sectors(news, config):
    L = []
    by_sector = defaultdict(int)
    for it in news:
        for s in it["matches"]["sectors"]:
            by_sector[s] += 1
    if by_sector:
        L.append("🏷 <b>6. News count by sector (today)</b>")
        for sid, cnt in sorted(by_sector.items(), key=lambda x: -x[1])[:8]:
            label = config["sectors"].get(sid, {}).get("label", sid)
            L.append(f"• {_h(label)}: {cnt}")
        L.append("")
    return L


def _build_top20_news_html(news):
    """Top-20 news in Telegraph-formatted HTML — links + score + reasons."""
    sorted_news = sorted(news, key=lambda x: -x.get("matches", {}).get("score", 0))
    items = sorted_news[:20]
    L = ["<h3>📰 Top 20 News (ranked by relevance)</h3>"]
    for it in items:
        score = it.get("matches", {}).get("score", 0)
        reasons = it.get("matches", {}).get("reasons", [])
        why = " · ".join(reasons[:3]) if reasons else "—"
        L.append(
            f'<p>• <a href="{_h(it["link"])}">{_h(truncate(it["title"], 140))}</a><br>'
            f'<i>{_h(it["source"])} · score <b>{score}</b> · {_h(why)}</i></p>'
        )
    return "\n".join(L)


def _build_sectors_html(news, config):
    """Sector counts in Telegraph HTML."""
    by_sector = defaultdict(int)
    for it in news:
        for s in it["matches"]["sectors"]:
            by_sector[s] += 1
    if not by_sector:
        return ""
    L = ["<h3>🏷 News count by sector (today)</h3>", "<ul>"]
    for sid, cnt in sorted(by_sector.items(), key=lambda x: -x[1])[:12]:
        label = config["sectors"].get(sid, {}).get("label", sid)
        L.append(f"<li>{_h(label)}: <b>{cnt}</b></li>")
    L.append("</ul>")
    return "\n".join(L)


def _build_opinion_smart_html(youtube, tweets, news, smart_money_news):
    """Opinion Leaders + Smart Money in Telegraph HTML — full lists."""
    L = []

    # YouTube
    if youtube:
        L.append("<h3>🎙 Opinion Leaders — YouTube</h3>")
        for v in youtube[:15]:
            url = v.get("link", "")
            L.append(
                f'<p>• <a href="{_h(url)}"><i>{_h(truncate(v.get("channel",""), 30))}</i>: '
                f'{_h(truncate(v["title"], 120))}</a></p>'
            )

    # Tweets — only relevant ones (passed filter)
    if tweets:
        L.append("<h3>🐦 Opinion Leaders — X / Tweets</h3>")
        for t in tweets[:20]:
            url = t.get("link", "")
            text = strip_html(t.get("summary", "")) or t["title"]
            L.append(
                f'<p>• <a href="{_h(url)}">@{_h(t["author_handle"])}</a>: '
                f'{_h(truncate(text, 200))}</p>'
            )

    # Smart-money news (Google News + RSS news mentioning tracked names)
    sm_news = [it for it in news if it.get("matches", {}).get("smart_money")]
    smn = smart_money_news or []
    if sm_news or smn:
        L.append("<h3>💰 Smart Money — tracked names mentioned</h3>")
        if smn:
            from collections import defaultdict as _dd
            by_person = _dd(list)
            for it in smn:
                by_person[it["tracked_person"]].append(it)
            for person, items in sorted(by_person.items(), key=lambda x: -len(x[1]))[:10]:
                L.append(f"<h4>{_h(person)} ({len(items)} mentions)</h4>")
                for it in items[:5]:
                    url = it.get("link", "")
                    L.append(
                        f'<p>• <a href="{_h(url)}">{_h(truncate(it["title"], 140))}</a></p>'
                    )
        if sm_news:
            L.append("<h4>From RSS / news feeds</h4>")
            for it in sm_news[:15]:
                names = ", ".join(it["matches"]["smart_money"][:3])
                url = it.get("link", "")
                L.append(
                    f'<p>• <b>{_h(names)}</b> — <a href="{_h(url)}">'
                    f'{_h(truncate(it["title"], 130))}</a></p>'
                )
    return "\n".join(L)


def build_telegram_digest_split(today, edgar, news, tweets, youtube, market_activity, config, state=None, smart_money_news=None):
    """Returns (msg1, msg2) tuple — TWO TG messages.

    USER FORMAT POLICY (2026-05-06):
      Msg #1 (compact): header + Top-7 News (ranked) + NASDAQ/NYSE Activity full table
      Msg #2 (link card): Telegraph URL → expanded article with:
        - Top 20 News (full ranked list with scores/reasons)
        - Sector counts (was section 6 in old format)
        - Opinion Leaders (YouTube + X tweets)
        - Smart Money (Google News + RSS mentions)

    Telegraph article is mandatory — provides the long-form for serious readers.
    Falls back to single-message format if Telegraph creation fails.
    """
    # Build Telegraph article (long-form)
    telegraph_html = "\n".join([
        f"<p><i>Generated {today.strftime('%a %b %d, %Y')}.</i></p>",
        _build_top20_news_html(news),
        _build_sectors_html(news, config),
        _build_opinion_smart_html(youtube, tweets, news, smart_money_news),
    ])
    telegraph_url = None
    if state is not None:
        token = state.get("telegraph_token")
        if not token:
            token = telegraph_create_account()
            if token:
                state["telegraph_token"] = token
        if token:
            title = f"PMVC Daily — {today.strftime('%a %b %d, %Y')} — Full digest"
            telegraph_url = telegraph_publish(token, title, telegraph_html)

    # Msg #1: compact
    msg1_lines = [
        f"📡 <b>PMVC Daily — {_h(today.strftime('%a %b %d'))}</b>",
        f"<i>{len(news)} news · {len(tweets)} tweets · {len(youtube)} YT videos</i>",
        "",
    ]
    msg1_lines += _build_section_news(news)            # Top 7 ranked
    # USER POLICY 2026-05-06: daily digest does NOT show Withdrawn section
    # (only weekly shows withdrawn count + list per Q5 decision).
    msg1_lines += _build_section_market(market_activity, compact=False, include_withdrawn=False)
    msg1 = "\n".join(msg1_lines).rstrip()

    # Msg #2: Telegraph link as standalone preview card
    if telegraph_url:
        msg2 = (
            f'📊 <b>Full digest →</b> <a href="{_h(telegraph_url)}">'
            f'Top 20 news · sector counts · opinion leaders · smart money</a>'
        )
    else:
        # Telegraph failed — fall back to inline mini-section in same message
        msg2 = "\n".join(
            _build_section_sectors(news, config)
            + _build_section_opinion(youtube, tweets)
            + _build_section_smart_money(news, tweets)
        ).rstrip() or "(Telegraph unavailable; no extras)"

    return msg1, msg2


def build_telegram_digest(today, edgar, news, tweets, youtube, market_activity, config, state=None, smart_money_news=None):
    """English HTML-formatted TG digest. Aim for one message; offload extras to Telegraph if too long."""
    header = [
        f"📡 <b>PMVC Daily — {_h(today.strftime('%a %b %d'))}</b>",
        f"<i>{len(news)} news · {len(tweets)} tweets · {len(youtube)} YT videos</i>",
        "",
    ]

    sec1 = _build_section_news(news)
    sec2 = _build_section_market(market_activity, compact=False)
    sec3 = _build_section_opinion(youtube, tweets)
    sec4 = _build_section_smart_money(news, tweets)
    sec5 = _build_section_priority(news, edgar, tweets)
    sec6 = _build_section_sectors(news, config)

    full = "\n".join(header + sec1 + sec2 + sec3 + sec4 + sec5 + sec6)

    # If under TG single-message limit, send as-is
    if len(full) < 3800:
        return full

    # Otherwise: keep core (1, 2, 5, 6) in TG, offload 3+4 to Telegraph
    extras_html = "\n".join(
        [f"<b>PMVC Daily — {today.isoformat()} — Opinion Leaders & Smart Money</b>", ""]
        + sec3 + sec4 + sec5
    )
    extras_url = None
    if state is not None:
        token = state.get("telegraph_token")
        if not token:
            token = telegraph_create_account()
            if token:
                state["telegraph_token"] = token
        if token:
            title = f"PMVC Daily — {today.strftime('%a %b %d')} — Opinion & Smart Money"
            extras_url = telegraph_publish(token, title, extras_html)

    # Compact section 2 to fit
    sec2_compact = _build_section_market(market_activity, compact=True)
    pieces = header + sec1 + sec2_compact + sec5 + sec6
    if extras_url:
        pieces.append(f"🎙💰 <b>Opinion Leaders & Smart Money:</b> <a href=\"{_h(extras_url)}\">full list →</a>")
        pieces.append("")
    else:
        # Telegraph failed — append minimal extras
        pieces += sec3[:1] + sec4[:1]

    return "\n".join(pieces)

    # === 1. NEWS ===
    top_news = news[:5]
    if top_news:
        L.append("📰 <b>1. News</b>")
        for it in top_news:
            L.append(f"• <a href=\"{_h(it['link'])}\">{_h(truncate(it['title'], 90))}</a>")
            L.append(f"  <i>{_h(it['source'])}</i>")
        L.append("")

    # === 2. NASDAQ / NYSE Activity (week) ===
    priced = market_activity["priced"]
    imminent = market_activity["imminent"]
    pipeline = market_activity["pipeline"]
    withdrawn = market_activity["withdrawn"]

    L.append("🏛 <b>2. NASDAQ / NYSE Activity (last 7 days)</b>")
    L.append(f"💵 Priced: {len(priced)} · ⏰ Imminent: {len(imminent)} · 📋 Pipeline: {len(pipeline)} · ❌ Withdrawn: {len(withdrawn)}")
    L.append("")

    if priced:
        L.append(f"💵 <b>Priced this week ({len(priced)}):</b>")
        for f in priced[:10]:
            t = f.get("ticker") or "—"
            c = truncate(f.get("company") or "?", 45)
            sectors = ", ".join(f["matches"]["sectors"])
            mark = "🎯 " if sectors else ""
            sec_str = f" · <i>{_h(sectors)}</i>" if sectors else ""
            L.append(f"• {mark}<b>{_h(t)}</b> · {_h(c)}{sec_str}")
        L.append("")

    if imminent:
        L.append(f"⏰ <b>Going public this week ({len(imminent)}):</b>")
        L.append("<i>8-A12B = securities-class registration filed 1-3 days before pricing</i>")
        for f in imminent[:10]:
            t = f.get("ticker") or "—"
            c = truncate(f.get("company") or "?", 45)
            sectors = ", ".join(f["matches"]["sectors"])
            mark = "🎯 " if sectors else ""
            sec_str = f" · <i>{_h(sectors)}</i>" if sectors else ""
            L.append(f"• {mark}<b>{_h(t)}</b> · {_h(c)}{sec_str}")
        L.append("")

    if pipeline:
        L.append(f"📋 <b>New in pipeline ({len(pipeline)}):</b>")
        for f in pipeline[:10]:
            t = f.get("ticker") or "—"
            c = truncate(f.get("company") or "?", 45)
            sectors = ", ".join(f["matches"]["sectors"])
            mark = "🎯 " if sectors else ""
            sec_str = f" · <i>{_h(sectors)}</i>" if sectors else ""
            L.append(f"• {mark}{_h(f['form'])} · <b>{_h(t)}</b> · {_h(c)}{sec_str}")
        L.append("")

    if withdrawn:
        L.append(f"❌ <b>Withdrawn / pulled IPOs ({len(withdrawn)}):</b>")
        for f in withdrawn[:10]:
            t = f.get("ticker") or "—"
            c = truncate(f.get("company") or "?", 50)
            L.append(f"• <b>{_h(t)}</b> · {_h(c)}")
        L.append("")

    # === 3. Opinion Leaders ===
    if youtube or tweets:
        L.append("🎙 <b>3. Opinion Leaders</b>")
        for v in youtube[:4]:
            L.append(f"• YT <i>{_h(truncate(v['channel'], 30))}</i>: {_h(truncate(v['title'], 80))}")
        ol_top = [t for t in tweets if t["matches"]["sectors"] or t["matches"]["ipo_candidates"] or t["matches"]["smart_money"]][:6]
        if not ol_top:
            ol_top = tweets[:4]
        for t in ol_top:
            text = strip_html(t["summary"]) or t["title"]
            L.append(f"• @{_h(t['author_handle'])}: {_h(truncate(text, 100))}")
        L.append("")

    # === 4. Smart Money — concentrated activity by tracked names ===
    sm_news = [it for it in news if it["matches"]["smart_money"]]
    sm_tweets = [t for t in tweets if t["matches"]["smart_money"]]
    smn = smart_money_news or []
    if sm_news or sm_tweets or smn:
        L.append("💰 <b>4. Smart Money — что говорят и делают tracked имена</b>")
        # Group smart-money news by tracked person (show clusters)
        if smn:
            from collections import defaultdict as _dd
            by_person = _dd(list)
            for it in smn:
                by_person[it["tracked_person"]].append(it)
            # Show top 5 most-mentioned people today
            for person, items in sorted(by_person.items(), key=lambda x: -len(x[1]))[:6]:
                top_item = items[0]
                ctx = ", ".join(top_item["matches"]["sectors"] or top_item["matches"]["ipo_candidates"] or [])
                ctx_str = f" · <i>{_h(ctx)}</i>" if ctx else ""
                L.append(f"• <b>{_h(person)}</b> ({len(items)} mentions){ctx_str}: {_h(truncate(top_item['title'], 90))}")
        # Plus sector news that already mention smart money
        for it in sm_news[:4]:
            names = ", ".join(it["matches"]["smart_money"][:3])
            L.append(f"• <b>{_h(names)}</b> in news: {_h(truncate(it['title'], 80))}")
        for t in sm_tweets[:3]:
            names = ", ".join(t["matches"]["smart_money"][:2])
            text = strip_html(t["summary"]) or t["title"]
            L.append(f"• 🐦 <b>{_h(names)}</b> via @{_h(t['author_handle'])}: {_h(truncate(text, 80))}")
        L.append("")

    # === 5. What to Watch ===
    priority = []
    for it in news:
        if it["matches"]["smart_money"] and (it["matches"]["ipo_candidates"] or it["matches"]["sectors"]):
            priority.append(("📰", it["title"]))
    for f in edgar:
        if f["form"] in ("S-1", "F-1", "8-A12B") and f["matches"]["sectors"]:
            priority.append(("🏛", f"{f['form']}: {f.get('ticker') or '—'} {f.get('company','?')}"))
    for t in tweets:
        if t["matches"]["ipo_candidates"]:
            text = strip_html(t["summary"]) or t["title"]
            priority.append(("🐦", f"@{t['author_handle']} → {', '.join(t['matches']['ipo_candidates'])}: {text[:80]}"))
    if priority:
        L.append("⚡ <b>5. What to Watch</b>")
        for icon, text in priority[:8]:
            L.append(f"• {icon} {_h(truncate(text, 130))}")
        L.append("")

    # === 6. Sector breakdown — counts ===
    by_sector = defaultdict(int)
    for it in news:
        for s in it["matches"]["sectors"]:
            by_sector[s] += 1
    if by_sector:
        L.append("🏷 <b>6. News count by sector (today)</b>")
        for sid, cnt in sorted(by_sector.items(), key=lambda x: -x[1])[:8]:
            label = config["sectors"].get(sid, {}).get("label", sid)
            L.append(f"• {_h(label)}: {cnt}")
        L.append("")

    return "\n".join(L)


# ---------- Weekly synthesis ----------
def _filter_niche(items):
    """Keep only items that match at least one PMVC sector."""
    return [it for it in items if it.get("matches", {}).get("sectors")]


def build_weekly_digest_split(today, edgar_relevant, news_relevant, tweets_relevant,
                                youtube_relevant, market_activity, config,
                                state=None, smart_money_news=None,
                                prev_snapshot=None):
    """Returns (msg1, msg2) — Sunday weekly digest. Niche-only IPO activity.
    Withdrawn shown WITH count + full list (per Q5).
    Format = Option A split (compact TG + Telegraph link)."""
    week_start = today - timedelta(days=7)
    week_label = f"{week_start.strftime('%b %d')}–{today.strftime('%b %d, %Y')}"

    # === Niche-only filtering for IPO buckets ===
    niche_priced = _filter_niche(market_activity["priced"])
    niche_imminent = _filter_niche(market_activity["imminent"])
    niche_pipeline = _filter_niche(market_activity["pipeline"])
    # Withdrawn: ALL companies (per user Q5 — full list, not just niche)
    all_withdrawn = market_activity["withdrawn"]
    # But also count niche subset for context
    niche_withdrawn = _filter_niche(all_withdrawn)

    niche_market = {
        "priced": niche_priced,
        "imminent": niche_imminent,
        "pipeline": niche_pipeline,
        "withdrawn": all_withdrawn,
        "model_counts": market_activity.get("model_counts", {}),
        "unusual_flag": market_activity.get("unusual_flag"),
    }

    # === Cross-week delta if previous snapshot available ===
    delta_str = ""
    if prev_snapshot:
        d_priced = len(niche_priced) - prev_snapshot.get("niche_priced", 0)
        d_pipeline = len(niche_pipeline) - prev_snapshot.get("niche_pipeline", 0)
        d_withdrawn = len(all_withdrawn) - prev_snapshot.get("withdrawn", 0)
        deltas = []
        if d_priced: deltas.append(f"priced {d_priced:+d}")
        if d_pipeline: deltas.append(f"pipeline {d_pipeline:+d}")
        if d_withdrawn: deltas.append(f"withdrawn {d_withdrawn:+d}")
        if deltas:
            delta_str = f" · vs prev week: {' · '.join(deltas)}"

    # === MSG #1: compact ===
    niche_news = _filter_niche(news_relevant)
    niche_news_sorted = sorted(niche_news, key=lambda x: -x.get("matches", {}).get("score", 0))

    msg1_lines = [
        f"📊 <b>PMVC Weekly — {_h(week_label)}</b>",
        f"<i>{len(niche_priced)} new niche IPOs · {len(niche_pipeline)} in pipeline · "
        f"{len(all_withdrawn)} withdrew ({len(niche_withdrawn)} in niche){_h(delta_str)}</i>",
        "",
    ]

    # 1. Top niche news (5)
    if niche_news_sorted:
        msg1_lines.append("📰 <b>1. Top niche news of the week</b>")
        for it in niche_news_sorted[:5]:
            score = it.get("matches", {}).get("score", 0)
            reasons = it.get("matches", {}).get("reasons", [])
            why = " · ".join(reasons[:3]) if reasons else "—"
            msg1_lines.append(f"• <a href=\"{_h(it['link'])}\">{_h(truncate(it['title'], 100))}</a>")
            msg1_lines.append(f"  <i>{_h(it['source'])}</i> · score <b>{score}</b> · <i>{_h(why)}</i>")
        msg1_lines.append("")

    # 2. Niche IPO activity (re-uses _build_section_market with include_withdrawn=True for full list)
    msg1_lines += _build_section_market(niche_market, compact=True, include_withdrawn=True)

    msg1 = "\n".join(msg1_lines).rstrip()

    # === Telegraph article (long-form) ===
    telegraph_html = "\n".join([
        f"<p><i>Week of {week_label}.</i></p>",
        _build_top20_news_html(niche_news_sorted),
        _build_sectors_html(news_relevant, config),
        _build_opinion_smart_html(youtube_relevant, tweets_relevant, news_relevant, smart_money_news),
    ])
    telegraph_url = None
    if state is not None:
        token = state.get("telegraph_token")
        if not token:
            token = telegraph_create_account()
            if token:
                state["telegraph_token"] = token
        if token:
            title = f"PMVC Weekly — {week_label} — Full digest"
            telegraph_url = telegraph_publish(token, title, telegraph_html)

    # === MSG #2: Telegraph link card ===
    if telegraph_url:
        msg2 = (
            f'📊 <b>Full weekly →</b> <a href="{_h(telegraph_url)}">'
            f'Top niche news · pipeline · sector breakdown · opinion leaders · smart money</a>'
        )
    else:
        msg2 = "(Telegraph unavailable — long-form not generated this week)"

    return msg1, msg2


def run_weekly(test_mode=False):
    """Cloud-native weekly digest (Sunday 19:00 Kyiv).
    Re-fetches EDGAR + RSS for past 7 days, filters to PMVC niches, sends 2-msg split.
    Idempotent via state.last_weekly_sent_date — safe under saturated cron.
    """
    import os as _os
    config = load_config()
    state = load_state()
    today = date.today()
    week_start = today - timedelta(days=7)

    # Idempotency — saturated cron fires multiple times per Sunday
    if (not test_mode
            and _os.environ.get("DRY_RUN") != "1"
            and _os.environ.get("FORCE_RESEND") != "1"
            and state.get("last_weekly_sent_date") == today.isoformat()):
        print(f"[weekly] IDEMPOTENT SKIP — already sent today ({today.isoformat()})", flush=True)
        return

    print(f"[weekly] window: {week_start.isoformat()} → {today.isoformat()}", flush=True)

    # 1. EDGAR week
    print("[weekly] fetching EDGAR (last 7 days)...", flush=True)
    edgar_week = fetch_edgar_filings(config["edgar_forms"], week_start.isoformat(), today.isoformat())
    edgar_relevant = []
    for f in edgar_week:
        company, ticker = parse_company_ticker(f["names"])
        f["company"] = company; f["ticker"] = ticker
        text = f"{company or ''} {f['form']} SIC{f['sic']}"
        f["matches"] = classify(text, config)
        if is_relevant(f["matches"], edgar_record=f, config=config):
            edgar_relevant.append(f)
    print(f"[weekly] EDGAR: {len(edgar_week)} total, {len(edgar_relevant)} relevant", flush=True)

    # 2. RSS news (latest items per feed; classify and keep niche+relevant)
    print("[weekly] fetching RSS news...", flush=True)
    news_relevant = []
    seen_link = set()
    for src in config["rss_sources"]:
        items = fetch_rss(src["url"])
        for it in items:
            uid = it["link"] or it["title"]
            if uid in seen_link:
                continue
            seen_link.add(uid)
            text = f"{it['title']} {strip_html(it['summary'])}"
            it["matches"] = classify(text, config)
            if is_relevant(it["matches"], config=config):
                it["source"] = src["name"]
                news_relevant.append(it)
        time.sleep(0.3)
    print(f"[weekly] news relevant: {len(news_relevant)}", flush=True)

    # 3. X tweets
    print("[weekly] fetching X feeds...", flush=True)
    tweets_relevant = []
    seen_tw = set()
    nitter = config.get("nitter_base", "https://nitter.net")
    for ol in config["opinion_leaders"]["twitter_handles"]:
        items = fetch_rss(f"{nitter}/{ol['handle']}/rss")
        for it in items:
            uid = it["link"] or it["title"]
            if uid in seen_tw:
                continue
            seen_tw.add(uid)
            text = f"{it['title']} {strip_html(it['summary'])}"
            it["matches"] = classify(text, config)
            it["author"] = ol["name"]; it["author_handle"] = ol["handle"]
            if is_relevant(it["matches"], config=config):
                tweets_relevant.append(it)
        time.sleep(0.3)
    print(f"[weekly] tweets: {len(tweets_relevant)}", flush=True)

    # 3b. Google News smart-money mentions
    smart_money_news = []
    seen_smn = set()
    for entry in config.get("google_news_smart_money", []):
        items = fetch_rss(entry["url"])
        for it in items[:10]:
            uid = it["link"] or it["title"]
            if uid in seen_smn:
                continue
            seen_smn.add(uid)
            text = f"{it['title']} {strip_html(it['summary'])}"
            it["matches"] = classify(text, config)
            it["tracked_person"] = entry["name"]
            if it["matches"]["sectors"] or it["matches"]["ipo_candidates"] or it["matches"].get("smart_money"):
                smart_money_news.append(it)
        time.sleep(0.25)
    print(f"[weekly] smart-money news: {len(smart_money_news)}", flush=True)

    # 4. YouTube
    youtube_relevant = []
    seen_yt = set()
    for ch in config["opinion_leaders"]["youtube_channels"]:
        items = fetch_rss(f"https://www.youtube.com/feeds/videos.xml?channel_id={ch['channel_id']}")
        for it in items:
            uid = it["link"] or it["title"]
            if uid in seen_yt:
                continue
            seen_yt.add(uid)
            text = f"{it['title']} {strip_html(it['summary'])}"
            it["matches"] = classify(text, config)
            it["channel"] = ch["name"]
            youtube_relevant.append(it)
        time.sleep(0.3)
    print(f"[weekly] youtube: {len(youtube_relevant)}", flush=True)

    # 5. Aggregate market activity for full week (no 1-day cutoff for weekly)
    print("[weekly] aggregating market activity...", flush=True)
    market_activity = aggregate_market_activity(edgar_week, config, since_date=week_start.isoformat())

    # 5b. Listing models for priced/imminent/pipeline
    models = detect_listing_models(week_start.isoformat(), today.isoformat())
    for bucket in ("priced", "imminent", "pipeline", "withdrawn"):
        for f in market_activity[bucket]:
            cik = f["ciks"][0] if f["ciks"] else ""
            f["listing_model"] = models.get(cik, "classic")
    model_counts = Counter(f["listing_model"] for f in market_activity["priced"])
    market_activity["model_counts"] = dict(model_counts)
    market_activity["unusual_flag"] = None
    total_priced = sum(model_counts.values())
    if total_priced >= 5 and model_counts.get("direct", 0) / total_priced > 0.4:
        market_activity["unusual_flag"] = (
            f"⚠️ Unusual: {model_counts.get('direct',0)} of {total_priced} priced this week "
            f"are DIRECT LISTINGS (no cash to company)."
        )

    # 6. Build digest with prev-week snapshot for delta
    prev_snap = state.get("last_weekly_snapshot")
    msg1, msg2 = build_weekly_digest_split(
        today, edgar_relevant, news_relevant, tweets_relevant, youtube_relevant,
        market_activity, config, state=state, smart_money_news=smart_money_news,
        prev_snapshot=prev_snap,
    )

    # 7. Send
    if config["telegram"]["enabled"]:
        if _os.environ.get("DRY_RUN") == "1":
            print(f"[weekly] DRY_RUN=1 — split built (msg1={len(msg1)}, msg2={len(msg2)}). First 300 of msg1:", flush=True)
            print(msg1[:300], flush=True)
            sent_ok = "skipped"
        else:
            env_file_cfg = config["telegram"].get("env_file")
            test_chat = _os.environ.get("TEST_RECIPIENT_CHAT_ID")
            if test_chat:
                _os.environ["IPO_NEWS_CHAT_ID"] = test_chat
                print(f"[weekly] TEST MODE — sending to chat {test_chat} (not production)", flush=True)
            ok1 = send_telegram(msg1, env_file_cfg)
            time.sleep(1)
            ok2 = send_telegram(msg2, env_file_cfg)
            sent_ok = bool(ok1 and ok2)
        print(f"[weekly] TG sent split (len={len(msg1)+len(msg2)}, ok={sent_ok})", flush=True)
        if sent_ok is True and not _os.environ.get("TEST_RECIPIENT_CHAT_ID"):
            state["last_weekly_sent_date"] = today.isoformat()
            state["last_weekly_sent_at"] = datetime.now().isoformat()
            # Snapshot for next week's delta comparison
            state["last_weekly_snapshot"] = {
                "niche_priced": len(_filter_niche(market_activity["priced"])),
                "niche_imminent": len(_filter_niche(market_activity["imminent"])),
                "niche_pipeline": len(_filter_niche(market_activity["pipeline"])),
                "withdrawn": len(market_activity["withdrawn"]),
            }

    save_state(state)
    print("[weekly] done", flush=True)


# ---------- Main ----------
if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "daily"
    print(f"[start] cmd={cmd}", flush=True)
    try:
        if cmd == "daily":
            run_daily(test_mode=False)
        elif cmd == "test":
            run_daily(test_mode=True)
        elif cmd == "weekly":
            run_weekly()
        else:
            print(f"unknown cmd: {cmd}", file=sys.stderr); sys.exit(2)
        print("[done]", flush=True)
    except Exception as e:
        import traceback
        print(f"[error] {e}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)
