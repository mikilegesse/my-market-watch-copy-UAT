#!/usr/bin/env python3
"""
🇪🇹 ETB Financial Terminal v35.0 (HYBRID ENGINE)
- BASE: v34.1 Smart Parser & Lifecycle Tracking.
- ADDED: v29.1 "Inventory Drop" logic for Partial Fills.
- RESULT: Detects New Ads, Removed Ads (Bought All), AND Volume Drops (Partial Buys).
"""

import requests
import statistics
import sys
import time
import csv
import os
import datetime
import json
import random
from concurrent.futures import ThreadPoolExecutor

# Try importing matplotlib
try:
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    import matplotlib.ticker as ticker
    GRAPH_ENABLED = True
except ImportError:
    GRAPH_ENABLED = False
    print("⚠️ Matplotlib not found.", file=sys.stderr)

# --- CONFIGURATION ---
P2P_ARMY_KEY = "YJU5RCZ2-P6VTVNNA"
HISTORY_FILE = "etb_history.csv"
TRADES_FILE = "recent_trades.json"
GRAPH_FILENAME = "etb_neon_terminal.png"
GRAPH_LIGHT_FILENAME = "etb_light_terminal.png"
HTML_FILENAME = "index.html"

# Settings
BURST_WAIT_TIME = 60
TRADE_RETENTION_MINUTES = 60
MAX_ADS_PER_SOURCE = 200
HISTORY_POINTS = 288

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Content-Type": "application/json",
    "Accept": "application/json"
}

# --- 1. SMART PARSING LOGIC (Keep v34.1) ---
def safe_get(data, keys, default=None):
    if isinstance(data, dict):
        for k in keys:
            if k in data and data[k] is not None:
                return data[k]
        for val in data.values():
            if isinstance(val, dict):
                res = safe_get(val, keys)
                if res is not None: return res
    return default

def parse_ad(raw_ad, source):
    id_keys = ['advNo', 'adv_no', 'orderNumber', 'id', 'itemNo', 'code']
    price_keys = ['price', 'unit_price', 'rate']
    vol_keys = ['surplusAmount', 'tradableQuantity', 'available_amount', 'amount', 'dynamicMaxSingleTransAmount', 'maxAmount', 'quantity', 'stock', 'available', 'lastQuantity']
    user_keys = ['nickName', 'nickname', 'advertiser_name', 'name', 'userName', 'advertiserName']

    try:
        price = float(safe_get(raw_ad, price_keys, 0))
        vol = float(safe_get(raw_ad, vol_keys, 0))
        user = safe_get(raw_ad, user_keys, "Trader")
        if user == "Trader":
             if 'advertiser' in raw_ad: user = safe_get(raw_ad['advertiser'], user_keys, "Trader")
             elif 'merchant' in raw_ad: user = safe_get(raw_ad['merchant'], user_keys, "Trader")
        
        ad_id = str(safe_get(raw_ad, id_keys, f"unknown_{random.randint(10000,99999)}"))

        return {
            "id": ad_id,
            "source": source,
            "advertiser": user,
            "price": price,
            "available": vol
        }
    except:
        return None

# --- 2. FETCHERS ---
def fetch_binance_direct(trade_type):
    url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
    ads = []
    page = 1
    payload = {"asset": "USDT", "fiat": "ETB", "merchantCheck": False, "page": 1, "rows": 20, "tradeType": trade_type, "payTypes": [], "countries": [], "publisherType": None}
    while len(ads) < MAX_ADS_PER_SOURCE:
        try:
            payload["page"] = page
            r = requests.post(url, headers=HEADERS, json=payload, timeout=5)
            data = r.json().get("data", [])
            if not data: break
            for d in data:
                parsed = parse_ad(d, "Binance")
                if parsed and parsed['price'] > 0: ads.append(parsed)
            page += 1
            time.sleep(0.2)
        except: break
    return ads[:MAX_ADS_PER_SOURCE]

def fetch_bybit(side):
    url = "https://api2.bybit.com/fiat/otc/item/online"
    ads = []
    page = 1
    h = HEADERS.copy(); h["Referer"] = "https://www.bybit.com/"
    while len(ads) < MAX_ADS_PER_SOURCE:
        try:
            r = requests.post(url, headers=h, json={"userId": "", "tokenId": "USDT", "currencyId": "ETB", "payment": [], "side": side, "size": "50", "page": str(page), "authMaker": False}, timeout=5)
            items = r.json().get("result", {}).get("items", [])
            if not items: break
            for i in items:
                parsed = parse_ad(i, "Bybit")
                if parsed and parsed['price'] > 0: ads.append(parsed)
            page += 1
            time.sleep(0.1)
        except: break
    return ads[:MAX_ADS_PER_SOURCE]

def fetch_p2p_army_ads(market, side):
    url = "https://p2p.army/v1/api/get_p2p_order_book"
    h = HEADERS.copy(); h["X-APIKEY"] = P2P_ARMY_KEY
    try:
        r = requests.post(url, headers=h, json={"market": market, "fiat": "ETB", "asset": "USDT", "side": side, "limit": MAX_ADS_PER_SOURCE}, timeout=10)
        data = r.json()
        raw = data.get("result", data.get("data", data.get("ads", [])))
        if not raw and isinstance(data, list): raw = data
        source_name = "Binance" if market.lower() == "binance" else "MEXC"
        clean = []
        for ad in raw[:MAX_ADS_PER_SOURCE]:
            parsed = parse_ad(ad, source_name)
            if parsed and parsed['price'] > 0: clean.append(parsed)
        return clean[:MAX_ADS_PER_SOURCE]
    except: return []

# --- 3. HYBRID LIFECYCLE ENGINE (THE MERGE) ---
def fetch_usdt_peg():
    try: return float(requests.get("https://api.coingecko.com/api/v3/simple/price?ids=tether&vs_currencies=usd", timeout=5).json()["tether"]["usd"])
    except: return 1.00

def fetch_official_rate():
    try: return float(requests.get("https://open.er-api.com/v6/latest/USD", timeout=5).json()["rates"]["ETB"])
    except: return None

def capture_market_snapshot():
    with ThreadPoolExecutor(max_workers=10) as ex:
        f_bin = ex.submit(lambda: fetch_p2p_army_ads("binance", "SELL"))
        f_mexc = ex.submit(lambda: fetch_p2p_army_ads("mexc", "SELL"))
        f_byb = ex.submit(lambda: fetch_bybit("1"))
        bin_data = f_bin.result() or []
        mexc_data = f_mexc.result() or []
        bybit_data = f_byb.result() or []
        if not bin_data: bin_data = fetch_binance_direct("SELL")
        total = len(bin_data) + len(mexc_data) + len(bybit_data)
        print(f"   📊 Collected {total} ads (Binance: {len(bin_data)}, MEXC: {len(mexc_data)}, Bybit: {len(bybit_data)})", file=sys.stderr)
        return bin_data + bybit_data + mexc_data

def remove_outliers(ads, peg):
    if len(ads) < 10: return ads
    prices = sorted([ad["price"] / peg for ad in ads])
    p10_threshold = prices[int(len(prices) * 0.10)]
    return [ad for ad in ads if (ad["price"] / peg) > p10_threshold]

def detect_hybrid_events(snapshot_before, snapshot_after, peg):
    """
    Combines:
    1. v34.1 (New/Gone IDs)
    2. v29.1 (Inventory Drops on existing IDs)
    """
    if not snapshot_before:
        print("   > First run - establishing baseline", file=sys.stderr)
        return []
    
    # Map IDs to Ad Objects
    prev_ads = {ad['id']: ad for ad in snapshot_before if ad.get('source', '').lower() in ['binance', 'mexc']}
    curr_ads = {ad['id']: ad for ad in snapshot_after if ad.get('source', '').lower() in ['binance', 'mexc']}
    
    prev_ids = set(prev_ads.keys())
    curr_ids = set(curr_ads.keys())
    
    # 1. Lifecycle: New & Gone
    new_ids = curr_ids - prev_ids 
    gone_ids = prev_ids - curr_ids
    
    # 2. Inventory: Common IDs
    common_ids = prev_ids.intersection(curr_ids)
    
    events = []
    
    # TYPE A: New Requests (Appeared)
    for ad_id in new_ids:
        ad = curr_ads[ad_id]
        if ad['available'] > 5:
            events.append({
                "type": "new_request",
                "source": ad['source'], "user": ad['advertiser'],
                "price": ad['price'] / peg, "vol_usd": ad['available'],
                "timestamp": time.time()
            })
            print(f"   🆕 NEW: {ad['source']} - {ad['advertiser']} req {ad['available']:,.0f} USDT", file=sys.stderr)

    # TYPE B: Bought Remaining (Disappeared)
    for ad_id in gone_ids:
        ad = prev_ads[ad_id]
        if ad['available'] > 5:
            events.append({
                "type": "bought_all",
                "source": ad['source'], "user": ad['advertiser'],
                "price": ad['price'] / peg, "vol_usd": ad['available'],
                "timestamp": time.time()
            })
            print(f"   ✅ BOUGHT ALL: {ad['source']} - {ad['advertiser']} bought {ad['available']:,.0f} USDT", file=sys.stderr)

    # TYPE C: Partial Fill (Inventory Drop) - FROM v29.1
    for ad_id in common_ids:
        prev_ad = prev_ads[ad_id]
        curr_ad = curr_ads[ad_id]
        
        # Check if inventory dropped
        if curr_ad['available'] < prev_ad['available']:
            diff = prev_ad['available'] - curr_ad['available']
            
            # Filter noise (must be > 5 USD drop)
            if diff > 5:
                events.append({
                    "type": "partial_fill",
                    "source": curr_ad['source'], "user": curr_ad['advertiser'],
                    "price": curr_ad['price'] / peg, "vol_usd": diff,
                    "timestamp": time.time()
                })
                print(f"   📉 PARTIAL: {curr_ad['source']} - {curr_ad['advertiser']} sold {diff:,.0f} USDT", file=sys.stderr)

    return events

# --- 4. DATA HANDLING ---
def load_recent_trades():
    if not os.path.exists(TRADES_FILE): return []
    try:
        with open(TRADES_FILE, "r") as f:
            all_trades = json.load(f)
        cutoff = time.time() - (TRADE_RETENTION_MINUTES * 60)
        return [t for t in all_trades if t.get("timestamp", 0) > cutoff]
    except: return []

def save_trades(new_events):
    recent = load_recent_trades()
    all_events = recent + new_events
    cutoff = time.time() - (TRADE_RETENTION_MINUTES * 60)
    filtered = [e for e in all_events if e.get("timestamp", 0) > cutoff]
    with open(TRADES_FILE, "w") as f: json.dump(filtered, f)

# --- 5. VISUALS & WEB ---
def analyze(prices, peg):
    if not prices: return None
    clean_prices = sorted([p for p in prices if 10 < p < 500])
    if len(clean_prices) < 2: return None
    adj = [p / peg for p in clean_prices]
    n = len(adj)
    try:
        quantiles = statistics.quantiles(adj, n=100, method="inclusive")
        p05, q1, median, q3, p95 = quantiles[4], quantiles[24], quantiles[49], quantiles[74], quantiles[94]
    except:
        median = statistics.median(adj)
        p05, q1, q3, p95 = adj[0], adj[int(n*0.25)], adj[int(n*0.75)], adj[-1]
    return {"median": median, "q1": q1, "q3": q3, "p05": p05, "p95": p95, "min": adj[0], "max": adj[-1], "raw_data": adj, "count": n}

def save_to_history(stats, official):
    file_exists = os.path.isfile(HISTORY_FILE)
    with open(HISTORY_FILE, "a", newline="") as f:
        w = csv.writer(f)
        if not file_exists: w.writerow(["Timestamp", "Median", "Q1", "Q3", "Official"])
        w.writerow([datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), round(stats["median"], 2), round(stats["q1"], 2), round(stats["q3"], 2), round(official, 2) if official else 0])

def load_history():
    if not os.path.isfile(HISTORY_FILE): return [], [], [], [], []
    d, m, q1, q3, off = [], [], [], [], []
    with open(HISTORY_FILE, "r") as f:
        reader = csv.reader(f)
        next(reader, None)
        for row in reader:
            try:
                d.append(datetime.datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S"))
                m.append(float(row[1])); q1.append(float(row[2])); q3.append(float(row[3])); off.append(float(row[4]))
            except: pass
    return (d[-HISTORY_POINTS:], m[-HISTORY_POINTS:], q1[-HISTORY_POINTS:], q3[-HISTORY_POINTS:], off[-HISTORY_POINTS:])

def generate_charts(stats, official_rate):
    if not GRAPH_ENABLED: return
    themes = [
        ("dark", GRAPH_FILENAME, {"bg": "#050505", "fg": "#00ff9d", "grid": "#222", "median": "#ff0055", "sec": "#00bfff", "fill": "#00ff9d", "alpha": 0.7}),
        ("light", GRAPH_LIGHT_FILENAME, {"bg": "#ffffff", "fg": "#1a1a1a", "grid": "#eee", "median": "#d63384", "sec": "#0d6efd", "fill": "#00a876", "alpha": 0.5})
    ]
    dates, medians, q1s, q3s, offs = load_history()
    
    for mode, filename, style in themes:
        plt.rcParams.update({"figure.facecolor": style["bg"], "axes.facecolor": style["bg"], "axes.edgecolor": style["fg"], "axes.labelcolor": style["fg"], "xtick.color": style["fg"], "ytick.color": style["fg"], "text.color": style["fg"]})
        fig = plt.figure(figsize=(12, 14))
        fig.suptitle(f"ETB LIQUIDITY SCANNER: {datetime.datetime.now().strftime('%H:%M')}", fontsize=20, color=style["fg"], fontweight="bold", y=0.97)
        
        ax1 = fig.add_subplot(2, 1, 1)
        data = stats["raw_data"]
        y_jitter = [1 + random.uniform(-0.12, 0.12) for _ in data]
        ax1.scatter(data, y_jitter, color=style["fg"], alpha=style["alpha"], s=30, edgecolors="none")
        ax1.axvline(stats["median"], color=style["median"], linewidth=3)
        ax1.axvline(stats["q1"], color=style["sec"], linewidth=2, linestyle="--", alpha=0.6)
        ax1.axvline(stats["q3"], color=style["sec"], linewidth=2, linestyle="--", alpha=0.6)
        ax1.text(stats["median"], 1.42, f"MEDIAN\n{stats['median']:.2f}", color=style["median"], ha="center", fontweight="bold")
        if official_rate: ax1.axvline(official_rate, color=style["fg"], linestyle=":", linewidth=1.5)
        margin = (stats["p95"] - stats["p05"]) * 0.1
        if margin == 0: margin = 1
        ax1.set_xlim([stats["p05"] - margin, stats["p95"] + margin])
        ax1.set_ylim(0.5, 1.5); ax1.set_yticks([])
        ax1.set_title("Live Market Depth (Top 200 ads)", color=style["fg"], loc="left", pad=10)
        ax1.grid(True, axis="x", color=style["grid"], linestyle="--")
        
        ax2 = fig.add_subplot(2, 1, 2)
        if len(dates) > 1:
            ax2.fill_between(dates, q1s, q3s, color=style["fill"], alpha=0.2, linewidth=0)
            ax2.plot(dates, medians, color=style["median"], linewidth=2)
            if any(offs): ax2.plot(dates, offs, color=style["fg"], linestyle="--", linewidth=1, alpha=0.5)
            ax2.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
            ax2.yaxis.set_major_formatter(ticker.FormatStrFormatter("%.1f"))
            ax2.yaxis.tick_right()
            ax2.grid(True, color=style["grid"], linewidth=0.5)
            ax2.set_title("Historical Trend (24h)", color=style["fg"], loc="left")
        
        plt.tight_layout(rect=[0, 0, 1, 0.95])
        plt.savefig(filename, dpi=150, facecolor=style["bg"])
        plt.close()

def update_website_html(stats, official, timestamp, current_ads, grouped_ads, peg):
    prem = ((stats["median"] - official) / official) * 100 if official else 0
    
    table_rows = ""
    for source, ads in grouped_ads.items():
        prices = [a["price"] for a in ads]
        s = analyze(prices, peg)
        if s: table_rows += f"<tr><td class='source-col'>{source}</td><td>{s['min']:.2f}</td><td>{s['q1']:.2f}</td><td class='med-col'>{s['median']:.2f}</td><td>{s['q3']:.2f}</td><td>{s['max']:.2f}</td><td>{s['count']}</td></tr>"
        else: table_rows += f"<tr><td>{source}</td><td colspan='6' style='opacity:0.5'>No Data</td></tr>"
    
    feed_html = ""
    recent_events = load_recent_trades()
    
    if recent_events:
        recent_events.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
        feed_html = f"<div style='color:#2ea043;font-size:0.9rem;margin-bottom:10px;'>✅ {len(recent_events)} market actions in last hour</div>"
        
        for event in recent_events[:30]:
            ts = datetime.datetime.fromtimestamp(event.get("timestamp", time.time()))
            age = time.time() - event.get("timestamp", time.time())
            age_str = f"{int(age/60)}min" if age >= 60 else f"{int(age)}s"
            source = event["source"]
            s_col = "#f3ba2f" if "Binance" in source else "#2e55e6"
            icon = "🟡" if "Binance" in source else "🔵"
            
            # --- DISPLAY LOGIC ---
            if event["type"] == "new_request":
                action = "<b style='color:#00bfff'>new request</b>"
                bg = "#00bfff"
            elif event["type"] == "bought_all":
                action = "<b style='color:#ff0055'>bought all (gone)</b>"
                bg = "#ff0055"
            else: # partial fill
                action = "<b style='color:#2ea043'>bought partial</b>"
                bg = "#2ea043"
            
            feed_html += f"""
            <div class="feed-item">
                <div class="feed-icon" style="background:{bg}">{icon}</div>
                <div class="feed-content">
                    <span class="feed-ts">{ts.strftime("%I:%M:%S %p")}</span> <span style="color:#666">({age_str})</span> → 
                    <span class="feed-source" style="color:{s_col};font-weight:bold">{source}</span>: 
                    <span class="feed-user">{event['user'][:15]}</span> 
                    {action} 
                    <span class="feed-vol">{event['vol_usd']:,.0f} USDT</span> 
                    @ <span class="feed-price">{event['price']:.2f} ETB</span>
                </div>
            </div>"""
    else:
        all_offers = [ad for ad in current_ads if ad.get("source", "").lower() in ["binance", "mexc"]]
        all_offers.sort(key=lambda x: x.get("price", 0) / peg, reverse=True)
        if all_offers:
            feed_html = f"<div style='color:#ffcc00;font-size:0.9rem;margin-bottom:10px;'>💡 No activity yet - Showing 30 Highest Priced Offers</div>"
            for offer in all_offers[:30]:
                source = offer.get("source", "Unknown")
                s_col = "#f3ba2f" if "Binance" in source else "#2e55e6"
                user = offer.get('advertiser', 'Unknown')[:15]
                vol_str = f"{offer.get('available', 0):,.0f}" if offer.get('available', 0) > 0 else "Liquid"
                feed_html += f"""<div class="feed-item"><div class="feed-icon" style="background:{s_col}">🟡</div><div class="feed-content"><span class="feed-source" style="color:{s_col}">{source}</span>: <span class="feed-user">{user}</span> offering <span class="feed-vol">{vol_str} USDT</span> @ <span class="feed-price">{offer.get('price', 0)/peg:.2f} ETB</span></div></div>"""
        else:
            feed_html = "<div style='color:#888;'>⏳ Waiting for market activity...</div>"
    
    html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta http-equiv="refresh" content="300">
        <title>ETB Market Watch v35.0</title>
        <style>
            :root {{ --bg: #050505; --card: #111; --text: #00ff9d; --sub: #ccc; --mute: #666; --accent: #ff0055; --link: #00bfff; --gold: #ffcc00; --border: #333; }}
            [data-theme="light"] {{ --bg: #f4f4f9; --card: #fff; --text: #1a1a1a; --sub: #333; --mute: #888; --accent: #d63384; --link: #0d6efd; --gold: #ffc107; --border: #ddd; }}
            body {{ background: var(--bg); color: var(--text); font-family: 'Courier New', monospace; margin: 0; padding: 20px; text-align: center; }}
            .container {{ max-width: 1200px; margin: 0 auto; display: grid; grid-template-columns: 2fr 1fr; gap: 20px; text-align: left; }}
            header {{ grid-column: span 2; text-align: center; margin-bottom: 20px; position: relative; }}
            .toggle {{ position: absolute; top: 0; right: 0; cursor: pointer; padding: 8px 16px; border: 1px solid var(--border); border-radius: 20px; background: var(--card); color: var(--sub); font-size: 0.8rem; }}
            .card {{ background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 20px; }}
            .ticker {{ text-align: center; padding: 30px; }}
            .price {{ font-size: 4rem; font-weight: bold; color: var(--sub); margin: 10px 0; }}
            .prem {{ color: var(--gold); font-size: 0.9rem; }}
            .chart img {{ width: 100%; border-radius: 8px; border: 1px solid var(--border); }}
            table {{ width: 100%; border-collapse: collapse; font-size: 0.9rem; }}
            th {{ text-align: left; padding: 12px; border-bottom: 2px solid var(--border); color: var(--text); }}
            td {{ padding: 12px; border-bottom: 1px solid var(--border); color: var(--sub); }}
            .feed-container {{ max-height: 600px; overflow-y: auto; }}
            .feed-item {{ display: flex; gap: 12px; padding: 10px; border-bottom: 1px solid var(--border); align-items: center; }}
            .feed-icon {{ width: 30px; height: 30px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 1.2rem; flex-shrink: 0; color: #fff; }}
            .feed-content {{ font-size: 0.85rem; color: var(--sub); }}
            footer {{ grid-column: span 2; margin-top: 40px; text-align: center; color: var(--mute); font-size: 0.7rem; }}
            @media (max-width: 900px) {{ .container {{ grid-template-columns: 1fr; }} header, footer {{ grid-column: span 1; }} }}
        </style>
    </head>
    <body>
        <div class="container">
            <header>
                <h1>ETB MARKET INTELLIGENCE</h1>
                <div style="color:var(--mute); letter-spacing:4px; font-size:0.8rem;">/// HYBRID ENGINE (SMART PARSER + INVENTORY DROPS) ///</div>
                <div class="toggle" onclick="toggleTheme()">🌓 Theme</div>
            </header>
            <div class="left-col">
                <div class="card ticker">
                    <div style="color:var(--mute); font-size:0.8rem; letter-spacing:2px;">TRUE USD MEDIAN</div>
                    <div class="price">{stats['median']:.2f} <span style="font-size:1.5rem;color:var(--mute)">ETB</span></div>
                    <span class="prem">Premium: +{prem:.2f}%</span>
                </div>
                <div class="card chart"><img src="{GRAPH_FILENAME}?v={cache_buster}" id="chartImg"></div>
                <div class="card"><table><thead><tr><th>Source</th><th>Min</th><th>Q1</th><th>Med</th><th>Q3</th><th>Max</th><th>Ads</th></tr></thead><tbody>{table_rows}</tbody></table></div>
            </div>
            <div class="right-col">
                <div class="card">
                    <div style="font-size:1.1rem;font-weight:bold;margin-bottom:15px;color:var(--text);">👀 Recent Market Actions</div>
                    <div class="feed-container">{feed_html}</div>
                </div>
            </div>
            <footer>Official: {official:.2f} ETB | Last Update: {timestamp} UTC</footer>
        </div>
        <script>
            const imgDark = "{GRAPH_FILENAME}?v={cache_buster}";
            const imgLight = "{GRAPH_LIGHT_FILENAME}?v={cache_buster}";
            const html = document.documentElement;
            (function() {{
                const theme = localStorage.getItem('theme') || 'dark';
                html.setAttribute('data-theme', theme);
                document.getElementById('chartImg').src = theme === 'light' ? imgLight : imgDark;
            }})();
            function toggleTheme() {{
                const current = html.getAttribute('data-theme');
                const next = current === 'light' ? 'dark' : 'light';
                html.setAttribute('data-theme', next);
                localStorage.setItem('theme', next);
                document.getElementById('chartImg').src = next === 'light' ? imgLight : imgDark;
            }}
        </script>
    </body>
    </html>
    """
    with open(HTML_FILENAME, "w") as f: f.write(html)

# --- 6. MAIN ---
def main():
    print("🔍 Running v35.0 (HYBRID ENGINE)...", file=sys.stderr)
    peg = fetch_usdt_peg() or 1.0
    
    print("   > Snapshot 1/2...", file=sys.stderr)
    snapshot_1 = capture_market_snapshot()
    snapshot_1 = remove_outliers(snapshot_1, peg)
    
    print(f"   > ⏳ Waiting {BURST_WAIT_TIME}s...", file=sys.stderr)
    time.sleep(BURST_WAIT_TIME)
    
    print("   > Snapshot 2/2...", file=sys.stderr)
    snapshot_2 = capture_market_snapshot()
    snapshot_2 = remove_outliers(snapshot_2, peg)
    official = fetch_official_rate() or 0.0
    
    grouped_ads = {"Binance": [], "Bybit": [], "MEXC": []}
    for ad in snapshot_2:
        if ad["source"] in grouped_ads: grouped_ads[ad["source"]].append(ad)
    
    if snapshot_2:
        new_events = detect_hybrid_events(snapshot_1, snapshot_2, peg)
        if new_events: save_trades(new_events)
        
        stats_prices = [ad["price"] for ad in snapshot_2 if ad["source"] != "Bybit"]
        stats = analyze(stats_prices, peg)
        
        if stats:
            save_to_history(stats, official)
            generate_charts(stats, official)
            update_website_html(stats, official, time.strftime("%Y-%m-%d %H:%M:%S"), snapshot_2, grouped_ads, peg)
        else: print("⚠️ Could not compute stats", file=sys.stderr)
    else: print("⚠️ No ads found", file=sys.stderr)
    print(f"✅ Complete!")

if __name__ == "__main__":
    main()
