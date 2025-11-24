#!/usr/bin/env python3
"""
🇪🇹 ETB Financial Terminal v33.1 (Inventory Tracking - Extended Window Hotfix)
- PROVEN: Uses v30.2's inventory tracking (available amount drops)
- EXTENDED: 90-second window instead of 30s (catches more trades!)
- LIMITED: 200 ads per source (faster collection)
- AUTO-FILTER: Removes bottom 10% outliers
- REAL: No simulation, only actual inventory drops
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
SNAPSHOT_FILE = "market_state.json"
GRAPH_FILENAME = "etb_neon_terminal.png"
GRAPH_LIGHT_FILENAME = "etb_light_terminal.png"
HTML_FILENAME = "index.html"

# ✅ EXTENDED: 90 seconds gives more time for real P2P trades
BURST_WAIT_TIME = 90

# Limit to 200 most recent ads per source
MAX_ADS_PER_SOURCE = 200

HISTORY_POINTS = 288  # 24 hours at 5-min intervals

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Content-Type": "application/json",
    "Accept": "application/json"
}

# --- 1. FETCHERS (LIMITED TO 200) ---
def fetch_official_rate():
    try:
        return float(requests.get("https://open.er-api.com/v6/latest/USD", timeout=5).json()["rates"]["ETB"])
    except:
        return None

def fetch_usdt_peg():
    try:
        return float(requests.get("https://api.coingecko.com/api/v3/simple/price?ids=tether&vs_currencies=usd", timeout=5).json()["tether"]["usd"])
    except:
        return 1.00

def fetch_binance_direct(trade_type):
    """Fetch top 200 Binance ads"""
    url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
    ads = []
    page = 1
    payload = {
        "asset": "USDT", "fiat": "ETB", "merchantCheck": False,
        "page": 1, "rows": 20, "tradeType": trade_type,
        "payTypes": [], "countries": [], "publisherType": None
    }
    
    while len(ads) < MAX_ADS_PER_SOURCE:
        try:
            payload["page"] = page
            r = requests.post(url, headers=HEADERS, json=payload, timeout=5)
            data = r.json().get("data", [])
            if not data: break
            
            for d in data:
                adv = d.get("adv", {})
                ads.append({
                    "source": "Binance",
                    "advertiser": d.get("advertiser", {}).get("nickName", "User"),
                    "price": float(adv.get("price")),
                    "available": float(adv.get("surplusAmount", 0)),
                })
                if len(ads) >= MAX_ADS_PER_SOURCE:
                    break
            page += 1
            time.sleep(0.2)
        except:
            break
    
    return ads[:MAX_ADS_PER_SOURCE]

def fetch_bybit(side):
    """Fetch top 200 Bybit ads"""
    url = "https://api2.bybit.com/fiat/otc/item/online"
    ads = []
    page = 1
    h = HEADERS.copy()
    h["Referer"] = "https://www.bybit.com/"
    
    while len(ads) < MAX_ADS_PER_SOURCE:
        try:
            r = requests.post(url, headers=h, json={
                "userId": "", "tokenId": "USDT", "currencyId": "ETB",
                "payment": [], "side": side, "size": "50",
                "page": str(page), "authMaker": False
            }, timeout=5)
            items = r.json().get("result", {}).get("items", [])
            if not items: break
            
            for i in items:
                ads.append({
                    "source": "Bybit",
                    "advertiser": i.get("nickName", "User"),
                    "price": float(i.get("price")),
                    "available": float(i.get("lastQuantity", 0)),
                })
                if len(ads) >= MAX_ADS_PER_SOURCE:
                    break
            page += 1
            time.sleep(0.1)
        except:
            break
    
    return ads[:MAX_ADS_PER_SOURCE]

def fetch_p2p_army_ads(market, side):
    """Fetch top 200 ads via P2P.Army API"""
    url = "https://p2p.army/v1/api/get_p2p_order_book"
    h = HEADERS.copy()
    h["X-APIKEY"] = P2P_ARMY_KEY
    
    try:
        r = requests.post(url, headers=h, json={
            "market": market, "fiat": "ETB", "asset": "USDT",
            "side": side, "limit": MAX_ADS_PER_SOURCE
        }, timeout=10)
        data = r.json()
        
        # v16.0's proven parsing
        raw = data.get("result", data.get("data", data.get("ads", [])))
        if not raw and isinstance(data, list):
            raw = data
        
        source_name = "Binance" if market.lower() == "binance" else "MEXC"
        clean = []
        
        for ad in raw[:MAX_ADS_PER_SOURCE]:
            if isinstance(ad, dict) and "price" in ad:
                try:
                    clean.append({
                        "source": source_name,
                        "advertiser": ad.get("advertiser_name", ad.get("nickname", "User")),
                        "price": float(ad["price"]),
                        "available": float(ad.get("available_amount", ad.get("amount", 0))),
                    })
                except:
                    continue
        
        print(f"   {source_name}: {len(clean)} ads", file=sys.stderr)
        return clean[:MAX_ADS_PER_SOURCE]
    except Exception as e:
        print(f"   {market} error: {e}", file=sys.stderr)
        return []

# --- 2. INVENTORY TRACKING (v30.2 METHOD) ---
def capture_market_snapshot():
    """Gets top 200 ads from each source"""
    with ThreadPoolExecutor(max_workers=10) as ex:
        f_bin = ex.submit(lambda: fetch_p2p_army_ads("binance", "SELL"))
        f_mexc = ex.submit(lambda: fetch_p2p_army_ads("mexc", "SELL"))
        f_byb = ex.submit(lambda: fetch_bybit("1"))
        
        bin_data = f_bin.result() or []
        mexc_data = f_mexc.result() or []
        bybit_data = f_byb.result() or []
        
        if not bin_data:
            bin_data = fetch_binance_direct("SELL")
        
        total = len(bin_data) + len(mexc_data) + len(bybit_data)
        print(f"   📊 Collected {total} ads (Binance: {len(bin_data)}, MEXC: {len(mexc_data)}, Bybit: {len(bybit_data)})", file=sys.stderr)
        
        return bin_data + bybit_data + mexc_data

def remove_outliers(ads, peg):
    """Auto-remove bottom 10% of prices"""
    if len(ads) < 10:
        return ads
    
    prices = sorted([ad["price"] / peg for ad in ads])
    p10_threshold = prices[int(len(prices) * 0.10)]
    
    filtered = [ad for ad in ads if (ad["price"] / peg) > p10_threshold]
    
    removed = len(ads) - len(filtered)
    if removed > 0:
        print(f"   🗑️ Removed {removed} outliers (bottom 10%)", file=sys.stderr)
    
    return filtered

def load_market_state():
    if os.path.exists(SNAPSHOT_FILE):
        try:
            with open(SNAPSHOT_FILE, "r") as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_market_state(current_ads):
    state = {}
    for ad in current_ads:
        # Use advertiser + price as key (more stable than ad ID)
        key = f"{ad['source']}_{ad['advertiser']}_{ad['price']}"
        state[key] = ad["available"]
    
    with open(SNAPSHOT_FILE, "w") as f:
        json.dump(state, f)

def detect_real_trades(current_ads, peg):
    """
    ✅ v30.2 METHOD: Track inventory drops
    If seller had 1000 USDT and now has 750 USDT → SOLD 250 USDT
    """
    prev_state = load_market_state()
    trades = []
    
    if not prev_state:
        print("   > First run - establishing baseline", file=sys.stderr)
        return trades
    
    for ad in current_ads:
        # Only track Binance and MEXC (Bybit unreliable)
        if ad["source"].lower() not in ["binance", "mexc"]:
            continue
        
        key = f"{ad['source']}_{ad['advertiser']}_{ad['price']}"
        
        if key in prev_state:
            prev_inventory = prev_state[key]
            curr_inventory = ad["available"]
            
            # Inventory Drop = Confirmed Trade!
            if curr_inventory < prev_inventory:
                diff = prev_inventory - curr_inventory
                
                # Only report significant trades (>100 USDT)
                if diff > 100:
                    trades.append({
                        "type": "trade",
                        "source": ad["source"],
                        "user": ad["advertiser"],
                        "price": ad["price"] / peg,
                        "vol_usd": diff,
                        "timestamp": datetime.datetime.now().isoformat()
                    })
                    print(f"   ✅ TRADE: {ad['source']} - {ad['advertiser'][:15]} sold {diff:,.0f} USDT @ {ad['price']/peg:.2f} ETB", file=sys.stderr)
    
    print(f"   > Detected {len(trades)} trades in {BURST_WAIT_TIME}s window", file=sys.stderr)
    return trades

# --- 3. ANALYTICS ---
def analyze(prices, peg):
    if not prices:
        return None
    
    clean_prices = sorted([p for p in prices if 10 < p < 500])
    if len(clean_prices) < 2:
        return None
    
    adj = [p / peg for p in clean_prices]
    n = len(adj)
    
    try:
        quantiles = statistics.quantiles(adj, n=100, method="inclusive")
        p05, q1, median, q3, p95 = quantiles[4], quantiles[24], quantiles[49], quantiles[74], quantiles[94]
    except:
        median = statistics.median(adj)
        p05, q1, q3, p95 = adj[0], adj[int(n*0.25)], adj[int(n*0.75)], adj[-1]
    
    return {
        "median": median, "q1": q1, "q3": q3,
        "p05": p05, "p95": p95,
        "min": adj[0], "max": adj[-1],
        "raw_data": adj, "count": n
    }

# --- 4. HISTORY ---
def save_to_history(stats, official):
    file_exists = os.path.isfile(HISTORY_FILE)
    with open(HISTORY_FILE, "a", newline="") as f:
        w = csv.writer(f)
        if not file_exists:
            w.writerow(["Timestamp", "Median", "Q1", "Q3", "Official"])
        w.writerow([
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            round(stats["median"], 2), round(stats["q1"], 2),
            round(stats["q3"], 2), round(official, 2) if official else 0
        ])

def load_history():
    if not os.path.isfile(HISTORY_FILE):
        return [], [], [], [], []
    
    d, m, q1, q3, off = [], [], [], [], []
    with open(HISTORY_FILE, "r") as f:
        reader = csv.reader(f)
        next(reader, None)
        for row in reader:
            try:
                d.append(datetime.datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S"))
                m.append(float(row[1]))
                q1.append(float(row[2]))
                q3.append(float(row[3]))
                off.append(float(row[4]))
            except:
                pass
    
    return (d[-HISTORY_POINTS:], m[-HISTORY_POINTS:], 
            q1[-HISTORY_POINTS:], q3[-HISTORY_POINTS:], off[-HISTORY_POINTS:])

# --- 5. GRAPH GENERATOR ---
def generate_charts(stats, official_rate):
    if not GRAPH_ENABLED:
        return
    
    themes = [
        ("dark", GRAPH_FILENAME, {
            "bg": "#050505", "fg": "#00ff9d", "grid": "#222",
            "median": "#ff0055", "sec": "#00bfff", "fill": "#00ff9d", "alpha": 0.7
        }),
        ("light", GRAPH_LIGHT_FILENAME, {
            "bg": "#ffffff", "fg": "#1a1a1a", "grid": "#eee",
            "median": "#d63384", "sec": "#0d6efd", "fill": "#00a876", "alpha": 0.5
        })
    ]
    dates, medians, q1s, q3s, offs = load_history()
    
    for mode, filename, style in themes:
        plt.rcParams.update({
            "figure.facecolor": style["bg"], "axes.facecolor": style["bg"],
            "axes.edgecolor": style["fg"], "axes.labelcolor": style["fg"],
            "xtick.color": style["fg"], "ytick.color": style["fg"],
            "text.color": style["fg"]
        })
        
        fig = plt.figure(figsize=(12, 14))
        fig.suptitle(
            f"ETB LIQUIDITY SCANNER: {datetime.datetime.now().strftime('%H:%M')}",
            fontsize=20, color=style["fg"], fontweight="bold", y=0.97
        )
        
        ax1 = fig.add_subplot(2, 1, 1)
        data = stats["raw_data"]
        y_jitter = [1 + random.uniform(-0.12, 0.12) for _ in data]
        ax1.scatter(data, y_jitter, color=style["fg"], alpha=style["alpha"], s=30, edgecolors="none")
        ax1.axvline(stats["median"], color=style["median"], linewidth=3)
        ax1.axvline(stats["q1"], color=style["sec"], linewidth=2, linestyle="--", alpha=0.6)
        ax1.axvline(stats["q3"], color=style["sec"], linewidth=2, linestyle="--", alpha=0.6)
        ax1.text(stats["median"], 1.42, f"MEDIAN\n{stats['median']:.2f}",
                color=style["median"], ha="center", fontweight="bold")
        ax1.text(stats["q1"], 0.58, f"Q1\n{stats['q1']:.2f}",
                color=style["sec"], ha="right", va="top")
        ax1.text(stats["q3"], 0.58, f"Q3\n{stats['q3']:.2f}",
                color=style["sec"], ha="left", va="top")
        
        if official_rate:
            ax1.axvline(official_rate, color=style["fg"], linestyle=":", linewidth=1.5)
        
        margin = (stats["p95"] - stats["p05"]) * 0.1
        if margin == 0: margin = 1
        ax1.set_xlim([stats["p05"] - margin, stats["p95"] + margin])
        ax1.set_ylim(0.5, 1.5)
        ax1.set_yticks([])
        ax1.set_title("Live Market Depth (Top 200 ads, auto-filtered)", color=style["fg"], loc="left", pad=10)
        ax1.grid(True, axis="x", color=style["grid"], linestyle="--")
        
        ax2 = fig.add_subplot(2, 1, 2)
        if len(dates) > 1:
            ax2.fill_between(dates, q1s, q3s, color=style["fill"], alpha=0.2, linewidth=0)
            ax2.plot(dates, medians, color=style["median"], linewidth=2)
            if any(offs):
                ax2.plot(dates, offs, color=style["fg"], linestyle="--", linewidth=1, alpha=0.5)
            ax2.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
            ax2.yaxis.set_major_formatter(ticker.FormatStrFormatter("%.1f"))
            ax2.yaxis.tick_right()
            ax2.grid(True, color=style["grid"], linewidth=0.5)
            ax2.set_title("Historical Trend (24h)", color=style["fg"], loc="left")
        
        plt.tight_layout(rect=[0, 0, 1, 0.95])
        plt.savefig(filename, dpi=150, facecolor=style["bg"])
        plt.close()

# --- 6. WEB GENERATOR ---
def update_website_html(stats, official, timestamp, trades, grouped_ads, peg):
    prem = ((stats["median"] - official) / official) * 100 if official else 0
    cache_buster = int(time.time())
    
    # Table
    table_rows = ""
    for source, ads in grouped_ads.items():
        prices = [a["price"] for a in ads]
        s = analyze(prices, peg)
        if s:
            table_rows += f"<tr><td class='source-col'>{source}</td><td>{s['min']:.2f}</td><td>{s['q1']:.2f}</td><td class='med-col'>{s['median']:.2f}</td><td>{s['q3']:.2f}</td><td>{s['max']:.2f}</td><td>{s['count']}</td></tr>"
        else:
            table_rows += f"<tr><td>{source}</td><td colspan='6' style='opacity:0.5'>No Data</td></tr>"
    
    # Trade Feed
    feed_html = ""
    
    if trades:
        for trade in trades[:30]:
            ts = datetime.datetime.fromisoformat(trade["timestamp"])
            time_str = ts.strftime("%I:%M:%S %p")
            
            source = trade["source"]
            s_col = "#f3ba2f" if "Binance" in source else "#2e55e6"
            icon = "🟡" if "Binance" in source else "🔵"
            
            feed_html += f"""
            <div class="feed-item">
                <div class="feed-icon" style="background:#2ea043">{icon}</div>
                <div class="feed-content">
                    <span class="feed-ts">{time_str}</span> → 
                    <span class="feed-source" style="color:{s_col};font-weight:bold">{source}</span>: 
                    <span class="feed-user">{trade['user'][:15]}</span> 
                    <b style="color:#2ea043">SOLD</b> 
                    <span class="feed-vol">{trade['vol_usd']:,.0f} USDT</span> 
                    @ <span class="feed-price">{trade['price']:.2f} ETB</span>
                </div>
            </div>"""
    else:
        feed_html = f"<div class='feed-item' style='color:#888'>⏳ No trades detected in {BURST_WAIT_TIME}s window. First run or market quiet.</div>"
    
    html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <meta http-equiv="refresh" content="300">
        <title>ETB Market Watch v33.1</title>
        <style>
            :root {{ --bg: #050505; --card: #111; --text: #00ff9d; --sub: #ccc; --mute: #666; --accent: #ff0055; --link: #00bfff; --gold: #ffcc00; --border: #333; }}
            [data-theme="light"] {{ --bg: #f4f4f9; --card: #fff; --text: #1a1a1a; --sub: #333; --mute: #888; --accent: #d63384; --link: #0d6efd; --gold: #ffc107; --border: #ddd; }}
            
            body {{ background: var(--bg); color: var(--text); font-family: 'Courier New', monospace; margin: 0; padding: 20px; text-align: center; transition: 0.3s; }}
            .container {{ max-width: 1200px; margin: 0 auto; display: grid; grid-template-columns: 2fr 1fr; gap: 20px; text-align: left; }}
            
            header {{ grid-column: span 2; text-align: center; margin-bottom: 20px; position: relative; }}
            h1 {{ font-size: 2.5rem; margin: 0; text-shadow: 0 0 10px var(--text); }}
            .toggle {{ position: absolute; top: 0; right: 0; cursor: pointer; padding: 8px 16px; border: 1px solid var(--border); border-radius: 20px; background: var(--card); color: var(--sub); font-size: 0.8rem; }}
            
            .left-col, .right-col {{ display: flex; flex-direction: column; gap: 20px; }}
            .card {{ background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 20px; box-shadow: 0 5px 15px rgba(0,0,0,0.05); }}
            
            .ticker {{ text-align: center; padding: 30px; background: linear-gradient(145deg, var(--card), var(--bg)); }}
            .price {{ font-size: 4rem; font-weight: bold; color: var(--sub); margin: 10px 0; }}
            .prem {{ color: var(--gold); font-size: 0.9rem; display: block; margin-top: 10px; }}
            
            .chart img {{ width: 100%; border-radius: 8px; display: block; border: 1px solid var(--border); }}
            
            table {{ width: 100%; border-collapse: collapse; font-size: 0.9rem; }}
            th {{ text-align: left; padding: 12px; border-bottom: 2px solid var(--border); color: var(--text); }}
            td {{ padding: 12px; border-bottom: 1px solid var(--border); color: var(--sub); }}
            .source-col {{ font-weight: bold; color: var(--text); }} .med-col {{ color: var(--accent); font-weight: bold; }}
            
            .feed-title {{ font-size: 1.1rem; font-weight: bold; margin-bottom: 15px; border-bottom: 1px solid var(--border); padding-bottom: 10px; color: var(--text); }}
            .feed-container {{ max-height: 600px; overflow-y: auto; padding-right: 5px; }}
            .feed-item {{ display: flex; gap: 12px; padding: 10px; border-bottom: 1px solid var(--border); align-items: center; }}
            .feed-icon {{ width: 30px; height: 30px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 1.2rem; flex-shrink: 0; color: #fff; }}
            .feed-content {{ font-size: 0.85rem; color: var(--sub); }}
            .feed-ts {{ color: var(--mute); font-family: monospace; }}
            .feed-user, .feed-price {{ font-weight: bold; color: var(--text); }}
            .feed-vol {{ font-weight: bold; color: var(--link); }}
            
            footer {{ grid-column: span 2; margin-top: 40px; text-align: center; color: var(--mute); font-size: 0.7rem; }}
            @media (max-width: 900px) {{ .container {{ grid-template-columns: 1fr; }} header, footer {{ grid-column: span 1; }} .price {{ font-size: 3rem; }} }}
        </style>
    </head>
    <body>
        <div class="container">
            <header>
                <h1>ETB MARKET INTELLIGENCE</h1>
                <div style="color:var(--mute); letter-spacing:4px; font-size:0.8rem;">/// INVENTORY TRACKING (90s window) ///</div>
                <div class="toggle" onclick="toggleTheme()">🌓 Theme</div>
            </header>

            <div class="left-col">
                <div class="card ticker">
                    <div style="color:var(--mute); font-size:0.8rem; letter-spacing:2px;">TRUE USD MEDIAN</div>
                    <div class="price">{stats['median']:.2f} <span style="font-size:1.5rem;color:var(--mute)">ETB</span></div>
                    <span class="prem">Black Market Premium: +{prem:.2f}%</span>
                </div>
                <div class="card chart">
                    <img src="{GRAPH_FILENAME}?v={cache_buster}" id="chartImg" alt="Market Chart">
                </div>
                <div class="card">
                    <table>
                        <thead><tr><th>Source</th><th>Min</th><th>Q1</th><th>Med</th><th>Q3</th><th>Max</th><th>Ads</th></tr></thead>
                        <tbody>{table_rows}</tbody>
                    </table>
                </div>
            </div>

            <div class="right-col">
                <div class="card">
                    <div class="feed-title">👀 Recent Market Actions</div>
                    <div class="feed-container">{feed_html}</div>
                </div>
            </div>

            <footer>
                Official: {official:.2f} ETB | Last Update: {timestamp} UTC | 
                Top 200 ads per source, 90s monitoring window
            </footer>
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
    
    with open(HTML_FILENAME, "w") as f:
        f.write(html)

# --- 7. MAIN (v30.2 BURST SCAN METHOD, FIXED BASELINE) ---
def main():
    print("🔍 Running v33.1 (Inventory Tracking - Extended)...", file=sys.stderr)
    
    # Use ONE peg for the whole run
    peg = fetch_usdt_peg() or 1.0
    
    # 1. SNAPSHOT 1  (baseline)
    print("   > Snapshot 1/2...", file=sys.stderr)
    snapshot_1 = capture_market_snapshot()
    snapshot_1 = remove_outliers(snapshot_1, peg)
    
    # Save baseline so detect_real_trades() can compare against it
    save_market_state(snapshot_1)
    
    # 2. WAIT (Extended to 90 seconds)
    print(f"   > ⏳ Waiting {BURST_WAIT_TIME}s to catch trades...", file=sys.stderr)
    time.sleep(BURST_WAIT_TIME)
    
    # 3. SNAPSHOT 2  (after 90s)
    print("   > Snapshot 2/2...", file=sys.stderr)
    snapshot_2 = capture_market_snapshot()
    snapshot_2 = remove_outliers(snapshot_2, peg)
    
    official = fetch_official_rate() or 0.0
    
    # Group by source
    grouped_ads = {"Binance": [], "Bybit": [], "MEXC": []}
    for ad in snapshot_2:
        if ad["source"] in grouped_ads:
            grouped_ads[ad["source"]].append(ad)
    
    if snapshot_2:
        # Detect trades via inventory tracking (between snapshot_1 and snapshot_2)
        trades = detect_real_trades(snapshot_2, peg)
        
        # Save new state for the NEXT run
        save_market_state(snapshot_2)
        
        # Stats (exclude Bybit from history)
        stats_prices = [ad["price"] for ad in snapshot_2 if ad["source"] != "Bybit"]
        stats = analyze(stats_prices, peg)
        
        if stats:
            save_to_history(stats, official)
            generate_charts(stats, official)
            update_website_html(
                stats, official,
                time.strftime("%Y-%m-%d %H:%M:%S"),
                trades, grouped_ads, peg
            )
        else:
            print("⚠️ Could not compute stats", file=sys.stderr)
    else:
        print("⚠️ No ads found", file=sys.stderr)
    
    print(f"✅ Complete. Total runtime: ~{BURST_WAIT_TIME + 30}s")

if __name__ == "__main__":
    main()
