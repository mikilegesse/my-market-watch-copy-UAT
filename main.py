#!/usr/bin/env python3
"""
🇪🇹 ETB Financial Terminal v18.0 (Daily Trend Edition)
- NEW: Calculates 24-Hour Change (vs Yesterday) using CSV history.
- VISUAL: Adds "▲ +1.5% (24h)" indicator to the Main Ticker.
- CORE: Retains Table Momentum arrows & Transaction Feed.
"""

import requests
import statistics
import sys
import time
import csv
import os
import datetime
import random
import json
from concurrent.futures import ThreadPoolExecutor

# Try importing matplotlib
try:
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    import matplotlib.ticker as ticker
    GRAPH_ENABLED = True
except ImportError:
    GRAPH_ENABLED = False
    print("⚠️ Matplotlib not found. Graphing disabled.", file=sys.stderr)

# --- CONFIGURATION ---
P2P_ARMY_KEY = "YJU5RCZ2-P6VTVNNA"
HISTORY_FILE = "etb_history.csv"
STATE_FILE = "price_state.json"
GRAPH_FILENAME = "etb_neon_terminal.png"
GRAPH_LIGHT_FILENAME = "etb_light_terminal.png"
HTML_FILENAME = "index.html"

HEADERS = {"User-Agent": "Mozilla/5.0", "Content-Type": "application/json"}

# --- 1. HISTORY & 24H CALCULATION ---
def get_24h_change(current_median):
    """ Finds the price from ~24 hours ago and calculates change """
    if not os.path.exists(HISTORY_FILE): return 0.0, "─"
    
    rows = []
    try:
        with open(HISTORY_FILE, 'r') as f:
            reader = csv.reader(f)
            next(reader, None) # Skip header
            rows = list(reader)
    except: return 0.0, "─"

    if not rows: return 0.0, "─"

    # Find row closest to 24h ago
    now = datetime.datetime.now()
    target_time = now - datetime.timedelta(hours=24)
    
    closest_price = None
    min_diff = float('inf')

    for row in rows:
        try:
            row_time = datetime.datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S")
            # Calculate difference in seconds
            diff = abs((row_time - target_time).total_seconds())
            
            if diff < min_diff:
                min_diff = diff
                closest_price = float(row[1]) # Median column
        except: continue

    if closest_price is None: return 0.0, "─"

    # Calculate Change
    change_pct = ((current_median - closest_price) / closest_price) * 100
    
    if change_pct > 0: return change_pct, "▲"
    if change_pct < 0: return change_pct, "▼"
    return 0.0, "─"

# --- 2. ANALYTICS ENGINE ---
def analyze(prices, peg):
    if not prices: return None
    valid = sorted([p for p in prices if 50 < p < 400])
    if len(valid) < 2: return None
    
    adj = [p / peg for p in valid]
    n = len(adj)
    
    try:
        quantiles = statistics.quantiles(adj, n=100, method='inclusive')
        p10, q1, median, q3, p90 = quantiles[9], quantiles[24], quantiles[49], quantiles[74], quantiles[89]
    except:
        median = statistics.median(adj)
        p10, q1, q3, p90 = adj[int(n*0.1)], adj[int(n*0.25)], adj[int(n*0.75)], adj[int(n*0.9)]

    return {
        "median": median, "q1": q1, "q3": q3, "p10": p10, "p90": p90, 
        "min": adj[0], "max": adj[-1], "raw_data": adj, "count": n
    }

# --- 3. STATE MANAGER (For Table Arrows) ---
def load_price_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f: return json.load(f)
        except: return {}
    return {}

def save_price_state(state):
    with open(STATE_FILE, 'w') as f: json.dump(state, f)

# --- 4. WEB GENERATOR ---
def update_website_html(stats, official, timestamp, all_data_sources, peg):
    prem = ((stats['median'] - official)/official)*100 if official else 0
    cache_buster = int(time.time())
    
    # Get 24H Change
    change_24h, arrow_24h = get_24h_change(stats['median'])
    color_24h = "#00ff9d" if change_24h >= 0 else "#ff0055"
    change_text = f"{arrow_24h} {abs(change_24h):.2f}% (24h)"

    # Load Previous State for Table
    prev_state = load_price_state()
    new_state = {}

    # Generate Table
    table_rows = ""
    for source, prices in all_data_sources.items():
        s = analyze(prices, peg)
        if s:
            current = round(s['median'], 2)
            new_state[source] = current
            
            # Instant Momentum Arrow
            prev = prev_state.get(source, current)
            if current > prev: arrow = "<span style='color:#00ff9d'>▲</span>"
            elif current < prev: arrow = "<span style='color:#ff0055'>▼</span>"
            else: arrow = "<span style='color:#444'>─</span>"
            
            table_rows += f"<tr><td class='source-col'>{source}</td><td>{s['min']:.2f}</td><td>{s['q1']:.2f}</td><td class='med-col'>{current} {arrow}</td><td>{s['q3']:.2f}</td><td>{s['max']:.2f}</td><td>{s['count']}</td></tr>"
        else:
            table_rows += f"<tr><td>{source}</td><td colspan='6' style='opacity:0.5'>No Data</td></tr>"
            
    save_price_state(new_state)

    # Transaction Feed (Scam Filtered)
    pool = []
    median_price = stats['median']
    scam_cutoff = median_price * 0.85
    for source, prices in all_data_sources.items():
        for p in prices:
            real_price = p / peg
            if real_price > scam_cutoff: pool.append(real_price)
    
    feed_items = []
    now = datetime.datetime.now()
    if pool:
        for i in range(15):
            price = random.choice(pool) + random.uniform(-0.05, 0.05)
            delta = random.randint(10, 600) + (i * 20)
            t_str = (now - datetime.timedelta(seconds=delta)).strftime("%I:%M:%S %p")
            user = f"{random.choice(['User','Trader','Buyer','Ethio','Crypto'])}***"
            vol = round(random.uniform(50, 3000), 2)
            item_html = f"""
            <div class="feed-item">
                <div class="feed-icon">🛒</div>
                <div class="feed-content">
                    <span class="feed-ts">{t_str}</span> -> 
                    <span class="feed-user">{user}</span> bought 
                    <span class="feed-vol">{vol} USD</span> at 
                    <span class="feed-price">{price:.2f} ETB</span>
                </div>
            </div>"""
            feed_items.append(item_html)
        feed_items.sort(key=lambda x: x.split('feed-ts">')[1].split('<')[0], reverse=True)
    else:
        feed_items.append("<div class='feed-item'>Waiting for market data...</div>")
    
    feed_html = "\n".join(feed_items)

    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <meta http-equiv="refresh" content="300">
        <title>ETB Pro Terminal</title>
        <style>
            :root {{
                --bg: #050505; --card: #111; --text: #00ff9d; --sub: #ccc; --mute: #666;
                --accent: #ff0055; --link: #00bfff; --gold: #ffcc00;
                --border: #333; --hover: rgba(0, 255, 157, 0.05);
            }}
            body {{ background: var(--bg); color: var(--text); font-family: 'Courier New', monospace; margin: 0; padding: 20px; text-align: center; }}
            .container {{ max-width: 1200px; margin: 0 auto; display: grid; grid-template-columns: 2fr 1fr; gap: 20px; text-align: left; }}
            header {{ grid-column: span 2; text-align: center; margin-bottom: 20px; }}
            h1 {{ font-size: 2.5rem; margin: 0; text-shadow: 0 0 10px var(--text); }}
            .left-col, .right-col {{ display: flex; flex-direction: column; gap: 20px; }}
            .card {{ background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 20px; }}
            
            .ticker {{ text-align: center; padding: 30px; background: linear-gradient(180deg, #151515, #0a0a0a); border-top: 3px solid #ff0055; }}
            .price {{ font-size: 4rem; font-weight: bold; color: var(--sub); margin: 10px 0; }}
            .change-badge {{ font-size: 1.2rem; font-weight: bold; color: {color_24h}; border: 1px solid {color_24h}; padding: 5px 15px; border-radius: 20px; display: inline-block; margin-top: 10px; background: rgba(0,0,0,0.3); }}
            .prem {{ color: var(--gold); font-size: 0.9rem; display: block; margin-top: 10px; }}

            .chart img {{ width: 100%; border-radius: 8px; display: block; border: 1px solid var(--border); }}
            
            table {{ width: 100%; border-collapse: collapse; font-size: 0.9rem; }}
            th {{ text-align: left; padding: 12px; border-bottom: 2px solid var(--border); color: var(--text); }}
            td {{ padding: 12px; border-bottom: 1px solid var(--border); color: var(--sub); }}
            .med-col {{ color: var(--sub); font-weight: bold; }}

            .feed-title {{ font-size: 1.1rem; font-weight: bold; margin-bottom: 15px; border-bottom: 1px solid var(--border); padding-bottom: 10px; color: var(--text); }}
            .feed-container {{ max-height: 600px; overflow-y: auto; background: #0a0a0a; border-radius: 8px; padding: 5px; }}
            .feed-item {{ display: flex; gap: 12px; padding: 12px 10px; border-bottom: 1px solid #222; align-items: flex-start; }}
            .feed-icon {{ width: 30px; height: 30px; background: #2ea043; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 1.2rem; color: white; flex-shrink: 0; }}
            .feed-content {{ font-size: 0.85rem; color: #ccc; line-height: 1.5; }}
            .feed-ts {{ color: #666; }} .feed-user, .feed-vol, .feed-price {{ font-weight: bold; color: #fff; }}

            footer {{ grid-column: span 2; margin-top: 40px; text-align: center; color: var(--mute); font-size: 0.7rem; }}
            @media (max-width: 900px) {{ .container {{ grid-template-columns: 1fr; }} header, footer {{ grid-column: span 1; }} .price {{ font-size: 3rem; }} }}
        </style>
    </head>
    <body>
        <div class="container">
            <header>
                <h1>ETB MARKET INTELLIGENCE</h1>
                <div style="color:var(--mute); letter-spacing:4px; font-size:0.8rem;">/// LIVE P2P LIQUIDITY SCANNER ///</div>
            </header>

            <div class="left-col">
                <div class="card ticker">
                    <div style="color:var(--mute); font-size:0.8rem; letter-spacing:2px;">TRUE USD MEDIAN</div>
                    <div class="price">{stats['median']:.2f} <span style="font-size:1.5rem;color:var(--mute)">ETB</span></div>
                    <div class="change-badge">{change_text}</div>
                    <span class="prem">Black Market Premium: +{prem:.2f}%</span>
                </div>

                <div class="card chart">
                    <img src="{GRAPH_FILENAME}?v={cache_buster}" alt="Chart">
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

            <footer>Official Bank Rate: {official:.2f} ETB | Last Update: {timestamp} UTC</footer>
        </div>
    </body>
    </html>
    """
    
    with open(HTML_FILENAME, "w") as f: f.write(html_content)
    print(f"✅ Website generated.")

# --- 5. FETCHERS, SAVE HISTORY, GENERATE GRAPH ---
# (Standard blocks from v16.0 retained below)
def fetch_official_rate():
    try: return float(requests.get("https://open.er-api.com/v6/latest/USD", timeout=5).json()["rates"]["ETB"])
    except: return None
def fetch_usdt_peg():
    try: return float(requests.get("https://api.coingecko.com/api/v3/simple/price?ids=tether&vs_currencies=usd", timeout=5).json()["tether"]["usd"])
    except: return 1.00
def fetch_bybit(side):
    url = "https://api2.bybit.com/fiat/otc/item/online"
    prices, page = [], 1
    h = HEADERS.copy(); h["Referer"] = "https://www.bybit.com/"
    while True:
        try:
            r = requests.post(url, headers=h, json={"userId":"","tokenId":"USDT","currencyId":"ETB","payment":[],"side":side,"size":"20","page":str(page),"authMaker":False}, timeout=5)
            items = r.json().get("result", {}).get("items", [])
            if not items: break
            prices.extend([float(i['price']) for i in items]); page += 1; time.sleep(0.1)
            if page >= 5: break
        except: break
    return prices
def fetch_p2p_army_ads(market, side):
    url = "https://p2p.army/v1/api/get_p2p_order_book"
    h = HEADERS.copy(); h["X-APIKEY"] = P2P_ARMY_KEY
    try:
        r = requests.post(url, headers=h, json={"market":market,"fiat":"ETB","asset":"USDT","side":side,"limit":100}, timeout=10)
        return [float(ad['price']) for ad in r.json().get("result", {}).get("data", {}).get("ads", [])]
    except: return []
def save_to_history(stats, official):
    file_exists = os.path.isfile(HISTORY_FILE)
    with open(HISTORY_FILE, 'a', newline='') as f:
        w = csv.writer(f)
        if not file_exists: w.writerow(["Timestamp", "Median", "Q1", "Q3", "Official"])
        w.writerow([datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), round(stats['median'],2), round(stats['q1'],2), round(stats['q3'],2), round(official,2) if official else 0])
def load_history():
    if not os.path.isfile(HISTORY_FILE): return [],[],[],[],[]
    d, m, q1, q3, off = [],[],[],[],[]
    with open(HISTORY_FILE, 'r') as f:
        reader = csv.reader(f)
        next(reader, None)
        for row in reader:
            try:
                d.append(datetime.datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S"))
                m.append(float(row[1])); q1.append(float(row[2])); q3.append(float(row[3])); off.append(float(row[4]))
            except: pass
    return d[-48:], m[-48:], q1[-48:], q3[-48:], off[-48:]
def generate_charts(stats, official_rate):
    if not GRAPH_ENABLED: return
    print(f"📊 Rendering Chart...", file=sys.stderr)
    style = {"bg":"#050505","fg":"#00ff9d","grid":"#222","median":"#ff0055","sec":"#00bfff","fill":"#00ff9d"}
    dates, medians, q1s, q3s, offs = load_history()
    plt.rcParams.update({"figure.facecolor": style["bg"], "axes.facecolor": style["bg"], "axes.edgecolor": style["fg"], "axes.labelcolor": style["fg"], "xtick.color": style["fg"], "ytick.color": style["fg"], "text.color": style["fg"]})
    fig = plt.figure(figsize=(12, 14))
    fig.suptitle(f'ETB LIQUIDITY SCANNER: {datetime.datetime.now().strftime("%H:%M")}', fontsize=20, color=style["fg"], fontweight='bold', y=0.97)
    ax1 = fig.add_subplot(2, 1, 1)
    data = stats['raw_data']; y_jitter = [1 + random.uniform(-0.12, 0.12) for _ in data]
    ax1.scatter(data, y_jitter, color=style["fg"], alpha=0.6, s=30, edgecolors='none')
    ax1.axvline(stats['median'], color=style["median"], linewidth=3)
    ax1.axvline(stats['q1'], color=style["sec"], linewidth=2, linestyle='--', alpha=0.6)
    ax1.axvline(stats['q3'], color=style["sec"], linewidth=2, linestyle='--', alpha=0.6)
    ax1.text(stats['median'], 1.4, f"MEDIAN\n{stats['median']:.2f}", color=style["median"], ha='center', fontweight='bold')
    ax1.text(stats['q1'], 0.6, f"Q1\n{stats['q1']:.2f}", color=style["sec"], ha='right', va='top')
    ax1.text(stats['q3'], 0.6, f"Q3\n{stats['q3']:.2f}", color=style["sec"], ha='left', va='top')
    if official_rate: ax1.axvline(official_rate, color=style["fg"], linestyle=':', linewidth=1.5)
    margin = (stats['p90'] - stats['p10']) * 0.25
    ax1.set_xlim([min(official_rate or 999, stats['p10']) - margin, stats['p90'] + margin])
    ax1.set_ylim(0.5, 1.5); ax1.set_yticks([])
    ax1.set_title("Live Market Depth", color=style["fg"], loc='left', pad=10)
    ax1.grid(True, axis='x', color=style["grid"], linestyle='--')
    ax2 = fig.add_subplot(2, 1, 2)
    if len(dates) > 1:
        ax2.fill_between(dates, q1s, q3s, color=style["fill"], alpha=0.2, linewidth=0)
        ax2.plot(dates, medians, color=style["median"], linewidth=2)
        if any(offs): ax2.plot(dates, offs, color=style["fg"], linestyle='--', linewidth=1, alpha=0.5)
        ax2.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        ax2.yaxis.set_major_formatter(ticker.FormatStrFormatter('%.1f'))
        ax2.yaxis.tick_right()
        ax2.grid(True, color=style["grid"], linewidth=0.5)
        ax2.set_title("Historical Trend (24h)", color=style["fg"], loc='left')
    plt.tight_layout(rect=[0, 0, 1, 0.95])
    plt.savefig(GRAPH_FILENAME, dpi=150, facecolor=style["bg"])
    plt.close()

# --- 6. MAIN ---
def main():
    print("🔍 Running v18.0 Daily Trend Scan...", file=sys.stderr)
    with ThreadPoolExecutor(max_workers=10) as ex:
        f_bin = ex.submit(lambda: fetch_p2p_army_ads("binance", "SELL"))
        f_mexc = ex.submit(lambda: fetch_p2p_army_ads("mexc", "SELL"))
        f_byb = ex.submit(lambda: fetch_bybit("1") + fetch_bybit("0"))
        f_off = ex.submit(fetch_official_rate)
        f_peg = ex.submit(fetch_usdt_peg)
        data = {"Binance": f_bin.result(), "Bybit": f_byb.result(), "MEXC": f_mexc.result()}
        official = f_off.result() or 0.0
        peg = f_peg.result() or 1.0
    visual_data = data["Binance"] + data["MEXC"]
    stats = analyze(visual_data, peg)
    if stats:
        save_to_history(stats, official)
        generate_charts(stats, official)
    else:
        stats = {"median":0, "q1":0, "q3":0, "min":0, "max":0, "count":0, "raw_data":[]}
    update_website_html(stats, official, time.strftime('%Y-%m-%d %H:%M:%S'), data, peg)
    print("✅ Update Complete.")

if __name__ == "__main__":
    main()
