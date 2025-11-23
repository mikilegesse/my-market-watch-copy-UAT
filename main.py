#!/usr/bin/env python3
"""
🇪🇹 ETB Financial Terminal v10.0 (Streamlined Feed)
- REMOVED: Theme Toggle & Hover Animations (Static Pro UI)
- ADDED: "Recent Market Actions" Feed at the bottom
- CORE: Uses P2P.Army for Binance/MEXC, Direct for Bybit
"""

import requests
import statistics
import sys
import time
import csv
import os
import datetime
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
    print("⚠️ Matplotlib not found. Graphing disabled.", file=sys.stderr)

# --- CONFIGURATION ---
P2P_ARMY_KEY = "YJU5RCZ2-P6VTVNNA"
HISTORY_FILE = "etb_history.csv"
GRAPH_FILENAME = "etb_neon_terminal.png"
HTML_FILENAME = "index.html"

HEADERS = {"User-Agent": "Mozilla/5.0", "Content-Type": "application/json"}

# --- 1. ANALYTICS ENGINE ---
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

# --- 2. WEB GENERATOR (Static + Feed) ---
def update_website_html(stats, official, timestamp, all_data_sources, peg):
    prem = ((stats['median'] - official)/official)*100 if official else 0
    cache_buster = int(time.time())
    
    # 1. Generate Table
    table_rows = ""
    for source, prices in all_data_sources.items():
        s = analyze(prices, peg)
        if s:
            table_rows += f"<tr><td style='font-weight:bold;color:#ccc'>{source}</td><td>{s['min']:.2f}</td><td>{s['q1']:.2f}</td><td style='color:#ff0055;font-weight:bold'>{s['median']:.2f}</td><td>{s['q3']:.2f}</td><td>{s['max']:.2f}</td><td>{s['count']}</td></tr>"
        else:
            table_rows += f"<tr><td>{source}</td><td colspan='6'>No Data</td></tr>"

    # 2. Generate "Recent Market Actions" (Simulated Feed)
    # We create fake "Completed Trades" based on real available prices
    feed_items = []
    current_time = datetime.datetime.now()
    
    # Flatten prices to create a pool of "executable" orders
    pool = []
    for source, prices in all_data_sources.items():
        for p in prices: pool.append((source, p/peg))
    
    # Generate 15 fake "recent" actions
    for i in range(15):
        if not pool: break
        trade = random.choice(pool)
        source, price = trade
        
        # Randomize time (e.g., "2 mins ago")
        delta_sec = random.randint(10, 600) + (i * 60)
        t_str = (current_time - datetime.timedelta(seconds=delta_sec)).strftime("%I:%M:%S %p")
        
        # Randomize User & Volume
        user = f"{random.choice(['Alex', 'Bini', 'Chala', 'Dawit', 'Ezra', 'Fikre', 'Girma'])}***"
        vol = round(random.uniform(10, 5000), 2)
        action_type = random.choice(["bought", "bought", "bought", "requested"]) # Mostly buys
        
        icon = "🛒" if action_type == "bought" else "❓"
        icon_bg = "#2ea043" if action_type == "bought" else "#888"
        
        item_html = f"""
        <div class="feed-item">
            <div class="feed-icon" style="background: {icon_bg}">{icon}</div>
            <div class="feed-info">
                <div class="feed-meta">{datetime.datetime.now().strftime('%m/%d/%Y')}, {t_str}</div>
                <div class="feed-desc">
                    <span class="feed-user">{user}</span> (BUYER) {action_type} 
                    <span class="feed-vol">{vol} USD</span> at 
                    <span class="feed-price">{price:.2f} ETB</span>
                </div>
            </div>
        </div>
        """
        feed_items.append(item_html)

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
            body {{ background-color: #050505; color: #00ff9d; font-family: 'Courier New', monospace; margin: 0; padding: 20px; text-align: center; }}
            .container {{ max-width: 1000px; margin: 0 auto; }}
            
            h1 {{ font-size: 2.2rem; margin: 0; text-shadow: 0 0 10px #00ff9d; letter-spacing: 2px; }}
            .sub {{ color: #666; font-size: 0.8rem; letter-spacing: 4px; margin-bottom: 30px; }}

            /* CARDS (Static, No Hover Movement) */
            .card {{ background: #111; border: 1px solid #333; border-radius: 12px; padding: 20px; margin-bottom: 20px; }}
            
            /* TICKER */
            .ticker {{ background: linear-gradient(180deg, #151515, #0a0a0a); border-top: 3px solid #ff0055; }}
            .price {{ font-size: 3.5rem; font-weight: bold; color: #fff; margin: 10px 0; text-shadow: 0 0 20px rgba(255,0,85,0.4); }}
            .prem {{ color: #ffcc00; border: 1px solid #ffcc00; padding: 4px 12px; border-radius: 20px; font-size: 0.9rem; }}

            /* GRAPH */
            .chart img {{ width: 100%; border-radius: 8px; display: block; border: 1px solid #333; }}

            /* TABLE */
            table {{ width: 100%; border-collapse: collapse; font-size: 0.9rem; }}
            th {{ text-align: left; padding: 12px; background: #1a1a1a; color: #888; text-transform: uppercase; font-size: 0.75rem; }}
            td {{ padding: 12px; border-bottom: 1px solid #222; color: #ccc; }}
            tr:last-child td {{ border-bottom: none; }}

            /* FEED (Recent Actions) */
            .feed-header {{ text-align: left; font-size: 1.1rem; font-weight: bold; margin-bottom: 15px; display: flex; align-items: center; gap: 10px; color: #fff; }}
            .feed-list {{ max-height: 400px; overflow-y: auto; background: #0a0a0a; border: 1px solid #333; border-radius: 8px; }}
            
            /* Custom Scrollbar */
            .feed-list::-webkit-scrollbar {{ width: 8px; }}
            .feed-list::-webkit-scrollbar-track {{ background: #111; }}
            .feed-list::-webkit-scrollbar-thumb {{ background: #333; border-radius: 4px; }}

            .feed-item {{ display: flex; gap: 15px; padding: 15px; border-bottom: 1px solid #222; align-items: flex-start; text-align: left; }}
            .feed-icon {{ width: 32px; height: 32px; border-radius: 50%; color: white; display: flex; align-items: center; justify-content: center; font-size: 1rem; flex-shrink: 0; }}
            .feed-info {{ display: flex; flex-direction: column; gap: 4px; }}
            .feed-meta {{ color: #666; font-size: 0.75rem; }}
            .feed-desc {{ color: #ddd; font-size: 0.9rem; line-height: 1.4; }}
            .feed-user {{ font-weight: bold; color: #fff; }}
            .feed-vol {{ font-weight: bold; color: #fff; }}
            .feed-price {{ font-weight: bold; color: #000; background: #00ff9d; padding: 0 4px; border-radius: 2px; }}

            footer {{ margin-top: 40px; color: #444; font-size: 0.7rem; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>ETB MARKET INTELLIGENCE</h1>
            <div class="sub">/// LIVE P2P LIQUIDITY SCANNER ///</div>

            <div class="card ticker">
                <div style="color:#888; font-size:0.8rem; letter-spacing:2px;">TRUE USD MEDIAN</div>
                <div class="price">{stats['median']:.2f} ETB</div>
                <span class="prem">Premium: +{prem:.2f}%</span>
            </div>

            <div class="card chart">
                <img src="{GRAPH_FILENAME}?v={cache_buster}" alt="Market Chart">
            </div>

            <div class="card">
                <table>
                    <thead><tr><th>Source</th><th>Min</th><th>Q1</th><th>Median</th><th>Q3</th><th>Max</th><th>Ads</th></tr></thead>
                    <tbody>{table_rows}</tbody>
                </table>
            </div>

            <div class="feed-header">👀 Recent Market Actions</div>
            <div class="feed-list">
                {feed_html}
            </div>

            <footer>
                Official Bank Rate: {official:.2f} ETB | System Update: {timestamp} UTC
            </footer>
        </div>
    </body>
    </html>
    """
    
    with open(HTML_FILENAME, "w") as f:
        f.write(html_content)
    print(f"✅ Website generated.")

# --- 3. FETCHERS ---
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
            prices.extend([float(i['price']) for i in items])
            if page >= 5: break
            page += 1; time.sleep(0.1)
        except: break
    return prices

def fetch_p2p_army_ads(market, side):
    url = "https://p2p.army/v1/api/get_p2p_order_book"
    prices = []
    h = HEADERS.copy(); h["X-APIKEY"] = P2P_ARMY_KEY
    try:
        payload = {"market": market, "fiat": "ETB", "asset": "USDT", "side": side, "limit": 100}
        r = requests.post(url, headers=h, json=payload, timeout=10)
        data = r.json()
        candidates = data.get("result", data.get("data", data.get("ads", [])))
        if not candidates and isinstance(data, list): candidates = data
        if candidates:
            for ad in candidates:
                if isinstance(ad, dict) and 'price' in ad: prices.append(float(ad['price']))
    except: pass
    return prices

# --- 4. HISTORY ---
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

# --- 5. GRAPH GENERATOR ---
def generate_charts(stats, official_rate):
    if not GRAPH_ENABLED: return
    print(f"📊 Rendering Chart...", file=sys.stderr)
    
    # Dark Mode Style Only
    style = {"bg":"#050505","fg":"#00ff9d","grid":"#222","median":"#ff0055","sec":"#00bfff","fill":"#00ff9d"}
    dates, medians, q1s, q3s, offs = load_history()

    plt.rcParams.update({"figure.facecolor": style["bg"], "axes.facecolor": style["bg"], "axes.edgecolor": style["fg"], "axes.labelcolor": style["fg"], "xtick.color": style["fg"], "ytick.color": style["fg"], "text.color": style["fg"]})
    fig = plt.figure(figsize=(12, 14))
    fig.suptitle(f'ETB LIQUIDITY SCANNER: {datetime.datetime.now().strftime("%H:%M")}', fontsize=20, color=style["fg"], fontweight='bold', y=0.97)

    # TOP: DOT PLOT
    ax1 = fig.add_subplot(2, 1, 1)
    data = stats['raw_data']
    y_jitter = [1 + random.uniform(-0.12, 0.12) for _ in data]
    ax1.scatter(data, y_jitter, color=style["fg"], alpha=0.6, s=30, edgecolors='none')
    ax1.axvline(stats['median'], color=style["median"], linewidth=3)
    ax1.axvline(stats['q1'], color=style["sec"], linewidth=2, linestyle='--', alpha=0.6)
    ax1.axvline(stats['q3'], color=style["sec"], linewidth=2, linestyle='--', alpha=0.6)
    
    # Fixed Labels
    ax1.text(stats['median'], 1.4, f"MEDIAN\n{stats['median']:.2f}", color=style["median"], ha='center', fontweight='bold')
    ax1.text(stats['q1'], 0.6, f"Q1\n{stats['q1']:.2f}", color=style["sec"], ha='right', va='top')
    ax1.text(stats['q3'], 0.6, f"Q3\n{stats['q3']:.2f}", color=style["sec"], ha='left', va='top')
    
    if official_rate: ax1.axvline(official_rate, color=style["fg"], linestyle=':', linewidth=1.5)
    
    margin = (stats['p90'] - stats['p10']) * 0.25
    ax1.set_xlim([min(official_rate or 999, stats['p10']) - margin, stats['p90'] + margin])
    ax1.set_ylim(0.5, 1.5); ax1.set_yticks([])
    ax1.set_title("Live Market Depth", color=style["fg"], loc='left', pad=10)
    ax1.grid(True, axis='x', color=style["grid"], linestyle='--')

    # BOTTOM: HISTORY
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
    print("🔍 Running v10.0 Scan...", file=sys.stderr)
    with ThreadPoolExecutor(max_workers=10) as ex:
        f_bin = ex.submit(lambda: fetch_p2p_army_ads("binance", "SELL"))
        f_mexc = ex.submit(lambda: fetch_p2p_army_ads("mexc", "SELL"))
        f_byb = ex.submit(lambda: fetch_bybit("1") + fetch_bybit("0"))
        f_off = ex.submit(fetch_official_rate)
        f_peg = ex.submit(fetch_usdt_peg)
        
        data = {"Binance": f_bin.result(), "Bybit": f_byb.result(), "MEXC": f_mexc.result()}
        official = f_off.result()
        peg = f_peg.result()

    visual_data = data["Binance"] + data["MEXC"]
    stats = analyze(visual_data, peg)
    
    if stats:
        save_to_history(stats, official)
        generate_charts(stats, official)
        update_website_html(stats, official, time.strftime('%Y-%m-%d %H:%M:%S'), data, peg)
        print("✅ Update Complete.")

if __name__ == "__main__":
    main()
