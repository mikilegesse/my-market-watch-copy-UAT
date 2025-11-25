#!/usr/bin/env python3
"""
🇪🇹 ETB Financial Terminal v31.0 (Hybrid Scraper)
- CORE: Replaced blocked APIs with Direct Browser-Mimicking Scrapers.
- FIX: "Min/Max" now filters out scams (prices < 85% of median) for accuracy.
- MEXC: Now uses the public web endpoint instead of the blocked API.
- BYBIT: Updated headers to prevent "No Data" errors.
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
HISTORY_FILE = "etb_history.csv"
SNAPSHOT_FILE = "market_state.json"
GRAPH_FILENAME = "etb_neon_terminal.png"
GRAPH_LIGHT_FILENAME = "etb_light_terminal.png"
HTML_FILENAME = "index.html"
BURST_WAIT_TIME = 30 

# Mimic Real Browser to bypass blocks
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Origin": "https://www.binance.com",
    "Referer": "https://www.binance.com/"
}

# --- 1. FETCHERS (DIRECT SCRAPERS) ---
def fetch_official_rate():
    try: return float(requests.get("https://open.er-api.com/v6/latest/USD", timeout=5).json()["rates"]["ETB"])
    except: return None

def fetch_usdt_peg():
    try: return float(requests.get("https://api.coingecko.com/api/v3/simple/price?ids=tether&vs_currencies=usd", timeout=5).json()["tether"]["usd"])
    except: return 1.00

def fetch_binance_direct(trade_type):
    """ Binance Web Scraper """
    url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
    ads = []
    page = 1
    payload = {
        "asset": "USDT", "fiat": "ETB", "merchantCheck": False, 
        "page": 1, "rows": 20, "tradeType": trade_type,
        "payTypes": [], "countries": [], "publisherType": None
    }
    while True:
        try:
            payload["page"] = page
            r = requests.post(url, headers=HEADERS, json=payload, timeout=5)
            data = r.json().get('data', [])
            if not data: break
            for d in data:
                adv = d.get('adv', {})
                ads.append({
                    'source': 'Binance',
                    'advertiser': d.get('advertiser', {}).get('nickName', 'Binance User'),
                    'price': float(adv.get('price')),
                    'available': float(adv.get('surplusAmount', 0)),
                    'min': float(adv.get('minSingleTransAmount', 0)),
                    'max': float(adv.get('maxSingleTransAmount', 0))
                })
            if page >= 5: break
            page += 1
            time.sleep(0.2)
        except: break
    return ads

def fetch_bybit_direct(side):
    """ Bybit Web Scraper """
    url = "https://api2.bybit.com/fiat/otc/item/online"
    ads = []
    page = 1
    h = HEADERS.copy()
    h.update({"Referer": "https://www.bybit.com/", "Origin": "https://www.bybit.com"})
    
    while True:
        try:
            payload = {"userId":"","tokenId":"USDT","currencyId":"ETB","payment":[],"side":side,"size":"20","page":str(page),"authMaker":False}
            r = requests.post(url, headers=h, json=payload, timeout=5)
            items = r.json().get("result", {}).get("items", [])
            if not items: break
            for i in items:
                ads.append({
                    'source': 'Bybit',
                    'advertiser': i.get('nickName', 'Bybit User'),
                    'price': float(i.get('price')),
                    'available': float(i.get('lastQuantity', 0)),
                    'min': float(i.get('minAmount', 0)),
                    'max': float(i.get('maxAmount', 0))
                })
            if page >= 5: break
            page += 1
            time.sleep(0.1)
        except: break
    return ads

def fetch_mexc_direct(side):
    """ MEXC Web Scraper (Replaces blocked API) """
    url = "https://p2p.mexc.com/api/v1/otc/online/list"
    ads = []
    page = 1
    h = HEADERS.copy()
    h.update({"Referer": "https://www.mexc.com/", "Origin": "https://www.mexc.com"})
    
    # Map side: 'SELL' means we buy from them (Advertiser Sells) -> "SELL" in MEXC payload
    mexc_side = "SELL" if side == "SELL" else "BUY"
    
    while True:
        try:
            payload = {
                "blockTrade": False, "currencyId": "ETB", "coinId": "USDT",
                "tradeType": mexc_side, "countryCode": None, 
                "payMethod": None, "page": page, "size": 20
            }
            r = requests.post(url, headers=h, json=payload, timeout=5)
            data = r.json()
            if not data.get("success"): break
            
            items = data.get("data", [])
            if not items: break
            
            for d in items:
                ads.append({
                    'source': 'MEXC',
                    'advertiser': d.get('merchantName', 'MEXC User'),
                    'price': float(d.get('price')),
                    'available': float(d.get('availableAmount', 0)),
                    'min': float(d.get('minTradeAmount', 0)),
                    'max': float(d.get('maxTradeAmount', 0))
                })
            if page >= 5: break
            page += 1
            time.sleep(0.1)
        except: break
    return ads

# --- 2. TAPE READER ---
def capture_market_snapshot():
    with ThreadPoolExecutor(max_workers=10) as ex:
        f_bin = ex.submit(lambda: fetch_binance_direct("SELL"))
        f_byb = ex.submit(lambda: fetch_bybit_direct("1")) # 1 = Sell to user
        f_mexc = ex.submit(lambda: fetch_mexc_direct("SELL"))
        return f_bin.result() + f_byb.result() + f_mexc.result()

def load_market_state():
    if os.path.exists(SNAPSHOT_FILE):
        try: with open(SNAPSHOT_FILE, 'r') as f: return json.load(f)
        except: return {}
    return {}

def save_market_state(current_ads):
    state = {}
    for ad in current_ads:
        key = f"{ad['source']}_{ad['advertiser']}_{ad['price']}"
        state[key] = ad['available']
    with open(SNAPSHOT_FILE, 'w') as f: json.dump(state, f)

def detect_real_trades(current_ads, peg):
    prev_state = load_market_state()
    trades = []
    for ad in current_ads:
        key = f"{ad['source']}_{ad['advertiser']}_{ad['price']}"
        if key in prev_state:
            prev = prev_state[key]; curr = ad['available']
            if curr < prev:
                diff = prev - curr
                if diff > 5:
                    trades.append({'type':'trade', 'source':ad['source'], 'user':ad['advertiser'], 'price':ad['price']/peg, 'vol_usd':diff})
    
    if not trades:
        # Fallback to best offers if no trades
        best = sorted(current_ads, key=lambda x: x['price'])[:20]
        for ad in best:
            trades.append({'type':'offer', 'source':ad['source'], 'user':ad['advertiser'], 'price':ad['price']/peg, 'vol_usd':ad['available']})
    return trades

# --- 3. ANALYTICS (Smart Min/Max) ---
def analyze(prices, peg):
    if not prices: return None
    # 1. Remove technical errors
    valid = sorted([p for p in prices if 10 < p < 500])
    if len(valid) < 2: return None
    
    adj = [p / peg for p in valid]
    n = len(adj)
    
    try:
        quantiles = statistics.quantiles(adj, n=100, method='inclusive')
        p05, p10, q1, median, q3, p95 = quantiles[4], quantiles[9], quantiles[24], quantiles[49], quantiles[74], quantiles[94]
    except:
        median = statistics.median(adj)
        p05, q1, q3, p95 = adj[0], adj[int(n*0.25)], adj[int(n*0.75)], adj[-1]

    # 2. Calculate "Smart Min/Max" (Filtered)
    # Exclude prices that are >15% cheaper than median (Scams)
    scam_floor = median * 0.85
    scam_ceiling = median * 1.30
    
    filtered_prices = [x for x in adj if x > scam_floor and x < scam_ceiling]
    
    if filtered_prices:
        smart_min = min(filtered_prices)
        smart_max = max(filtered_prices)
    else:
        smart_min = adj[0]
        smart_max = adj[-1]

    return {"median": median, "q1": q1, "q3": q3, "p05": p05, "p95": p95, "min": smart_min, "max": smart_max, "raw_data": adj, "count": n}

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
    
    themes = [
        ("dark", GRAPH_FILENAME, {"bg":"#050505","fg":"#00ff9d","grid":"#222","median":"#ff0055","sec":"#00bfff","fill":"#00ff9d","alpha":0.7}),
        ("light", GRAPH_LIGHT_FILENAME, {"bg":"#ffffff","fg":"#1a1a1a","grid":"#eee","median":"#d63384","sec":"#0d6efd","fill":"#00a876","alpha":0.5})
    ]
    dates, medians, q1s, q3s, offs = load_history()

    for mode, filename, style in themes:
        plt.rcParams.update({"figure.facecolor": style["bg"], "axes.facecolor": style["bg"], "axes.edgecolor": style["fg"], "axes.labelcolor": style["fg"], "xtick.color": style["fg"], "ytick.color": style["fg"], "text.color": style["fg"]})
        fig = plt.figure(figsize=(12, 14))
        fig.suptitle(f'ETB LIQUIDITY SCANNER: {datetime.datetime.now().strftime("%H:%M")}', fontsize=20, color=style["fg"], fontweight='bold', y=0.97)

        ax1 = fig.add_subplot(2, 1, 1)
        data = stats['raw_data']
        y_jitter = [1 + random.uniform(-0.12, 0.12) for _ in data]
        ax1.scatter(data, y_jitter, color=style["fg"], alpha=style["alpha"], s=30, edgecolors='none')
        ax1.axvline(stats['median'], color=style["median"], linewidth=3)
        ax1.axvline(stats['q1'], color=style["sec"], linewidth=2, linestyle='--', alpha=0.6)
        ax1.axvline(stats['q3'], color=style["sec"], linewidth=2, linestyle='--', alpha=0.6)
        
        ax1.text(stats['median'], 1.42, f"MEDIAN\n{stats['median']:.2f}", color=style["median"], ha='center', fontweight='bold')
        ax1.text(stats['q1'], 0.58, f"Q1\n{stats['q1']:.2f}", color=style["sec"], ha='right', va='top')
        ax1.text(stats['q3'], 0.58, f"Q3\n{stats['q3']:.2f}", color=style["sec"], ha='left', va='top')
        if official_rate: ax1.axvline(official_rate, color=style["fg"], linestyle=':', linewidth=1.5)
        
        margin = (stats['p95'] - stats['p05']) * 0.1
        if margin == 0: margin = 1
        ax1.set_xlim([stats['p05'] - margin, stats['p95'] + margin])
        ax1.set_ylim(0.5, 1.5); ax1.set_yticks([])
        ax1.set_title("Live Market Depth (Smart Zoom)", color=style["fg"], loc='left', pad=10)
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
        plt.savefig(filename, dpi=150, facecolor=style["bg"])
        plt.close()

# --- 6. WEB GENERATOR ---
def update_website_html(stats, official, timestamp, actions, grouped_ads, peg):
    prem = ((stats['median'] - official)/official)*100 if official else 0
    cache_buster = int(time.time())
    
    table_rows = ""
    for source, ads in grouped_ads.items():
        prices = [a['price'] for a in ads]
        s = analyze(prices, peg)
        if s:
            table_rows += f"<tr><td class='source-col'>{source}</td><td>{s['min']:.2f}</td><td>{s['q1']:.2f}</td><td class='med-col'>{s['median']:.2f}</td><td>{s['q3']:.2f}</td><td>{s['max']:.2f}</td><td>{s['count']}</td></tr>"
        else:
            table_rows += f"<tr><td>{source}</td><td colspan='6' style='opacity:0.5'>No Data</td></tr>"

    feed_html = ""
    now_str = datetime.datetime.now().strftime("%H:%M")
    actions.sort(key=lambda x: (x['type'] == 'offer', x.get('vol_usd', 0)), reverse=True)
    
    for item in actions[:25]: 
        if item['type'] == 'trade':
            icon, bg, text = "🛒", "#2ea043", f"<b style='color:#2ea043'>BOUGHT</b> <span class='feed-vol'>{item['vol_usd']:,.2f} USDT</span>"
        else:
            icon, bg, text = "🏷️", "#333", f"Listing <span class='feed-vol'>{item['vol_usd']:,.0f} USDT</span>"
        
        s_col = "#f3ba2f" if "Binance" in item['source'] else "#000" if "Bybit" in item['source'] else "#2e55e6"
        
        feed_html += f"""
        <div class="feed-item">
            <div class="feed-icon" style="background:{bg}">{icon}</div>
            <div class="feed-content">
                <span class="feed-ts">{now_str}</span> -> 
                <span class="feed-source" style="color:{s_col}">{item['source']}</span> 
                <span class="feed-user">{item['user'][:10]}</span> {text} @ 
                <span class="feed-price">{item['price']:.2f} ETB</span>
            </div>
        </div>"""

    html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <meta http-equiv="refresh" content="300">
        <title>ETB Market Watch</title>
        <style>
            :root {{ --bg: #050505; --card: #111; --text: #00ff9d; --sub: #ccc; --mute: #666; --accent: #ff0055; --link: #00bfff; --gold: #ffcc00; --border: #333; --hover: rgba(0,255,157,0.05); }}
            [data-theme="light"] {{ --bg: #f4f4f9; --card: #fff; --text: #1a1a1a; --sub: #333; --mute: #888; --accent: #d63384; --link: #0d6efd; --gold: #ffc107; --border: #ddd; --hover: rgba(0,0,0,0.05); }}
            
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
            
            .chart img {{ width: 100%; border-radius: 8px; display: block; border: 1px solid var(--border); transition: 0.3s; }}
            
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
                <div style="color:var(--mute); letter-spacing:4px; font-size:0.8rem;">/// LIVE P2P TAPE READER ///</div>
                <div class="toggle" onclick="toggleTheme()">🌓 Theme</div>
            </header>

            <div class="left-col">
                <div class="card ticker">
                    <div style="color:var(--mute); font-size:0.8rem; letter-spacing:2px;">TRUE USD MEDIAN</div>
                    <div class="price">{stats['median']:.2f} <span style="font-size:1.5rem;color:var(--mute)">ETB</span></div>
                    <span class="prem">Black Market Premium: +{prem:.2f}%</span>
                </div>
                <div class="card chart">
                    <img src="{GRAPH_FILENAME}?v={cache_buster}" id="chartImg" alt="Chart">
                </div>
                <div class="card">
                    <table><thead><tr><th>Source</th><th>Min</th><th>Q1</th><th>Med</th><th>Q3</th><th>Max</th><th>Ads</th></tr></thead><tbody>{table_rows}</tbody></table>
                </div>
            </div>

            <div class="right-col">
                <div class="card">
                    <div class="feed-title">📢 Real-Time Tape (Trades & Quotes)</div>
                    <div class="feed-container">{feed_html}</div>
                </div>
            </div>

            <footer>Official Bank Rate: {official:.2f} ETB | Last Update: {timestamp} UTC</footer>
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
    print("✅ Website generated.")

# --- 7. MAIN ---
def main():
    print("🔍 Running v31.0 Hybrid Scraper...", file=sys.stderr)
    
    # 1. SNAPSHOT 1
    print("   > Snapshot 1/2...", file=sys.stderr)
    with ThreadPoolExecutor(max_workers=10) as ex:
        f_bin = ex.submit(lambda: fetch_binance_direct("SELL"))
        f_byb = ex.submit(lambda: fetch_bybit_direct("1"))
        f_mexc = ex.submit(lambda: fetch_mexc_direct("SELL"))
        snap1 = f_bin.result() + f_byb.result() + f_mexc.result()

    # 2. WAIT
    print(f"   > Waiting {BURST_WAIT_TIME}s...", file=sys.stderr)
    time.sleep(BURST_WAIT_TIME)

    # 3. SNAPSHOT 2
    print("   > Snapshot 2/2...", file=sys.stderr)
    with ThreadPoolExecutor(max_workers=10) as ex:
        f_bin = ex.submit(lambda: fetch_binance_direct("SELL"))
        f_byb = ex.submit(lambda: fetch_bybit_direct("1"))
        f_mexc = ex.submit(lambda: fetch_mexc_direct("SELL"))
        
        f_off = ex.submit(fetch_official_rate)
        f_peg = ex.submit(fetch_usdt_peg)
        
        bin_ads = f_bin.result()
        mexc_ads = f_mexc.result()
        byb_ads = f_byb.result()
        official = f_off.result() or 0.0
        peg = f_peg.result() or 1.0

    snap2 = bin_ads + byb_ads + mexc_ads
    grouped_ads = {"Binance": bin_ads, "Bybit": byb_ads, "MEXC": mexc_ads}
    
    if snap2:
        real_actions = detect_real_trades(snap2, peg)
        save_market_state(snap2)
        
        # FIX: Pass list of prices
        all_prices = [x['price'] for x in snap2]
        stats = analyze(all_prices, peg)
        
        if stats:
            save_to_history(stats, official)
            generate_charts(stats, official)
            
        update_website_html(stats, official, time.strftime('%Y-%m-%d %H:%M:%S'), real_actions, grouped_ads, peg)
    else:
        print("⚠️ CRITICAL: No ads found.", file=sys.stderr)
        update_website_html({"median":0, "min":0, "q1":0, "q3":0, "max":0, "count":0, "raw_data":[]}, official, "ERROR", [], grouped_ads, peg)

    print("✅ Update Complete.")

if __name__ == "__main__":
    main()
