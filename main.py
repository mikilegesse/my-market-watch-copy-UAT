#!/usr/bin/env python3
"""
🇪🇹 ETB Financial Terminal v26.0 (Crash Proof)
- FIX: Resolved 'float object is not subscriptable' error in Analytics Engine.
- CORE: "Tape Reader" logic (Burst Scan) to detect real trades.
- VISUAL: Transaction Feed + Neon Charts.
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
    print("⚠️ Matplotlib not found.", file=sys.stderr)

# --- CONFIGURATION ---
P2P_ARMY_KEY = "YJU5RCZ2-P6VTVNNA"
HISTORY_FILE = "etb_history.csv"
STATE_FILE = "price_state.json"
GRAPH_FILENAME = "etb_neon_terminal.png"
HTML_FILENAME = "index.html"
BURST_WAIT_TIME = 45 

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Content-Type": "application/json"
}

# --- 1. ANALYTICS ENGINE (FIXED) ---
def analyze(prices, peg):
    """ 
    FIXED: Now accepts a simple list of floats (prices), 
    instead of trying to extract 'price' key from them.
    """
    if not prices: return None
    
    # Sanity Filter
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

# --- 2. FETCHERS ---
def fetch_official_rate():
    try: return float(requests.get("https://open.er-api.com/v6/latest/USD", timeout=5).json()["rates"]["ETB"])
    except: return None

def fetch_usdt_peg():
    try: return float(requests.get("https://api.coingecko.com/api/v3/simple/price?ids=tether&vs_currencies=usd", timeout=5).json()["tether"]["usd"])
    except: return 1.00

def fetch_binance_direct(trade_type):
    url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
    ads = []
    page = 1
    payload = {
        "asset": "USDT", "fiat": "ETB", "merchantCheck": False, 
        "page": 1, "rows": 20, "tradeType": trade_type,
        "payTypes": [], "countries": [], "publisherType": None, "clientType": "web"
    }
    while True:
        try:
            payload["page"] = page
            r = requests.post(url, headers=HEADERS, json=payload, timeout=10)
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
            if page >= 3: break
            page += 1
            time.sleep(0.2)
        except: break
    return ads

def fetch_bybit(side):
    url = "https://api2.bybit.com/fiat/otc/item/online"
    ads = []
    page = 1
    h = HEADERS.copy(); h["Referer"] = "https://www.bybit.com/"
    while True:
        try:
            r = requests.post(url, headers=h, json={"userId":"","tokenId":"USDT","currencyId":"ETB","payment":[],"side":side,"size":"20","page":str(page),"authMaker":False}, timeout=5)
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
            if page >= 3: break
            page += 1; time.sleep(0.1)
        except: break
    return ads

def fetch_mexc_api(side):
    url = "https://p2p.army/v1/api/get_p2p_order_book"
    ads = []
    h = HEADERS.copy(); h["X-APIKEY"] = P2P_ARMY_KEY
    try:
        r = requests.post(url, headers=h, json={"market":"mexc","fiat":"ETB","asset":"USDT","side":side,"limit":100}, timeout=10)
        data = r.json().get("result", {}).get("data", {}).get("ads", [])
        for d in data:
            ads.append({
                'source': 'MEXC',
                'advertiser': d.get('advertiser_name', 'MEXC User'),
                'price': float(d.get('price')),
                'available': float(d.get('available_amount', 0)),
                'min': float(d.get('min_amount', 0)),
                'max': float(d.get('max_amount', 0))
            })
    except: pass
    return ads

# --- 3. TAPE READER LOGIC ---
def capture_market_snapshot():
    with ThreadPoolExecutor(max_workers=10) as ex:
        f_bin = ex.submit(lambda: fetch_binance_direct("SELL"))
        f_byb = ex.submit(lambda: fetch_bybit("1"))
        f_mexc = ex.submit(lambda: fetch_mexc_api("SELL"))
        return f_bin.result() + f_byb.result() + f_mexc.result()

def detect_market_moves(old_snap, new_snap, peg):
    actions = []
    # Map: "User_Price" -> Available Amount
    old_map = {f"{x['advertiser']}_{x['price']}": x['available'] for x in old_snap}
    
    for new_ad in new_snap:
        key = f"{new_ad['advertiser']}_{new_ad['price']}"
        if key in old_map:
            old_amt = old_map[key]
            new_amt = new_ad['available']
            # Inventory drop = Sale
            if new_amt < old_amt:
                diff = old_amt - new_amt
                if diff > 5: 
                    actions.append({
                        'type': 'trade',
                        'source': new_ad['source'],
                        'user': new_ad['advertiser'],
                        'price': new_ad['price'],
                        'vol_usd': diff,
                        'vol_etb': diff * new_ad['price']
                    })
    
    if actions:
        actions.sort(key=lambda x: x['vol_usd'], reverse=True)
        return actions
    
    # Fallback: Top Offers if no trades
    best_offers = sorted(new_snap, key=lambda x: x['price'])[:15]
    for offer in best_offers:
        actions.append({
            'type': 'offer',
            'source': offer['source'],
            'user': offer['advertiser'],
            'price': offer['price'],
            'min': offer['min'],
            'max': offer['max']
        })
    return actions

# --- 4. WEB GENERATOR ---
def update_website_html(stats, official, timestamp, actions, grouped_ads, peg):
    prem = ((stats['median'] - official)/official)*100 if official else 0
    cache_buster = int(time.time())
    
    table_rows = ""
    for source, ads_list in grouped_ads.items():
        # FIX: Extract prices from objects before analyzing
        prices = [ad['price'] for ad in ads_list]
        s = analyze(prices, peg)
        if s:
            table_rows += f"<tr><td class='source-col'>{source}</td><td>{s['min']:.2f}</td><td>{s['q1']:.2f}</td><td class='med-col'>{s['median']:.2f}</td><td>{s['q3']:.2f}</td><td>{s['max']:.2f}</td><td>{s['count']}</td></tr>"
        else:
            table_rows += f"<tr><td>{source}</td><td colspan='6' style='opacity:0.5'>No Data</td></tr>"

    # Feed Generation
    feed_html = ""
    now_str = datetime.datetime.now().strftime("%H:%M")
    
    for item in actions[:15]: 
        if item['type'] == 'trade':
            icon, icon_bg = "🛒", "#2ea043"
            html_item = f"""
            <div class="feed-item">
                <div class="feed-icon" style="background:{icon_bg}">{icon}</div>
                <div class="feed-content">
                    <span class="feed-ts">{now_str}</span> -> 
                    <span class="feed-source" style="color:#fff">{item['source']}</span>: 
                    <span class="feed-user">{item['user'][:4]}***</span> 
                    <b style="color:#2ea043">BOUGHT</b> 
                    <span class="feed-vol">{item['vol_usd']:,.2f} USDT</span> 
                    @ <span class="feed-price">{item['price']:.2f} ETB</span>
                </div>
            </div>"""
        else:
            icon, icon_bg = "🏷️", "#333"
            s_col = "#f3ba2f" if item['source'] == "Binance" else "#00bfff"
            html_item = f"""
            <div class="feed-item">
                <div class="feed-icon" style="background:{icon_bg}">{icon}</div>
                <div class="feed-content">
                    <span class="feed-ts">{now_str}</span> -> 
                    <span class="feed-source" style="color:{s_col}">{item['source']}</span> Offer: 
                    <span class="feed-user">{item['user'][:10]}</span> listing @ 
                    <span class="feed-price">{item['price']:.2f} ETB</span>
                    <span style="color:#666; font-size:0.8em; display:block">Limit: {item['min']:,.0f} - {item['max']:,.0f} ETB</span>
                </div>
            </div>"""
        feed_html += html_item

    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate" />
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <meta http-equiv="refresh" content="300">
        <title>ETB Pro Terminal</title>
        <style>
            :root {{ --bg: #050505; --card: #111; --text: #00ff9d; --sub: #ccc; --mute: #666; --accent: #ff0055; --link: #00bfff; --gold: #ffcc00; --border: #333; }}
            body {{ background: var(--bg); color: var(--text); font-family: 'Courier New', monospace; margin: 0; padding: 20px; text-align: center; }}
            .container {{ max-width: 1200px; margin: 0 auto; display: grid; grid-template-columns: 2fr 1fr; gap: 20px; text-align: left; }}
            header {{ grid-column: span 2; text-align: center; margin-bottom: 20px; }}
            h1 {{ font-size: 2.5rem; margin: 0; text-shadow: 0 0 10px var(--text); }}
            .left-col, .right-col {{ display: flex; flex-direction: column; gap: 20px; }}
            .card {{ background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 20px; }}
            .ticker {{ text-align: center; padding: 30px; background: linear-gradient(180deg, #151515, #0a0a0a); border-top: 3px solid #ff0055; }}
            .price {{ font-size: 4rem; font-weight: bold; color: var(--sub); margin: 10px 0; }}
            .prem {{ color: var(--gold); font-size: 0.9rem; display: block; margin-top: 10px; }}
            .chart img {{ width: 100%; border-radius: 8px; display: block; border: 1px solid var(--border); }}
            table {{ width: 100%; border-collapse: collapse; font-size: 0.9rem; }}
            th {{ text-align: left; padding: 12px; border-bottom: 2px solid var(--border); color: var(--text); }}
            td {{ padding: 12px; border-bottom: 1px solid var(--border); color: var(--sub); }}
            .med-col {{ color: var(--sub); font-weight: bold; }}
            .feed-title {{ font-size: 1.1rem; font-weight: bold; margin-bottom: 15px; border-bottom: 1px solid var(--border); padding-bottom: 10px; color: var(--text); }}
            .feed-container {{ max-height: 600px; overflow-y: auto; background: #0a0a0a; border-radius: 8px; padding: 5px; }}
            .feed-container::-webkit-scrollbar {{ width: 6px; }}
            .feed-container::-webkit-scrollbar-thumb {{ background: var(--border); }}
            .feed-item {{ display: flex; gap: 12px; padding: 10px; border-bottom: 1px solid #222; align-items: center; }}
            .feed-icon {{ width: 30px; height: 30px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 1.2rem; flex-shrink: 0; }}
            .feed-content {{ font-size: 0.85rem; color: #ccc; }}
            .feed-ts {{ color: #00ff9d; font-weight: bold; font-family: monospace; }}
            .feed-user {{ font-weight: bold; color: #fff; }}
            .feed-vol {{ font-weight: bold; color: #fff; }}
            .feed-price {{ font-weight: bold; color: #fff; }}
            footer {{ grid-column: span 2; margin-top: 40px; text-align: center; color: var(--mute); font-size: 0.7rem; }}
            @media (max-width: 900px) {{ .container {{ grid-template-columns: 1fr; }} header, footer {{ grid-column: span 1; }} }}
        </style>
    </head>
    <body>
        <div class="container">
            <header>
                <h1>ETB MARKET INTELLIGENCE</h1>
                <div style="color:var(--mute); letter-spacing:4px; font-size:0.8rem;">/// LIVE P2P TAPE READER ///</div>
            </header>
            <div class="left-col">
                <div class="card ticker">
                    <div style="color:var(--mute); font-size:0.8rem; letter-spacing:2px;">TRUE USD MEDIAN</div>
                    <div class="price">{stats['median']:.2f} <span style="font-size:1.5rem;color:var(--mute)">ETB</span></div>
                    <span class="prem">Black Market Premium: +{prem:.2f}%</span>
                </div>
                <div class="card chart"><img src="{GRAPH_FILENAME}?v={cache_buster}" alt="Chart"></div>
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
    </body>
    </html>
    """
    with open(HTML_FILENAME, "w") as f: f.write(html_content)
    print(f"✅ Website generated.")

# --- 5. HISTORY & GRAPH ---
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
    print("🔍 Running v26.0 Crash Proof Scan...", file=sys.stderr)
    
    # 1. FETCH INITIAL STATE
    print("   > Snapshot 1/2 (Base)...", file=sys.stderr)
    snapshot_1 = capture_market_snapshot()
    
    # 2. WAIT FOR TRADES
    print(f"   > Waiting {BURST_WAIT_TIME}s for market moves...", file=sys.stderr)
    time.sleep(BURST_WAIT_TIME)
    
    # 3. FETCH FINAL STATE
    print("   > Snapshot 2/2 (Compare)...", file=sys.stderr)
    snapshot_2 = capture_market_snapshot()
    
    # 4. DETECT & GENERATE
    official = fetch_official_rate() or 0.0
    peg = fetch_usdt_peg() or 1.0
    
    # FIX: Pass the list of prices, not objects
    stats = analyze([x['price'] for x in snapshot_2], peg)
    
    actions = detect_market_moves(snapshot_1, snapshot_2, peg)
    
    if stats:
        save_to_history(stats, official)
        generate_charts(stats, official)
    else:
        stats = {"median":0, "q1":0, "q3":0, "min":0, "max":0, "count":0, "raw_data":[]}

    # Group ads for table
    grouped_ads = {"Binance":[], "Bybit":[], "MEXC":[]}
    for ad in snapshot_2:
        if ad['source'] in grouped_ads: grouped_ads[ad['source']].append(ad)

    update_website_html(stats, official, time.strftime('%Y-%m-%d %H:%M:%S'), actions, grouped_ads, peg)
    print("✅ Update Complete.")

if __name__ == "__main__":
    main()
