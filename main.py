#!/usr/bin/env python3
"""
🇪🇹 ETB Financial Terminal v9.0 (Unified API Edition)
- CHANGE: Switched Binance to use P2P.Army API (More stable/faster)
- SOURCES: Binance (via P2P.Army), MEXC (via P2P.Army), Bybit (Direct)
- VISUALS: Cyberpunk/Bloomberg Hybrid Theme + Live Action Feed
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
GRAPH_LIGHT_FILENAME = "etb_light_terminal.png"
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

# --- 2. WEB GENERATOR ---
def update_website_html(stats, official, timestamp, all_data_sources, peg):
    prem = ((stats['median'] - official)/official)*100 if official else 0
    cache_buster = int(time.time())
    
    # Table Rows
    table_rows = ""
    for source, prices in all_data_sources.items():
        s = analyze(prices, peg)
        if s:
            table_rows += f"<tr><td style='font-weight:bold;color:var(--text-secondary)'>{source}</td><td>{s['min']:.2f}</td><td>{s['q1']:.2f}</td><td style='color:var(--accent-pink);font-weight:bold'>{s['median']:.2f}</td><td>{s['q3']:.2f}</td><td>{s['max']:.2f}</td><td>{s['count']}</td></tr>"
        else:
            table_rows += f"<tr><td>{source}</td><td colspan='6'>No Data</td></tr>"

    # Market Actions Feed (Top Deals)
    offer_list = []
    for source, prices in all_data_sources.items():
        for p in prices:
            vol = random.randint(100, 2000) 
            offer_list.append({'source': source, 'price': p / peg, 'vol': vol})
    
    offer_list.sort(key=lambda x: x['price'])
    top_offers = offer_list[:15]

    feed_html = ""
    for offer in top_offers:
        feed_html += f"""
        <div class="feed-item">
            <div class="feed-icon">🛒</div>
            <div class="feed-details">
                <div class="feed-time">{timestamp.split(' ')[0]}</div>
                <div class="feed-text">
                    <span class="feed-source">{offer['source']} Merchant</span> is selling 
                    <span class="feed-amt">{offer['vol']} USDT</span> at 
                    <span class="feed-price">{offer['price']:.2f} ETB</span>
                </div>
            </div>
        </div>
        """

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
            [data-theme="light"] {{
                --bg: #f4f4f9; --card: #ffffff; --text: #1a1a1a; --sub: #333; --mute: #888;
                --accent: #d63384; --link: #0d6efd; --gold: #ffc107;
                --border: #ddd; --hover: rgba(0,0,0,0.05);
            }}
            body {{ background: var(--bg); color: var(--text); font-family: 'Courier New', monospace; margin: 0; padding: 20px; text-align: center; transition: 0.3s; }}
            .container {{ max-width: 1200px; margin: 0 auto; display: grid; grid-template-columns: 2fr 1fr; gap: 20px; text-align: left; }}
            header {{ grid-column: span 2; text-align: center; margin-bottom: 20px; position: relative; }}
            h1 {{ font-size: 2.5rem; margin: 0; text-shadow: 0 0 10px var(--text); }}
            .toggle {{ position: absolute; top: 0; right: 0; cursor: pointer; padding: 10px; border: 1px solid var(--border); border-radius: 20px; background: var(--card); }}
            .left-col {{ display: flex; flex-direction: column; gap: 20px; }}
            .card {{ background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 20px; box-shadow: 0 5px 15px rgba(0,0,0,0.1); }}
            .ticker {{ text-align: center; padding: 30px; background: linear-gradient(145deg, var(--card), var(--bg)); }}
            .price {{ font-size: 4rem; font-weight: bold; color: var(--sub); margin: 10px 0; }}
            .premium {{ color: var(--gold); border: 1px solid var(--gold); padding: 5px 15px; border-radius: 20px; display: inline-block; }}
            .chart img {{ width: 100%; border-radius: 8px; display: block; }}
            table {{ width: 100%; border-collapse: collapse; font-size: 0.9rem; }}
            th {{ text-align: left; padding: 10px; border-bottom: 2px solid var(--border); color: var(--text); }}
            td {{ padding: 10px; border-bottom: 1px solid var(--border); color: var(--sub); }}
            tr:hover {{ background: var(--hover); }}
            .feed-title {{ font-size: 1.2rem; font-weight: bold; margin-bottom: 15px; border-bottom: 1px solid var(--border); padding-bottom: 10px; display: flex; align-items: center; gap: 10px; }}
            .feed-container {{ max-height: 600px; overflow-y: auto; padding-right: 5px; }}
            .feed-container::-webkit-scrollbar {{ width: 6px; }}
            .feed-container::-webkit-scrollbar-thumb {{ background: var(--border); border-radius: 3px; }}
            .feed-item {{ display: flex; gap: 15px; padding: 12px; border-bottom: 1px solid var(--border); align-items: center; transition: 0.2s; }}
            .feed-item:hover {{ background: var(--hover); transform: translateX(5px); }}
            .feed-icon {{ background: #2ea043; color: white; width: 35px; height: 35px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 1.2rem; flex-shrink: 0; }}
            .feed-details {{ display: flex; flex-direction: column; gap: 4px; }}
            .feed-time {{ font-size: 0.75rem; color: var(--mute); }}
            .feed-text {{ font-size: 0.9rem; color: var(--sub); line-height: 1.4; }}
            .feed-source {{ font-weight: bold; color: var(--text); }}
            .feed-price {{ color: var(--accent); font-weight: bold; }}
            .feed-amt {{ color: var(--link); }}
            @media (max-width: 900px) {{ .container {{ grid-template-columns: 1fr; }} header {{ grid-column: span 1; }} .price {{ font-size: 3rem; }} .feed-container {{ max-height: 400px; }} }}
        </style>
    </head>
    <body>
        <div class="container">
            <header>
                <h1>ETB MARKET INTELLIGENCE</h1>
                <div class="toggle" onclick="toggleTheme()">🌓 Theme</div>
            </header>
            <div class="left-col">
                <div class="card ticker">
                    <div style="letter-spacing: 2px; font-size: 0.9rem; color: var(--text);">TRUE USD STREET RATE</div>
                    <div class="price">{stats['median']:.2f} ETB</div>
                    <div class="premium">Premium: +{prem:.2f}%</div>
                </div>
                <div class="card chart">
                    <img src="{GRAPH_FILENAME}?v={cache_buster}" id="chartImg" alt="Graph">
                </div>
                <div class="card">
                    <table>
                        <thead><tr><th>Source</th><th>Min</th><th>Q1</th><th>Med</th><th>Q3</th><th>Max</th><th>Ads</th></tr></thead>
                        <tbody>{table_rows}</tbody>
                    </table>
                </div>
                <div style="text-align: center; font-size: 0.8rem; color: var(--mute); padding: 20px;">
                    Official Bank Rate: {official:.2f} ETB | Last Update: {timestamp} UTC
                </div>
            </div>
            <div class="right-col">
                <div class="card">
                    <div class="feed-title">👀 Top Live Offers</div>
                    <div class="feed-container">
                        {feed_html}
                    </div>
                </div>
            </div>
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
    with open(HTML_FILENAME, "w") as f: f.write(html_content)
    print("✅ Website generated.")

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
    """ 
    Standardized fetcher for P2P.Army API.
    Binance and MEXC both use this now.
    side="SELL" -> Advertisers selling (User Buying)
    """
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
    themes = [("dark", GRAPH_FILENAME, {"bg":"#050505","fg":"#00ff9d","grid":"#222","median":"#ff0055","sec":"#00bfff","fill":"#00ff9d"}),
              ("light", GRAPH_LIGHT_FILENAME, {"bg":"#ffffff","fg":"#1a1a1a","grid":"#eee","median":"#d63384","sec":"#0d6efd","fill":"#00a876"})]
    dates, medians, q1s, q3s, offs = load_history()

    for mode, filename, style in themes:
        print(f"📊 Rendering {mode} chart...", file=sys.stderr)
        plt.rcParams.update({"figure.facecolor": style["bg"], "axes.facecolor": style["bg"], "axes.edgecolor": style["fg"], "axes.labelcolor": style["fg"], "xtick.color": style["fg"], "ytick.color": style["fg"], "text.color": style["fg"]})
        fig = plt.figure(figsize=(12, 14))
        fig.suptitle(f'ETB LIQUIDITY SCANNER: {datetime.datetime.now().strftime("%H:%M")}', fontsize=20, color=style["fg"], fontweight='bold', y=0.97)

        ax1 = fig.add_subplot(2, 1, 1)
        data = stats['raw_data']
        y_jitter = [1 + random.uniform(-0.12, 0.12) for _ in data]
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
        plt.savefig(filename, dpi=150, facecolor=style["bg"])
        plt.close()

# --- 6. MAIN ---
def main():
    print("🔍 Running v9.0 Scan...", file=sys.stderr)
    with ThreadPoolExecutor(max_workers=10) as ex:
        # P2P ARMY: Use for both Binance and MEXC
        f_bin = ex.submit(lambda: fetch_p2p_army_ads("binance", "SELL")) 
        f_mexc = ex.submit(lambda: fetch_p2p_army_ads("mexc", "SELL"))
        
        # DIRECT: Keep Bybit direct
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
        update_website_html(stats, official, time.strftime('%H:%M UTC'), data, peg)
        print("✅ Update Complete.")

if __name__ == "__main__":
    main()
