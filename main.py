#!/usr/bin/env python3
"""
🇪🇹 ETB Financial Terminal v7.1 (Final Fix)
- VISUALS: Two themes (Neon/Dark + Bloomberg/Light)
- FIXES: Smart Label Spacing (No Overlap)
- WEB: Light/Dark Toggle + Cache Busting
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
    print("⚠️ Matplotlib not found.", file=sys.stderr)

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
    
    mean_val = statistics.mean(adj)
    median_val = statistics.median(adj)
    try:
        quantiles = statistics.quantiles(adj, n=100, method='inclusive')
        p10, q1, q3, p90 = quantiles[9], quantiles[24], quantiles[74], quantiles[89]
    except:
        p10, q1, q3, p90 = adj[int(n*0.1)], adj[int(n*0.25)], adj[int(n*0.75)], adj[int(n*0.9)]

    return {
        "median": median_val, "mean": mean_val,
        "q1": q1, "q3": q3, "p10": p10, "p90": p90, "min": adj[0], "max": adj[-1],
        "raw_data": adj, "count": n
    }

# --- 2. WEB GENERATOR (With Cache Busting) ---
def update_website_html(stats, official, timestamp, all_data_sources, peg):
    prem = ((stats['median'] - official)/official)*100 if official else 0
    cache_buster = int(time.time()) # Forces browser to load new image
    
    table_rows = ""
    for source, prices in all_data_sources.items():
        s = analyze(prices, peg)
        if s:
            table_rows += f"""
            <tr>
                <td style="font-weight: bold;" class="source-name">{source}</td>
                <td>{s['min']:.2f}</td>
                <td>{s['q1']:.2f}</td>
                <td class="median-cell">{s['median']:.2f}</td>
                <td>{s['q3']:.2f}</td>
                <td>{s['max']:.2f}</td>
                <td>{s['count']}</td>
            </tr>"""
        else:
            table_rows += f"<tr><td>{source}</td><td colspan='6' class='no-data'>No Data</td></tr>"

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
                --bg-primary: #030303; --bg-secondary: #0a0a0a; --bg-card: #000;
                --text-primary: #00ff9d; --text-secondary: #ccc; --text-muted: #666;
                --accent-pink: #ff0055; --accent-blue: #00bfff; --accent-yellow: #ffcc00;
                --border-color: #333; --border-hover: #00ff9d; --table-hover: rgba(0, 255, 157, 0.05);
                --shadow-color: rgba(0, 255, 157, 0.05);
            }}
            [data-theme="light"] {{
                --bg-primary: #f0f2f5; --bg-secondary: #ffffff; --bg-card: #ffffff;
                --text-primary: #1a1a1a; --text-secondary: #333; --text-muted: #666;
                --accent-pink: #d63384; --accent-blue: #0d6efd; --accent-yellow: #ffc107;
                --border-color: #ddd; --border-hover: #0d6efd; --table-hover: rgba(13, 110, 253, 0.05);
                --shadow-color: rgba(0, 0, 0, 0.05);
            }}
            @keyframes fadeInUp {{ from {{ opacity: 0; transform: translateY(20px); }} to {{ opacity: 1; transform: translateY(0); }} }}
            @keyframes pulseGlow {{ 0%, 100% {{ filter: drop-shadow(0 0 5px currentColor); }} 50% {{ filter: drop-shadow(0 0 15px currentColor); }} }}
            
            body {{ background-color: var(--bg-primary); color: var(--text-primary); font-family: 'Courier New', monospace; text-align: center; padding: 20px; margin: 0; transition: background-color 0.3s; }}
            .container {{ max-width: 1100px; margin: 0 auto; animation: fadeInUp 0.6s ease-out; }}
            
            /* Header */
            h1 {{ font-size: 2.5rem; margin-bottom: 5px; letter-spacing: 2px; text-transform: uppercase; }}
            .subtext {{ color: var(--text-muted); font-size: 0.8rem; margin-bottom: 30px; letter-spacing: 4px; }}
            
            /* Toggle */
            .theme-toggle {{ position: absolute; top: 20px; right: 20px; background: var(--bg-secondary); border: 2px solid var(--border-color); border-radius: 50px; padding: 8px 15px; cursor: pointer; display: flex; align-items: center; gap: 8px; color: var(--text-secondary); font-size: 0.85rem; transition: 0.3s; }}
            .theme-toggle:hover {{ border-color: var(--border-hover); }}

            /* Ticker */
            .ticker-card {{ background: linear-gradient(145deg, var(--bg-secondary), var(--bg-card)); border: 1px solid var(--border-color); padding: 30px; border-radius: 15px; box-shadow: 0 5px 25px var(--shadow-color); margin-bottom: 40px; position: relative; overflow: hidden; }}
            .ticker-card::before {{ content: ''; position: absolute; top: 0; left: 0; width: 100%; height: 3px; background: linear-gradient(90deg, var(--text-primary), var(--accent-pink), var(--text-primary)); background-size: 200% auto; animation: slide 3s linear infinite; }}
            @keyframes slide {{ to {{ background-position: 200% center; }} }}
            
            .price {{ font-size: 4.5rem; font-weight: bold; color: var(--text-secondary); animation: pulseGlow 3s infinite; margin: 15px 0; }}
            .unit {{ font-size: 1.5rem; color: var(--text-muted); font-weight: normal; }}
            .label {{ color: var(--text-primary); font-size: 0.9rem; text-transform: uppercase; letter-spacing: 3px; font-weight: bold; }}
            .premium {{ background: rgba(255, 204, 0, 0.1); color: var(--accent-yellow); padding: 5px 15px; border-radius: 20px; font-size: 1rem; display: inline-block; border: 1px solid var(--accent-yellow); }}

            /* Graph */
            .chart-container {{ margin-bottom: 40px; }}
            .chart-wrapper {{ position: relative; width: 100%; border: 2px solid var(--border-color); border-radius: 15px; overflow: hidden; box-shadow: 0 5px 30px var(--shadow-color); transition: 0.3s; background: var(--bg-secondary); }}
            .chart-wrapper:hover {{ border-color: var(--border-hover); transform: translateY(-5px); }}
            .chart-wrapper img {{ width: 100%; height: auto; display: block; }}

            /* Table */
            .data-table {{ width: 100%; border-collapse: separate; border-spacing: 0; background: var(--bg-secondary); border-radius: 12px; overflow: hidden; border: 1px solid var(--border-color); }}
            .data-table th {{ background: var(--bg-card); color: var(--text-primary); padding: 15px; font-size: 0.85rem; text-transform: uppercase; border-bottom: 2px solid var(--border-color); }}
            .data-table td {{ padding: 15px; border-bottom: 1px solid var(--border-color); color: var(--text-secondary); font-size: 0.95rem; }}
            .data-table tr:hover td {{ background: var(--table-hover); color: var(--text-primary); }}
            .source-name {{ color: var(--text-secondary) !important; }}
            .median-cell {{ color: var(--accent-pink) !important; font-weight: bold; font-size: 1.1em; }}
            .no-data {{ color: var(--text-muted) !important; }}

            /* Footer */
            .bank-card {{ margin-top: 40px; padding-top: 20px; border-top: 1px solid var(--border-color); }}
            .bank-rate {{ color: var(--accent-blue); font-size: 1.8rem; font-weight: bold; margin-top: 10px; }}
            footer {{ margin-top: 50px; color: var(--text-muted); font-size: 0.75rem; letter-spacing: 1px; }}
            
            @media (max-width: 768px) {{ .price {{ font-size: 3rem; }} .theme-toggle {{ position: static; margin-bottom: 20px; justify-content: center; }} }}
        </style>
    </head>
    <body>
        <div class="container">
            <header>
                <div class="theme-toggle" onclick="toggleTheme()">
                    <span>🌓</span> <span>Switch Theme</span>
                </div>
                <h1>ETB MARKET INTELLIGENCE</h1>
                <div class="subtext">/// LIVE P2P LIQUIDITY SCANNER ///</div>
            </header>

            <div class="ticker-card">
                <div class="label">True USD Street Rate</div>
                <div class="price">{stats['median']:.2f} <span class="unit">ETB</span></div>
                <div class="premium">Black Market Premium: +{prem:.2f}%</div>
            </div>

            <div class="chart-container">
                <div class="chart-wrapper">
                    <img src="{GRAPH_FILENAME}?v={cache_buster}" alt="Market Analysis Chart" id="graphImage">
                </div>
            </div>

            <div class="table-container">
                <table class="data-table">
                    <thead>
                        <tr><th>Source</th><th>Min</th><th>Q1 (Low)</th><th>Median</th><th>Q3 (High)</th><th>Max</th><th>Ads</th></tr>
                    </thead>
                    <tbody>{table_rows}</tbody>
                </table>
            </div>

            <div class="bank-card">
                <div class="label">Official Bank Rate (Ref)</div>
                <div class="bank-rate">{official:.2f} ETB</div>
            </div>

            <footer>
                SYSTEM UPDATE: {timestamp} UTC | SOURCE PROTOCOLS: BINANCE, BYBIT, MEXC
            </footer>
        </div>

        <script>
            const imgDark = "{GRAPH_FILENAME}?v={cache_buster}";
            const imgLight = "{GRAPH_LIGHT_FILENAME}?v={cache_buster}";
            
            // Initialize Theme
            (function() {{
                const savedTheme = localStorage.getItem('theme') || 'dark';
                document.documentElement.setAttribute('data-theme', savedTheme);
                document.getElementById('graphImage').src = savedTheme === 'light' ? imgLight : imgDark;
            }})();

            function toggleTheme() {{
                const html = document.documentElement;
                const current = html.getAttribute('data-theme');
                const newTheme = current === 'light' ? 'dark' : 'light';
                
                html.setAttribute('data-theme', newTheme);
                localStorage.setItem('theme', newTheme);
                document.getElementById('graphImage').src = newTheme === 'light' ? imgLight : imgDark;
            }}
        </script>
    </body>
    </html>
    """
    
    with open(HTML_FILENAME, "w") as f:
        f.write(html_content)
    print(f"✅ Website ({HTML_FILENAME}) generated.")

# --- 3. FETCHERS ---
def fetch_official_rate():
    try: return float(requests.get("https://open.er-api.com/v6/latest/USD", timeout=5).json()["rates"]["ETB"])
    except: return None

def fetch_usdt_peg():
    try: return float(requests.get("https://api.coingecko.com/api/v3/simple/price?ids=tether&vs_currencies=usd", timeout=5).json()["tether"]["usd"])
    except: return 1.00

def fetch_binance(trade_type):
    url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
    prices, page = [], 1
    while True:
        try:
            r = requests.post(url, headers=HEADERS, json={"asset":"USDT","fiat":"ETB","merchantCheck":False,"page":page,"rows":20,"tradeType":trade_type}, timeout=5)
            ads = r.json().get('data', [])
            if not ads: break
            prices.extend([float(ad['adv']['price']) for ad in ads])
            if page >= 5: break
            page += 1; time.sleep(0.1)
        except: break
    return prices

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

def fetch_mexc(side):
    url = "https://p2p.army/v1/api/get_p2p_order_book"
    h = HEADERS.copy(); h["X-APIKEY"] = P2P_ARMY_KEY
    try:
        r = requests.post(url, headers=h, json={"market":"mexc","fiat":"ETB","asset":"USDT","side":side,"limit":100}, timeout=10)
        return [float(ad['price']) for ad in r.json().get("result", {}).get("data", {}).get("ads", [])]
    except: return []

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

# --- 5. DUAL-THEME GRAPH GENERATOR ---
def generate_charts(stats, official_rate):
    if not GRAPH_ENABLED: return
    
    # Define Styles for Dark and Light modes
    themes = [
        ("dark", GRAPH_FILENAME, {
            "bg": "#000000", "fg": "#00ff9d", "grid": "#222", 
            "median": "#ff0055", "secondary": "#00bfff", "fill": "#00ff9d", "dot_alpha": 0.6
        }),
        ("light", GRAPH_LIGHT_FILENAME, {
            "bg": "#ffffff", "fg": "#1a1a1a", "grid": "#e0e0e0", 
            "median": "#d63384", "secondary": "#0d6efd", "fill": "#00a876", "dot_alpha": 0.4
        })
    ]

    dates, medians, q1s, q3s, offs = load_history()

    for mode, filename, style in themes:
        print(f"📊 Rendering {mode} chart...", file=sys.stderr)
        plt.rcParams.update({"figure.facecolor": style["bg"], "axes.facecolor": style["bg"], "axes.edgecolor": style["fg"], "axes.labelcolor": style["fg"], "xtick.color": style["fg"], "ytick.color": style["fg"], "text.color": style["fg"]})
        
        fig = plt.figure(figsize=(12, 14))
        fig.suptitle(f'ETB LIQUIDITY SCANNER: {datetime.datetime.now().strftime("%H:%M")}', fontsize=20, color=style["fg"], fontweight='bold', y=0.97)

        # TOP: DOT PLOT
        ax1 = fig.add_subplot(2, 1, 1)
        data = stats['raw_data']
        y_jitter = [1 + random.uniform(-0.12, 0.12) for _ in data]
        ax1.scatter(data, y_jitter, color=style["fg"], alpha=style["dot_alpha"], s=30, edgecolors='none')
        
        ax1.axvline(stats['median'], color=style["median"], linewidth=3, label='Median')
        ax1.axvline(stats['q1'], color=style["secondary"], linewidth=2, linestyle='--', alpha=0.6)
        ax1.axvline(stats['q3'], color=style["secondary"], linewidth=2, linestyle='--', alpha=0.6)

        # SMART LABELS (Prevent Overlap)
        # Median at Top Center
        ax1.text(stats['median'], 1.4, f"MEDIAN\n{stats['median']:.2f}", color=style["median"], ha='center', fontweight='bold', fontsize=12)
        # Q1 at Bottom Left
        ax1.text(stats['q1'], 0.6, f"Q1 (Low)\n{stats['q1']:.2f}", color=style["secondary"], ha='right', va='top', fontsize=10)
        # Q3 at Bottom Right
        ax1.text(stats['q3'], 0.6, f"Q3 (High)\n{stats['q3']:.2f}", color=style["secondary"], ha='left', va='top', fontsize=10)

        if official_rate:
            ax1.axvline(official_rate, color=style["fg"], linestyle=':', linewidth=1.5)
            ax1.text(official_rate, 0.65, f"Bank\n{official_rate:.0f}", color=style["fg"], ha='center', fontsize=9)

        margin = (stats['p90'] - stats['p10']) * 0.25
        ax1.set_xlim([min(official_rate or 999, stats['p10']) - margin, stats['p90'] + margin])
        ax1.set_ylim(0.5, 1.5)
        ax1.set_title("Live Market Depth", color=style["fg"], loc='left', pad=10)
        ax1.set_yticks([])
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
            ax2.grid(True, color=style["grid"], linewidth=0.5, linestyle='-')
            ax2.set_title("Historical Trend (24h)", color=style["fg"], loc='left')
        else:
            ax2.text(0.5, 0.5, "Building History...", ha='center', color='gray')

        plt.tight_layout(rect=[0, 0, 1, 0.95])
        plt.savefig(filename, dpi=150, facecolor=style["bg"])
        plt.close()

# --- 6. MAIN EXECUTION ---
def main():
    print("🔍 Initializing ETB Pro Terminal...", file=sys.stderr)
    with ThreadPoolExecutor(max_workers=10) as ex:
        f_bin = ex.submit(lambda: fetch_binance("BUY") + fetch_binance("SELL"))
        f_byb = ex.submit(lambda: fetch_bybit("1") + fetch_bybit("0"))
        f_mexc = ex.submit(lambda: fetch_p2p_army_ads("mexc", "SELL"))
        f_off = ex.submit(fetch_official_rate)
        f_peg = ex.submit(fetch_usdt_peg)
        data = {"Binance": f_bin.result(), "Bybit": f_byb.result(), "MEXC": f_mexc.result()}
        official = f_off.result()
        peg = f_peg.result()

    visual_data = data["Binance"] + data["MEXC"]
    stats = analyze(visual_data, peg)
    
    if stats:
        save_to_history(stats, official)
        generate_charts(stats, official) # Generates BOTH images
        update_website_html(stats, official, time.strftime('%Y-%m-%d %H:%M:%S'), data, peg)
        print("✅ Update Complete.")

if __name__ == "__main__":
    main()
