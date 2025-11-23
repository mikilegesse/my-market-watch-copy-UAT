#!/usr/bin/env python3
"""
🇪🇹 ETB Financial Terminal v5.0 (Pro Edition)
- WEB: Now includes a detailed Data Table & Formatted Numbers
- VISUALS: Neon Dot Plot + History Chart
- DATA: Auto-logs to etb_history.csv
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

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Content-Type": "application/json"
}

# --- 1. ANALYTICS ENGINE (Moved up for use in HTML gen) ---
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

# --- 2. WEB GENERATOR (Table + Cards) ---
def update_website_html(stats, official, timestamp, all_data_sources, peg):
    """ Generates Cyberpunk HTML with Data Table """
    prem = ((stats['median'] - official)/official)*100 if official else 0
    
    # Generate Table Rows
    table_rows = ""
    for source, prices in all_data_sources.items():
        s = analyze(prices, peg)
        if s:
            table_rows += f"""
            <tr>
                <td style="color: #fff; font-weight: bold;">{source}</td>
                <td>{s['min']:.2f}</td>
                <td>{s['q1']:.2f}</td>
                <td style="color: #ff0055; font-weight: bold;">{s['median']:.2f}</td>
                <td>{s['q3']:.2f}</td>
                <td>{s['max']:.2f}</td>
                <td>{s['count']}</td>
            </tr>
            """
        else:
            table_rows += f"<tr><td>{source}</td><td colspan='6'>No Data</td></tr>"

    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <meta http-equiv="refresh" content="300">
        <title>ETB Pro Terminal</title>
        <style>
            body {{ background-color: #050505; color: #00ff9d; font-family: 'Courier New', monospace; text-align: center; padding: 20px; margin: 0; }}
            .container {{ max-width: 1100px; margin: 0 auto; }}
            
            /* Header */
            h1 {{ text-shadow: 0 0 15px #00ff9d; font-size: 2.2rem; margin-bottom: 5px; letter-spacing: 2px; }}
            .subtext {{ color: #666; font-size: 0.8rem; margin-bottom: 30px; }}

            /* Big Ticker Card */
            .ticker-card {{ 
                background: linear-gradient(145deg, #111, #0a0a0a); 
                border: 1px solid #333; 
                padding: 20px; 
                border-radius: 12px; 
                box-shadow: 0 0 25px rgba(0, 255, 157, 0.05);
                margin-bottom: 30px;
                position: relative;
                overflow: hidden;
            }}
            .ticker-card::before {{ content: ''; position: absolute; top: 0; left: 0; width: 100%; height: 4px; background: #ff0055; }}
            
            .price {{ font-size: 4rem; font-weight: bold; color: #fff; text-shadow: 0 0 20px rgba(255, 0, 85, 0.5); margin: 10px 0; }}
            .unit {{ font-size: 1.5rem; color: #666; }}
            .label {{ color: #888; font-size: 0.9rem; text-transform: uppercase; letter-spacing: 3px; }}
            .premium {{ background: #222; color: #ffcc00; padding: 5px 15px; border-radius: 20px; font-size: 1rem; display: inline-block; border: 1px solid #444; }}

            /* Graph */
            .chart-container img {{ width: 100%; border: 1px solid #333; border-radius: 12px; opacity: 0.9; transition: opacity 0.3s; }}
            .chart-container img:hover {{ opacity: 1; }}

            /* Data Table */
            .data-table {{ width: 100%; margin-top: 30px; border-collapse: collapse; background: #111; border-radius: 8px; overflow: hidden; }}
            .data-table th {{ background: #1a1a1a; color: #888; padding: 12px; font-size: 0.8rem; text-transform: uppercase; border-bottom: 2px solid #333; }}
            .data-table td {{ padding: 12px; border-bottom: 1px solid #222; color: #ccc; font-size: 0.9rem; }}
            .data-table tr:hover {{ background: #161616; }}
            
            /* Bank Rate Footer */
            .bank-card {{ margin-top: 30px; border-top: 1px solid #333; padding-top: 20px; }}
            .bank-rate {{ color: #00bfff; font-size: 1.5rem; font-weight: bold; }}
            
            footer {{ margin-top: 40px; color: #444; font-size: 0.7rem; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>ETB MARKET INTELLIGENCE</h1>
            <div class="subtext">LIVE P2P LIQUIDITY SCANNER</div>

            <div class="ticker-card">
                <div class="label">True USD Street Rate</div>
                <div class="price">{stats['median']:.2f} <span class="unit">ETB</span></div>
                <div class="premium">Black Market Premium: +{prem:.2f}%</div>
            </div>

            <div class="chart-container">
                <img src="{GRAPH_FILENAME}" alt="Market Analysis Chart">
            </div>

            <table class="data-table">
                <thead>
                    <tr>
                        <th>Source</th>
                        <th>Min</th>
                        <th>Q1 (Low)</th>
                        <th>Median</th>
                        <th>Q3 (High)</th>
                        <th>Max</th>
                        <th>Ads</th>
                    </tr>
                </thead>
                <tbody>
                    {table_rows}
                </tbody>
            </table>

            <div class="bank-card">
                <div class="label">Official Bank Rate</div>
                <div class="bank-rate">{official:.2f} ETB</div>
            </div>

            <footer>
                LAST UPDATED: {timestamp} UTC | SOURCE: Binance, Bybit, MEXC
            </footer>
        </div>
    </body>
    </html>
    """
    
    with open(HTML_FILENAME, "w") as f:
        f.write(html_content)
    print(f"✅ Website ({HTML_FILENAME}) generated locally.")

# --- 3. FETCHERS ---
def fetch_official_rate():
    try:
        r = requests.get("https://open.er-api.com/v6/latest/USD", timeout=5)
        return float(r.json().get("rates", {}).get("ETB"))
    except: return None

def fetch_usdt_peg():
    try:
        url = "https://api.coingecko.com/api/v3/simple/price?ids=tether&vs_currencies=usd"
        r = requests.get(url, timeout=5)
        return float(r.json().get("tether", {}).get("usd", 1.0))
    except: return 1.00

def fetch_binance(trade_type):
    url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
    prices = []
    page = 1
    while True:
        try:
            payload = {"asset": "USDT", "fiat": "ETB", "merchantCheck": False, "page": page, "rows": 20, "tradeType": trade_type}
            r = requests.post(url, headers=HEADERS, json=payload, timeout=5)
            ads = r.json().get('data', [])
            if not ads: break
            prices.extend([float(ad['adv']['price']) for ad in ads if ad.get('adv', {}).get('price')])
            if page >= 5: break
            page += 1
            time.sleep(0.1)
        except: break
    return prices

def fetch_bybit(side_id):
    url = "https://api2.bybit.com/fiat/otc/item/online"
    prices = []
    page = 1
    h = HEADERS.copy(); h["Referer"] = "https://www.bybit.com/"
    while True:
        try:
            payload = {"userId": "", "tokenId": "USDT", "currencyId": "ETB", "payment": [], "side": side_id, "size": "20", "page": str(page), "authMaker": False}
            r = requests.post(url, headers=h, json=payload, timeout=5)
            items = r.json().get("result", {}).get("items", [])
            if not items: break
            prices.extend([float(item.get('price')) for item in items if item.get('price')])
            if page >= 5: break
            page += 1
            time.sleep(0.1)
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
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(HISTORY_FILE, 'a', newline='') as f:
        writer = csv.writer(f)
        if not file_exists: writer.writerow(["Timestamp", "Median", "Q1", "Q3", "Official"])
        writer.writerow([now, round(stats['median'], 2), round(stats['q1'], 2), round(stats['q3'], 2), round(official, 2) if official else 0])

def load_history():
    if not os.path.isfile(HISTORY_FILE): return [], [], [], [], []
    d, m, q1, q3, off = [], [], [], [], []
    with open(HISTORY_FILE, 'r') as f:
        reader = csv.reader(f)
        next(reader, None)
        for row in reader:
            try:
                d.append(datetime.datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S"))
                m.append(float(row[1])); q1.append(float(row[2])); q3.append(float(row[3])); off.append(float(row[4]))
            except: pass
    return d, m, q1, q3, off

# --- 5. VISUALIZATION ---
def generate_dashboard(stats, official_rate):
    if not GRAPH_ENABLED: return
    print(f"📊 Rendering Dashboard...", file=sys.stderr)
    
    plt.style.use('dark_background')
    fig = plt.figure(figsize=(12, 14))
    fig.suptitle(f'ETB LIQUIDITY SCANNER: {datetime.datetime.now().strftime("%H:%M")}', fontsize=20, color='#00ff9d', fontweight='bold', y=0.97)

    # --- TOP: JITTERED DOT PLOT ---
    ax1 = fig.add_subplot(2, 1, 1)
    data = stats['raw_data']
    y_jitter = [1 + random.uniform(-0.15, 0.15) for _ in data]
    ax1.scatter(data, y_jitter, color='#00ff9d', alpha=0.6, s=25, label='Ad Price')
    
    # Laser Lines
    ax1.axvline(stats['median'], color='#ff0055', linewidth=3, linestyle='-', alpha=0.9)
    ax1.axvline(stats['q1'], color='#00bfff', linewidth=1.5, linestyle='--', alpha=0.7)
    ax1.axvline(stats['q3'], color='#00bfff', linewidth=1.5, linestyle='--', alpha=0.7)
    
    # Annotations
    ax1.text(stats['median'], 1.35, f"MEDIAN\n{stats['median']:.2f}", color='#ff0055', ha='center', fontweight='bold', fontsize=12)
    ax1.text(stats['q1'], 0.6, f"Q1\n{stats['q1']:.2f}", color='#00bfff', ha='center', fontsize=9)
    ax1.text(stats['q3'], 0.6, f"Q3\n{stats['q3']:.2f}", color='#00bfff', ha='center', fontsize=9)

    if official_rate:
        ax1.axvline(official_rate, color='white', linestyle=':', linewidth=1)
        ax1.text(official_rate, 0.6, f"Bank\n{official_rate:.0f}", color='white', ha='center', fontsize=9)

    margin = (stats['p90'] - stats['p10']) * 0.25
    ax1.set_xlim([min(official_rate or 999, stats['p10']) - margin, stats['p90'] + margin])
    ax1.set_ylim(0.5, 1.5)
    ax1.set_title("Live Market Depth (Binance + MEXC)", color='white', loc='left', pad=15)
    ax1.set_yticks([]); ax1.set_xlabel("Price (ETB / True USD)")
    ax1.grid(True, axis='x', linestyle='--', alpha=0.15)

    # --- BOTTOM: HISTORY LINE CHART ---
    ax2 = fig.add_subplot(2, 1, 2)
    dates, medians, q1s, q3s, offs = load_history()
    
    if len(dates) > 1:
        ax2.fill_between(dates, q1s, q3s, color='#00ff9d', alpha=0.2, linewidth=0)
        ax2.plot(dates, medians, color='#ff0055', linewidth=2, label='Median Rate')
        if any(offs): ax2.plot(dates, offs, color='white', linestyle='--', linewidth=1, alpha=0.5, label='Official')
            
        ax2.set_title("Historical Trend", color='white', loc='left', pad=15)
        ax2.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        plt.setp(ax2.xaxis.get_majorticklabels(), rotation=0, ha='center', color='#888')
        ax2.yaxis.set_major_formatter(ticker.FormatStrFormatter('%.1f'))
        ax2.yaxis.tick_right()
        plt.setp(ax2.yaxis.get_majorticklabels(), color='#888')
        ax2.grid(True, which='major', axis='both', linestyle='-', color='#222', linewidth=1)
        ax2.set_facecolor('#0d0d0d')
    else:
        ax2.text(0.5, 0.5, "Building Time Series...\nRun again later to see line chart.", ha='center', va='center', color='gray')

    plt.tight_layout(rect=[0, 0, 1, 0.95])
    plt.savefig(GRAPH_FILENAME, dpi=150, facecolor='black')
    print(f"✅ Graph Saved: {GRAPH_FILENAME}", file=sys.stderr)

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

    # Aggregate Data (Binance + MEXC for Visuals)
    visual_prices = data["Binance"] + data["MEXC"]
    visual_stats = analyze(visual_prices, peg)
    
    # Save History & Generate Visuals
    if visual_stats: 
        save_to_history(visual_stats, official)
        generate_dashboard(visual_stats, official)
        # Pass ALL data to the web generator to build the table
        update_website_html(visual_stats, official, time.strftime('%Y-%m-%d %H:%M:%S'), data, peg)

    # Console Output (Optional, since we have the web now)
    print("\n" + "="*80)
    print(f"✅ Auto-Update Complete: {time.strftime('%H:%M:%S')}")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()
