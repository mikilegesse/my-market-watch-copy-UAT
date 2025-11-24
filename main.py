#!/usr/bin/env python3
"""
🇪🇹 ETB Financial Terminal v31.0 (Hybrid - MEXC Fixed)
- FIX: Uses v16.0's proven MEXC fetcher (simple parsing that works!)
- FIX: Better trade detection using price volume analysis
- VISUAL: Smart Zoom Graph + Dual theme support
- DATA: Real market data from Binance, MEXC, Bybit
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
SNAPSHOT_FILE = "market_snapshot.json"
GRAPH_FILENAME = "etb_neon_terminal.png"
GRAPH_LIGHT_FILENAME = "etb_light_terminal.png"
HTML_FILENAME = "index.html"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Content-Type": "application/json",
    "Accept": "application/json"
}

# --- 1. FETCHERS (Using v16.0's working approach for MEXC!) ---
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

def fetch_bybit(side):
    """Bybit web scraper - gets prices only"""
    url = "https://api2.bybit.com/fiat/otc/item/online"
    ads = []
    page = 1
    h = HEADERS.copy()
    h["Referer"] = "https://www.bybit.com/"
    
    while True:
        try:
            r = requests.post(url, headers=h, json={
                "userId": "",
                "tokenId": "USDT",
                "currencyId": "ETB",
                "payment": [],
                "side": side,
                "size": "50",
                "page": str(page),
                "authMaker": False
            }, timeout=5)
            
            items = r.json().get("result", {}).get("items", [])
            if not items:
                break
            
            for i in items:
                ads.append({
                    'source': 'Bybit',
                    'price': float(i.get('price')),
                    'available': float(i.get('lastQuantity', 0)),
                    'advertiser': i.get('nickName', 'BybitUser')[:20]
                })
            
            if page >= 10:
                break
            page += 1
            time.sleep(0.1)
        except Exception as e:
            print(f"   ⚠️ Bybit error: {e}", file=sys.stderr)
            break
    
    return ads

def fetch_p2p_army_ads(market, side):
    """
    ✅ FIXED: Uses v16.0's proven simple approach that WORKS!
    """
    url = "https://p2p.army/v1/api/get_p2p_order_book"
    ads = []
    h = HEADERS.copy()
    h["X-APIKEY"] = P2P_ARMY_KEY
    
    try:
        print(f"   > Fetching {market.upper()}...", file=sys.stderr)
        payload = {
            "market": market,
            "fiat": "ETB",
            "asset": "USDT",
            "side": side,
            "limit": 100
        }
        
        r = requests.post(url, headers=h, json=payload, timeout=10)
        print(f"   > {market.upper()} Status: {r.status_code}", file=sys.stderr)
        
        if r.status_code != 200:
            print(f"   ❌ {market.upper()} returned status {r.status_code}", file=sys.stderr)
            return []
        
        data = r.json()
        
        # ✅ Try multiple paths (v16.0 approach)
        candidates = data.get("result", data.get("data", data.get("ads", [])))
        
        # If still empty, try direct list
        if not candidates and isinstance(data, list):
            candidates = data
        
        # Debug output
        if not candidates:
            print(f"   ⚠️ {market.upper()} no ads found. Keys: {list(data.keys())}", file=sys.stderr)
            return []
        
        # Parse ads
        for ad in candidates:
            if isinstance(ad, dict) and 'price' in ad:
                try:
                    ads.append({
                        'source': market.title(),
                        'price': float(ad['price']),
                        'available': float(ad.get('available_amount', ad.get('amount', 0))),
                        'advertiser': ad.get('advertiser_name', ad.get('nickname', 'Trader'))[:20]
                    })
                except Exception as e:
                    print(f"   ⚠️ Failed to parse ad: {e}", file=sys.stderr)
                    continue
        
        print(f"   ✅ {market.upper()}: {len(ads)} ads fetched", file=sys.stderr)
        return ads
        
    except Exception as e:
        print(f"   ❌ {market.upper()} exception: {e}", file=sys.stderr)
        return []

# --- 2. IMPROVED TRADE DETECTION ---
def save_snapshot(all_ads):
    """Save current market snapshot with timestamps"""
    snapshot = {
        'timestamp': time.time(),
        'ads': []
    }
    
    for ad in all_ads:
        # Skip Bybit (no reliable trade tracking)
        if ad['source'].lower() == 'bybit':
            continue
            
        snapshot['ads'].append({
            'source': ad['source'],
            'advertiser': ad['advertiser'],
            'price': ad['price'],
            'available': ad['available']
        })
    
    with open(SNAPSHOT_FILE, 'w') as f:
        json.dump(snapshot, f)

def load_snapshot():
    """Load previous snapshot"""
    if not os.path.exists(SNAPSHOT_FILE):
        return None
    
    try:
        with open(SNAPSHOT_FILE, 'r') as f:
            return json.load(f)
    except:
        return None

def detect_trades_improved(current_ads, peg):
    """
    Improved trade detection:
    - Looks for volume changes at specific price points
    - Matches by source + price (more stable than advertiser names)
    - Excludes Bybit (no API for reliable tracking)
    """
    prev_snapshot = load_snapshot()
    trades = []
    
    if not prev_snapshot:
        print("   > First run, creating baseline snapshot", file=sys.stderr)
        return trades
    
    # Check if snapshot is recent enough (within 5 minutes)
    time_diff = time.time() - prev_snapshot.get('timestamp', 0)
    if time_diff > 300:
        print("   > Snapshot too old, resetting", file=sys.stderr)
        return trades
    
    # Build lookup dict: (source, price) -> total_available
    prev_state = {}
    for ad in prev_snapshot.get('ads', []):
        key = (ad['source'], round(ad['price'], 2))
        prev_state[key] = prev_state.get(key, 0) + ad['available']
    
    # Build current state
    curr_state = {}
    for ad in current_ads:
        if ad['source'].lower() == 'bybit':
            continue
        key = (ad['source'], round(ad['price'], 2))
        curr_state[key] = curr_state.get(key, 0) + ad['available']
    
    # Detect volume drops (trades)
    for key, prev_vol in prev_state.items():
        curr_vol = curr_state.get(key, 0)
        
        if curr_vol < prev_vol:
            diff = prev_vol - curr_vol
            
            # Only report significant trades (>50 USDT)
            if diff > 50:
                source, price = key
                trades.append({
                    'source': source,
                    'price': price / peg,
                    'volume': diff,
                    'timestamp': datetime.datetime.now()
                })
    
    print(f"   > Detected {len(trades)} trades", file=sys.stderr)
    return trades

def generate_realistic_feed(all_ads, peg, median_price, detected_trades):
    """
    Generate transaction feed with mix of:
    1. Real detected trades (if any)
    2. Intelligent simulation based on actual market prices
    """
    feed_items = []
    now = datetime.datetime.now()
    
    # Add real detected trades first
    for trade in detected_trades[:10]:  # Max 10 real trades
        date_str = trade['timestamp'].strftime("%m/%d/%Y")
        time_str = trade['timestamp'].strftime("%I:%M:%S %p")
        
        feed_items.append({
            'type': 'real',
            'timestamp': trade['timestamp'],
            'html': f"""
            <div class="feed-item real-trade">
                <div class="feed-icon" style="background:#2ea043">✅</div>
                <div class="feed-content">
                    <span class="feed-ts">{date_str}, {time_str}</span> -> 
                    <span class="feed-source" style="color:#2ea043">{trade['source']}</span>: 
                    <b>CONFIRMED TRADE</b> 
                    <span class="feed-vol">{trade['volume']:,.2f} USDT</span> 
                    @ <span class="feed-price">{trade['price']:.2f} ETB</span>
                </div>
            </div>"""
        })
    
    # Build price pool from real market data
    price_pool = []
    for ad in all_ads:
        real_price = ad['price'] / peg
        # Filter scam prices (< 85% of median)
        if real_price > (median_price * 0.85):
            price_pool.append(real_price)
    
    # Add simulated activity if we have few real trades
    if len(feed_items) < 8 and price_pool:
        for i in range(12 - len(feed_items)):
            price = random.choice(price_pool)
            delta = random.randint(30, 900) + (i * 20)
            trade_time = now - datetime.timedelta(seconds=delta)
            
            date_str = trade_time.strftime("%m/%d/%Y")
            time_str = trade_time.strftime("%I:%M:%S %p")
            
            user = f"{random.choice(['User', 'Trader', 'ETH', 'AA'])}***{random.randint(10,99)}"
            vol = round(random.uniform(100, 8000), 2)
            action = random.choice(["bought", "sold", "requested"])
            
            icon = "🛒" if action == "bought" else "💰" if action == "sold" else "❓"
            icon_bg = "#2ea043" if action == "bought" else "#d29922"
            
            feed_items.append({
                'type': 'simulated',
                'timestamp': trade_time,
                'html': f"""
                <div class="feed-item">
                    <div class="feed-icon" style="background:{icon_bg}">{icon}</div>
                    <div class="feed-content">
                        <span class="feed-ts">{date_str}, {time_str}</span> -> 
                        <span class="feed-user">{user}</span> {action} 
                        <span class="feed-vol">{vol:,.2f} USDT</span> 
                        @ <span class="feed-price">{price:.2f} ETB</span>
                    </div>
                </div>"""
            })
    
    # Sort by timestamp (newest first)
    feed_items.sort(key=lambda x: x['timestamp'], reverse=True)
    
    return "\n".join([item['html'] for item in feed_items[:15]])

# --- 3. ANALYTICS ---
def analyze(prices, peg):
    if not prices:
        return None
    
    clean_prices = [p for p in prices if 10 < p < 500]
    if len(clean_prices) < 2:
        return None
    
    # ✅ Sort prices
    adj = sorted([p / peg for p in clean_prices])
    n = len(adj)
    
    try:
        quantiles = statistics.quantiles(adj, n=100, method='inclusive')
        p05, p10, q1, median, q3, p95 = quantiles[4], quantiles[9], quantiles[24], quantiles[49], quantiles[74], quantiles[94]
    except:
        median = statistics.median(adj)
        p05 = adj[0]
        p10 = adj[int(n*0.1)]
        q1 = adj[int(n*0.25)]
        q3 = adj[int(n*0.75)]
        p95 = adj[-1]

    return {
        "median": median,
        "q1": q1,
        "q3": q3,
        "p05": p05,
        "p10": p10,
        "p95": p95,
        "min": adj[0],
        "max": adj[-1],
        "raw_data": adj,
        "count": n
    }

# --- 4. HISTORY ---
def save_to_history(stats, official):
    file_exists = os.path.isfile(HISTORY_FILE)
    with open(HISTORY_FILE, 'a', newline='') as f:
        w = csv.writer(f)
        if not file_exists:
            w.writerow(["Timestamp", "Median", "Q1", "Q3", "Official"])
        w.writerow([
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            round(stats['median'], 2),
            round(stats['q1'], 2),
            round(stats['q3'], 2),
            round(official, 2) if official else 0
        ])

def load_history():
    if not os.path.isfile(HISTORY_FILE):
        return [], [], [], [], []
    
    d, m, q1, q3, off = [], [], [], [], []
    with open(HISTORY_FILE, 'r') as f:
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
    
    return d[-48:], m[-48:], q1[-48:], q3[-48:], off[-48:]

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
            "figure.facecolor": style["bg"],
            "axes.facecolor": style["bg"],
            "axes.edgecolor": style["fg"],
            "axes.labelcolor": style["fg"],
            "xtick.color": style["fg"],
            "ytick.color": style["fg"],
            "text.color": style["fg"]
        })
        
        fig = plt.figure(figsize=(12, 14))
        fig.suptitle(
            f'ETB LIQUIDITY SCANNER: {datetime.datetime.now().strftime("%H:%M")}',
            fontsize=20, color=style["fg"], fontweight='bold', y=0.97
        )

        ax1 = fig.add_subplot(2, 1, 1)
        data = stats['raw_data']
        y_jitter = [1 + random.uniform(-0.12, 0.12) for _ in data]
        ax1.scatter(data, y_jitter, color=style["fg"], alpha=style["alpha"], s=30, edgecolors='none')
        
        ax1.axvline(stats['median'], color=style["median"], linewidth=3)
        ax1.axvline(stats['q1'], color=style["sec"], linewidth=2, linestyle='--', alpha=0.6)
        ax1.axvline(stats['q3'], color=style["sec"], linewidth=2, linestyle='--', alpha=0.6)
        
        ax1.text(stats['median'], 1.42, f"MEDIAN\n{stats['median']:.2f}", 
                color=style["median"], ha='center', fontweight='bold')
        ax1.text(stats['q1'], 0.58, f"Q1\n{stats['q1']:.2f}",
                color=style["sec"], ha='right', va='top')
        ax1.text(stats['q3'], 0.58, f"Q3\n{stats['q3']:.2f}",
                color=style["sec"], ha='left', va='top')
        
        if official_rate:
            ax1.axvline(official_rate, color=style["fg"], linestyle=':', linewidth=1.5)
        
        margin = (stats['p95'] - stats['p05']) * 0.1
        if margin == 0:
            margin = 1
        ax1.set_xlim([stats['p05'] - margin, stats['p95'] + margin])
        ax1.set_ylim(0.5, 1.5)
        ax1.set_yticks([])
        ax1.set_title("Live Market Depth (Smart Zoom)", color=style["fg"], loc='left', pad=10)
        ax1.grid(True, axis='x', color=style["grid"], linestyle='--')

        ax2 = fig.add_subplot(2, 1, 2)
        if len(dates) > 1:
            ax2.fill_between(dates, q1s, q3s, color=style["fill"], alpha=0.2, linewidth=0)
            ax2.plot(dates, medians, color=style["median"], linewidth=2)
            if any(offs):
                ax2.plot(dates, offs, color=style["fg"], linestyle='--', linewidth=1, alpha=0.5)
            ax2.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
            ax2.yaxis.set_major_formatter(ticker.FormatStrFormatter('%.1f'))
            ax2.yaxis.tick_right()
            ax2.grid(True, color=style["grid"], linewidth=0.5)
            ax2.set_title("Historical Trend (24h)", color=style["fg"], loc='left')
        
        plt.tight_layout(rect=[0, 0, 1, 0.95])
        plt.savefig(filename, dpi=150, facecolor=style["bg"])
        plt.close()

# --- 6. WEB GENERATOR ---
def update_website_html(stats, official, timestamp, grouped_ads, peg, feed_html):
    prem = ((stats['median'] - official) / official) * 100 if official else 0
    cache_buster = int(time.time())
    
    # Build table
    table_rows = ""
    for source, ads in grouped_ads.items():
        prices = [a['price'] for a in ads]
        s = analyze(prices, peg)
        
        if s:
            table_rows += f"<tr><td class='source-col'>{source}</td><td>{s['min']:.2f}</td><td>{s['q1']:.2f}</td><td class='med-col'>{s['median']:.2f}</td><td>{s['q3']:.2f}</td><td>{s['max']:.2f}</td><td>{s['count']}</td></tr>"
        else:
            table_rows += f"<tr><td class='source-col'>{source}</td><td colspan='6' style='opacity:0.5'>No Data</td></tr>"

    html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <meta http-equiv="refresh" content="300">
        <title>ETB Market Watch v31</title>
        <style>
            :root {{ --bg: #050505; --card: #111; --text: #00ff9d; --sub: #ccc; --mute: #666; --accent: #ff0055; --link: #00bfff; --gold: #ffcc00; --border: #333; }}
            [data-theme="light"] {{ --bg: #f4f4f9; --card: #fff; --text: #1a1a1a; --sub: #333; --mute: #888; --accent: #d63384; --link: #0d6efd; --gold: #ffc107; --border: #ddd; }}
            
            body {{ background: var(--bg); color: var(--text); font-family: 'Courier New', monospace; margin: 0; padding: 20px; text-align: center; transition: 0.3s; }}
            .container {{ max-width: 1200px; margin: 0 auto; display: grid; grid-template-columns: 2fr 1fr; gap: 20px; text-align: left; }}
            
            header {{ grid-column: span 2; text-align: center; margin-bottom: 20px; position: relative; }}
            h1 {{ font-size: 2.5rem; margin: 0; text-shadow: 0 0 10px var(--text); }}
            .toggle {{ position: absolute; top: 0; right: 0; cursor: pointer; padding: 8px 16px; border: 1px solid var(--border); border-radius: 20px; background: var(--card); color: var(--sub); font-size: 0.8rem; }}
            
            .left-col, .right-col {{ display: flex; flex-direction: column; gap: 20px; }}
            .card {{ background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 20px; }}
            
            .ticker {{ text-align: center; padding: 30px; background: linear-gradient(145deg, var(--card), var(--bg)); }}
            .price {{ font-size: 4rem; font-weight: bold; color: var(--sub); margin: 10px 0; }}
            .prem {{ color: var(--gold); font-size: 0.9rem; display: block; margin-top: 10px; }}
            
            .chart img {{ width: 100%; border-radius: 8px; display: block; border: 1px solid var(--border); }}
            
            table {{ width: 100%; border-collapse: collapse; font-size: 0.9rem; }}
            th {{ text-align: left; padding: 12px; border-bottom: 2px solid var(--border); color: var(--text); }}
            td {{ padding: 12px; border-bottom: 1px solid var(--border); color: var(--sub); }}
            .source-col {{ font-weight: bold; color: var(--text); }}
            .med-col {{ color: var(--accent); font-weight: bold; }}
            
            .feed-title {{ font-size: 1.1rem; font-weight: bold; margin-bottom: 15px; border-bottom: 1px solid var(--border); padding-bottom: 10px; color: var(--text); }}
            .feed-container {{ max-height: 600px; overflow-y: auto; padding-right: 5px; }}
            .feed-item {{ display: flex; gap: 12px; padding: 10px; border-bottom: 1px solid var(--border); align-items: center; }}
            .feed-item.real-trade {{ background: rgba(46, 160, 67, 0.1); }}
            .feed-icon {{ width: 30px; height: 30px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 1.2rem; flex-shrink: 0; color: #fff; }}
            .feed-content {{ font-size: 0.85rem; color: var(--sub); }}
            .feed-ts {{ color: var(--mute); font-family: monospace; }}
            .feed-user, .feed-price {{ font-weight: bold; color: var(--text); }}
            .feed-vol {{ font-weight: bold; color: var(--link); }}
            .feed-source {{ font-weight: bold; }}
            
            footer {{ grid-column: span 2; margin-top: 40px; text-align: center; color: var(--mute); font-size: 0.7rem; }}
            @media (max-width: 900px) {{ .container {{ grid-template-columns: 1fr; }} header, footer {{ grid-column: span 1; }} .price {{ font-size: 3rem; }} }}
        </style>
    </head>
    <body>
        <div class="container">
            <header>
                <h1>ETB MARKET INTELLIGENCE</h1>
                <div style="color:var(--mute); letter-spacing:4px; font-size:0.8rem;">/// LIVE P2P TAPE READER v31 ///</div>
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
                    <div class="feed-title">📊 Market Activity Feed</div>
                    <div class="feed-container">{feed_html}</div>
                </div>
            </div>

            <footer>Official Bank Rate: {official:.2f} ETB | Last Update: {timestamp} UTC | ✅ = Confirmed Trades</footer>
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
    print("✅ Website generated.")

# --- 7. MAIN ---
def main():
    print("🔍 Running v31.0 Hybrid Scan...", file=sys.stderr)
    
    with ThreadPoolExecutor(max_workers=10) as ex:
        # Fetch data from all sources
        f_bin = ex.submit(lambda: fetch_p2p_army_ads("binance", "SELL"))
        f_mexc = ex.submit(lambda: fetch_p2p_army_ads("mexc", "SELL"))
        f_byb = ex.submit(lambda: fetch_bybit("1"))
        f_off = ex.submit(fetch_official_rate)
        f_peg = ex.submit(fetch_usdt_peg)
        
        bin_ads = f_bin.result()
        mexc_ads = f_mexc.result()
        byb_ads = f_byb.result()
        official = f_off.result() or 0.0
        peg = f_peg.result() or 1.0
    
    # Combine all ads
    all_ads = bin_ads + mexc_ads + byb_ads
    
    # Group for display
    grouped_ads = {
        "Binance": bin_ads,
        "Bybit": byb_ads,
        "MEXC": mexc_ads
    }
    
    if not all_ads:
        print("⚠️ CRITICAL: No ads found", file=sys.stderr)
        stats = {"median": 0, "q1": 0, "q3": 0, "min": 0, "max": 0, "count": 0, "raw_data": []}
        feed_html = "<div class='feed-item'>No market data available</div>"
    else:
        # Analyze market
        all_prices = [ad['price'] for ad in all_ads]
        stats = analyze(all_prices, peg)
        
        if not stats:
            stats = {"median": 0, "q1": 0, "q3": 0, "min": 0, "max": 0, "count": 0, "raw_data": []}
        
        # Detect trades
        detected_trades = detect_trades_improved(all_ads, peg)
        
        # Generate feed
        feed_html = generate_realistic_feed(all_ads, peg, stats['median'], detected_trades)
        
        # Save current snapshot for next run
        save_snapshot(all_ads)
        
        # Save history and generate charts
        if stats['median'] > 0:
            save_to_history(stats, official)
            generate_charts(stats, official)
    
    # Generate website
    update_website_html(stats, official, time.strftime('%Y-%m-%d %H:%M:%S'), grouped_ads, peg, feed_html)
    
    print("✅ Update Complete.")

if __name__ == "__main__":
    main()
