#!/usr/bin/env python3
"""
🇪🇹 ETB Financial Terminal v7.0 (Enhanced Edition)
- FIXED: Q1/Q3 label overlaps with smart positioning
- ENHANCED: Beautiful gradient styling and improved visuals
- NEW: Light/Dark mode toggle in web interface
- CORE: All previous functionality retained
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
    from matplotlib.patches import Rectangle
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

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Content-Type": "application/json"
}

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

# --- 2. WEB GENERATOR (Light/Dark Mode Toggle) ---
def update_website_html(stats, official, timestamp, all_data_sources, peg):
    """ Generates animated HTML with light/dark mode toggle """
    prem = ((stats['median'] - official)/official)*100 if official else 0
    
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
            </tr>
            """
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
            /* --- ROOT VARIABLES --- */
            :root {{
                --bg-primary: #030303;
                --bg-secondary: #0a0a0a;
                --bg-card: #000;
                --text-primary: #00ff9d;
                --text-secondary: #ccc;
                --text-muted: #666;
                --accent-pink: #ff0055;
                --accent-blue: #00bfff;
                --accent-yellow: #ffcc00;
                --border-color: #333;
                --border-hover: #00ff9d;
                --table-hover: rgba(0, 255, 157, 0.05);
                --shadow-color: rgba(0, 255, 157, 0.05);
            }}

            [data-theme="light"] {{
                --bg-primary: #f5f5f5;
                --bg-secondary: #ffffff;
                --bg-card: #fafafa;
                --text-primary: #00a876;
                --text-secondary: #333;
                --text-muted: #888;
                --accent-pink: #d63384;
                --accent-blue: #0d6efd;
                --accent-yellow: #ffc107;
                --border-color: #ddd;
                --border-hover: #00a876;
                --table-hover: rgba(0, 168, 118, 0.05);
                --shadow-color: rgba(0, 0, 0, 0.1);
            }}

            /* --- ANIMATIONS --- */
            @keyframes fadeInUp {{ from {{ opacity: 0; transform: translateY(30px); }} to {{ opacity: 1; transform: translateY(0); }} }}
            @keyframes pulseGlow {{ 
                0%, 100% {{ filter: drop-shadow(0 0 20px currentColor); }} 
                50% {{ filter: drop-shadow(0 0 40px currentColor); }} 
            }}
            @keyframes borderPulse {{ 
                0%, 100% {{ border-color: var(--border-color); box-shadow: 0 0 25px var(--shadow-color); }} 
                50% {{ border-color: var(--border-hover); box-shadow: 0 0 35px var(--shadow-color); }} 
            }}
            @keyframes slide {{ to {{ background-position: 200% center; }} }}

            /* --- BASE STYLES --- */
            * {{ margin: 0; padding: 0; box-sizing: border-box; }}
            body {{ 
                background-color: var(--bg-primary); 
                color: var(--text-primary); 
                font-family: 'Courier New', monospace; 
                text-align: center; 
                padding: 20px; 
                transition: background-color 0.3s, color 0.3s; 
            }}
            .container {{ max-width: 1100px; margin: 0 auto; animation: fadeInUp 0.8s ease-out; }}
            
            /* --- HEADER --- */
            header {{ position: relative; margin-bottom: 40px; }}
            h1 {{ 
                text-shadow: 0 0 15px var(--text-primary); 
                font-size: 2.5rem; 
                margin-bottom: 5px; 
                letter-spacing: 2px; 
                text-transform: uppercase; 
            }}
            .subtext {{ 
                color: var(--text-muted); 
                font-size: 0.8rem; 
                margin-bottom: 20px; 
                letter-spacing: 4px; 
            }}

            /* --- THEME TOGGLE --- */
            .theme-toggle {{
                position: absolute;
                top: 0;
                right: 0;
                background: var(--bg-secondary);
                border: 2px solid var(--border-color);
                border-radius: 50px;
                padding: 8px 20px;
                cursor: pointer;
                display: flex;
                align-items: center;
                gap: 10px;
                transition: all 0.3s;
                font-size: 0.9rem;
                color: var(--text-secondary);
            }}
            .theme-toggle:hover {{ 
                border-color: var(--border-hover); 
                box-shadow: 0 0 20px var(--shadow-color);
            }}
            .theme-icon {{ font-size: 1.2rem; }}

            /* --- TICKER CARD --- */
            .ticker-card {{ 
                background: linear-gradient(145deg, var(--bg-secondary), var(--bg-card)); 
                border: 1px solid var(--border-color); 
                padding: 30px; 
                border-radius: 15px; 
                box-shadow: 0 0 25px var(--shadow-color);
                margin-bottom: 40px;
                position: relative;
                overflow: hidden;
                animation: borderPulse 4s infinite, fadeInUp 1s ease-out;
            }}
            .ticker-card::before {{ 
                content: ''; 
                position: absolute; 
                top: 0; 
                left: 0; 
                width: 100%; 
                height: 3px; 
                background: linear-gradient(90deg, var(--text-primary), var(--accent-pink), var(--text-primary)); 
                animation: slide 3s linear infinite; 
                background-size: 200% auto; 
            }}
            
            .price {{ 
                font-size: 4.5rem; 
                font-weight: bold; 
                color: var(--text-secondary); 
                animation: pulseGlow 2s infinite; 
                margin: 15px 0; 
            }}
            .unit {{ font-size: 1.5rem; color: var(--text-muted); font-weight: normal; }}
            .label {{ 
                color: var(--text-primary); 
                font-size: 0.9rem; 
                text-transform: uppercase; 
                letter-spacing: 3px; 
                font-weight: bold; 
            }}
            .premium {{ 
                background: rgba(255, 204, 0, 0.1); 
                color: var(--accent-yellow); 
                padding: 8px 20px; 
                border-radius: 30px; 
                font-size: 1.1rem; 
                display: inline-block; 
                border: 1px solid var(--accent-yellow); 
                text-shadow: 0 0 10px rgba(255, 204, 0, 0.3); 
            }}

            /* --- GRAPH --- */
            .chart-container {{ 
                margin-bottom: 40px; 
                animation: fadeInUp 1s ease-out 0.3s backwards;
                perspective: 1000px;
            }}
            .chart-wrapper {{
                position: relative;
                width: 100%;
                padding-bottom: 116.67%; /* 12:14 aspect ratio */
                background: var(--bg-secondary);
                border: 2px solid var(--border-color);
                border-radius: 15px;
                overflow: hidden;
                box-shadow: 0 5px 30px var(--shadow-color);
                transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
                transform-style: preserve-3d;
            }}
            .chart-wrapper:hover {{ 
                border-color: var(--border-hover); 
                box-shadow: 0 8px 50px var(--shadow-color), 0 0 60px var(--border-hover);
                transform: translateY(-10px) scale(1.02);
            }}
            .chart-wrapper::before {{
                content: '';
                position: absolute;
                top: -2px;
                left: -2px;
                right: -2px;
                bottom: -2px;
                background: linear-gradient(45deg, var(--text-primary), var(--accent-pink), var(--accent-blue), var(--text-primary));
                background-size: 300% 300%;
                border-radius: 15px;
                opacity: 0;
                transition: opacity 0.4s;
                z-index: -1;
                animation: gradientShift 3s ease infinite;
            }}
            .chart-wrapper:hover::before {{
                opacity: 0.5;
            }}
            @keyframes gradientShift {{
                0%, 100% {{ background-position: 0% 50%; }}
                50% {{ background-position: 100% 50%; }}
            }}
            .chart-wrapper img {{ 
                position: absolute;
                top: 0;
                left: 0;
                width: 100%;
                height: 100%;
                object-fit: cover;
                transition: transform 0.4s cubic-bezier(0.4, 0, 0.2, 1), opacity 0.3s ease;
            }}
            .chart-wrapper:hover img {{
                transform: scale(1.05);
            }}
            .chart-wrapper .zoom-hint {{
                position: absolute;
                bottom: 15px;
                right: 15px;
                background: rgba(0, 0, 0, 0.7);
                color: var(--text-primary);
                padding: 8px 15px;
                border-radius: 20px;
                font-size: 0.75rem;
                opacity: 0;
                transform: translateY(10px);
                transition: all 0.3s;
                pointer-events: none;
                backdrop-filter: blur(10px);
            }}
            .chart-wrapper:hover .zoom-hint {{
                opacity: 1;
                transform: translateY(0);
            }}

            /* --- DATA TABLE --- */
            .table-container {{ animation: fadeInUp 1s ease-out 0.6s backwards; }}
            .data-table {{ 
                width: 100%; 
                border-collapse: separate; 
                border-spacing: 0; 
                background: var(--bg-secondary); 
                border-radius: 12px; 
                overflow: hidden; 
                border: 1px solid var(--border-color); 
            }}
            .data-table th {{ 
                background: var(--bg-card); 
                color: var(--text-primary); 
                padding: 15px; 
                font-size: 0.85rem; 
                text-transform: uppercase; 
                letter-spacing: 1px; 
                border-bottom: 2px solid var(--border-color); 
            }}
            .data-table td {{ 
                padding: 15px; 
                border-bottom: 1px solid var(--border-color); 
                color: var(--text-secondary); 
                font-size: 0.95rem; 
                transition: all 0.2s; 
            }}
            .data-table tr:hover td {{ 
                background: var(--table-hover); 
                color: var(--text-primary); 
                border-bottom-color: var(--border-hover); 
            }}
            .data-table tr:last-child td {{ border-bottom: none; }}
            .source-name {{ color: var(--text-secondary) !important; }}
            .median-cell {{ 
                color: var(--accent-pink) !important; 
                font-weight: bold; 
                font-size: 1.1em; 
            }}
            .no-data {{ color: var(--text-muted) !important; }}

            /* --- BANK RATE & FOOTER --- */
            .bank-card {{ 
                margin-top: 40px; 
                padding-top: 20px; 
                border-top: 1px solid var(--border-color); 
                animation: fadeInUp 1s ease-out 0.9s backwards; 
            }}
            .bank-rate {{ 
                color: var(--accent-blue); 
                font-size: 1.8rem; 
                font-weight: bold; 
                text-shadow: 0 0 10px var(--accent-blue); 
                margin-top: 10px; 
            }}
            
            footer {{ 
                margin-top: 50px; 
                color: var(--text-muted); 
                font-size: 0.75rem; 
                letter-spacing: 1px; 
                animation: fadeInUp 1s ease-out 1.2s backwards; 
            }}

            /* --- RESPONSIVE --- */
            @media (max-width: 768px) {{
                h1 {{ font-size: 1.8rem; }}
                .price {{ font-size: 3rem; }}
                .theme-toggle {{ position: static; margin-bottom: 20px; }}
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <header>
                <div class="theme-toggle" onclick="toggleTheme()">
                    <span class="theme-icon">🌓</span>
                    <span class="theme-text">Toggle Theme</span>
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
                    <img src="{GRAPH_FILENAME}" alt="Market Analysis Chart" id="graphImage">
                    <div class="zoom-hint">🔍 Hover to zoom</div>
                </div>
            </div>

            <div class="table-container">
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
            // Load saved theme IMMEDIATELY (before images load)
            (function() {{
                const savedTheme = localStorage.getItem('theme') || 'dark';
                document.documentElement.setAttribute('data-theme', savedTheme);
                
                // Set correct graph image immediately
                const graphImage = document.getElementById('graphImage');
                if (graphImage) {{
                    graphImage.src = savedTheme === 'light' 
                        ? '{GRAPH_LIGHT_FILENAME}' 
                        : '{GRAPH_FILENAME}';
                }}
            }})();

            // Theme Toggle Logic
            function toggleTheme() {{
                const html = document.documentElement;
                const currentTheme = html.getAttribute('data-theme');
                const newTheme = currentTheme === 'light' ? 'dark' : 'light';
                const graphImage = document.getElementById('graphImage');
                
                html.setAttribute('data-theme', newTheme);
                localStorage.setItem('theme', newTheme);
                
                // Update graph image with smooth transition
                if (graphImage) {{
                    graphImage.style.opacity = '0.5';
                    setTimeout(() => {{
                        graphImage.src = newTheme === 'light' 
                            ? '{GRAPH_LIGHT_FILENAME}' 
                            : '{GRAPH_FILENAME}';
                        graphImage.onload = () => {{
                            graphImage.style.opacity = '1';
                        }};
                    }}, 150);
                }}
            }}

            // Add tilt effect on mouse move
            document.addEventListener('DOMContentLoaded', () => {{
                const chartWrapper = document.querySelector('.chart-wrapper');
                if (chartWrapper) {{
                    chartWrapper.addEventListener('mousemove', (e) => {{
                        const rect = chartWrapper.getBoundingClientRect();
                        const x = e.clientX - rect.left;
                        const y = e.clientY - rect.top;
                        
                        const centerX = rect.width / 2;
                        const centerY = rect.height / 2;
                        
                        const rotateX = (y - centerY) / 20;
                        const rotateY = (centerX - x) / 20;
                        
                        chartWrapper.style.transform = `translateY(-10px) scale(1.02) rotateX(${{rotateX}}deg) rotateY(${{rotateY}}deg)`;
                    }});
                    
                    chartWrapper.addEventListener('mouseleave', () => {{
                        chartWrapper.style.transform = '';
                    }});
                }}
            }});
        </script>
    </body>
    </html>
    """
    
    with open(HTML_FILENAME, "w") as f:
        f.write(html_content)
    print(f"✅ Website ({HTML_FILENAME}) generated with light/dark mode toggle.")

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

# --- 5. ENHANCED VISUALIZATION (Smart Label Positioning) ---
def generate_dashboard(stats, official_rate, theme='dark'):
    if not GRAPH_ENABLED: return
    print(f"📊 Rendering {theme.title()} Dashboard...", file=sys.stderr)
    
    # Theme configuration
    if theme == 'light':
        plt.style.use('default')
        bg_color = '#ffffff'
        text_color = '#333333'
        grid_color = '#dddddd'
        primary_color = '#00a876'
        accent_color = '#d63384'
        secondary_color = '#0d6efd'
        output_file = GRAPH_LIGHT_FILENAME
    else:
        plt.style.use('dark_background')
        bg_color = '#000000'
        text_color = '#00ff9d'
        grid_color = '#222222'
        primary_color = '#00ff9d'
        accent_color = '#ff0055'
        secondary_color = '#00bfff'
        output_file = GRAPH_FILENAME
    
    fig = plt.figure(figsize=(12, 14), facecolor=bg_color)
    fig.suptitle(f'ETB LIQUIDITY SCANNER: {datetime.datetime.now().strftime("%H:%M")}', 
                 fontsize=20, color=primary_color, fontweight='bold', y=0.97)

    # --- TOP: ENHANCED DOT PLOT WITH SMART LABELS ---
    ax1 = fig.add_subplot(2, 1, 1, facecolor=bg_color)
    data = stats['raw_data']
    y_jitter = [1 + random.uniform(-0.12, 0.12) for _ in data]
    
    # Gradient scatter points
    scatter = ax1.scatter(data, y_jitter, c=data, cmap='viridis' if theme == 'light' else 'plasma',
                         alpha=0.7, s=30, edgecolors='white' if theme == 'light' else 'black', 
                         linewidth=0.5, label='Ad Price')
    
    # Statistical lines with gradient effect
    ax1.axvline(stats['median'], color=accent_color, linewidth=3, linestyle='-', alpha=0.9, zorder=5)
    ax1.axvline(stats['q1'], color=secondary_color, linewidth=2, linestyle='--', alpha=0.7)
    ax1.axvline(stats['q3'], color=secondary_color, linewidth=2, linestyle='--', alpha=0.7)
    
    # Smart label positioning to avoid overlaps
    q_spread = stats['q3'] - stats['q1']
    median_q1_gap = stats['median'] - stats['q1']
    median_q3_gap = stats['q3'] - stats['median']
    
    # Median label (always top center)
    ax1.text(stats['median'], 1.42, f"MEDIAN\n{stats['median']:.2f}", 
             color=accent_color, ha='center', va='bottom', fontweight='bold', fontsize=12,
             bbox=dict(boxstyle='round,pad=0.5', facecolor=bg_color, edgecolor=accent_color, alpha=0.8))
    
    # Q1 label positioning
    if median_q1_gap < q_spread * 0.3:  # If Q1 is close to median
        # Place Q1 on the left side, vertically centered
        ax1.text(stats['q1'] - q_spread * 0.05, 1.0, f"Q1\n{stats['q1']:.2f}", 
                 color=secondary_color, ha='right', va='center', fontsize=10,
                 bbox=dict(boxstyle='round,pad=0.4', facecolor=bg_color, edgecolor=secondary_color, alpha=0.7))
    else:
        # Place Q1 at bottom
        ax1.text(stats['q1'], 0.58, f"Q1 (Low)\n{stats['q1']:.2f}", 
                 color=secondary_color, ha='center', va='top', fontsize=10,
                 bbox=dict(boxstyle='round,pad=0.4', facecolor=bg_color, edgecolor=secondary_color, alpha=0.7))
    
    # Q3 label positioning
    if median_q3_gap < q_spread * 0.3:  # If Q3 is close to median
        # Place Q3 on the right side, vertically centered
        ax1.text(stats['q3'] + q_spread * 0.05, 1.0, f"Q3\n{stats['q3']:.2f}", 
                 color=secondary_color, ha='left', va='center', fontsize=10,
                 bbox=dict(boxstyle='round,pad=0.4', facecolor=bg_color, edgecolor=secondary_color, alpha=0.7))
    else:
        # Place Q3 at bottom
        ax1.text(stats['q3'], 0.58, f"Q3 (High)\n{stats['q3']:.2f}", 
                 color=secondary_color, ha='center', va='top', fontsize=10,
                 bbox=dict(boxstyle='round,pad=0.4', facecolor=bg_color, edgecolor=secondary_color, alpha=0.7))

    # Official rate marker
    if official_rate:
        ax1.axvline(official_rate, color=text_color, linestyle=':', linewidth=1.5, alpha=0.6)
        ax1.text(official_rate, 0.6, f"Bank\n{official_rate:.0f}", 
                 color=text_color, ha='center', fontsize=9, alpha=0.8,
                 bbox=dict(boxstyle='round,pad=0.3', facecolor=bg_color, edgecolor=text_color, alpha=0.5))

    # Enhanced styling
    margin = (stats['p90'] - stats['p10']) * 0.25
    ax1.set_xlim([min(official_rate or 999, stats['p10']) - margin, stats['p90'] + margin])
    ax1.set_ylim(0.5, 1.5)
    ax1.set_title("Live Market Depth (Binance + MEXC)", color=text_color, loc='left', pad=15, fontweight='bold')
    ax1.set_yticks([])
    ax1.set_xlabel("Price (ETB / True USD)", color=text_color, fontweight='bold')
    ax1.grid(True, axis='x', linestyle='--', alpha=0.2, color=grid_color)
    ax1.spines['top'].set_visible(False)
    ax1.spines['right'].set_visible(False)
    ax1.spines['left'].set_visible(False)
    ax1.tick_params(colors=text_color)

    # --- BOTTOM: ENHANCED HISTORY LINE CHART ---
    ax2 = fig.add_subplot(2, 1, 2, facecolor=bg_color)
    dates, medians, q1s, q3s, offs = load_history()
    
    if len(dates) > 1:
        # Gradient fill
        ax2.fill_between(dates, q1s, q3s, color=primary_color, alpha=0.15, linewidth=0, label='Q1-Q3 Range')
        
        # Main line with shadow effect
        ax2.plot(dates, medians, color=accent_color, linewidth=3, label='Median Rate', zorder=3)
        ax2.plot(dates, medians, color=accent_color, linewidth=6, alpha=0.2, zorder=2)  # Shadow
        
        if any(offs): 
            ax2.plot(dates, offs, color=text_color, linestyle='--', linewidth=1.5, 
                    alpha=0.4, label='Official', zorder=1)
            
        ax2.set_title("Historical Trend", color=text_color, loc='left', pad=15, fontweight='bold')
        ax2.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        ax2.yaxis.set_major_formatter(ticker.FormatStrFormatter('%.1f'))
        
        # Styling
        plt.setp(ax2.xaxis.get_majorticklabels(), rotation=0, ha='center', color=text_color)
        ax2.yaxis.tick_right()
        plt.setp(ax2.yaxis.get_majorticklabels(), color=text_color)
        ax2.grid(True, which='major', axis='both', linestyle='-', color=grid_color, linewidth=0.5, alpha=0.3)
        ax2.legend(loc='upper left', framealpha=0.8, facecolor=bg_color, edgecolor=grid_color)
        ax2.spines['top'].set_visible(False)
        ax2.spines['right'].set_visible(False)
        ax2.tick_params(colors=text_color)
    else:
        ax2.text(0.5, 0.5, "Building Time Series...\nRun again later to see line chart.", 
                ha='center', va='center', color=text_color, alpha=0.5)
        ax2.set_xticks([])
        ax2.set_yticks([])

    plt.tight_layout(rect=[0, 0, 1, 0.95])
    plt.savefig(output_file, dpi=150, facecolor=bg_color, edgecolor='none')
    print(f"✅ {theme.title()} Graph Saved: {output_file}", file=sys.stderr)
    plt.close()

# --- 6. MAIN EXECUTION ---
def main():
    print("🔍 Initializing ETB Enhanced Terminal...", file=sys.stderr)
    
    with ThreadPoolExecutor(max_workers=10) as ex:
        f_bin = ex.submit(lambda: fetch_binance("BUY") + fetch_binance("SELL"))
        f_byb = ex.submit(lambda: fetch_bybit("1") + fetch_bybit("0"))
        f_mexc = ex.submit(lambda: fetch_p2p_army_ads("mexc", "SELL"))
        f_off = ex.submit(fetch_official_rate)
        f_peg = ex.submit(fetch_usdt_peg)
        
        data = {"Binance": f_bin.result(), "Bybit": f_byb.result(), "MEXC": f_mexc.result()}
        official = f_off.result()
        peg = f_peg.result()

    # Aggregate Data
    visual_prices = data["Binance"] + data["MEXC"]
    visual_stats = analyze(visual_prices, peg)
    
    # Save History & Generate Visuals
    if visual_stats: 
        save_to_history(visual_stats, official)
        
        # Generate both dark and light theme graphs
        generate_dashboard(visual_stats, official, theme='dark')
        generate_dashboard(visual_stats, official, theme='light')
        
        # Generate HTML with theme toggle
        update_website_html(visual_stats, official, time.strftime('%Y-%m-%d %H:%M:%S'), data, peg)

    print("\n" + "="*80)
    print(f"✅ Auto-Update Complete: {time.strftime('%H:%M:%S')}")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()
