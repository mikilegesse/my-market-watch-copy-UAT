#!/usr/bin/env python3
"""
🇪🇹 ETB Financial Terminal v36.1 (Robinhood Edition - Fixed)
- UI/UX: Classic terminal colors (#00ff9d green) with Robinhood layout
- TRACKING: Both BUYERS and SELLERS (inventory increases + decreases) ✅ FIXED
- DISPLAY: Both Binance and MEXC transactions ✅ FIXED
- INTERACTIVE: Time period filters (LIVE, 1H, 1D, 1W, 1M, 3M, YTD, 1Y)
- GRAPHS: Trading-style smooth charts with glow effects
- ANIMATIONS: Smooth transitions, hover effects, arrow indicators
- FILTERING: Smart outlier removal (bottom 10%)
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
TRADES_FILE = "recent_trades.json"
GRAPH_FILENAME = "etb_neon_terminal.png"
GRAPH_LIGHT_FILENAME = "etb_light_terminal.png"
HTML_FILENAME = "index.html"

# v29.1's proven timing
BURST_WAIT_TIME = 45

# Show trades from last 24 HOURS (not just 1 hour)
TRADE_RETENTION_MINUTES = 1440  # 24 hours

# Limit to 200 most recent ads per source
MAX_ADS_PER_SOURCE = 200

HISTORY_POINTS = 288

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Content-Type": "application/json",
    "Accept": "application/json"
}

# --- 1. FETCHERS ---
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
    """Direct Scraper for Binance (Bypasses API Block)"""
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
                })
                if len(ads) >= MAX_ADS_PER_SOURCE:
                    break
            page += 1
            time.sleep(0.2)
        except:
            break
    
    return ads[:MAX_ADS_PER_SOURCE]

def fetch_bybit(side):
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
                    'source': 'Bybit',
                    'advertiser': i.get('nickName', 'Bybit User'),
                    'price': float(i.get('price')),
                    'available': float(i.get('lastQuantity', 0)),
                })
                if len(ads) >= MAX_ADS_PER_SOURCE:
                    break
            page += 1
            time.sleep(0.1)
        except:
            break
    
    return ads[:MAX_ADS_PER_SOURCE]

def fetch_mexc_api(side):
    """✅ v16.0's EXACT proven MEXC method - returns full ad data"""
    url = "https://p2p.army/v1/api/get_p2p_order_book"
    ads = []
    h = HEADERS.copy()
    h["X-APIKEY"] = P2P_ARMY_KEY
    
    try:
        payload = {"market": "mexc", "fiat": "ETB", "asset": "USDT", "side": side, "limit": 100}
        r = requests.post(url, headers=h, json=payload, timeout=10)
        data = r.json()
        
        candidates = data.get("result", data.get("data", data.get("ads", [])))
        if not candidates and isinstance(data, list):
            candidates = data
        
        if candidates:
            for ad in candidates:
                if isinstance(ad, dict) and 'price' in ad:
                    try:
                        ads.append({
                            'source': 'MEXC',
                            'advertiser': ad.get('advertiser_name', ad.get('nickname', 'MEXC User')),
                            'price': float(ad['price']),
                            'available': float(ad.get('available_amount', ad.get('amount', 0))),
                        })
                    except:
                        continue
        
        print(f"   MEXC: {len(ads)} ads", file=sys.stderr)
    except Exception as e:
        print(f"   MEXC error: {e}", file=sys.stderr)
    
    return ads

# --- 2. INVENTORY TRACKING (ENHANCED: BUYERS + SELLERS!) ---
def capture_market_snapshot():
    with ThreadPoolExecutor(max_workers=10) as ex:
        f_bin = ex.submit(lambda: fetch_binance_direct("SELL"))
        f_byb = ex.submit(lambda: fetch_bybit("1"))
        f_mexc = ex.submit(lambda: fetch_mexc_api("SELL"))
        f_peg = ex.submit(fetch_usdt_peg)
        
        bin_data = f_bin.result() or []
        mexc_data = f_mexc.result() or []
        bybit_data = f_byb.result() or []
        peg = f_peg.result() or 1.0
        
        total_before = len(bin_data) + len(mexc_data) + len(bybit_data)
        print(f"   📊 Collected {total_before} ads (Binance: {len(bin_data)}, MEXC: {len(mexc_data)}, Bybit: {len(bybit_data)})", file=sys.stderr)
        
        # Remove lowest 10% from Binance and MEXC (unreliable cheap offers)
        bin_data = remove_outliers(bin_data, peg)
        mexc_data = remove_outliers(mexc_data, peg)
        
        total_after = len(bin_data) + len(mexc_data) + len(bybit_data)
        print(f"   ✂️ After filtering: {total_after} ads (removed {total_before - total_after} outliers)", file=sys.stderr)
        
        return bin_data + bybit_data + mexc_data

def remove_outliers(ads, peg):
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
    """Load previous snapshot state from file"""
    if os.path.exists(SNAPSHOT_FILE):
        try:
            with open(SNAPSHOT_FILE, 'r') as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_market_state(current_ads):
    """Save current snapshot state to file"""
    state = {}
    for ad in current_ads:
        key = f"{ad['source']}_{ad['advertiser']}_{ad['price']}"
        state[key] = ad['available']
    
    with open(SNAPSHOT_FILE, 'w') as f:
        json.dump(state, f)

def detect_real_trades(current_ads, peg):
    """
    ✅ ENHANCED: Track BOTH buyers and sellers
    - Inventory DROP → Someone SOLD
    - Inventory INCREASE → Someone BOUGHT
    """
    prev_state = load_market_state()
    
    if not prev_state:
        print("   > First run - establishing baseline", file=sys.stderr)
        return []
    
    trades = []
    sources_checked = {'Binance': 0, 'MEXC': 0}
    
    for ad in current_ads:
        # Only track Binance and MEXC
        if ad['source'] not in ['Binance', 'MEXC']:
            continue
        
        sources_checked[ad['source']] += 1
        
        key = f"{ad['source']}_{ad['advertiser']}_{ad['price']}"
        
        if key in prev_state:
            prev_inventory = prev_state[key]
            curr_inventory = ad['available']
            
            # Calculate difference
            diff = abs(curr_inventory - prev_inventory)
            
            # SELL: Inventory dropped
            if curr_inventory < prev_inventory and diff > 5:
                trades.append({
                    'type': 'sell',
                    'source': ad['source'],
                    'user': ad['advertiser'],
                    'price': ad['price'] / peg,
                    'vol_usd': diff,
                    'timestamp': time.time()
                })
                print(f"   🔴 SELL: {ad['source']} - {ad['advertiser'][:15]} sold {diff:,.0f} USDT @ {ad['price']/peg:.2f} ETB", file=sys.stderr)
            
            # BUY: Inventory increased
            elif curr_inventory > prev_inventory and diff > 5:
                trades.append({
                    'type': 'buy',
                    'source': ad['source'],
                    'user': ad['advertiser'],
                    'price': ad['price'] / peg,
                    'vol_usd': diff,
                    'timestamp': time.time()
                })
                print(f"   🟢 BUY: {ad['source']} - {ad['advertiser'][:15]} bought {diff:,.0f} USDT @ {ad['price']/peg:.2f} ETB", file=sys.stderr)
    
    print(f"   > Checked ads: Binance={sources_checked['Binance']}, MEXC={sources_checked['MEXC']}", file=sys.stderr)
    print(f"   > Detected {len(trades)} trades ({len([t for t in trades if t['type']=='buy'])} buys, {len([t for t in trades if t['type']=='sell'])} sells)", file=sys.stderr)
    return trades

def load_recent_trades():
    if not os.path.exists(TRADES_FILE):
        return []
    
    try:
        with open(TRADES_FILE, "r") as f:
            all_trades = json.load(f)
        
        cutoff = time.time() - (TRADE_RETENTION_MINUTES * 60)
        recent = [t for t in all_trades if t.get("timestamp", 0) > cutoff]
        
        print(f"   > Loaded {len(recent)} trades from last 24h", file=sys.stderr)
        return recent
    except:
        return []

def save_trades(new_trades):
    recent = load_recent_trades()
    all_trades = recent + new_trades
    
    cutoff = time.time() - (TRADE_RETENTION_MINUTES * 60)
    filtered = [t for t in all_trades if t.get("timestamp", 0) > cutoff]
    
    with open(TRADES_FILE, "w") as f:
        json.dump(filtered, f)
    
    print(f"   > Saved {len(filtered)} trades to history (last 24h)", file=sys.stderr)

# --- 3. ANALYTICS ---
def analyze(prices, peg):
    if not prices:
        return None
    
    prices_float = []
    for item in prices:
        if isinstance(item, (int, float)):
            prices_float.append(float(item))
        elif isinstance(item, dict) and 'price' in item:
            prices_float.append(float(item['price']))
    
    clean_prices = sorted([p for p in prices_float if 10 < p < 500])
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

def calculate_price_distribution(ads, peg, bin_size=5):
    """Calculate how many ads fall within each 5 ETB price band"""
    if not ads:
        return []
    
    prices = []
    for ad in ads:
        if isinstance(ad, dict) and 'price' in ad:
            prices.append(ad['price'] / peg)
    
    if not prices:
        return []
    
    bins = {}
    for price in prices:
        bin_start = int(price / bin_size) * bin_size
        bin_key = f"{bin_start}-{bin_start + bin_size}"
        
        if bin_key not in bins:
            bins[bin_key] = 0
        bins[bin_key] += 1
    
    sorted_bins = sorted(bins.items(), key=lambda x: float(x[0].split('-')[0]))
    return sorted_bins

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

# --- 5. GRAPH GENERATOR (Trading Style) ---
def generate_charts(stats, official_rate):
    if not GRAPH_ENABLED:
        return
    
    themes = [
        ("dark", GRAPH_FILENAME, {
            "bg": "#000000", "fg": "#00ff9d", "grid": "#1a1a1a",
            "median": "#ff0055", "sec": "#00bfff", "fill": "#00ff9d", "alpha": 0.8
        }),
        ("light", GRAPH_LIGHT_FILENAME, {
            "bg": "#ffffff", "fg": "#1a1a1a", "grid": "#f0f0f0",
            "median": "#d63384", "sec": "#0d6efd", "fill": "#00a876", "alpha": 0.6
        })
    ]
    dates, medians, q1s, q3s, offs = load_history()
    
    for mode, filename, style in themes:
        plt.style.use('seaborn-v0_8-darkgrid' if mode == 'dark' else 'seaborn-v0_8-whitegrid')
        
        plt.rcParams.update({
            "figure.facecolor": style["bg"], "axes.facecolor": style["bg"],
            "axes.edgecolor": style["fg"], "axes.labelcolor": style["fg"],
            "xtick.color": style["fg"], "ytick.color": style["fg"],
            "text.color": style["fg"], "grid.color": style["grid"],
            "grid.alpha": 0.3, "font.family": "monospace"
        })
        
        fig = plt.figure(figsize=(14, 16))
        fig.suptitle(
            f"ETB LIQUIDITY SCANNER: {datetime.datetime.now().strftime('%H:%M')}",
            fontsize=22, color=style["fg"], fontweight="bold", y=0.98
        )
        
        # Top: Market Depth Scatter
        ax1 = fig.add_subplot(2, 1, 1)
        data = stats["raw_data"]
        y_jitter = [1 + random.uniform(-0.08, 0.08) for _ in data]
        
        # Trading-style scatter with glow effect
        ax1.scatter(data, y_jitter, color=style["fg"], alpha=style["alpha"], 
                   s=40, edgecolors='none', linewidths=0)
        
        # Add subtle glow for median line
        for offset in [0.3, 0.2, 0.1]:
            ax1.axvline(stats["median"], color=style["median"], 
                       linewidth=4, alpha=offset*0.5)
        ax1.axvline(stats["median"], color=style["median"], linewidth=3, alpha=1.0)
        
        # Q1 and Q3 lines with subtle styling
        ax1.axvline(stats["q1"], color=style["sec"], linewidth=2, 
                   linestyle="--", alpha=0.7)
        ax1.axvline(stats["q3"], color=style["sec"], linewidth=2, 
                   linestyle="--", alpha=0.7)
        
        # Labels with better positioning
        ax1.text(stats["median"], 1.45, f"MEDIAN\\n{stats['median']:.2f}",
                color=style["median"], ha="center", fontweight="bold", 
                fontsize=11, va="bottom")
        ax1.text(stats["q1"], 0.55, f"Q1\\n{stats['q1']:.2f}",
                color=style["sec"], ha="right", va="top", fontsize=9)
        ax1.text(stats["q3"], 0.55, f"Q3\\n{stats['q3']:.2f}",
                color=style["sec"], ha="left", va="top", fontsize=9)
        
        if official_rate:
            ax1.axvline(official_rate, color=style["fg"], 
                       linestyle=":", linewidth=2, alpha=0.4)
        
        margin = (stats["p95"] - stats["p05"]) * 0.12
        if margin == 0: margin = 1
        ax1.set_xlim([stats["p05"] - margin, stats["p95"] + margin])
        ax1.set_ylim(0.45, 1.55)
        ax1.set_yticks([])
        ax1.set_title("Live Market Depth", color=style["fg"], 
                     loc="left", pad=12, fontsize=13, fontweight="600")
        ax1.grid(True, axis="x", color=style["grid"], linestyle="-", 
                linewidth=0.5, alpha=0.4)
        ax1.spines['top'].set_visible(False)
        ax1.spines['right'].set_visible(False)
        ax1.spines['left'].set_visible(False)
        
        # Bottom: Historical Trend (Trading Chart Style)
        ax2 = fig.add_subplot(2, 1, 2)
        if len(dates) > 1:
            # Smooth the line using interpolation
            from scipy.interpolate import make_interp_spline
            import numpy as np
            
            if len(dates) > 3:
                # Convert dates to numbers for interpolation
                date_nums = mdates.date2num(dates)
                
                # Create smooth curve
                try:
                    x_smooth = np.linspace(date_nums[0], date_nums[-1], 300)
                    spl_median = make_interp_spline(date_nums, medians, k=3)
                    median_smooth = spl_median(x_smooth)
                    
                    spl_q1 = make_interp_spline(date_nums, q1s, k=3)
                    q1_smooth = spl_q1(x_smooth)
                    
                    spl_q3 = make_interp_spline(date_nums, q3s, k=3)
                    q3_smooth = spl_q3(x_smooth)
                    
                    dates_smooth = mdates.num2date(x_smooth)
                    
                    # Fill area with gradient effect
                    ax2.fill_between(dates_smooth, q1_smooth, q3_smooth, 
                                    color=style["fill"], alpha=0.15, linewidth=0)
                    
                    # Main line with glow effect (trading style)
                    for lw, alpha in [(5, 0.2), (3, 0.4), (2, 1.0)]:
                        ax2.plot(dates_smooth, median_smooth, color=style["median"], 
                                linewidth=lw, alpha=alpha)
                except:
                    # Fallback to simple plot
                    ax2.fill_between(dates, q1s, q3s, color=style["fill"], 
                                    alpha=0.2, linewidth=0)
                    ax2.plot(dates, medians, color=style["median"], linewidth=2.5)
            else:
                ax2.fill_between(dates, q1s, q3s, color=style["fill"], 
                                alpha=0.2, linewidth=0)
                ax2.plot(dates, medians, color=style["median"], linewidth=2.5)
            
            # Official rate line
            if any(offs):
                ax2.plot(dates, offs, color=style["fg"], linestyle="--", 
                        linewidth=1.5, alpha=0.4, label="Official Rate")
            
            # Formatting
            ax2.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
            ax2.yaxis.set_major_formatter(ticker.FormatStrFormatter("%.1f"))
            ax2.yaxis.tick_right()
            ax2.yaxis.set_label_position("right")
            ax2.grid(True, color=style["grid"], linewidth=0.5, alpha=0.3)
            ax2.set_title("24h Price Trend", color=style["fg"], 
                         loc="left", fontsize=13, fontweight="600", pad=12)
            ax2.spines['top'].set_visible(False)
            ax2.spines['left'].set_visible(False)
            
            # Add current price label
            if len(medians) > 0:
                latest_price = medians[-1]
                ax2.text(0.99, 0.97, f"Current: {latest_price:.2f} ETB",
                        transform=ax2.transAxes, ha='right', va='top',
                        fontsize=10, color=style["fg"], fontweight="bold",
                        bbox=dict(boxstyle='round,pad=0.5', 
                                facecolor=style["bg"], 
                                edgecolor=style["fg"], alpha=0.8))
        
        plt.tight_layout(rect=[0, 0, 1, 0.96])
        plt.savefig(filename, dpi=180, facecolor=style["bg"], 
                   edgecolor='none', bbox_inches='tight')
        plt.close()
        
    print(f"✅ Generated trading-style charts", file=sys.stderr)

# --- 6. WEB GENERATOR (ROBINHOOD EDITION!) ---
def update_website_html(stats, official, timestamp, current_ads, grouped_ads, peg):
    prem = ((stats["median"] - official) / official) * 100 if official else 0
    cache_buster = int(time.time())
    
    # Calculate price change (compare current to median from 1 day ago)
    dates, medians, _, _, _ = load_history()
    price_change = 0
    price_change_pct = 0
    if len(medians) > 0:
        old_median = medians[0] if len(medians) > 0 else stats["median"]
        price_change = stats["median"] - old_median
        price_change_pct = (price_change / old_median * 100) if old_median > 0 else 0
    
    # Arrow direction
    arrow = "↗" if price_change > 0 else "↘" if price_change < 0 else "→"
    change_color = "#00C805" if price_change > 0 else "#FF3B30" if price_change < 0 else "#8E8E93"
    
    # Table 1: Source Summary
    table_rows = ""
    for source, ads in grouped_ads.items():
        prices = [a["price"] for a in ads]
        s = analyze(prices, peg)
        if s:
            table_rows += f"<tr><td class='source-col'>{source}</td><td>{s['min']:.2f}</td><td>{s['q1']:.2f}</td><td class='med-col'>{s['median']:.2f}</td><td>{s['q3']:.2f}</td><td>{s['max']:.2f}</td><td>{s['count']}</td></tr>"
        else:
            table_rows += f"<tr><td>{source}</td><td colspan='6' style='opacity:0.5'>No Data</td></tr>"
    
    # Table 2: Price Distribution
    distribution = calculate_price_distribution(current_ads, peg, bin_size=5)
    dist_rows = ""
    if distribution:
        for price_range, count in distribution:
            max_count = max([c for _, c in distribution])
            style = "font-weight:bold;color:var(--accent)" if count == max_count else ""
            dist_rows += f"<tr><td style='{style}'>{price_range} ETB</td><td style='{style}'>{count}</td></tr>"
    else:
        dist_rows = "<tr><td colspan='2' style='opacity:0.5'>No Data</td></tr>"
    
    # Feed with time filters
    recent_trades = load_recent_trades()
    
    # Count buys and sells
    buys_count = len([t for t in recent_trades if t.get('type') == 'buy'])
    sells_count = len([t for t in recent_trades if t.get('type') == 'sell'])
    
    feed_html = generate_feed_html(recent_trades, peg)
    
    html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <meta http-equiv="refresh" content="300">
        <title>ETB Market v36.1 - Terminal Edition</title>
        <style>
            * {{ margin: 0; padding: 0; box-sizing: border-box; }}
            
            :root {{
                --bg: #000000;
                --card: #0a0a0a;
                --card-hover: #151515;
                --text: #00ff9d;
                --text-secondary: #888;
                --green: #00ff9d;
                --red: #ff0055;
                --orange: #ffcc00;
                --border: #222;
                --accent: #00bfff;
            }}
            
            [data-theme="light"] {{
                --bg: #F2F2F7;
                --card: #FFFFFF;
                --card-hover: #F9F9F9;
                --text: #1a1a1a;
                --text-secondary: #666;
                --green: #00a876;
                --red: #d63384;
                --orange: #ff9500;
                --border: #ddd;
                --accent: #0d6efd;
            }}
            
            body {{
                background: var(--bg);
                color: var(--text);
                font-family: 'Courier New', 'SF Mono', Monaco, monospace;
                overflow-x: hidden;
                transition: background 0.3s ease;
            }}
            
            .container {{
                max-width: 1400px;
                margin: 0 auto;
                padding: 20px;
            }}
            
            /* HEADER */
            header {{
                display: flex;
                justify-content: space-between;
                align-items: center;
                padding: 20px 0;
                border-bottom: 1px solid var(--border);
                margin-bottom: 30px;
            }}
            
            .logo {{
                font-size: 24px;
                font-weight: 700;
                letter-spacing: -0.5px;
            }}
            
            .theme-toggle {{
                background: var(--card);
                border: 1px solid var(--border);
                border-radius: 20px;
                padding: 8px 16px;
                cursor: pointer;
                transition: all 0.2s ease;
                color: var(--text);
                font-size: 14px;
            }}
            
            .theme-toggle:hover {{
                background: var(--card-hover);
                transform: translateY(-1px);
            }}
            
            /* MAIN GRID */
            .main-grid {{
                display: grid;
                grid-template-columns: 1fr 400px;
                gap: 20px;
                margin-bottom: 30px;
            }}
            
            /* PRICE CARD (Robinhood style) */
            .price-card {{
                background: var(--card);
                border-radius: 16px;
                padding: 30px;
                border: 1px solid var(--border);
                transition: all 0.3s ease;
            }}
            
            .price-card:hover {{
                border-color: var(--accent);
                box-shadow: 0 8px 30px rgba(10, 132, 255, 0.15);
            }}
            
            .price-label {{
                color: var(--text-secondary);
                font-size: 14px;
                font-weight: 500;
                letter-spacing: 0.5px;
                text-transform: uppercase;
                margin-bottom: 10px;
            }}
            
            .price-value {{
                font-size: 52px;
                font-weight: 700;
                letter-spacing: -2px;
                margin-bottom: 15px;
                line-height: 1;
            }}
            
            .price-change {{
                display: inline-flex;
                align-items: center;
                gap: 6px;
                font-size: 18px;
                font-weight: 600;
                padding: 6px 12px;
                border-radius: 8px;
                background: rgba(0, 200, 5, 0.1);
            }}
            
            .price-change.negative {{
                background: rgba(255, 59, 48, 0.1);
                color: var(--red);
            }}
            
            .price-change.positive {{
                background: rgba(0, 200, 5, 0.1);
                color: var(--green);
            }}
            
            .arrow {{
                font-size: 24px;
                line-height: 1;
            }}
            
            .premium-badge {{
                display: inline-block;
                background: linear-gradient(135deg, var(--orange), #FF6B00);
                color: white;
                padding: 8px 16px;
                border-radius: 20px;
                font-size: 13px;
                font-weight: 600;
                margin-top: 15px;
            }}
            
            /* TIME PERIOD SELECTOR (Robinhood style) */
            .time-selector {{
                display: flex;
                gap: 8px;
                padding: 20px;
                background: var(--card);
                border-radius: 16px;
                border: 1px solid var(--border);
                overflow-x: auto;
            }}
            
            .time-btn {{
                background: transparent;
                border: none;
                color: var(--text-secondary);
                padding: 8px 16px;
                border-radius: 8px;
                cursor: pointer;
                font-size: 14px;
                font-weight: 600;
                transition: all 0.2s ease;
                white-space: nowrap;
            }}
            
            .time-btn:hover {{
                background: var(--card-hover);
                color: var(--text);
            }}
            
            .time-btn.active {{
                background: var(--accent);
                color: white;
            }}
            
            /* CHART */
            .chart-card {{
                background: var(--card);
                border-radius: 16px;
                padding: 20px;
                border: 1px solid var(--border);
                margin-bottom: 20px;
            }}
            
            .chart-card img {{
                width: 100%;
                border-radius: 12px;
                display: block;
            }}
            
            /* TABLES */
            .table-card {{
                background: var(--card);
                border-radius: 16px;
                padding: 20px;
                border: 1px solid var(--border);
                margin-bottom: 20px;
            }}
            
            .table-card h3 {{
                font-size: 18px;
                font-weight: 700;
                margin-bottom: 15px;
                color: var(--text);
            }}
            
            table {{
                width: 100%;
                border-collapse: collapse;
                font-size: 14px;
            }}
            
            th {{
                text-align: left;
                padding: 12px;
                color: var(--text-secondary);
                font-weight: 600;
                font-size: 12px;
                text-transform: uppercase;
                letter-spacing: 0.5px;
                border-bottom: 1px solid var(--border);
            }}
            
            td {{
                padding: 12px;
                color: var(--text);
                border-bottom: 1px solid var(--border);
            }}
            
            tr:last-child td {{
                border-bottom: none;
            }}
            
            .source-col {{
                font-weight: 600;
            }}
            
            .med-col {{
                color: var(--accent);
                font-weight: 700;
            }}
            
            /* FEED PANEL */
            .feed-panel {{
                background: var(--card);
                border-radius: 16px;
                border: 1px solid var(--border);
                height: fit-content;
                position: sticky;
                top: 20px;
            }}
            
            .feed-header {{
                padding: 20px;
                border-bottom: 1px solid var(--border);
            }}
            
            .feed-title {{
                font-size: 18px;
                font-weight: 700;
                margin-bottom: 15px;
            }}
            
            .feed-container {{
                max-height: 600px;
                overflow-y: auto;
                padding: 10px;
            }}
            
            .feed-container::-webkit-scrollbar {{
                width: 6px;
            }}
            
            .feed-container::-webkit-scrollbar-thumb {{
                background: var(--border);
                border-radius: 3px;
            }}
            
            /* FEED ITEMS */
            .feed-item {{
                display: flex;
                align-items: flex-start;
                gap: 12px;
                padding: 12px;
                border-radius: 12px;
                margin-bottom: 8px;
                transition: all 0.2s ease;
                cursor: pointer;
            }}
            
            .feed-item:hover {{
                background: var(--card-hover);
            }}
            
            .feed-icon {{
                width: 36px;
                height: 36px;
                border-radius: 50%;
                display: flex;
                align-items: center;
                justify-content: center;
                font-size: 18px;
                flex-shrink: 0;
                font-weight: 600;
            }}
            
            .feed-icon.buy {{
                background: rgba(0, 200, 5, 0.15);
                color: var(--green);
            }}
            
            .feed-icon.sell {{
                background: rgba(255, 59, 48, 0.15);
                color: var(--red);
            }}
            
            .feed-content {{
                flex: 1;
                font-size: 13px;
                line-height: 1.5;
            }}
            
            .feed-meta {{
                display: flex;
                justify-content: space-between;
                color: var(--text-secondary);
                font-size: 12px;
                margin-bottom: 4px;
            }}
            
            .feed-text {{
                color: var(--text);
            }}
            
            .feed-user {{
                font-weight: 600;
                color: var(--text);
            }}
            
            .feed-amount {{
                font-weight: 700;
                color: var(--accent);
            }}
            
            .feed-price {{
                font-weight: 600;
            }}
            
            /* FOOTER */
            footer {{
                text-align: center;
                padding: 30px 20px;
                color: var(--text-secondary);
                font-size: 13px;
                border-top: 1px solid var(--border);
                margin-top: 40px;
            }}
            
            /* RESPONSIVE */
            @media (max-width: 1024px) {{
                .main-grid {{
                    grid-template-columns: 1fr;
                }}
                
                .feed-panel {{
                    position: relative;
                    top: 0;
                }}
                
                .price-value {{
                    font-size: 42px;
                }}
            }}
            
            /* ANIMATIONS */
            @keyframes slideIn {{
                from {{
                    opacity: 0;
                    transform: translateY(10px);
                }}
                to {{
                    opacity: 1;
                    transform: translateY(0);
                }}
            }}
            
            .feed-item {{
                animation: slideIn 0.3s ease;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <header>
                <div class="logo">🇪🇹 ETB MARKET</div>
                <button class="theme-toggle" onclick="toggleTheme()">
                    <span id="theme-icon">🌙</span> Theme
                </button>
            </header>
            
            <div class="main-grid">
                <div class="left-column">
                    <!-- PRICE CARD -->
                    <div class="price-card">
                        <div class="price-label">ETB/USD MEDIAN RATE</div>
                        <div class="price-value">{stats['median']:.2f} <span style="font-size:28px;color:var(--text-secondary);font-weight:400">ETB</span></div>
                        <div class="price-change {('positive' if price_change > 0 else 'negative' if price_change < 0 else '')}">
                            <span class="arrow">{arrow}</span>
                            <span>{abs(price_change):.2f} ETB ({abs(price_change_pct):.2f}%) Today</span>
                        </div>
                        <div class="premium-badge">
                            Black Market Premium: +{prem:.2f}%
                        </div>
                    </div>
                    
                    <!-- TIME SELECTOR -->
                    <div class="time-selector" style="margin-top: 20px;">
                        <button class="time-btn active" data-period="live" onclick="filterTrades('live')">LIVE</button>
                        <button class="time-btn" data-period="1h" onclick="filterTrades('1h')">1H</button>
                        <button class="time-btn" data-period="1d" onclick="filterTrades('1d')">1D</button>
                        <button class="time-btn" data-period="1w" onclick="filterTrades('1w')">1W</button>
                        <button class="time-btn" data-period="1m" onclick="filterTrades('1m')">1M</button>
                        <button class="time-btn" data-period="3m" onclick="filterTrades('3m')">3M</button>
                        <button class="time-btn" data-period="ytd" onclick="filterTrades('ytd')">YTD</button>
                        <button class="time-btn" data-period="1y" onclick="filterTrades('1y')">1Y</button>
                    </div>
                    
                    <!-- CHART -->
                    <div class="chart-card">
                        <img src="{GRAPH_FILENAME}?v={cache_buster}" id="chartImg" alt="Market Chart">
                    </div>
                    
                    <!-- TABLES -->
                    <div class="table-card">
                        <h3>Market Summary by Source</h3>
                        <table>
                            <thead>
                                <tr>
                                    <th>Source</th>
                                    <th>Min</th>
                                    <th>Q1</th>
                                    <th>Med</th>
                                    <th>Q3</th>
                                    <th>Max</th>
                                    <th>Ads</th>
                                </tr>
                            </thead>
                            <tbody>{table_rows}</tbody>
                        </table>
                    </div>
                    
                    <div class="table-card">
                        <h3>📊 Price Distribution (5 ETB Bands)</h3>
                        <table>
                            <thead>
                                <tr>
                                    <th>Price Range</th>
                                    <th>Ad Count</th>
                                </tr>
                            </thead>
                            <tbody>{dist_rows}</tbody>
                        </table>
                    </div>
                </div>
                
                <!-- FEED PANEL -->
                <div class="feed-panel">
                    <div class="feed-header">
                        <div class="feed-title">Market Activity</div>
                        <div style="color:var(--text-secondary);font-size:13px;margin-bottom:10px" id="feedStats">
                            <span style="color:var(--green)">🟢 {buys_count} Buys</span> • <span style="color:var(--red)">🔴 {sells_count} Sells</span>
                        </div>
                        <div style="display:flex;gap:8px;margin-top:10px;">
                            <button class="source-filter-btn active" data-source="all" onclick="filterBySource('all')" style="background:var(--accent);color:white;border:none;padding:6px 12px;border-radius:8px;font-size:12px;font-weight:600;cursor:pointer;">
                                All
                            </button>
                            <button class="source-filter-btn" data-source="Binance" onclick="filterBySource('Binance')" style="background:transparent;color:var(--text-secondary);border:1px solid var(--border);padding:6px 12px;border-radius:8px;font-size:12px;font-weight:600;cursor:pointer;">
                                🟡 Binance
                            </button>
                            <button class="source-filter-btn" data-source="MEXC" onclick="filterBySource('MEXC')" style="background:transparent;color:var(--text-secondary);border:1px solid var(--border);padding:6px 12px;border-radius:8px;font-size:12px;font-weight:600;cursor:pointer;">
                                🔵 MEXC
                            </button>
                        </div>
                    </div>
                    <div class="feed-container" id="feedContainer">
                        {feed_html}
                    </div>
                </div>
            </div>
            
            <footer>
                Official Rate: {official:.2f} ETB | Last Update: {timestamp} UTC<br>
                v36.1 Terminal Edition • Tracking inventory changes (45s window, 24h history)
            </footer>
        </div>
        
        <script>
            const allTrades = {json.dumps(recent_trades)};
            const imgDark = "{GRAPH_FILENAME}?v={cache_buster}";
            const imgLight = "{GRAPH_LIGHT_FILENAME}?v={cache_buster}";
            let currentPeriod = 'live';
            let currentSource = 'all';
            
            // Theme toggle
            function toggleTheme() {{
                const html = document.documentElement;
                const current = html.getAttribute('data-theme');
                const next = current === 'light' ? 'dark' : 'light';
                html.setAttribute('data-theme', next);
                localStorage.setItem('theme', next);
                document.getElementById('chartImg').src = next === 'light' ? imgLight : imgDark;
                document.getElementById('theme-icon').textContent = next === 'light' ? '☀️' : '🌙';
            }}
            
            // Initialize theme
            (function() {{
                const theme = localStorage.getItem('theme') || 'dark';
                document.documentElement.setAttribute('data-theme', theme);
                document.getElementById('chartImg').src = theme === 'light' ? imgLight : imgDark;
                document.getElementById('theme-icon').textContent = theme === 'light' ? '☀️' : '🌙';
            }})();
            
            // Filter trades by source
            function filterBySource(source) {{
                currentSource = source;
                
                // Update active button
                document.querySelectorAll('.source-filter-btn').forEach(btn => {{
                    if (btn.dataset.source === source) {{
                        btn.style.background = 'var(--accent)';
                        btn.style.color = 'white';
                        btn.style.border = 'none';
                    }} else {{
                        btn.style.background = 'transparent';
                        btn.style.color = 'var(--text-secondary)';
                        btn.style.border = '1px solid var(--border)';
                    }}
                }});
                
                // Re-filter with current period and source
                filterTrades(currentPeriod);
            }}
            
            // Filter trades by time period
            function filterTrades(period) {{
                currentPeriod = period;
                // Update active button
                document.querySelectorAll('.time-btn').forEach(btn => {{
                    btn.classList.remove('active');
                }});
                document.querySelector(`[data-period="${{period}}"]`).classList.add('active');
                
                // Filter trades
                const now = Date.now() / 1000;
                let cutoff = 0;
                
                switch(period) {{
                    case '1h': cutoff = now - 3600; break;
                    case '1d': cutoff = now - 86400; break;
                    case '1w': cutoff = now - 604800; break;
                    case '1m': cutoff = now - 2592000; break;
                    case '3m': cutoff = now - 7776000; break;
                    case 'ytd': 
                        const start = new Date(new Date().getFullYear(), 0, 1);
                        cutoff = start.getTime() / 1000;
                        break;
                    case '1y': cutoff = now - 31536000; break;
                    case 'live':
                    default: cutoff = 0;
                }}
                
                let filtered = allTrades.filter(t => t.timestamp > cutoff);
                
                // Filter by source
                if (currentSource !== 'all') {{
                    filtered = filtered.filter(t => t.source === currentSource);
                }}
                
                // Update feed
                renderFeed(filtered);
                
                // Update stats
                const buys = filtered.filter(t => t.type === 'buy').length;
                const sells = filtered.filter(t => t.type === 'sell').length;
                document.getElementById('feedStats').innerHTML = 
                    `<span style="color:var(--green)">🟢 ${buys} Buys</span> • <span style="color:var(--red)">🔴 ${sells} Sells</span>`;
            }}
            
            function renderFeed(trades) {
                const container = document.getElementById('feedContainer');
                
                if (trades.length === 0) {
                    container.innerHTML = '<div style="padding:20px;text-align:center;color:var(--text-secondary)">No trades in this period</div>';
                    return;
                }
                
                const html = trades.slice(0, 50).reverse().map(trade => {
                    const date = new Date(trade.timestamp * 1000);
                    const time = date.toLocaleTimeString('en-US', {hour: '2-digit', minute: '2-digit'});
                    const ageMin = Math.floor((Date.now() / 1000 - trade.timestamp) / 60);
                    const age = ageMin < 60 ? `${ageMin}m ago` : `${Math.floor(ageMin/60)}h ago`;
                    
                    const isBuy = trade.type === 'buy';
                    const icon = isBuy ? '↗' : '↘';
                    const action = isBuy ? 'BOUGHT' : 'SOLD';
                    const color = isBuy ? 'var(--green)' : 'var(--red)';
                    const sourceColor = trade.source === 'Binance' ? '#F3BA2F' : '#2E55E6';
                    const sourceEmoji = trade.source === 'Binance' ? '🟡' : '🔵';
                    
                    return `
                        <div class="feed-item">
                            <div class="feed-icon ${trade.type}">
                                ${icon}
                            </div>
                            <div class="feed-content">
                                <div class="feed-meta">
                                    <span>${time}</span>
                                    <span>${age}</span>
                                </div>
                                <div class="feed-text">
                                    ${sourceEmoji} <span class="feed-user">${trade.user.substring(0, 15)}</span>
                                    <span style="color:${sourceColor};font-weight:600">(${trade.source})</span>
                                    <b style="color:${color}">${action}</b>
                                    <span class="feed-amount">${trade.vol_usd.toFixed(0)} USDT</span>
                                    @ <span class="feed-price">${trade.price.toFixed(2)} ETB</span>
                                </div>
                            </div>
                        </div>
                    `;
                }).join('');
                
                container.innerHTML = html;
            }
            
            // Initialize
            filterTrades('live');
        </script>
    </body>
    </html>
    """
    
    with open(HTML_FILENAME, "w") as f:
        f.write(html)

def generate_feed_html(trades, peg):
    """Generate initial feed HTML server-side"""
    if not trades:
        return '<div style="padding:20px;text-align:center;color:var(--text-secondary)">Waiting for market activity...</div>'
    
    html = ""
    for trade in sorted(trades, key=lambda x: x.get('timestamp', 0), reverse=True)[:50]:
        ts = datetime.datetime.fromtimestamp(trade.get("timestamp", time.time()))
        time_str = ts.strftime("%I:%M %p")
        age_seconds = time.time() - trade.get("timestamp", time.time())
        age_str = f"{int(age_seconds/60)}min ago" if age_seconds >= 60 else f"{int(age_seconds)}s ago"
        
        trade_type = trade.get('type', 'sell')
        is_buy = trade_type == 'buy'
        icon = "↗" if is_buy else "↘"
        action = "BOUGHT" if is_buy else "SOLD"
        icon_class = "buy" if is_buy else "sell"
        action_color = "var(--green)" if is_buy else "var(--red)"
        
        source = trade.get('source', 'Unknown')
        
        html += f"""
        <div class="feed-item">
            <div class="feed-icon {icon_class}">
                {icon}
            </div>
            <div class="feed-content">
                <div class="feed-meta">
                    <span>{time_str}</span>
                    <span>{age_str}</span>
                </div>
                <div class="feed-text">
                    <span class="feed-user">{trade.get('user', 'Unknown')[:12]}</span> ({source})
                    <b style="color:{action_color}">{action}</b>
                    <span class="feed-amount">{trade.get('vol_usd', 0):,.0f} USDT</span>
                    @ <span class="feed-price">{trade.get('price', 0):.2f} ETB</span>
                </div>
            </div>
        </div>"""
    
    return html

# --- 7. MAIN ---
def main():
    print("🔍 Running v36.1 (Terminal Edition - All Fixes Applied)...", file=sys.stderr)
    
    # 1. SNAPSHOT 1
    print("   > Snapshot 1/2...", file=sys.stderr)
    snapshot_1 = capture_market_snapshot()
    
    # 2. WAIT (v29.1's 45 seconds)
    print(f"   > ⏳ Waiting {BURST_WAIT_TIME}s to catch trades...", file=sys.stderr)
    time.sleep(BURST_WAIT_TIME)
    
    # 3. SNAPSHOT 2
    print("   > Snapshot 2/2...", file=sys.stderr)
    with ThreadPoolExecutor(max_workers=10) as ex:
        f_bin = ex.submit(lambda: fetch_binance_direct("SELL"))
        f_byb = ex.submit(lambda: fetch_bybit("1"))
        f_mexc = ex.submit(lambda: fetch_mexc_api("SELL"))
        f_off = ex.submit(fetch_official_rate)
        f_peg = ex.submit(fetch_usdt_peg)
        
        bin_ads = f_bin.result() or []
        mexc_ads = f_mexc.result() or []
        byb_ads = f_byb.result() or []
        official = f_off.result() or 0.0
        peg = f_peg.result() or 1.0
    
    # Filter outliers from snapshot 2 as well
    bin_ads = remove_outliers(bin_ads, peg)
    mexc_ads = remove_outliers(mexc_ads, peg)
    
    snapshot_2 = bin_ads + byb_ads + mexc_ads
    grouped_ads = {"Binance": bin_ads, "Bybit": byb_ads, "MEXC": mexc_ads}
    
    if snapshot_2:
        # Enhanced tracking: buyers + sellers
        new_trades = detect_real_trades(snapshot_2, peg)
        
        # Save state for next run
        save_market_state(snapshot_2)
        
        # Save trades to persistent history
        if new_trades:
            save_trades(new_trades)
        
        # Stats (exclude Bybit from history)
        all_prices = [x['price'] for x in snapshot_2 if x.get('source') != 'Bybit']
        stats = analyze(all_prices, peg)
        
        if stats:
            save_to_history(stats, official)
            generate_charts(stats, official)
            update_website_html(
                stats, official,
                time.strftime("%Y-%m-%d %H:%M:%S"),
                snapshot_2, grouped_ads, peg
            )
        else:
            print("⚠️ Could not compute stats", file=sys.stderr)
    else:
        print("⚠️ No ads found", file=sys.stderr)
    
    buys = len([t for t in (new_trades if 'new_trades' in locals() else []) if t.get('type') == 'buy'])
    sells = len([t for t in (new_trades if 'new_trades' in locals() else []) if t.get('type') == 'sell'])
    print(f"✅ Complete! Detected {buys} buys, {sells} sells this run.")

if __name__ == "__main__":
    main()
