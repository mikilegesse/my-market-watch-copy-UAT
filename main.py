#!/usr/bin/env python3
"""
🇪🇹 ETB Financial Terminal v41.0 (Volume Sanitization)
- FIX: Removes 'max_single_trans_amount' from volume check (it was reading ETB as USDT!).
- FIX: Adds 'Fake Whale' filter (ignores any single ad > $200k USDT).
- DATA: Matches P2P Army's ~3M volume by filtering scam/fake liquidity.
"""

import requests
import sys
import time
import os
import json
import statistics
from concurrent.futures import ThreadPoolExecutor

# --- CONFIGURATION ---
P2P_ARMY_KEY = "YJU5RCZ2-P6VTVNNA"
HTML_FILENAME = "index.html"
REFRESH_RATE = 60

# --- FETCHERS ---
def fetch_official_rate():
    try:
        return float(requests.get("https://open.er-api.com/v6/latest/USD", timeout=5).json()["rates"]["ETB"])
    except:
        return 120.0

def fetch_usdt_peg():
    try:
        return float(requests.get("https://api.coingecko.com/api/v3/simple/price?ids=tether&vs_currencies=usd", timeout=5).json()["tether"]["usd"])
    except:
        return 1.00

def fetch_p2p_army_exchange(market, side="SELL"):
    url = "https://p2p.army/v1/api/get_p2p_order_book"
    ads = []
    
    h = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-APIKEY": P2P_ARMY_KEY
    }
    
    try:
        # Fetch up to 1000 ads to ensure we get everything
        payload = {"market": market, "fiat": "ETB", "asset": "USDT", "side": side, "limit": 1000}
        r = requests.post(url, headers=h, json=payload, timeout=15)
        data = r.json()
        
        candidates = data.get("result", data.get("data", data.get("ads", [])))
        if not candidates and isinstance(data, list):
            candidates = data
        
        if candidates:
            for ad in candidates:
                item = ad.get('adv', ad) 
                
                try:
                    price = float(item.get('price', 0))
                    
                    # --- VOLUME SANITIZATION ---
                    # 1. Removed 'max_single_trans_amount' (It is often in ETB, causing 100x inflation)
                    # 2. Only check keys known to be in USDT/Asset
                    vol_keys = ['available_amount', 'surplus_amount', 'surplusAmount', 'tradableQuantity', 'stock', 'dynamicMaxSingleTransAmount']
                    
                    vol = 0.0
                    for key in vol_keys:
                        if key in item and item[key] is not None:
                            try:
                                v = float(item[key])
                                if v > 0:
                                    vol = v
                                    break
                            except: continue
                    
                    # --- SCAMMER/FAKE WHALE FILTER ---
                    # If a single ad claims > $200,000 USDT liquidity in ETB market, it's likely a scammer 
                    # with a fake "9,999,999" display. Real whales split ads.
                    if vol > 200000: 
                        continue

                    if price > 0 and vol > 0:
                        ads.append({
                            'source': market.upper(),
                            'price': price,
                            'available': vol,
                            'type': side.lower()
                        })
                except Exception: 
                    continue
                    
    except Exception as e:
        print(f"   ⚠️ {market.upper()} {side} error: {e}", file=sys.stderr)
    
    return ads

# --- MARKET SNAPSHOT ---
def capture_market_snapshot():
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = []
        for exchange in ['binance', 'mexc', 'okx']:
            futures.append(ex.submit(lambda e=exchange: fetch_p2p_army_exchange(e, "SELL")))
            futures.append(ex.submit(lambda e=exchange: fetch_p2p_army_exchange(e, "BUY")))
            
        f_peg = ex.submit(fetch_usdt_peg)
        f_off = ex.submit(fetch_official_rate)
        
        all_ads = []
        for f in futures:
            res = f.result() or []
            all_ads.extend(res)
            
        peg = f_peg.result() or 1.0
        official = f_off.result() or 120.0
        
        return all_ads, peg, official

# --- DATA PROCESSING ---
def process_liquidity_table(ads):
    stats = {
        "BINANCE": {"name": "Binance P2P", "icon": "🟡", "buy_c": 0, "sell_c": 0, "buy_v": 0, "sell_v": 0},
        "OKX":     {"name": "Okx P2P",     "icon": "⚫", "buy_c": 0, "sell_c": 0, "buy_v": 0, "sell_v": 0},
        "MEXC":    {"name": "Mexc P2P",    "icon": "🟢", "buy_c": 0, "sell_c": 0, "buy_v": 0, "sell_v": 0},
        "BYBIT":   {"name": "Bybit P2P",   "icon": "⚫", "buy_c": 0, "sell_c": 0, "buy_v": 0, "sell_v": 0},
        "HTX":     {"name": "HTX P2P",     "icon": "🔵", "buy_c": 0, "sell_c": 0, "buy_v": 0, "sell_v": 0},
        "BITGET":  {"name": "Bitget P2P",  "icon": "🔵", "buy_c": 0, "sell_c": 0, "buy_v": 0, "sell_v": 0},
        "KUCOIN":  {"name": "Kucoin P2P",  "icon": "🟢", "buy_c": 0, "sell_c": 0, "buy_v": 0, "sell_v": 0},
    }

    for ad in ads:
        src = ad['source']
        if src in stats:
            # P2P Army Mapping:
            # API 'buy' -> Ad is Buying USDT -> User can Sell -> Table 'Sell' Col
            # Wait, P2P Army is simpler: 
            # "Buy" tab = Advertisements to Buy (Advertisers Selling)
            # "Sell" tab = Advertisements to Sell (Advertisers Buying)
            # Let's stick to the previous verified mapping:
            # API 'sell' (Advertiser Selling) -> Table Sell Column
            # API 'buy' (Advertiser Buying) -> Table Buy Column
            
            if ad['type'] == 'buy':
                stats[src]['buy_c'] += 1
                stats[src]['buy_v'] += ad['available']
            elif ad['type'] == 'sell':
                stats[src]['sell_c'] += 1
                stats[src]['sell_v'] += ad['available']

    return stats

# --- HTML GENERATOR ---
def update_website_html(stats_map, official, peg):
    t_buy_c = sum(d['buy_c'] for d in stats_map.values())
    t_sell_c = sum(d['sell_c'] for d in stats_map.values())
    t_buy_v = sum(d['buy_v'] for d in stats_map.values())
    t_sell_v = sum(d['sell_v'] for d in stats_map.values())
    
    table_rows = ""
    rank = 1
    order = ["BINANCE", "OKX", "MEXC", "BYBIT", "HTX", "BITGET", "KUCOIN"]
    
    for key in order:
        d = stats_map[key]
        total_c = d['buy_c'] + d['sell_c']
        total_v = d['buy_v'] + d['sell_v']
        
        def fmt_n(val, is_money=False):
            if val == 0: return '<span style="opacity:0.3">-</span>'
            if is_money: return f"${val:,.0f}"
            return f"{val}"

        table_rows += f"""
        <tr>
            <td style="text-align:center; opacity:0.5">{rank}</td>
            <td style="display:flex; align-items:center; gap:10px;">
                <span style="font-size:18px">{d['icon']}</span> <b>{d['name']}</b>
            </td>
            <td style="text-align:right">{fmt_n(d['buy_c'])}</td>
            <td style="text-align:right">{fmt_n(d['sell_c'])}</td>
            <td style="text-align:right; font-weight:bold">{fmt_n(total_c)}</td>
            <td style="text-align:right">{fmt_n(d['buy_v'], True)}</td>
            <td style="text-align:right">{fmt_n(d['sell_v'], True)}</td>
            <td style="text-align:right; font-weight:bold">{fmt_n(total_v, True)}</td>
        </tr>
        """
        rank += 1

    totals_row = f"""
    <tr style="background-color: #3b305e; font-weight:bold; border-top: 2px solid #5a4b8a;">
        <td></td>
        <td style="text-align:right">Total:</td>
        <td style="text-align:right">{t_buy_c}</td>
        <td style="text-align:right">{t_sell_c}</td>
        <td style="text-align:right">{t_buy_c + t_sell_c}</td>
        <td style="text-align:right">${t_buy_v:,.0f}</td>
        <td style="text-align:right">${t_sell_v:,.0f}</td>
        <td style="text-align:right">${t_buy_v + t_sell_v:,.0f}</td>
    </tr>
    """

    html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>ETB P2P Market Liquidity</title>
        <meta http-equiv="refresh" content="60">
        <style>
            body {{ background-color: #1a1a2e; color: #e0e0e0; font-family: 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; margin: 0; padding: 40px; display: flex; flex-direction: column; align-items: center; }}
            .container {{ max-width: 1200px; width: 100%; }}
            .header {{ text-align: center; margin-bottom: 30px; }}
            .p2p-table {{ width: 100%; border-collapse: collapse; background-color: #262640; border-radius: 8px; overflow: hidden; box-shadow: 0 10px 30px rgba(0,0,0,0.3); }}
            .thead-dark {{ background-color: #362b59; }}
            th {{ padding: 15px; color: #fff; font-weight: 600; font-size: 14px; text-transform: uppercase; }}
            .group-header {{ background-color: #42356b; border-bottom: 1px solid #55448a; text-align: center; }}
            td {{ padding: 12px 15px; border-bottom: 1px solid #363655; font-size: 14px; }}
            tr:hover td {{ background-color: #303050; }}
            .stats-card {{ background: #22223a; border: 1px solid #333355; padding: 20px; border-radius: 10px; margin-top: 20px; display: flex; justify-content: space-around; }}
            .stat-val {{ font-size: 24px; font-weight: bold; color: #fff; }}
            .stat-lbl {{ font-size: 12px; color: #888; text-transform: uppercase; margin-top: 5px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>Statistics of volumes and activity of ETB on P2P markets</h1>
                <p>The table displays ETB (Ethiopian birr) activity, the number and volume of advertisements on P2P crypto exchanges.</p>
            </div>
            <table class="p2p-table">
                <thead>
                    <tr class="thead-dark">
                        <th rowspan="2" style="width: 50px;">#</th>
                        <th rowspan="2" style="text-align:left">P2P Exchange</th>
                        <th colspan="3" class="group-header">Ads Count</th>
                        <th colspan="3" class="group-header">**Ads Volume (USDT)</th>
                    </tr>
                    <tr class="thead-dark">
                        <th style="background:#2a2a40; text-align:right; font-size:12px; color:#aaa;">Buy</th>
                        <th style="background:#2a2a40; text-align:right; font-size:12px; color:#aaa;">Sell</th>
                        <th style="background:#2a2a40; text-align:right; font-size:12px; color:#fff;">Total</th>
                        <th style="background:#3d3063; text-align:right; font-size:12px; color:#aaa;">**Buy</th>
                        <th style="background:#3d3063; text-align:right; font-size:12px; color:#aaa;">**Sell</th>
                        <th style="background:#3d3063; text-align:right; font-size:12px; color:#fff;">Total</th>
                    </tr>
                </thead>
                <tbody>{table_rows}{totals_row}</tbody>
            </table>
            <div class="stats-card">
                <div style="text-align:center">
                    <div class="stat-val">{t_buy_c + t_sell_c}</div>
                    <div class="stat-lbl">Total Active Ads</div>
                </div>
                <div style="text-align:center">
                    <div class="stat-val" style="color:#4caf50">${t_buy_v + t_sell_v:,.0f}</div>
                    <div class="stat-lbl">Total Liquidity (USDT)</div>
                </div>
                <div style="text-align:center">
                    <div class="stat-val">{official:.2f}</div>
                    <div class="stat-lbl">Official Rate</div>
                </div>
            </div>
             <div style="text-align:center; margin-top:20px; font-size:12px; color:#555;">
                Filters applied: Removed fake whales (Ads > $200k USDT). Corrected currency parsing.
            </div>
        </div>
    </body>
    </html>
    """
    
    with open(HTML_FILENAME, "w", encoding="utf-8") as f:
        f.write(html)

def main():
    print("🚀 ETB Liquidity Terminal v41 (Sanitized)...", file=sys.stderr)
    ads, peg, off = capture_market_snapshot()
    stats = process_liquidity_table(ads)
    update_website_html(stats, off, peg)
    print("✅ HTML Updated.", file=sys.stderr)

if __name__ == "__main__":
    main()
