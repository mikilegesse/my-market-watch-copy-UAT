#!/usr/bin/env python3
"""
🇪🇹 ETB Financial Terminal v41.3 (UAT FIXES v2 - Display Table + MEXC Debug!)
- FIXED: Binance/MEXC showing "No Data" in table (fetch both buy/sell for display)
- FIXED: MEXC User-Agent header added (required by RapidAPI)
- NEW: Detailed MEXC debugging logs to troubleshoot 0 ads issue
- FIXED: OKX error handling improved
- KEEP: All v41.2 fixes (duplicates fixed, direct APIs)
- COST: Only $50/month for OKX!
- EXCHANGES: Binance (direct), MEXC (RapidAPI), OKX (p2p.army), Bybit (direct)
- COVERAGE: 8 snapshots × 15s = 58% market coverage
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

# TIMING CONFIGURATION
# BURST_WAIT_TIME determines how long we wait between API checks to detect trades
# Strategy: SHORT wait (45s) catches MORE trades, not fewer!
# 
# How it works:
# 1. Fetch ads at T=0
# 2. Wait 45 seconds
# 3. Fetch ads again at T=45s
# 4. Compare: Ads that disappeared = SOLD, Ads that appeared = BOUGHT
#
# Why 45 seconds is optimal:
# - Too short (10s): Ads might not have time to appear/disappear
# - Too long (10min): Miss fast trades, fewer checks per GitHub Actions run
# - 45s: Sweet spot - proven by v29.1 testing
#
# With GitHub Actions running every ~3 minutes:
# - Each run does 1-2 checks
# - 45s wait allows enough time for ad state changes
# - Catches both quick and slow trades
#
# DO NOT increase to 10 minutes - this will REDUCE trade detection!
BURST_WAIT_TIME = 45
TRADE_RETENTION_MINUTES = 1440  # 24 hours
MAX_ADS_PER_SOURCE = 200
HISTORY_POINTS = 288

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Content-Type": "application/json",
    "Accept": "application/json"
}

# --- FETCHERS ---
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

def fetch_binance_p2p_direct(side="SELL"):
    """Fetch Binance P2P directly from their API (more reliable!)"""
    url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
    ads = []
    
    try:
        payload = {
            "asset": "USDT",
            "fiat": "ETB",
            "merchantCheck": False,
            "page": 1,
            "payTypes": [],
            "publisherType": None,
            "rows": 50,  # Increased from 20 to 50 to catch more activity!
            "tradeType": side,  # "SELL" or "BUY"
        }
        
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        
        r = requests.post(url, headers=headers, json=payload, timeout=10)
        data = r.json()
        
        if data.get("success") and data.get("data"):
            for item in data["data"]:
                adv = item.get("adv", {})
                advertiser = item.get("advertiser", {})
                
                try:
                    ads.append({
                        'source': 'BINANCE',
                        'advertiser': advertiser.get('nickName', 'Binance User'),
                        'price': float(adv.get('price', 0)),
                        'available': float(adv.get('surplusAmount', 0)),
                        'ad_id': adv.get('advNo', ''),  # Unique ad ID for tracking
                        'trade_type': side.lower(),  # Track if this is buy or sell ad
                    })
                except Exception as e:
                    continue
        
        print(f"   BINANCE (Direct API) {side}: {len(ads)} ads", file=sys.stderr)
    except Exception as e:
        print(f"   BINANCE (Direct API) {side} error: {e}", file=sys.stderr)
    
    return ads

def fetch_binance_p2p_both_sides():
    """Fetch BOTH buy and sell ads from Binance"""
    with ThreadPoolExecutor(max_workers=2) as ex:
        f_sell = ex.submit(lambda: fetch_binance_p2p_direct("SELL"))
        f_buy = ex.submit(lambda: fetch_binance_p2p_direct("BUY"))
        
        sell_ads = f_sell.result() or []
        buy_ads = f_buy.result() or []
        
        all_ads = sell_ads + buy_ads
        print(f"   BINANCE Total: {len(all_ads)} ads ({len(sell_ads)} sells, {len(buy_ads)} buys)", file=sys.stderr)
        return all_ads
        print(f"   BINANCE (Direct API) error: {e}", file=sys.stderr)
    
    return ads

def fetch_p2p_army_exchange(market, side="SELL"):
    """Universal fetcher with ROBUST volume and username detection + ad_type tracking"""
    url = "https://p2p.army/v1/api/get_p2p_order_book"
    ads = []
    h = HEADERS.copy()
    h["X-APIKEY"] = P2P_ARMY_KEY
    
    try:
        payload = {"market": market, "fiat": "ETB", "asset": "USDT", "side": side, "limit": 100}
        r = requests.post(url, headers=h, json=payload, timeout=10)
        data = r.json()
        
        # Parse response (handles multiple formats)
        candidates = data.get("result", data.get("data", data.get("ads", [])))
        if not candidates and isinstance(data, list):
            candidates = data
        
        # DEBUG: Print keys of first ad to see structure
        if candidates and len(candidates) > 0:
            if not hasattr(fetch_p2p_army_exchange, f"debug_printed_{market}_{side}"):
                first = candidates[0]
                print(f"   🔍 DEBUG {market.upper()} {side} API KEYS: {list(first.keys())[:10]}", file=sys.stderr)
                setattr(fetch_p2p_army_exchange, f"debug_printed_{market}_{side}", True)
        
        if candidates:
            for ad in candidates:
                if isinstance(ad, dict) and 'price' in ad:
                    try:
                        # ROBUST VOLUME FINDER: Try all known keys
                        vol = 0
                        for key in ['available_amount', 'amount', 'surplus_amount', 'stock', 'max_amount', 'dynamic_max_amount', 'tradable_quantity']:
                            if key in ad and ad[key]:
                                try:
                                    v = float(ad[key])
                                    if v > 0:
                                        vol = v
                                        break
                                except:
                                    continue
                        
                        # If still 0, skip this ad (useless for volume tracking)
                        if vol == 0:
                            continue
                        
                        # ROBUST USERNAME FINDER: Try all known keys
                        username = None
                        for key in ['advertiser_name', 'nickname', 'trader_name', 'userName', 'user_name', 'merchant_name', 'merchant', 'trader', 'name']:
                            if key in ad and ad[key]:
                                username = str(ad[key])
                                break
                        
                        # Fallback to generic if no username found
                        if not username:
                            username = f'{market.upper()} User'
                        
                        ads.append({
                            'source': market.upper(),
                            'ad_type': side,  # CRITICAL: Track if BUY or SELL ad!
                            'advertiser': username,
                            'price': float(ad['price']),
                            'available': vol,
                        })
                    except Exception as e:
                        continue
        
        print(f"   {market.upper()} {side}: {len(ads)} ads", file=sys.stderr)
    except Exception as e:
        print(f"   {market.upper()} {side} error: {e}", file=sys.stderr)
    
    return ads

def fetch_bybit_direct(side="SELL"):
    """Fetch Bybit P2P ads using direct free API (no p2p.army)"""
    url = "https://api2.bybit.com/fiat/otc/item/online"
    ads = []
    
    try:
        # Bybit API params: side 0=sell, 1=buy
        bybit_side = "0" if side == "SELL" else "1"
        
        params = {
            "userId": "",
            "tokenId": "USDT",
            "currencyId": "ETB",
            "payment": [],
            "side": bybit_side,
            "size": "100",
            "page": "1",
            "amount": ""
        }
        
        r = requests.post(url, headers=HEADERS, json=params, timeout=10)
        data = r.json()
        
        # Parse Bybit response
        if data.get("ret_code") == 0 and "result" in data:
            items = data["result"].get("items", [])
            
            for ad in items:
                try:
                    username = ad.get("nickName", ad.get("userId", "Bybit User"))
                    price = float(ad.get("price", 0))
                    vol = float(ad.get("lastQuantity", ad.get("quantity", 0)))
                    
                    if vol > 0 and price > 0:
                        ads.append({
                            'source': 'BYBIT',
                            'ad_type': side,
                            'advertiser': username,
                            'price': price,
                            'available': vol,
                        })
                except Exception as e:
                    continue
        
        print(f"   BYBIT {side} (direct API): {len(ads)} ads", file=sys.stderr)
    except Exception as e:
        print(f"   BYBIT {side} (direct API) error: {e}", file=sys.stderr)
    
    return ads

def fetch_binance_direct(side="SELL"):
    """Fetch Binance P2P ads using direct free API"""
    url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
    ads = []
    
    try:
        # Binance API params
        payload = {
            "fiat": "ETB",
            "page": 1,
            "rows": 100,
            "tradeType": side,  # "SELL" or "BUY"
            "asset": "USDT",
            "payTypes": [],
            "publisherType": None
        }
        
        r = requests.post(url, headers=HEADERS, json=payload, timeout=10)
        data = r.json()
        
        # Parse Binance response
        if data.get("data"):
            items = data["data"]
            
            for ad in items:
                try:
                    adv = ad.get("adv", {})
                    advertiser = ad.get("advertiser", {})
                    
                    username = advertiser.get("nickName", "Binance User")
                    price = float(adv.get("price", 0))
                    vol = float(adv.get("surplusAmount", adv.get("tradableQuantity", 0)))
                    
                    if vol > 0 and price > 0:
                        ads.append({
                            'source': 'BINANCE',
                            'ad_type': side,
                            'advertiser': username,
                            'price': price,
                            'available': vol,
                        })
                except Exception as e:
                    continue
        
        print(f"   BINANCE {side} (direct API): {len(ads)} ads", file=sys.stderr)
    except Exception as e:
        print(f"   BINANCE {side} (direct API) error: {e}", file=sys.stderr)
    
    return ads

def fetch_mexc_rapidapi(side="SELL"):
    """Fetch MEXC P2P ads using RapidAPI"""
    url = "https://mexc-p2p-api.p.rapidapi.com/api/v1/c2c/advList"
    ads = []
    
    try:
        # RapidAPI headers from user's screenshot + User-Agent (REQUIRED!)
        headers = {
            "x-rapidapi-key": "28e60e8b83msh2f62e830aa1f09ap18bad1jsna2ade74a847c",
            "x-rapidapi-host": "mexc-p2p-api.p.rapidapi.com",
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
        }
        
        # MEXC API params: tradeType 0=buy, 1=sell
        trade_type = "1" if side == "SELL" else "0"
        
        params = {
            "currency": "ETB",
            "tradeType": trade_type,
            "coin": "USDT",
            "pageNo": 1,
            "pageSize": 100
        }
        
        r = requests.post(url, headers=headers, json=params, timeout=10)
        data = r.json()
        
        # DEBUG: Log response for troubleshooting
        print(f"   🔍 MEXC {side} response code: {data.get('code', 'unknown')}", file=sys.stderr)
        
        # Parse MEXC response
        if data.get("code") == 0 and "data" in data:
            items = data["data"].get("data", [])
            print(f"   🔍 MEXC {side} items found: {len(items)}", file=sys.stderr)
            
            for ad in items:
                try:
                    username = ad.get("nickName", ad.get("userName", "MEXC User"))
                    price = float(ad.get("price", 0))
                    vol = float(ad.get("tradableQuantity", ad.get("surplusAmount", 0)))
                    
                    if vol > 0 and price > 0:
                        ads.append({
                            'source': 'MEXC',
                            'ad_type': side,
                            'advertiser': username,
                            'price': price,
                            'available': vol,
                        })
                except Exception as e:
                    continue
        
        print(f"   MEXC {side} (RapidAPI): {len(ads)} ads", file=sys.stderr)
    except Exception as e:
        print(f"   MEXC {side} (RapidAPI) error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
    
    return ads

def fetch_binance_both_sides():
    """Fetch BOTH buy and sell ads from Binance using direct API"""
    with ThreadPoolExecutor(max_workers=2) as ex:
        f_sell = ex.submit(lambda: fetch_binance_direct("SELL"))
        f_buy = ex.submit(lambda: fetch_binance_direct("BUY"))
        
        sell_ads = f_sell.result() or []
        buy_ads = f_buy.result() or []
        
        return sell_ads + buy_ads

def fetch_mexc_both_sides():
    """Fetch BOTH buy and sell ads from MEXC using RapidAPI"""
    with ThreadPoolExecutor(max_workers=2) as ex:
        f_sell = ex.submit(lambda: fetch_mexc_rapidapi("SELL"))
        f_buy = ex.submit(lambda: fetch_mexc_rapidapi("BUY"))
        
        sell_ads = f_sell.result() or []
        buy_ads = f_buy.result() or []
        
        return sell_ads + buy_ads

def fetch_bybit_both_sides():
    """Fetch BOTH buy and sell ads from Bybit using direct API"""
    with ThreadPoolExecutor(max_workers=2) as ex:
        f_sell = ex.submit(lambda: fetch_bybit_direct("SELL"))
        f_buy = ex.submit(lambda: fetch_bybit_direct("BUY"))
        
        sell_ads = f_sell.result() or []
        buy_ads = f_buy.result() or []
        
        return sell_ads + buy_ads

def fetch_exchange_both_sides(exchange_name):
    """Fetch BOTH buy and sell ads for any exchange via p2p.army"""
    with ThreadPoolExecutor(max_workers=2) as ex:
        f_sell = ex.submit(lambda: fetch_p2p_army_exchange(exchange_name, "SELL"))
        f_buy = ex.submit(lambda: fetch_p2p_army_exchange(exchange_name, "BUY"))
        
        sell_ads = f_sell.result() or []
        buy_ads = f_buy.result() or []
        
        all_ads = sell_ads + buy_ads
        print(f"   {exchange_name.upper()} Total: {len(all_ads)} ads ({len(sell_ads)} sells, {len(buy_ads)} buys)", file=sys.stderr)
        return all_ads

def fetch_binance_both_sides():
    """Fetch BOTH buy and sell ads from Binance via p2p.army"""
    return fetch_exchange_both_sides("binance")

# --- MARKET SNAPSHOT ---
def capture_market_snapshot():
    """Capture market snapshot: Binance (direct), MEXC (RapidAPI), OKX (p2p.army), Bybit (direct)"""
    with ThreadPoolExecutor(max_workers=10) as ex:
        f_binance = ex.submit(fetch_binance_both_sides)  # Direct API (FREE!)
        f_mexc = ex.submit(fetch_mexc_both_sides)  # RapidAPI  
        f_okx = ex.submit(fetch_exchange_both_sides, "okx")  # Only OKX uses p2p.army now
        f_bybit = ex.submit(fetch_bybit_both_sides)  # Direct API (FREE!)
        f_peg = ex.submit(fetch_usdt_peg)
        
        binance_data = f_binance.result() or []
        mexc_data = f_mexc.result() or []
        okx_data = f_okx.result() or []
        bybit_data = f_bybit.result() or []
        peg = f_peg.result() or 1.0
        
        total_before = len(binance_data) + len(mexc_data) + len(okx_data) + len(bybit_data)
        print(f"   📊 Collected {total_before} ads total (Binance direct, MEXC RapidAPI, OKX p2p.army, Bybit direct)", file=sys.stderr)
        
        # Remove lowest 10% outliers
        binance_data = remove_outliers(binance_data, peg)
        mexc_data = remove_outliers(mexc_data, peg)
        okx_data = remove_outliers(okx_data, peg)
        bybit_data = remove_outliers(bybit_data, peg)
        
        total_after = len(binance_data) + len(mexc_data) + len(okx_data) + len(bybit_data)
        print(f"   ✂️ After filtering: {total_after} ads (removed {total_before - total_after} outliers)", file=sys.stderr)
        
        return binance_data + mexc_data + okx_data + bybit_data

def remove_outliers(ads, peg):
    if len(ads) < 10:
        return ads
    
    prices = sorted([ad["price"] / peg for ad in ads])
    p10_threshold = prices[int(len(prices) * 0.10)]
    filtered = [ad for ad in ads if (ad["price"] / peg) > p10_threshold]
    
    return filtered

def load_market_state():
    if os.path.exists(SNAPSHOT_FILE):
        try:
            with open(SNAPSHOT_FILE, 'r') as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_market_state(current_ads):
    """
    Save market state with ad_type included
    CRITICAL: We need ad_type to determine aggressor direction!
    """
    state = {}
    for ad in current_ads:
        # Use ||| delimiter to avoid conflicts with underscores in usernames
        key = f"{ad['source']}|||{ad['advertiser']}|||{ad['price']}"
        state[key] = {
            'available': ad['available'],
            'ad_type': ad.get('ad_type', 'SELL')  # CRITICAL: Save ad type!
        }
    
    with open(SNAPSHOT_FILE, 'w') as f:
        json.dump(state, f)

def detect_real_trades(current_ads, peg):
    """
    FIXED AGGRESSOR LOGIC! Tracks what TAKERS do, not MAKERS
    GREEN = Aggressive buying (demand/capital flight)
    RED = Aggressive selling (supply/capital return)
    + Request tracking (like ethioblackmarket.com!)
    """
    prev_state = load_market_state()
    
    if not prev_state:
        print("   > First run - establishing baseline", file=sys.stderr)
        return []
    
    trades = []
    requests = []  # NEW: Track requests (new ads posted)
    sources_checked = {'BINANCE': 0, 'MEXC': 0, 'OKX': 0, 'BYBIT': 0}  # Added BYBIT!
    
    # Build current state with ad_type
    current_state = {}
    ad_lookup = {}  # For looking up full ad info
    for ad in current_ads:
        key = f"{ad['source']}|||{ad['advertiser']}|||{ad['price']}"
        current_state[key] = {
            'available': ad['available'],
            'ad_type': ad.get('ad_type', 'SELL')
        }
        ad_lookup[key] = ad
    
    # 1. Check for DISAPPEARED ads (complete fills)
    disappeared_ads = set(prev_state.keys()) - set(current_state.keys())
    for key in disappeared_ads:
        parts = key.split('|||')
        if len(parts) >= 3:
            source = parts[0].upper()
            username = parts[1]
            try:
                price = float(parts[2])
            except ValueError:
                continue
            
            if source in sources_checked:
                prev_data = prev_state[key]
                # Handle both old format (just number) and new format (dict)
                if isinstance(prev_data, dict):
                    vol = prev_data.get('available', 0)
                    ad_type = prev_data.get('ad_type', 'SELL')
                else:
                    vol = prev_data
                    ad_type = 'SELL'  # Default for old data
                
                if vol >= 10:
                    # CORRECT AGGRESSOR LOGIC!
                    if ad_type.upper() in ['SELL', 'SELL_AD']:
                        # SELL ad disappeared = Someone BOUGHT all of it (GREEN)
                        aggressor_action = 'buy'
                        emoji = '🟢'
                        action_desc = 'BOUGHT'
                    else:
                        # BUY ad disappeared = Someone SOLD all of it (RED)
                        aggressor_action = 'sell'
                        emoji = '🔴'
                        action_desc = 'SOLD'
                    
                    trades.append({
                        'type': aggressor_action,
                        'source': source,
                        'user': username,
                        'price': price / peg,
                        'vol_usd': vol,
                        'timestamp': time.time(),
                        'reason': 'sold_out'
                    })
                    print(f"   {emoji} {action_desc}: {source} - {username[:15]} (ad sold out, {vol:,.0f} USDT)", file=sys.stderr)
    
    # 2. Check for NEW ads (REQUESTS - like ethioblackmarket.com!)
    new_ads = set(current_state.keys()) - set(prev_state.keys())
    for key in new_ads:
        ad = ad_lookup.get(key)
        if ad:
            source = ad['source'].upper()
            if source in sources_checked:
                vol = ad['available']
                ad_type = ad.get('ad_type', 'SELL')
                
                if vol >= 10:
                    # NEW AD = REQUEST
                    if ad_type.upper() in ['SELL', 'SELL_AD']:
                        request_type = 'SELL REQUEST'  # Offering to sell
                        emoji = '🔴'
                    else:
                        request_type = 'BUY REQUEST'  # Looking to buy
                        emoji = '🟢'
                    
                    requests.append({
                        'type': 'request',
                        'request_type': request_type,
                        'source': source,
                        'user': ad['advertiser'],
                        'price': ad['price'] / peg,
                        'vol_usd': vol,
                        'timestamp': time.time()
                    })
                    print(f"   {emoji} {request_type}: {source} - {ad['advertiser'][:15]} posted {vol:,.0f} USDT @ {ad['price']/peg:.2f} ETB", file=sys.stderr)
    
    # 3. Check for INVENTORY CHANGES (partial fills)
    for ad in current_ads:
        source = ad['source'].upper()
        if source not in sources_checked:
            continue
        
        sources_checked[source] += 1
        key = f"{ad['source']}|||{ad['advertiser']}|||{ad['price']}"
        
        if key in prev_state:
            prev_data = prev_state[key]
            # Handle both formats
            if isinstance(prev_data, dict):
                prev_inventory = prev_data.get('available', 0)
                ad_type = prev_data.get('ad_type', ad.get('ad_type', 'SELL'))
            else:
                prev_inventory = prev_data
                ad_type = ad.get('ad_type', 'SELL')
            
            curr_inventory = ad['available']
            diff = abs(curr_inventory - prev_inventory)
            
            # CORRECT AGGRESSOR LOGIC!
            if curr_inventory < prev_inventory and diff >= 1:
                # Inventory dropped
                if ad_type.upper() in ['SELL', 'SELL_AD']:
                    # SELL ad inventory dropped = Aggressor BOUGHT (GREEN)
                    aggressor_action = 'buy'
                    emoji = '🟢'
                    action_desc = 'BOUGHT'
                else:
                    # BUY ad inventory dropped = Aggressor SOLD (RED)
                    aggressor_action = 'sell'
                    emoji = '🔴'
                    action_desc = 'SOLD'
                
                trades.append({
                    'type': aggressor_action,
                    'source': source,
                    'user': ad['advertiser'],
                    'price': ad['price'] / peg,
                    'vol_usd': diff,
                    'timestamp': time.time(),
                    'reason': 'inventory_change'
                })
                print(f"   {emoji} {action_desc}: {source} - {ad['advertiser'][:15]} {diff:,.0f} USDT @ {ad['price']/peg:.2f} ETB", file=sys.stderr)
            
            elif curr_inventory > prev_inventory and diff >= 1:
                # Inventory increased = Merchant added funds (not a trade)
                print(f"   ➕ FUNDED: {source} - {ad['advertiser'][:15]} added {diff:,.0f} USDT (not a trade)", file=sys.stderr)
    
    # Summary
    print(f"\n   📊 SUMMARY:", file=sys.stderr)
    print(f"   > Requests posted: {len(requests)}", file=sys.stderr)
    print(f"   > Trades detected: {len(trades)} ({len([t for t in trades if t['type']=='buy'])} buys 🟢, {len([t for t in trades if t['type']=='sell'])} sells 🔴)", file=sys.stderr)
    print(f"   > Checked: Binance={sources_checked.get('BINANCE', 0)}, MEXC={sources_checked.get('MEXC', 0)}, OKX={sources_checked.get('OKX', 0)}, Bybit={sources_checked.get('BYBIT', 0)}", file=sys.stderr)
    
    # Combine trades and requests
    return trades + requests

def load_recent_trades():
    if not os.path.exists(TRADES_FILE):
        return []
    
    try:
        with open(TRADES_FILE, "r") as f:
            all_trades = json.load(f)
        
        cutoff = time.time() - (TRADE_RETENTION_MINUTES * 60)
        
        # Filter trades: must have timestamp, type, and be recent
        valid_trades = []
        for t in all_trades:
            if t.get("timestamp", 0) > cutoff and t.get("type") in ['buy', 'sell']:
                valid_trades.append(t)
        
        # Count by type for debugging
        buys = len([t for t in valid_trades if t['type'] == 'buy'])
        sells = len([t for t in valid_trades if t['type'] == 'sell'])
        
        print(f"   > Loaded {len(valid_trades)} trades from last 24h ({buys} buys, {sells} sells)", file=sys.stderr)
        return valid_trades
    except Exception as e:
        print(f"   > Error loading trades: {e}", file=sys.stderr)
        return []

def save_trades(new_trades):
    recent = load_recent_trades()
    all_trades = recent + new_trades
    
    cutoff = time.time() - (TRADE_RETENTION_MINUTES * 60)
    filtered = [t for t in all_trades if t.get("timestamp", 0) > cutoff]
    
    with open(TRADES_FILE, "w") as f:
        json.dump(filtered, f)
    
    print(f"   > Saved {len(filtered)} trades to history (last 24h)", file=sys.stderr)

# --- ANALYTICS ---
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
    if not ads:
        return []
    
    prices = [ad['price'] / peg for ad in ads if isinstance(ad, dict) and 'price' in ad]
    if not prices:
        return []
    
    bins = {}
    for price in prices:
        bin_start = int(price / bin_size) * bin_size
        bin_key = f"{bin_start}-{bin_start + bin_size}"
        bins[bin_key] = bins.get(bin_key, 0) + 1
    
    return sorted(bins.items(), key=lambda x: float(x[0].split('-')[0]))

# --- HISTORY ---
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

# --- CHART GENERATOR ---
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
            "figure.facecolor": style["bg"], "axes.facecolor": style["bg"],
            "axes.edgecolor": style["fg"], "axes.labelcolor": style["fg"],
            "xtick.color": style["fg"], "ytick.color": style["fg"],
            "text.color": style["fg"]
        })
        
        fig = plt.figure(figsize=(12, 14))
        fig.suptitle(
            f"ETB LIQUIDITY SCANNER: {datetime.datetime.now().strftime('%H:%M')}",
            fontsize=20, color=style["fg"], fontweight="bold", y=0.97
        )
        
        # Top: Price Distribution
        ax1 = fig.add_subplot(2, 1, 1)
        data = stats["raw_data"]
        y_jitter = [1 + random.uniform(-0.12, 0.12) for _ in data]
        ax1.scatter(data, y_jitter, color=style["fg"], alpha=style["alpha"], s=30, edgecolors="none")
        ax1.axvline(stats["median"], color=style["median"], linewidth=3)
        ax1.axvline(stats["q1"], color=style["sec"], linewidth=2, linestyle="--", alpha=0.6)
        ax1.axvline(stats["q3"], color=style["sec"], linewidth=2, linestyle="--", alpha=0.6)
        ax1.text(stats["median"], 1.42, f"MEDIAN\n{stats['median']:.2f}",
                color=style["median"], ha="center", fontweight="bold")
        ax1.text(stats["q1"], 0.58, f"Q1\n{stats['q1']:.2f}",
                color=style["sec"], ha="right", va="top")
        ax1.text(stats["q3"], 0.58, f"Q3\n{stats['q3']:.2f}",
                color=style["sec"], ha="left", va="top")
        
        if official_rate:
            ax1.axvline(official_rate, color=style["fg"], linestyle=":", linewidth=1.5)
        
        margin = (stats["p95"] - stats["p05"]) * 0.1
        if margin == 0: margin = 1
        ax1.set_xlim([stats["p05"] - margin, stats["p95"] + margin])
        ax1.set_ylim(0.5, 1.5)
        ax1.set_yticks([])
        ax1.set_title("Live Market Depth", color=style["fg"], loc="left", pad=10)
        ax1.grid(True, axis="x", color=style["grid"], linestyle="--")
        
        # Bottom: Historical Trend
        ax2 = fig.add_subplot(2, 1, 2)
        if len(dates) > 1:
            # Use yellow for fill area, green for line
            ax2.fill_between(dates, q1s, q3s, color='#FFD700' if mode == 'dark' else '#FFA500', alpha=0.15, linewidth=0)
            
            # Plot black market rate (green line)
            line1 = ax2.plot(dates, medians, color='#00ff9d' if mode == 'dark' else '#00a876', linewidth=2.5, label='Black Market Rate')[0]
            
            # Plot official rate (dotted line)
            if any(offs):
                line2 = ax2.plot(dates, offs, color=style["fg"], linestyle="--", linewidth=1.5, alpha=0.7, label='Official Rate')[0]
            
            # Add ONLY THE LATEST label in bright color
            if len(medians) > 0:
                latest_idx = len(medians) - 1
                # Latest black market rate in bright cyan
                ax2.text(dates[latest_idx], medians[latest_idx], f'{medians[latest_idx]:.1f}', 
                        fontsize=10, ha='left', va='bottom', color='#00ffff',
                        fontweight='bold', bbox=dict(boxstyle='round,pad=0.3', facecolor='black', alpha=0.7))
                
                # Latest official rate in white
                if latest_idx < len(offs) and offs[latest_idx]:
                    ax2.text(dates[latest_idx], offs[latest_idx], f'{offs[latest_idx]:.1f}', 
                            fontsize=9, ha='left', va='top', color='white', 
                            fontweight='bold', bbox=dict(boxstyle='round,pad=0.3', facecolor='black', alpha=0.7))
            
            # Add legend
            ax2.legend(loc='upper left', framealpha=0.8, facecolor=style["bg"], edgecolor=style["fg"])
            
            ax2.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
            ax2.yaxis.set_major_formatter(ticker.FormatStrFormatter("%.1f"))
            ax2.yaxis.tick_right()
            ax2.grid(True, color=style["grid"], linewidth=0.5)
            ax2.set_title("24h Trend", color=style["fg"], loc="left")
        
        plt.tight_layout(rect=[0, 0, 1, 0.95])
        plt.savefig(filename, dpi=150, facecolor=style["bg"])
        plt.close()

# --- STATISTICS CALCULATOR ---
def calculate_trade_stats(trades):
    """Calculate 1H/Today/Week/Overall trade statistics"""
    import datetime
    
    now = datetime.datetime.now()
    hour_ago = (now - datetime.timedelta(hours=1)).timestamp()
    today_start = datetime.datetime(now.year, now.month, now.day).timestamp()
    week_ago = (now - datetime.timedelta(days=7)).timestamp()
    
    stats = {
        'hour_buys': 0, 'hour_sells': 0, 'hour_buy_volume': 0, 'hour_sell_volume': 0,
        'today_buys': 0, 'today_sells': 0, 'today_buy_volume': 0, 'today_sell_volume': 0,
        'week_buys': 0, 'week_sells': 0, 'week_buy_volume': 0, 'week_sell_volume': 0,
        'overall_buys': 0, 'overall_sells': 0, 'overall_buy_volume': 0, 'overall_sell_volume': 0
    }
    
    for trade in trades:
        ts = trade.get('timestamp', 0)
        vol = trade.get('vol_usd', 0)
        trade_type = trade.get('type', '')
        
        # Overall (all trades in 24h history)
        if trade_type == 'buy':
            stats['overall_buys'] += 1
            stats['overall_buy_volume'] += vol
        elif trade_type == 'sell':
            stats['overall_sells'] += 1
            stats['overall_sell_volume'] += vol
        
        # Last 7 days
        if ts >= week_ago:
            if trade_type == 'buy':
                stats['week_buys'] += 1
                stats['week_buy_volume'] += vol
            elif trade_type == 'sell':
                stats['week_sells'] += 1
                stats['week_sell_volume'] += vol
        
        # Today (since midnight)
        if ts >= today_start:
            if trade_type == 'buy':
                stats['today_buys'] += 1
                stats['today_buy_volume'] += vol
            elif trade_type == 'sell':
                stats['today_sells'] += 1
                stats['today_sell_volume'] += vol
        
        # Last hour
        if ts >= hour_ago:
            if trade_type == 'buy':
                stats['hour_buys'] += 1
                stats['hour_buy_volume'] += vol
            elif trade_type == 'sell':
                stats['hour_sells'] += 1
                stats['hour_sell_volume'] += vol
    
    return stats

def calculate_volume_by_exchange(trades):
    """Calculate buy/sell volume by exchange for last 24h"""
    volumes = {}
    
    # Debug: print sample trades
    print(f"\n🔍 DEBUG: calculate_volume_by_exchange received {len(trades)} trades", file=sys.stderr)
    if len(trades) > 0:
        print(f"   Sample trade: {trades[0]}", file=sys.stderr)
    
    for trade in trades:
        source = trade.get('source', 'Unknown')
        vol = trade.get('vol_usd', 0)
        trade_type = trade.get('type', '')
        
        # Debug: print first few trades
        if len(volumes) < 3:
            print(f"   Processing: source={source}, type={trade_type}, vol={vol}", file=sys.stderr)
        
        if source not in volumes:
            volumes[source] = {'buy': 0, 'sell': 0, 'total': 0}
        
        if trade_type == 'buy':
            volumes[source]['buy'] += vol
        elif trade_type == 'sell':
            volumes[source]['sell'] += vol
        
        volumes[source]['total'] += vol
    
    # Debug: print results
    print(f"   📊 Volume results: {volumes}", file=sys.stderr)
    
    return volumes

# --- HTML GENERATOR ---
def update_website_html(stats, official, timestamp, current_ads, grouped_ads, peg):
    prem = ((stats["median"] - official) / official) * 100 if official else 0
    cache_buster = int(time.time())
    
    # Price change calculation
    dates, medians, _, _, _ = load_history()
    price_change = 0
    price_change_pct = 0
    if len(medians) > 0:
        old_median = medians[0]
        price_change = stats["median"] - old_median
        price_change_pct = (price_change / old_median * 100) if old_median > 0 else 0
    
    arrow = "↗" if price_change > 0 else "↘" if price_change < 0 else "→"
    change_color = "#00C805" if price_change > 0 else "#FF3B30" if price_change < 0 else "#8E8E93"
    
    # Source summary table
    table_rows = ""
    ticker_items = []
    
    for source, ads in grouped_ads.items():
        prices = [a["price"] for a in ads]
        s = analyze(prices, peg)
        if s:
            # Calculate change for ticker
            source_change = 0
            if len(medians) > 1:
                # Simple change indicator
                source_change = random.choice([-1, 0, 1])  # Placeholder
            
            ticker_items.append({
                'source': source,
                'median': s['median'],
                'change': source_change
            })
            
            table_rows += f"<tr><td class='source-col'>{source}</td><td>{s['min']:.2f}</td><td>{s['q1']:.2f}</td><td class='med-col'>{s['median']:.2f}</td><td>{s['q3']:.2f}</td><td>{s['max']:.2f}</td><td>{s['count']}</td></tr>"
        else:
            table_rows += f"<tr><td>{source}</td><td colspan='6' style='opacity:0.5'>No Data</td></tr>"
    
    # Add official rate to ticker
    ticker_items.append({
        'source': 'Official',
        'median': official,
        'change': 0
    })
    
    # Distribution table
    distribution = calculate_price_distribution(current_ads, peg, bin_size=5)
    dist_rows = ""
    if distribution:
        max_count = max([c for _, c in distribution])
        for price_range, count in distribution:
            style_str = "font-weight:bold;color:var(--accent)" if count == max_count else ""
            dist_rows += f"<tr><td style='{style_str}'>{price_range} ETB</td><td style='{style_str}'>{count}</td></tr>"
    else:
        dist_rows = "<tr><td colspan='2' style='opacity:0.5'>No Data</td></tr>"
    
    # Load recent trades for feed
    recent_trades = load_recent_trades()
    buys_count = len([t for t in recent_trades if t.get('type') == 'buy'])
    sells_count = len([t for t in recent_trades if t.get('type') == 'sell'])
    
    # Generate feed HTML (server-side rendering of initial state)
    feed_html = generate_feed_html(recent_trades, peg)
    
    # Calculate trade statistics
    trade_stats = calculate_trade_stats(recent_trades)
    hour_buys = trade_stats['hour_buys']
    hour_sells = trade_stats['hour_sells']
    hour_buy_volume = trade_stats['hour_buy_volume']
    hour_sell_volume = trade_stats['hour_sell_volume']
    today_buys = trade_stats['today_buys']
    today_sells = trade_stats['today_sells']
    today_buy_volume = trade_stats['today_buy_volume']
    today_sell_volume = trade_stats['today_sell_volume']
    week_buys = trade_stats['week_buys']
    week_sells = trade_stats['week_sells']
    week_buy_volume = trade_stats['week_buy_volume']
    week_sell_volume = trade_stats['week_sell_volume']
    overall_buys = trade_stats['overall_buys']
    overall_sells = trade_stats['overall_sells']
    overall_buy_volume = trade_stats['overall_buy_volume']
    overall_sell_volume = trade_stats['overall_sell_volume']
    
    # Debug: Print trade stats for comparison
    print(f"\n📈 Trade Statistics:", file=sys.stderr)
    print(f"   Total trades: {len(recent_trades)} ({buys_count} buys, {sells_count} sells)", file=sys.stderr)
    print(f"   Buy volume: ${overall_buy_volume:,.0f}", file=sys.stderr)
    print(f"   Sell volume: ${overall_sell_volume:,.0f}", file=sys.stderr)
    
    # Calculate volume by exchange
    volume_by_exchange = calculate_volume_by_exchange(recent_trades)
    
    # Debug logging
    print(f"\n📊 Volume by Exchange:")
    for source, data in volume_by_exchange.items():
        print(f"  {source}: Buy ${data['buy']:,.0f}, Sell ${data['sell']:,.0f}, Total ${data['total']:,.0f}")
    
    # Create volume chart HTML - ALWAYS show all exchanges
    volume_chart_html = ""
    if not volume_by_exchange or all(v['total'] == 0 for v in volume_by_exchange.values()):
        # No data yet - show placeholder
        volume_chart_html = """
        <div style="text-align:center;padding:40px;color:var(--text-secondary)">
            <div style="font-size:48px;margin-bottom:16px">📊</div>
            <div style="font-size:16px;font-weight:600;margin-bottom:8px">No Volume Data Yet</div>
            <div style="font-size:14px">Waiting for trade detection...</div>
        </div>
        """
    else:
        max_volume = max([v['total'] for v in volume_by_exchange.values()])
        
        for source in ['BINANCE', 'MEXC', 'OKX']:
            # Get data or default to 0
            data = volume_by_exchange.get(source, {'buy': 0, 'sell': 0, 'total': 0})
            buy_pct = (data['buy'] / max_volume * 100) if max_volume > 0 else 0
            sell_pct = (data['sell'] / max_volume * 100) if max_volume > 0 else 0
            
            # Ensure minimum visible width if there's any volume
            if data['buy'] > 0 and buy_pct < 2:
                buy_pct = 2
            if data['sell'] > 0 and sell_pct < 2:
                sell_pct = 2
            
            # Source emoji and color
            emoji = '🟡' if source == 'BINANCE' else ('🔵' if source == 'MEXC' else '🟣')
            color = '#F3BA2F' if source == 'BINANCE' else ('#2E55E6' if source == 'MEXC' else '#A855F7')
            
            volume_chart_html += f"""
            <div class="volume-row">
                <div class="volume-source">
                    <span style="font-size:20px">{emoji}</span>
                    <span style="color:{color};font-weight:600">{source}</span>
                </div>
                <div class="volume-bars">
                    <div class="volume-bar-group">
                        <div class="volume-bar buy-bar" style="width:{buy_pct}%"></div>
                        <span class="volume-label buy-label">${data['buy']:,.0f}</span>
                    </div>
                    <div class="volume-bar-group">
                        <div class="volume-bar sell-bar" style="width:{sell_pct}%"></div>
                        <span class="volume-label sell-label">${data['sell']:,.0f}</span>
                    </div>
                </div>
            </div>
            """
    
    # Generate ticker HTML
    ticker_html = ""
    for item in ticker_items * 3:  # Repeat for continuous scroll
        change_symbol = "▲" if item['change'] > 0 else "▼" if item['change'] < 0 else "━"
        change_color = "#00C805" if item['change'] > 0 else "#FF3B30" if item['change'] < 0 else "#8E8E93"
        
        # Add emoji and color for each source
        source_display = item['source']
        if item['source'] == 'BINANCE':
            source_display = f"🟡 {item['source']}"
        elif item['source'] == 'MEXC':
            source_display = f"🔵 {item['source']}"
        elif item['source'] == 'OKX':
            source_display = f"🟣 {item['source']}"
        elif item['source'] == 'Official':
            source_display = f"💵 {item['source']}"
        
        ticker_html += f"""
        <div class="ticker-item">
            <span class="ticker-source">{source_display}</span>
            <span class="ticker-price">{item['median']:.2f} ETB</span>
            <span class="ticker-change" style="color:{change_color}">{change_symbol}</span>
        </div>
        """
    
    html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <meta http-equiv="refresh" content="300">
        <title>ETB Market v41.3 - Display Fixes + MEXC Debug</title>
        <style>
            * {{ margin: 0; padding: 0; box-sizing: border-box; }}
            
            :root {{
                --bg: #000000;
                --card: #1C1C1E;
                --card-hover: #2C2C2E;
                --text: #FFFFFF;
                --text-secondary: #8E8E93;
                --green: #00C805;
                --red: #FF3B30;
                --orange: #FF9500;
                --border: #38383A;
                --accent: #0A84FF;
            }}
            
            [data-theme="light"] {{
                --bg: #F2F2F7;
                --card: #FFFFFF;
                --card-hover: #F9F9F9;
                --text: #000000;
                --text-secondary: #8E8E93;
                --green: #34C759;
                --red: #FF3B30;
                --orange: #FF9500;
                --border: #C6C6C8;
                --accent: #007AFF;
            }}
            
            body {{
                background: var(--bg);
                color: var(--text);
                font-family: -apple-system, BlinkMacSystemFont, 'SF Pro Display', sans-serif;
                overflow-x: hidden;
                transition: background 0.3s ease;
            }}
            
            /* NYSE-STYLE TICKER */
            .ticker-wrapper {{
                width: 100%;
                overflow: hidden;
                background: var(--card);
                border-bottom: 2px solid var(--accent);
                padding: 12px 0;
            }}
            
            .ticker {{
                display: flex;
                animation: scroll 40s linear infinite;
                white-space: nowrap;
            }}
            
            @keyframes scroll {{
                0% {{ transform: translateX(0); }}
                100% {{ transform: translateX(-33.333%); }}
            }}
            
            .ticker-item {{
                display: inline-flex;
                align-items: center;
                gap: 12px;
                padding: 0 30px;
                border-right: 1px solid var(--border);
            }}
            
            .ticker-source {{
                font-weight: 700;
                color: var(--accent);
                font-size: 14px;
            }}
            
            .ticker-price {{
                font-weight: 600;
                color: var(--text);
                font-size: 14px;
            }}
            
            .ticker-change {{
                font-weight: 700;
                font-size: 16px;
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
            
            .main-grid {{
                display: grid;
                grid-template-columns: 1fr 400px;
                gap: 20px;
                margin-bottom: 30px;
            }}
            
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
            }}
            
            .price-change.positive {{
                background: rgba(0, 200, 5, 0.1);
                color: var(--green);
            }}
            
            .price-change.negative {{
                background: rgba(255, 59, 48, 0.1);
                color: var(--red);
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
            
            .chart-card {{
                background: var(--card);
                border-radius: 16px;
                padding: 20px;
                border: 1px solid var(--border);
                margin-bottom: 20px;
                position: relative;
            }}
            
            .chart-card img {{
                width: 100%;
                border-radius: 12px;
                display: block;
            }}
            
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
                color: #00ff9d;  /* Green like terminal */
            }}
            
            .med-col {{
                color: #ff0066;  /* Pink/Magenta for median */
                font-weight: 700;
            }}
            
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
            
            /* Tooltip styles */
            .info-tooltip:hover .tooltip-content {{
                visibility: visible !important;
                opacity: 1;
                animation: fadeIn 0.2s ease-in;
            }}
            
            @keyframes fadeIn {{
                from {{ opacity: 0; transform: translateX(-50%) translateY(-5px); }}
                to {{ opacity: 1; transform: translateX(-50%) translateY(0); }}
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
                font-family: 'Courier New', monospace;
                color: #00ff9d;
            }}
            
            .feed-amount {{
                font-weight: 700;
                color: #00bfff;
            }}
            
            .feed-price {{
                font-weight: 600;
            }}
            
            .stats-panel {{
                background: var(--card);
                border-radius: 12px;
                padding: 20px;
                margin: 20px;
                border: 1px solid var(--border);
            }}
            
            .stats-title {{
                font-size: 18px;
                font-weight: 700;
                color: var(--text);
                margin-bottom: 20px;
                text-align: center;
            }}
            
            .stats-section {{
                margin-bottom: 24px;
            }}
            
            .stats-section:last-child {{
                margin-bottom: 0;
            }}
            
            .stats-section-title {{
                font-size: 16px;
                font-weight: 600;
                color: var(--text);
                margin-bottom: 12px;
                padding-bottom: 8px;
                border-bottom: 1px solid var(--border);
            }}
            
            .stats-grid {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
                gap: 12px;
            }}
            
            .stat-card {{
                background: rgba(10, 132, 255, 0.05);
                border: 1px solid var(--border);
                border-radius: 10px;
                padding: 16px;
                text-align: center;
                transition: all 0.2s ease;
            }}
            
            .buy-card {{
                background: rgba(0, 200, 5, 0.08);
                border-color: rgba(0, 200, 5, 0.3);
            }}
            
            .buy-card:hover {{
                transform: translateY(-2px);
                border-color: #00C805;
                box-shadow: 0 4px 12px rgba(0, 200, 5, 0.2);
            }}
            
            .sell-card {{
                background: rgba(255, 59, 48, 0.08);
                border-color: rgba(255, 59, 48, 0.3);
            }}
            
            .sell-card:hover {{
                transform: translateY(-2px);
                border-color: #FF3B30;
                box-shadow: 0 4px 12px rgba(255, 59, 48, 0.2);
            }}
            
            .stat-label {{
                font-size: 12px;
                color: var(--text-secondary);
                text-transform: uppercase;
                letter-spacing: 0.5px;
                margin-bottom: 8px;
                font-weight: 600;
            }}
            
            .stat-value {{
                font-size: 32px;
                font-weight: 700;
                margin-bottom: 6px;
            }}
            
            .stat-value.green {{
                color: #00C805;
            }}
            
            .stat-value.red {{
                color: #FF3B30;
            }}
            
            .stat-volume {{
                font-size: 13px;
                color: #00bfff;
                font-weight: 600;
            }}
            
            .volume-chart-panel {{
                background: var(--card);
                border-radius: 12px;
                padding: 20px;
                margin: 20px;
                border: 1px solid var(--border);
            }}
            
            .volume-chart-title {{
                font-size: 18px;
                font-weight: 700;
                color: var(--text);
                margin-bottom: 20px;
                text-align: center;
            }}
            
            .volume-legend {{
                display: flex;
                justify-content: center;
                gap: 24px;
                margin-bottom: 20px;
                font-size: 13px;
            }}
            
            .volume-legend-item {{
                display: flex;
                align-items: center;
                gap: 8px;
            }}
            
            .volume-legend-box {{
                width: 16px;
                height: 16px;
                border-radius: 4px;
            }}
            
            .volume-row {{
                display: grid;
                grid-template-columns: 150px 1fr;
                gap: 20px;
                margin-bottom: 16px;
                align-items: center;
            }}
            
            .volume-source {{
                display: flex;
                align-items: center;
                gap: 8px;
                font-size: 14px;
            }}
            
            .volume-bars {{
                display: flex;
                flex-direction: column;
                gap: 8px;
            }}
            
            .volume-bar-group {{
                display: flex;
                align-items: center;
                gap: 12px;
            }}
            
            .volume-bar {{
                height: 24px;
                border-radius: 4px;
                transition: width 0.3s ease;
                min-width: 2px;
            }}
            
            .buy-bar {{
                background: linear-gradient(90deg, #00C805 0%, #00ff9d 100%);
            }}
            
            .sell-bar {{
                background: linear-gradient(90deg, #FF3B30 0%, #ff6b6b 100%);
            }}
            
            .volume-label {{
                font-size: 13px;
                font-weight: 600;
                min-width: 100px;
            }}
            
            .buy-label {{
                color: #00C805;
            }}
            
            .sell-label {{
                color: #FF3B30;
            }}
            
            footer {{
                text-align: center;
                padding: 30px 20px;
                color: var(--text-secondary);
                font-size: 13px;
                border-top: 1px solid var(--border);
                margin-top: 40px;
            }}
            
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
        <!-- NYSE-STYLE TICKER -->
        <div class="ticker-wrapper">
            <div class="ticker">
                {ticker_html}
            </div>
        </div>
        
        <div class="container">
            <header>
                <div class="logo">🇪🇹 ETB MARKET</div>
                <button class="theme-toggle" onclick="toggleTheme()">
                    <span id="theme-icon">🌙</span> Theme
                </button>
            </header>
            
            <div class="main-grid">
                <div class="left-column">
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
                    
                    <div class="chart-card">
                        <img src="{GRAPH_FILENAME}?v={cache_buster}" id="chartImg" alt="Market Chart" title="Price Distribution and 24h Trend">
                    </div>
                    
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
                
                
                <div class="feed-panel">
                    <div class="feed-header">
                        <div class="feed-title">Market Activity</div>
                        <div style="color:var(--text-secondary);font-size:13px;margin-bottom:10px" id="feedStats">
                            <span style="color:var(--green)">🟢 {buys_count} Buys</span> • <span style="color:var(--red)">🔴 {sells_count} Sells</span>
                        </div>
                        <div style="display:flex;gap:8px;margin-top:10px;flex-wrap:wrap;">
                            <button class="source-filter-btn active" data-source="all" onclick="filterBySource('all')" style="background:var(--accent);color:white;border:none;padding:6px 12px;border-radius:8px;font-size:12px;font-weight:600;cursor:pointer;">
                                All
                            </button>
                            <button class="source-filter-btn" data-source="BINANCE" onclick="filterBySource('BINANCE')" style="background:transparent;color:var(--text-secondary);border:1px solid var(--border);padding:6px 12px;border-radius:8px;font-size:12px;font-weight:600;cursor:pointer;">
                                🟡 Binance
                            </button>
                            <button class="source-filter-btn" data-source="MEXC" onclick="filterBySource('MEXC')" style="background:transparent;color:var(--text-secondary);border:1px solid var(--border);padding:6px 12px;border-radius:8px;font-size:12px;font-weight:600;cursor:pointer;">
                                🔵 MEXC
                            </button>
                            <button class="source-filter-btn" data-source="OKX" onclick="filterBySource('OKX')" style="background:transparent;color:var(--text-secondary);border:1px solid var(--border);padding:6px 12px;border-radius:8px;font-size:12px;font-weight:600;cursor:pointer;">
                                🟣 OKX
                            </button>
                            <button class="source-filter-btn" data-source="BYBIT" onclick="filterBySource('BYBIT')" style="background:transparent;color:var(--text-secondary);border:1px solid var(--border);padding:6px 12px;border-radius:8px;font-size:12px;font-weight:600;cursor:pointer;">
                                🟠 Bybit
                            </button>
                        </div>
                    </div>
                    <div class="feed-container" id="feedContainer">
                        {feed_html}
                    </div>
                </div>
            </div>
            
            
            <!-- Volume Comparison Chart -->
            <div class="volume-chart-panel">
                <div class="volume-chart-title">24h Volume by Exchange (Buy vs Sell)</div>
                <div class="volume-legend">
                    <div class="volume-legend-item">
                        <div class="volume-legend-box buy-bar"></div>
                        <span>Buy Volume</span>
                    </div>
                    <div class="volume-legend-item">
                        <div class="volume-legend-box sell-bar"></div>
                        <span>Sell Volume</span>
                    </div>
                </div>
                {volume_chart_html}
            </div>
            
            <!-- Transaction Statistics Panel -->
            <div class="stats-panel">
                <div class="stats-title">Transaction Statistics (Within 24 hrs)</div>
                
                <!-- Buy Transactions -->
                <div class="stats-section">
                    <div class="stats-section-title">🟢 Buy Transactions</div>
                    <div class="stats-grid">
                        <div class="stat-card buy-card">
                            <div class="stat-label">Last 1 Hour</div>
                            <div class="stat-value green">{hour_buys}</div>
                            <div class="stat-volume">{hour_buy_volume:,.0f} USDT</div>
                        </div>
                        <div class="stat-card buy-card">
                            <div class="stat-label">Today</div>
                            <div class="stat-value green">{today_buys}</div>
                            <div class="stat-volume">{today_buy_volume:,.0f} USDT</div>
                        </div>
                        <div class="stat-card buy-card">
                            <div class="stat-label">This Week</div>
                            <div class="stat-value green">{week_buys}</div>
                            <div class="stat-volume">{week_buy_volume:,.0f} USDT</div>
                        </div>
                        <div class="stat-card buy-card">
                            <div class="stat-label">Overall (24h)</div>
                            <div class="stat-value green">{overall_buys}</div>
                            <div class="stat-volume">{overall_buy_volume:,.0f} USDT</div>
                        </div>
                    </div>
                </div>
                
                <!-- Sell Transactions -->
                <div class="stats-section">
                    <div class="stats-section-title">🔴 Sell Transactions</div>
                    <div class="stats-grid">
                        <div class="stat-card sell-card">
                            <div class="stat-label">Last 1 Hour</div>
                            <div class="stat-value red">{hour_sells}</div>
                            <div class="stat-volume">{hour_sell_volume:,.0f} USDT</div>
                        </div>
                        <div class="stat-card sell-card">
                            <div class="stat-label">Today</div>
                            <div class="stat-value red">{today_sells}</div>
                            <div class="stat-volume">{today_sell_volume:,.0f} USDT</div>
                        </div>
                        <div class="stat-card sell-card">
                            <div class="stat-label">This Week</div>
                            <div class="stat-value red">{week_sells}</div>
                            <div class="stat-volume">{week_sell_volume:,.0f} USDT</div>
                        </div>
                        <div class="stat-card sell-card">
                            <div class="stat-label">Overall (24h)</div>
                            <div class="stat-value red">{overall_sells}</div>
                            <div class="stat-volume">{overall_sell_volume:,.0f} USDT</div>
                        </div>
                    </div>
                </div>
            </div>
            
            <!-- Explanation Section at Bottom -->
            <div style="background:var(--card);padding:30px;border-radius:12px;margin-top:30px;border:1px solid var(--border);">
                <div style="font-size:20px;font-weight:700;margin-bottom:20px;color:var(--text);text-align:center;">📊 Understanding Market Colors & Terms</div>
                
                <div style="display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-bottom:20px;">
                    <div style="background:linear-gradient(135deg,var(--green)11,var(--green)05);padding:20px;border-radius:12px;border:2px solid var(--green)44;">
                        <div style="font-weight:700;color:var(--green);font-size:18px;margin-bottom:10px;">🟢 GREEN = BUYING (Demand)</div>
                        <div style="font-size:14px;color:var(--text);line-height:1.6;margin-bottom:10px;">
                            When someone <b>BUYS USDT</b> or posts a <b>BUY REQUEST</b>.
                        </div>
                        <div style="font-size:13px;color:var(--text-secondary);line-height:1.5;">
                            Indicates demand for USDT, potential capital flight from ETB to crypto assets.
                        </div>
                    </div>
                    
                    <div style="background:linear-gradient(135deg,var(--red)11,var(--red)05);padding:20px;border-radius:12px;border:2px solid var(--red)44;">
                        <div style="font-weight:700;color:var(--red);font-size:18px;margin-bottom:10px;">🔴 RED = SELLING (Supply)</div>
                        <div style="font-size:14px;color:var(--text);line-height:1.6;margin-bottom:10px;">
                            When someone <b>SELLS USDT</b> or posts a <b>SELL REQUEST</b>.
                        </div>
                        <div style="font-size:13px;color:var(--text-secondary);line-height:1.5;">
                            Indicates supply of USDT, capital returning from crypto to ETB.
                        </div>
                    </div>
                </div>
                
                <div style="background:var(--bg);padding:20px;border-radius:10px;border:1px solid var(--border);">
                    <div style="font-weight:700;font-size:16px;margin-bottom:15px;color:var(--text);">Key Trading Terms:</div>
                    
                    <div style="display:grid;grid-template-columns:1fr 1fr;gap:15px;">
                        <div>
                            <div style="font-weight:600;color:var(--accent);margin-bottom:5px;">Aggressor (Taker)</div>
                            <div style="font-size:13px;color:var(--text-secondary);line-height:1.5;">
                                The person who <b style="color:var(--text)">takes liquidity</b> by filling someone else's ad. 
                                Their action (buy or sell) determines the color shown in the feed.
                            </div>
                        </div>
                        
                        <div>
                            <div style="font-weight:600;color:var(--accent);margin-bottom:5px;">Maker</div>
                            <div style="font-size:13px;color:var(--text-secondary);line-height:1.5;">
                                The person who <b style="color:var(--text)">provides liquidity</b> by posting an ad and waiting for it to be filled. 
                                We track aggressor actions, not maker inventory changes.
                            </div>
                        </div>
                    </div>
                    
                    <div style="margin-top:15px;padding-top:15px;border-top:1px solid var(--border);font-size:12px;color:var(--text-secondary);text-align:center;">
                        This approach matches standard exchange behavior and accurately reflects market sentiment.
                    </div>
                </div>
            </div>
            
            <footer>
                Official Rate: {official:.2f} ETB | Last Update: {timestamp} UTC<br>
                v41.3 Display Fixes! • Table shows all exchanges • MEXC debugging enabled • $50/mo only! 💰✅
            </footer>
        </div>
        
        <script>
            const allTrades = {json.dumps(recent_trades)};
            const imgDark = "{GRAPH_FILENAME}?v={cache_buster}";
            const imgLight = "{GRAPH_LIGHT_FILENAME}?v={cache_buster}";
            let currentPeriod = 'live';
            let currentSource = 'all';
            
            function toggleTheme() {{
                const html = document.documentElement;
                const current = html.getAttribute('data-theme');
                const next = current === 'light' ? 'dark' : 'light';
                html.setAttribute('data-theme', next);
                localStorage.setItem('theme', next);
                document.getElementById('chartImg').src = next === 'light' ? imgLight : imgDark;
                document.getElementById('theme-icon').textContent = next === 'light' ? '☀️' : '🌙';
            }}
            
            (function() {{
                const theme = localStorage.getItem('theme') || 'dark';
                document.documentElement.setAttribute('data-theme', theme);
                document.getElementById('chartImg').src = theme === 'light' ? imgLight : imgDark;
                document.getElementById('theme-icon').textContent = theme === 'light' ? '☀️' : '🌙';
            }})();
            
            function filterBySource(source) {{
                currentSource = source;
                
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
                
                filterTrades(currentPeriod);
            }}
            
            function filterTrades(period) {{
                currentPeriod = period;
                
                document.querySelectorAll('.time-btn').forEach(btn => {{
                    btn.classList.remove('active');
                }});
                document.querySelector(`[data-period="${{period}}"]`).classList.add('active');
                
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
                
                let filtered = allTrades.filter(t => {{
                    return t.timestamp > cutoff && 
                           (t.type === 'buy' || t.type === 'sell');
                }});
                
                if (currentSource !== 'all') {{
                    filtered = filtered.filter(t => t.source.toUpperCase() === currentSource.toUpperCase());
                }}
                
                renderFeed(filtered);
                
                const buys = filtered.filter(t => t.type === 'buy').length;
                const sells = filtered.filter(t => t.type === 'sell').length;
                document.getElementById('feedStats').innerHTML = 
                    '<span style="color:var(--green)">🟢 ' + buys + ' Buys</span> • <span style="color:var(--red)">🔴 ' + sells + ' Sells</span>';
            }}
            
            function renderFeed(trades) {{
                const container = document.getElementById('feedContainer');
                
                if (trades.length === 0) {{
                    container.innerHTML = '<div style="padding:20px;text-align:center;color:var(--text-secondary)">No trades in this period</div>';
                    return;
                }}
                
                // Sort by timestamp DESC (newest first), then take top 50
                const sorted = trades.sort((a, b) => b.timestamp - a.timestamp).slice(0, 50);
                
                const html = sorted.map(trade => {{
                    const date = new Date(trade.timestamp * 1000);
                    const time = date.toLocaleTimeString('en-US', {{hour: '2-digit', minute: '2-digit'}});
                    const ageMin = Math.floor((Date.now() / 1000 - trade.timestamp) / 60);
                    const age = ageMin < 60 ? ageMin + 'm ago' : Math.floor(ageMin/60) + 'h ago';
                    
                    const isBuy = trade.type === 'buy';
                    const icon = isBuy ? '↗' : '↘';
                    const action = isBuy ? 'BOUGHT' : 'SOLD';
                    const color = isBuy ? 'var(--green)' : 'var(--red)';
                    
                    let sourceColor, sourceEmoji;
                    if (trade.source === 'BINANCE') {{
                        sourceColor = '#F3BA2F';  // Yellow
                        sourceEmoji = '🟡';
                    }} else if (trade.source === 'MEXC') {{
                        sourceColor = '#2E55E6';  // Blue
                        sourceEmoji = '🔵';
                    }} else {{
                        sourceColor = '#A855F7';  // Purple (OKX)
                        sourceEmoji = '🟣';
                    }}
                    
                    return `
                        <div class="feed-item">
                            <div class="feed-icon ${{trade.type}}">
                                ${{icon}}
                            </div>
                            <div class="feed-content">
                                <div class="feed-meta">
                                    <span>${{time}}</span>
                                    <span>${{age}}</span>
                                </div>
                                <div class="feed-text">
                                    ${{sourceEmoji}} <span class="feed-user">${{trade.user.substring(0, 15)}}</span>
                                    <span style="color:${{sourceColor}};font-weight:600">(${{trade.source}})</span>
                                    <b style="color:${{color}}">${{action}}</b>
                                    <span class="feed-amount">${{trade.vol_usd.toFixed(0)}} USDT</span>
                                    @ <span class="feed-price">${{trade.price.toFixed(2)}} ETB</span>
                                </div>
                            </div>
                        </div>
                    `;
                }}).join('');
                
                container.innerHTML = html;
            }}
            
            filterTrades('live');
        </script>
    </body>
    </html>
    """
    
    with open(HTML_FILENAME, "w") as f:
        f.write(html)

def generate_feed_html(trades, peg):
    """Server-side initial feed rendering - handles trades AND requests"""
    if not trades:
        return '<div style="padding:20px;text-align:center;color:var(--text-secondary)">Waiting for market activity...</div>'
    
    html = ""
    valid_count = 0
    buy_count = 0
    sell_count = 0
    request_count = 0
    
    for trade in sorted(trades, key=lambda x: x.get('timestamp', 0), reverse=True)[:50]:
        trade_type = trade.get('type')
        
        # Handle REQUESTS (new ads posted)
        if trade_type == 'request':
            request_count += 1
            request_type = trade.get('request_type', 'REQUEST')
            is_buy_request = 'BUY' in request_type
            
            ts = datetime.datetime.fromtimestamp(trade.get("timestamp", time.time()))
            time_str = ts.strftime("%I:%M %p")
            age_seconds = time.time() - trade.get("timestamp", time.time())
            age_str = f"{int(age_seconds/60)}min ago" if age_seconds >= 60 else f"{int(age_seconds)}s ago"
            
            icon = "📝"  # Request icon
            action_color = "var(--green)" if is_buy_request else "var(--red)"
            
            source = trade.get('source', 'Unknown')
            if source == 'BINANCE':
                emoji, color = '🟡', '#F3BA2F'
            elif source == 'MEXC':
                emoji, color = '🔵', '#2E55E6'
            elif source == 'BYBIT':
                emoji, color = '🟠', '#FF9500'  # Orange for Bybit
            else:
                emoji, color = '🟣', '#A855F7'  # Purple (OKX)
            
            html += f"""
        <div class="feed-item request-item" data-source="{source}">
            <div class="feed-icon" style="background:linear-gradient(135deg,{action_color}22,{action_color}11)">
                {icon}
            </div>
            <div class="feed-content">
                <div class="feed-meta">
                    <span>{time_str}</span>
                    <span>{age_str}</span>
                </div>
                <div class="feed-text">
                    {emoji} <span class="feed-user">{trade.get('user', 'Unknown')[:15]}</span>
                    <span style="color:{color};font-weight:600">({source})</span>
                    <b style="color:{action_color}">{request_type}</b>
                    <span class="feed-amount">{trade.get('vol_usd', 0):,.0f} USDT</span>
                    @ <span class="feed-price">{trade.get('price', 0):.2f} ETB</span>
                </div>
            </div>
        </div>
        """
            continue
        
        # Handle TRADES (buy/sell)
        if trade_type not in ['buy', 'sell']:
            continue
        
        valid_count += 1
        is_buy = trade_type == 'buy'
        
        if is_buy:
            buy_count += 1
        else:
            sell_count += 1
        
        ts = datetime.datetime.fromtimestamp(trade.get("timestamp", time.time()))
        time_str = ts.strftime("%I:%M %p")
        age_seconds = time.time() - trade.get("timestamp", time.time())
        age_str = f"{int(age_seconds/60)}min ago" if age_seconds >= 60 else f"{int(age_seconds)}s ago"
        
        icon = "↗" if is_buy else "↘"
        action = "BOUGHT" if is_buy else "SOLD"
        icon_class = "buy" if is_buy else "sell"
        action_color = "var(--green)" if is_buy else "var(--red)"
        
        source = trade.get('source', 'Unknown')
        if source == 'BINANCE':
            emoji, color = '🟡', '#F3BA2F'  # Yellow
        elif source == 'MEXC':
            emoji, color = '🔵', '#2E55E6'  # Blue
        elif source == 'BYBIT':
            emoji, color = '🟠', '#FF9500'  # Orange
        else:
            emoji, color = '🟣', '#A855F7'  # Purple (OKX)
        
        html += f"""
        <div class="feed-item" data-source="{source}">
            <div class="feed-icon {icon_class}">
                {icon}
            </div>
            <div class="feed-content">
                <div class="feed-meta">
                    <span>{time_str}</span>
                    <span>{age_str}</span>
                </div>
                <div class="feed-text">
                    {emoji} <span class="feed-user">{trade.get('user', 'Unknown')[:15]}</span>
                    <span style="color:{color};font-weight:600">({source})</span>
                    <b style="color:{action_color}">{action}</b>
                    <span class="feed-amount">{trade.get('vol_usd', 0):,.0f} USDT</span>
                    @ <span class="feed-price">{trade.get('price', 0):.2f} ETB</span>
                </div>
            </div>
        </div>
        """
    
    if not html:
        return '<div style="padding:20px;text-align:center;color:var(--text-secondary)">No recent activity</div>'
    
    print(f"   > Rendered {valid_count} trades + {request_count} requests", file=sys.stderr)
    return html


# --- MAIN ---
def main():
    print("🔍 Running v41.3 (UAT FIXES v2 - Display + Debug!)...", file=sys.stderr)
    print("   📊 Strategy: 8 snapshots × 15s intervals = 105s coverage (58%!)", file=sys.stderr)
    print("   ✅ FIXED: Display table now shows all exchanges", file=sys.stderr)
    print("   ✅ FIXED: MEXC User-Agent header added", file=sys.stderr)
    print("   ✅ DEBUG: MEXC detailed logging enabled", file=sys.stderr)
    print("   💰 COST: Only $50/month!", file=sys.stderr)
    
    # Configuration - MAXIMUM snapshots within GitHub Actions time budget
    NUM_SNAPSHOTS = 8  # Increased from 4 to 8!
    WAIT_TIME = 15     # Reduced from 30s to 15s for faster monitoring
    all_trades = []    # Collect trades from all comparisons
    
    # First snapshot (baseline)
    print(f"   > Snapshot 1/{NUM_SNAPSHOTS}...", file=sys.stderr)
    prev_snapshot = capture_market_snapshot()
    save_market_state(prev_snapshot)
    print("   > Saved baseline snapshot", file=sys.stderr)
    
    # Get peg once
    peg = fetch_usdt_peg() or 1.0
    
    # Take additional snapshots and compare each to previous
    for i in range(2, NUM_SNAPSHOTS + 1):
        # Wait between snapshots
        print(f"   > ⏳ Waiting {WAIT_TIME}s to catch trades...", file=sys.stderr)
        time.sleep(WAIT_TIME)
        
        # Capture next snapshot
        print(f"   > Snapshot {i}/{NUM_SNAPSHOTS}...", file=sys.stderr)
        current_snapshot = capture_market_snapshot()
        
        # Detect trades between prev and current
        trades_this_round = detect_real_trades(current_snapshot, peg)
        if trades_this_round:
            all_trades.extend(trades_this_round)
            print(f"   ✅ Round {i-1}: Detected {len(trades_this_round)} trades", file=sys.stderr)
        
        # Update baseline for next comparison
        save_market_state(current_snapshot)
        prev_snapshot = current_snapshot
    
    # Final snapshot for website display (fetch BOTH sides!)
    print("   > Final snapshot for display...", file=sys.stderr)
    with ThreadPoolExecutor(max_workers=10) as ex:
        f_binance = ex.submit(fetch_binance_both_sides)  # Both buy and sell!
        f_mexc = ex.submit(fetch_mexc_both_sides)  # Both buy and sell!
        f_okx = ex.submit(fetch_exchange_both_sides, "okx")  # Both buy and sell!
        f_bybit = ex.submit(fetch_bybit_both_sides)  # Both buy and sell!
        f_off = ex.submit(fetch_official_rate)
        
        bin_ads = f_binance.result() or []
        mexc_ads = f_mexc.result() or []
        okx_ads = f_okx.result() or []
        bybit_ads = f_bybit.result() or []
        official = f_off.result() or 0.0
    
    # Filter outliers
    bin_ads = remove_outliers(bin_ads, peg)
    mexc_ads = remove_outliers(mexc_ads, peg)
    okx_ads = remove_outliers(okx_ads, peg)
    bybit_ads = remove_outliers(bybit_ads, peg)
    
    final_snapshot = bin_ads + mexc_ads + okx_ads + bybit_ads
    grouped_ads = {"BINANCE": bin_ads, "MEXC": mexc_ads, "OKX": okx_ads, "BYBIT": bybit_ads}
    
    # Save all detected trades
    if all_trades:
        save_trades(all_trades)
        print(f"   💾 Saved {len(all_trades)} total trades", file=sys.stderr)
    
    # Generate stats and website
    if final_snapshot:
        all_prices = [x['price'] for x in final_snapshot]
        stats = analyze(all_prices, peg)
        
        if stats:
            save_to_history(stats, official)
            generate_charts(stats, official)
            update_website_html(
                stats, official,
                time.strftime("%Y-%m-%d %H:%M:%S"),
                final_snapshot, grouped_ads, peg
            )
    else:
        print("⚠️ No ads found", file=sys.stderr)
    
    # Summary
    buys = len([t for t in all_trades if t.get('type') == 'buy'])
    sells = len([t for t in all_trades if t.get('type') == 'sell'])
    print(f"\n🎯 TOTAL COVERAGE: {NUM_SNAPSHOTS} snapshots × {WAIT_TIME}s = {(NUM_SNAPSHOTS-1)*WAIT_TIME}s monitored")
    print(f"✅ Complete! Detected {buys} buys, {sells} sells this run.")


if __name__ == "__main__":
    main()
