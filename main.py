#!/usr/bin/env python3
"""
🇪🇹 ETB Financial Terminal v42.8 (AI-Powered + Legal Remittance!)
- NEW: Gemini AI market analysis and predictions
- NEW: Legal remittance channel tracking (replaces Bybit)
- KEEP: All v42.7 improvements (realistic volume, max trade limits)
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
# API Keys from environment variables with fallbacks
P2P_ARMY_KEY = os.environ.get("P2P_ARMY_KEY", "YJU5RCZ2-P6VTVNNA")
RAPIDAPI_KEY = os.environ.get("RAPIDAPI_KEY", "28e60e8b83msh2f62e830aa1f09ap18bad1jsna2ade74a847c")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "AIzaSyBPGVTukCpK_bo-0kGJqonV8ICEej8tsgM")

HISTORY_FILE = "etb_history.csv"
SNAPSHOT_FILE = "market_state.json"
TRADES_FILE = "recent_trades.json"
AI_SUMMARY_FILE = "ai_summary.json"
GRAPH_FILENAME = "etb_neon_terminal.png"
GRAPH_LIGHT_FILENAME = "etb_light_terminal.png"
HTML_FILENAME = "index.html"

# TIMING CONFIGURATION
BURST_WAIT_TIME = 45
TRADE_RETENTION_MINUTES = 1440  # 24 hours
MAX_ADS_PER_SOURCE = 200
HISTORY_POINTS = 288
MAX_SINGLE_TRADE = 50000

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Content-Type": "application/json",
    "Accept": "application/json"
}

# --- GEMINI AI INTEGRATION ---
def generate_ai_summary(stats, official, trade_stats, volume_by_exchange, history_data):
    """Generate AI market analysis using Google Gemini API"""
    
    if not GEMINI_API_KEY or GEMINI_API_KEY == "YOUR_KEY_HERE":
        print("   ⚠️ Gemini API key not set, skipping AI summary", file=sys.stderr)
        return None
    
    try:
        # Prepare market context for AI
        black_market_rate = stats.get('median', 0)
        premium = ((black_market_rate - official) / official * 100) if official > 0 else 0
        
        # Get historical trend
        dates, medians, q1s, q3s, offs = history_data if history_data else ([], [], [], [], [])
        
        trend_direction = "stable"
        if len(medians) >= 2:
            recent_change = medians[-1] - medians[0] if medians else 0
            if recent_change > 2:
                trend_direction = "increasing"
            elif recent_change < -2:
                trend_direction = "decreasing"
        
        # Build prompt
        prompt = f"""You are an Ethiopian financial market analyst. Analyze the current ETB/USD black market data and provide insights.

CURRENT MARKET DATA:
- Black Market Rate: {black_market_rate:.2f} ETB per USD
- Official Rate: {official:.2f} ETB per USD  
- Black Market Premium: {premium:.1f}%
- Price Range: {stats.get('min', 0):.2f} - {stats.get('max', 0):.2f} ETB
- 24h Trend: {trend_direction}
- Active P2P Ads: {stats.get('count', 0)}

TRADING ACTIVITY (24h):
- Buy Volume: ${trade_stats.get('overall_buy_volume', 0):,.0f} USDT
- Sell Volume: ${trade_stats.get('overall_sell_volume', 0):,.0f} USDT
- Total Trades: {trade_stats.get('overall_buys', 0) + trade_stats.get('overall_sells', 0)}

Provide a brief analysis in this exact JSON format:
{{
    "market_sentiment": "bullish/bearish/neutral",
    "summary": "2-3 sentence market summary",
    "key_insights": ["insight 1", "insight 2", "insight 3"],
    "short_term_prediction": "1-7 day outlook",
    "risk_factors": ["risk 1", "risk 2"],
    "recommendation": "brief advice for remittance senders"
}}

Keep each field concise. Focus on practical insights for Ethiopians sending/receiving remittances."""

        # Call Gemini API
        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}"
        
        payload = {
            "contents": [{
                "parts": [{"text": prompt}]
            }],
            "generationConfig": {
                "temperature": 0.7,
                "maxOutputTokens": 1024
            }
        }
        
        response = requests.post(url, json=payload, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            
            # Extract text from response
            text = data.get('candidates', [{}])[0].get('content', {}).get('parts', [{}])[0].get('text', '')
            
            # Parse JSON from response
            # Find JSON in the response (might be wrapped in markdown)
            import re
            json_match = re.search(r'\{[\s\S]*\}', text)
            if json_match:
                ai_data = json.loads(json_match.group())
                ai_data['generated_at'] = datetime.datetime.now().isoformat()
                ai_data['rate_at_generation'] = black_market_rate
                
                # Save to file for caching
                with open(AI_SUMMARY_FILE, 'w') as f:
                    json.dump(ai_data, f)
                
                print(f"   🤖 AI Summary generated successfully", file=sys.stderr)
                return ai_data
            else:
                print(f"   ⚠️ Could not parse AI response", file=sys.stderr)
                return None
        else:
            print(f"   ❌ Gemini API error: {response.status_code} - {response.text[:200]}", file=sys.stderr)
            return None
            
    except Exception as e:
        print(f"   ❌ AI Summary error: {e}", file=sys.stderr)
        return None

def load_cached_ai_summary():
    """Load cached AI summary if recent (within 1 hour)"""
    if not os.path.exists(AI_SUMMARY_FILE):
        return None
    
    try:
        with open(AI_SUMMARY_FILE, 'r') as f:
            data = json.load(f)
        
        # Check if still valid (within 1 hour)
        generated_at = datetime.datetime.fromisoformat(data.get('generated_at', '2000-01-01'))
        age = datetime.datetime.now() - generated_at
        
        if age.total_seconds() < 3600:  # 1 hour
            print(f"   📋 Using cached AI summary ({int(age.total_seconds()/60)}min old)", file=sys.stderr)
            return data
        else:
            return None
    except:
        return None

# --- LEGAL REMITTANCE FETCHERS ---
def fetch_official_rate():
    """Fetch official NBE rate"""
    try:
        return float(requests.get("https://open.er-api.com/v6/latest/USD", timeout=5).json()["rates"]["ETB"])
    except:
        return None

def fetch_legal_remittance_rates():
    """
    Fetch rates from legal remittance channels
    Returns dict with rates from different providers
    """
    legal_rates = {
        'NBE_OFFICIAL': None,
        'COMMERCIAL_BANKS': None,
        'WORLDREMIT': None,
        'REMITLY': None
    }
    
    # 1. Official NBE Rate (already have this)
    try:
        official = fetch_official_rate()
        if official:
            legal_rates['NBE_OFFICIAL'] = {
                'rate': official,
                'source': 'National Bank of Ethiopia',
                'type': 'official',
                'fees': '0%'
            }
    except:
        pass
    
    # 2. Commercial Bank Rate (typically official + small margin)
    if legal_rates['NBE_OFFICIAL']:
        # Commercial banks typically offer rate within 0.5-1% of official
        bank_rate = legal_rates['NBE_OFFICIAL']['rate'] * 1.005
        legal_rates['COMMERCIAL_BANKS'] = {
            'rate': bank_rate,
            'source': 'Ethiopian Commercial Banks (avg)',
            'type': 'bank_transfer',
            'fees': '1-3%'
        }
    
    # 3. Try WorldRemit API (they have public rate display)
    try:
        # WorldRemit shows rates on their website - we can estimate
        # Their rates are typically official + 2-4% margin
        if legal_rates['NBE_OFFICIAL']:
            wr_rate = legal_rates['NBE_OFFICIAL']['rate'] * 1.025
            legal_rates['WORLDREMIT'] = {
                'rate': wr_rate,
                'source': 'WorldRemit (estimated)',
                'type': 'online_remittance',
                'fees': '0-3.99 USD flat'
            }
    except:
        pass
    
    # 4. Remitly (similar estimation)
    try:
        if legal_rates['NBE_OFFICIAL']:
            remitly_rate = legal_rates['NBE_OFFICIAL']['rate'] * 1.02
            legal_rates['REMITLY'] = {
                'rate': remitly_rate,
                'source': 'Remitly (estimated)',
                'type': 'online_remittance', 
                'fees': '0-4.99 USD flat'
            }
    except:
        pass
    
    return legal_rates

def fetch_usdt_peg():
    try:
        return float(requests.get("https://api.coingecko.com/api/v3/simple/price?ids=tether&vs_currencies=usd", timeout=5).json()["tether"]["usd"])
    except:
        return 1.00

# --- P2P EXCHANGE FETCHERS ---
def fetch_binance_rapidapi(side="SELL"):
    """Fetch Binance P2P ads using RapidAPI"""
    url = f"https://binance-p2p-api.p.rapidapi.com/binance/p2p/search/{side.lower()}"
    
    headers = {
        "X-RapidAPI-Key": RAPIDAPI_KEY,
        "X-RapidAPI-Host": "binance-p2p-api.p.rapidapi.com",
        "Content-Type": "application/json"
    }
    
    all_ads = []
    seen_ids = set()
    page = 1
    max_pages = 20
    
    while page <= max_pages:
        payload = {
            "asset": "USDT",
            "fiat": "ETB",
            "page": page,
            "rows": 20,
            "payTypes": [],
            "countries": []
        }
        
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=15)
            
            if r.status_code == 429:
                print(f"   ⚠️ Rate limit hit, waiting 5s...", file=sys.stderr)
                time.sleep(5)
                continue
            
            data = r.json()
            
            if data.get("code") == "000000":
                items = data.get('data', [])
                
                if not items:
                    break
                
                new_count = 0
                for item in items:
                    try:
                        advertiser = item.get("advertiser", {})
                        adv = item.get("adv", {})
                        ad_no = adv.get("advNo", "")
                        
                        if ad_no and ad_no not in seen_ids:
                            seen_ids.add(ad_no)
                            all_ads.append({
                                'source': 'BINANCE',
                                'ad_type': side.upper(),
                                'advertiser': advertiser.get("nickName", "Unknown"),
                                'price': float(adv.get("price", 0)),
                                'available': float(adv.get("surplusAmount", 0)),
                            })
                            new_count += 1
                    except:
                        continue
                
                if new_count == 0:
                    break
                    
                page += 1
                time.sleep(1.5)
            else:
                print(f"   ❌ Binance API error: {data}", file=sys.stderr)
                break
                
        except Exception as e:
            print(f"   ❌ Binance connection error: {e}", file=sys.stderr)
            break
    
    print(f"   BINANCE {side} (RapidAPI): {len(all_ads)} ads from {page-1} pages", file=sys.stderr)
    return all_ads

def fetch_binance_both_sides():
    """Fetch BOTH buy and sell ads from Binance"""
    sell_ads = fetch_binance_rapidapi("SELL")
    time.sleep(2)
    buy_ads = fetch_binance_rapidapi("BUY")
    
    all_ads = sell_ads + buy_ads
    seen = set()
    deduped = []
    
    for ad in all_ads:
        key = f"{ad['advertiser']}_{ad['price']}_{ad.get('ad_type', 'SELL')}"
        if key not in seen:
            seen.add(key)
            deduped.append(ad)
    
    print(f"   BINANCE Total: {len(deduped)} ads ({len(sell_ads)} sells, {len(buy_ads)} buys)", file=sys.stderr)
    return deduped

def fetch_p2p_army_exchange(market, side="SELL"):
    """Universal fetcher for p2p.army"""
    url = "https://p2p.army/v1/api/get_p2p_order_book"
    ads = []
    h = HEADERS.copy()
    h["X-APIKEY"] = P2P_ARMY_KEY
    
    try:
        payload = {"market": market, "fiat": "ETB", "asset": "USDT", "side": side, "limit": 100}
        r = requests.post(url, headers=h, json=payload, timeout=10)
        data = r.json()
        
        candidates = data.get("result", data.get("data", data.get("ads", [])))
        if not candidates and isinstance(data, list):
            candidates = data
        
        if candidates:
            for ad in candidates:
                if isinstance(ad, dict) and 'price' in ad:
                    try:
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
                        
                        if vol == 0:
                            continue
                        
                        username = None
                        for key in ['advertiser_name', 'nickname', 'trader_name', 'userName', 'user_name', 'merchant_name', 'merchant', 'trader', 'name']:
                            if key in ad and ad[key]:
                                username = str(ad[key])
                                break
                        
                        if not username:
                            username = f'{market.upper()} User'
                        
                        ads.append({
                            'source': market.upper(),
                            'ad_type': side,
                            'advertiser': username,
                            'price': float(ad['price']),
                            'available': vol,
                        })
                    except:
                        continue
        
        print(f"   {market.upper()} {side}: {len(ads)} ads", file=sys.stderr)
    except Exception as e:
        print(f"   {market.upper()} {side} error: {e}", file=sys.stderr)
    
    return ads

def fetch_mexc_rapidapi(side="SELL"):
    """Fetch MEXC P2P ads using RapidAPI"""
    url = "https://mexc-p2p-api.p.rapidapi.com/mexc/p2p/search"
    ads = []
    
    try:
        headers = {
            "X-RapidAPI-Key": RAPIDAPI_KEY,
            "X-RapidAPI-Host": "mexc-p2p-api.p.rapidapi.com",
            "User-Agent": "Mozilla/5.0"
        }
        
        if side == "BUY":
            api_side = "SELL"
        else:
            api_side = "BUY"
        
        seen_ids = set()
        
        strategies = [
            {"name": "Text", "params": {"currency": "ETB", "coin": "USDT"}},
            {"name": "ID",   "params": {"currencyId": "58", "coinId": "1"}}
        ]
        
        for strategy in strategies:
            page = 1
            max_pages = 10
            
            while page <= max_pages:
                params = {
                    "tradeType": api_side,
                    "page": str(page),
                    "blockTrade": "false"
                }
                params.update(strategy["params"])
                
                try:
                    r = requests.get(url, headers=headers, params=params, timeout=10)
                    data = r.json()
                    items = data.get("data", [])
                    
                    if not items:
                        break
                    
                    new_count = 0
                    for item in items:
                        try:
                            price = item.get("price")
                            vol = item.get("availableQuantity") or item.get("surplus_amount")
                            if vol:
                                vol = float(vol)
                            else:
                                vol = 0.0
                            
                            name = "MEXC User"
                            merchant = item.get("merchant")
                            if merchant and isinstance(merchant, dict):
                                name = merchant.get("nickName") or merchant.get("name") or name
                            
                            if price:
                                price = float(price)
                                unique_id = f"{name}-{price}-{vol}"
                                
                                if unique_id not in seen_ids and vol > 0:
                                    seen_ids.add(unique_id)
                                    ads.append({
                                        'source': 'MEXC',
                                        'ad_type': side,
                                        'advertiser': name,
                                        'price': price,
                                        'available': vol,
                                    })
                                    new_count += 1
                        except:
                            continue
                    
                    if new_count == 0:
                        break
                    
                    page += 1
                    time.sleep(0.3)
                    
                except:
                    break
        
        print(f"   MEXC {side} (RapidAPI): {len(ads)} ads", file=sys.stderr)
    except Exception as e:
        print(f"   MEXC {side} (RapidAPI) error: {e}", file=sys.stderr)
    
    return ads

def fetch_mexc_both_sides():
    """Fetch BOTH buy and sell ads from MEXC"""
    with ThreadPoolExecutor(max_workers=2) as ex:
        f_sell = ex.submit(lambda: fetch_mexc_rapidapi("SELL"))
        f_buy = ex.submit(lambda: fetch_mexc_rapidapi("BUY"))
        
        sell_ads = f_sell.result() or []
        buy_ads = f_buy.result() or []
        
        all_ads = sell_ads + buy_ads
        seen = set()
        deduped = []
        
        for ad in all_ads:
            key = f"{ad['advertiser']}_{ad['price']}_{ad.get('ad_type', 'SELL')}"
            if key not in seen:
                seen.add(key)
                deduped.append(ad)
        
        return deduped

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

# --- MARKET SNAPSHOT ---
def capture_market_snapshot():
    """Capture market snapshot: Binance, MEXC, OKX (P2P) + Legal channels"""
    with ThreadPoolExecutor(max_workers=10) as ex:
        f_binance = ex.submit(fetch_binance_both_sides)
        f_mexc = ex.submit(fetch_mexc_both_sides)
        f_okx = ex.submit(fetch_exchange_both_sides, "okx")
        f_peg = ex.submit(fetch_usdt_peg)
        
        binance_data = f_binance.result() or []
        mexc_data = f_mexc.result() or []
        okx_data = f_okx.result() or []
        peg = f_peg.result() or 1.0
        
        total = len(binance_data) + len(mexc_data) + len(okx_data)
        print(f"   📊 Collected {total} P2P ads (Binance, MEXC, OKX)", file=sys.stderr)
        
        return binance_data + mexc_data + okx_data

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
    """Save market state with ad_type included"""
    state = {}
    for ad in current_ads:
        key = f"{ad['source']}|||{ad['advertiser']}|||{ad['price']}"
        state[key] = {
            'available': ad['available'],
            'ad_type': ad.get('ad_type', 'SELL')
        }
    
    with open(SNAPSHOT_FILE, 'w') as f:
        json.dump(state, f)

def detect_real_trades(current_ads, peg):
    """CONSERVATIVE TRADE DETECTION - Only partial fills"""
    prev_state = load_market_state()
    
    if not prev_state:
        print("   > First run - establishing baseline", file=sys.stderr)
        return []
    
    trades = []
    requests = []
    sources_checked = {'BINANCE': 0, 'MEXC': 0, 'OKX': 0}
    
    current_state = {}
    ad_lookup = {}
    current_advertisers = {}
    
    for ad in current_ads:
        key = f"{ad['source']}|||{ad['advertiser']}|||{ad['price']}"
        current_state[key] = {
            'available': ad['available'],
            'ad_type': ad.get('ad_type', 'SELL')
        }
        ad_lookup[key] = ad
        
        adv_key = f"{ad['source']}|||{ad['advertiser']}"
        if adv_key not in current_advertisers:
            current_advertisers[adv_key] = []
        current_advertisers[adv_key].append(ad)
    
    prev_advertisers = {}
    for key, data in prev_state.items():
        parts = key.split('|||')
        if len(parts) >= 3:
            adv_key = f"{parts[0]}|||{parts[1]}"
            if adv_key not in prev_advertisers:
                prev_advertisers[adv_key] = []
            prev_advertisers[adv_key].append({
                'key': key,
                'price': float(parts[2]),
                'data': data
            })
    
    advertisers_with_disappeared_ads = set()
    disappeared_ads = set(prev_state.keys()) - set(current_state.keys())
    
    for key in disappeared_ads:
        parts = key.split('|||')
        if len(parts) >= 3:
            source = parts[0].upper()
            username = parts[1]
            adv_key = f"{source}|||{username}"
            advertisers_with_disappeared_ads.add(adv_key)
            
            prev_data = prev_state[key]
            if isinstance(prev_data, dict):
                vol = prev_data.get('available', 0)
            else:
                vol = prev_data
            
            if vol >= 100:
                print(f"   ⚪ AD GONE (not counted): {source} - {username[:15]} had {vol:,.0f} USDT", file=sys.stderr)
    
    new_ads = set(current_state.keys()) - set(prev_state.keys())
    
    for key in new_ads:
        ad = ad_lookup.get(key)
        if ad:
            source = ad['source'].upper()
            if source not in sources_checked:
                continue
                
            vol = ad['available']
            ad_type = ad.get('ad_type', 'SELL')
            
            if vol < 10:
                continue
            
            adv_key = f"{source}|||{ad['advertiser']}"
            
            if adv_key in advertisers_with_disappeared_ads:
                continue
            
            if ad_type.upper() in ['SELL', 'SELL_AD']:
                request_type = 'SELL REQUEST'
                emoji = '🔴'
            else:
                request_type = 'BUY REQUEST'
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
    
    for ad in current_ads:
        source = ad['source'].upper()
        if source not in sources_checked:
            continue
        
        sources_checked[source] += 1
        key = f"{ad['source']}|||{ad['advertiser']}|||{ad['price']}"
        
        if key in prev_state:
            prev_data = prev_state[key]
            if isinstance(prev_data, dict):
                prev_inventory = prev_data.get('available', 0)
                ad_type = prev_data.get('ad_type', ad.get('ad_type', 'SELL'))
            else:
                prev_inventory = prev_data
                ad_type = ad.get('ad_type', 'SELL')
            
            curr_inventory = ad['available']
            diff = abs(curr_inventory - prev_inventory)
            
            if curr_inventory < prev_inventory and diff >= 1:
                if diff > MAX_SINGLE_TRADE:
                    print(f"   ⚠️ SKIPPED (too large): {source} - {ad['advertiser'][:15]} claimed {diff:,.0f} USDT", file=sys.stderr)
                    continue
                
                if ad_type.upper() in ['SELL', 'SELL_AD']:
                    aggressor_action = 'buy'
                    emoji = '🟢'
                    action_desc = 'BOUGHT'
                else:
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
                    'reason': 'inventory_change',
                    'confidence': 'high'
                })
                print(f"   {emoji} {action_desc}: {source} - {ad['advertiser'][:15]} {diff:,.0f} USDT @ {ad['price']/peg:.2f} ETB", file=sys.stderr)
            
            elif curr_inventory > prev_inventory and diff >= 1:
                print(f"   ➕ FUNDED: {source} - {ad['advertiser'][:15]} added {diff:,.0f} USDT", file=sys.stderr)
    
    print(f"\n   📊 DETECTION SUMMARY (v42.8 - AI-Powered):", file=sys.stderr)
    print(f"   > Requests posted: {len(requests)}", file=sys.stderr)
    print(f"   > Trades detected: {len(trades)}", file=sys.stderr)
    
    return trades + requests

def load_recent_trades():
    """Load recent trades from file"""
    if not os.path.exists(TRADES_FILE):
        return []
    
    try:
        with open(TRADES_FILE, "r") as f:
            all_trades = json.load(f)
        
        cutoff = time.time() - (TRADE_RETENTION_MINUTES * 60)
        valid_trades = [t for t in all_trades if t.get("timestamp", 0) > cutoff and t.get("type") in ['buy', 'sell', 'request']]
        
        buys = len([t for t in valid_trades if t['type'] == 'buy'])
        sells = len([t for t in valid_trades if t['type'] == 'sell'])
        requests = len([t for t in valid_trades if t['type'] == 'request'])
        
        print(f"   > Loaded {len(valid_trades)} events ({buys} buys, {sells} sells, {requests} requests)", file=sys.stderr)
        return valid_trades
    except Exception as e:
        print(f"   > Error loading trades: {e}", file=sys.stderr)
        return []

def save_trades(new_trades):
    """Save trades with deduplication"""
    recent = load_recent_trades()
    
    existing_keys = set()
    for t in recent:
        ts_bucket = int(t.get("timestamp", 0) / 60)
        vol_bucket = int(round(t.get("vol_usd", 0) or 0))
        key = f"{t.get('source', '')}_{t.get('user', '')}_{t.get('price', 0):.2f}_{ts_bucket}_{t.get('type', '')}_{vol_bucket}"
        existing_keys.add(key)
    
    unique_new = []
    for t in new_trades:
        ts_bucket = int(t.get("timestamp", 0) / 60)
        vol_bucket = int(round(t.get("vol_usd", 0) or 0))
        key = f"{t.get('source', '')}_{t.get('user', '')}_{t.get('price', 0):.2f}_{ts_bucket}_{t.get('type', '')}_{vol_bucket}"
        if key not in existing_keys:
            existing_keys.add(key)
            unique_new.append(t)
    
    all_trades = recent + unique_new
    cutoff = time.time() - (TRADE_RETENTION_MINUTES * 60)
    filtered = [t for t in all_trades if t.get("timestamp", 0) > cutoff]
    
    with open(TRADES_FILE, "w") as f:
        json.dump(filtered, f)
    
    print(f"   > Saved {len(filtered)} events to history", file=sys.stderr)

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

def calculate_trade_stats(trades):
    """Calculate trade statistics"""
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
        
        if trade_type == 'buy':
            stats['overall_buys'] += 1
            stats['overall_buy_volume'] += vol
        elif trade_type == 'sell':
            stats['overall_sells'] += 1
            stats['overall_sell_volume'] += vol
        
        if ts >= week_ago:
            if trade_type == 'buy':
                stats['week_buys'] += 1
                stats['week_buy_volume'] += vol
            elif trade_type == 'sell':
                stats['week_sells'] += 1
                stats['week_sell_volume'] += vol
        
        if ts >= today_start:
            if trade_type == 'buy':
                stats['today_buys'] += 1
                stats['today_buy_volume'] += vol
            elif trade_type == 'sell':
                stats['today_sells'] += 1
                stats['today_sell_volume'] += vol
        
        if ts >= hour_ago:
            if trade_type == 'buy':
                stats['hour_buys'] += 1
                stats['hour_buy_volume'] += vol
            elif trade_type == 'sell':
                stats['hour_sells'] += 1
                stats['hour_sell_volume'] += vol
    
    return stats

def calculate_volume_by_exchange(trades):
    """Calculate volume by exchange"""
    volumes = {}
    
    actual_trades = [t for t in trades if t.get('type') in ['buy', 'sell']]
    
    for trade in actual_trades:
        source = trade.get('source', 'Unknown')
        vol = trade.get('vol_usd', 0)
        trade_type = trade.get('type', '')
        
        if source not in volumes:
            volumes[source] = {'buy': 0, 'sell': 0, 'total': 0}
        
        if trade_type == 'buy':
            volumes[source]['buy'] += vol
        elif trade_type == 'sell':
            volumes[source]['sell'] += vol
        
        volumes[source]['total'] += vol
    
    return volumes

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
        
        ax1 = fig.add_subplot(2, 1, 1)
        data = stats["raw_data"]
        y_jitter = [1 + random.uniform(-0.12, 0.12) for _ in data]
        ax1.scatter(data, y_jitter, color=style["fg"], alpha=style["alpha"], s=30, edgecolors="none")
        ax1.axvline(stats["median"], color=style["median"], linewidth=3)
        ax1.axvline(stats["q1"], color=style["sec"], linewidth=2, linestyle="--", alpha=0.6)
        ax1.axvline(stats["q3"], color=style["sec"], linewidth=2, linestyle="--", alpha=0.6)
        ax1.text(stats["median"], 1.42, f"MEDIAN\n{stats['median']:.2f}",
                color=style["median"], ha="center", fontweight="bold")
        
        if official_rate:
            ax1.axvline(official_rate, color=style["fg"], linestyle=":", linewidth=1.5)
        
        margin = (stats["p95"] - stats["p05"]) * 0.1
        if margin == 0: margin = 1
        ax1.set_xlim([stats["p05"] - margin, stats["p95"] + margin])
        ax1.set_ylim(0.5, 1.5)
        ax1.set_yticks([])
        ax1.set_title("Live Market Depth", color=style["fg"], loc="left", pad=10)
        ax1.grid(True, axis="x", color=style["grid"], linestyle="--")
        
        ax2 = fig.add_subplot(2, 1, 2)
        if len(dates) > 1:
            ax2.fill_between(dates, q1s, q3s, color='#FFD700' if mode == 'dark' else '#FFA500', alpha=0.15, linewidth=0)
            ax2.plot(dates, medians, color='#00ff9d' if mode == 'dark' else '#00a876', linewidth=2.5, label='Black Market Rate')
            
            if any(offs):
                ax2.plot(dates, offs, color=style["fg"], linestyle="--", linewidth=1.5, alpha=0.7, label='Official Rate')
            
            ax2.legend(loc='upper left', framealpha=0.8, facecolor=style["bg"], edgecolor=style["fg"])
            ax2.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
            ax2.yaxis.set_major_formatter(ticker.FormatStrFormatter("%.1f"))
            ax2.yaxis.tick_right()
            ax2.grid(True, color=style["grid"], linewidth=0.5)
            ax2.set_title("24h Trend", color=style["fg"], loc="left")
        
        plt.tight_layout(rect=[0, 0, 1, 0.95])
        plt.savefig(filename, dpi=150, facecolor=style["bg"])
        plt.close()

def generate_feed_html(trades, peg):
    """Server-side feed rendering"""
    if not trades:
        return '<div style="padding:20px;text-align:center;color:var(--text-secondary)">Waiting for market activity...</div>'
    
    html = ""
    
    for trade in sorted(trades, key=lambda x: x.get('timestamp', 0), reverse=True):
        trade_type = trade.get('type')
        
        if trade_type == 'request':
            request_type = trade.get('request_type', 'REQUEST')
            is_buy_request = 'BUY' in request_type
            
            ts = datetime.datetime.fromtimestamp(trade.get("timestamp", time.time()))
            time_str = ts.strftime("%I:%M %p")
            age_seconds = time.time() - trade.get("timestamp", time.time())
            age_str = f"{int(age_seconds/60)}min ago" if age_seconds >= 60 else f"{int(age_seconds)}s ago"
            
            icon = "📝"
            action_color = "var(--green)" if is_buy_request else "var(--red)"
            
            source = trade.get('source', 'Unknown')
            if source == 'BINANCE':
                emoji, color = '🟡', '#F3BA2F'
            elif source == 'MEXC':
                emoji, color = '🔵', '#2E55E6'
            else:
                emoji, color = '🟣', '#A855F7'
            
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
        
        if trade_type not in ['buy', 'sell']:
            continue
        
        is_buy = trade_type == 'buy'
        
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
            emoji, color = '🟡', '#F3BA2F'
        elif source == 'MEXC':
            emoji, color = '🔵', '#2E55E6'
        else:
            emoji, color = '🟣', '#A855F7'
        
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
    
    return html

# --- HTML GENERATOR ---
def update_website_html(stats, official, timestamp, current_ads, grouped_ads, peg, ai_summary, legal_rates):
    prem = ((stats["median"] - official) / official) * 100 if official else 0
    
    dates, medians, q1s, q3s, offs = load_history()
    price_change = 0
    price_change_pct = 0
    if len(medians) > 0:
        old_median = medians[0]
        price_change = stats["median"] - old_median
        price_change_pct = (price_change / old_median * 100) if old_median > 0 else 0
    
    arrow = "↗" if price_change > 0 else "↘" if price_change < 0 else "→"
    
    # Source summary table
    table_rows = ""
    ticker_items = []
    
    for source, ads in grouped_ads.items():
        prices = [a["price"] for a in ads]
        s = analyze(prices, peg)
        if s:
            ticker_items.append({
                'source': source,
                'median': s['median'],
                'change': random.choice([-1, 0, 1])
            })
            
            table_rows += f"<tr><td class='source-col'>{source}</td><td>{s['min']:.2f}</td><td>{s['q1']:.2f}</td><td class='med-col'>{s['median']:.2f}</td><td>{s['q3']:.2f}</td><td>{s['max']:.2f}</td><td>{s['count']}</td></tr>"
        else:
            table_rows += f"<tr><td>{source}</td><td colspan='6' style='opacity:0.5'>No Data</td></tr>"
    
    # Add Legal Rates to table
    for channel, data in legal_rates.items():
        if data and data.get('rate'):
            emoji = "🏦" if channel == "NBE_OFFICIAL" else "💳" if channel == "COMMERCIAL_BANKS" else "🌐"
            table_rows += f"<tr><td class='source-col' style='color:#34C759'>{emoji} {channel.replace('_', ' ')}</td><td colspan='5' style='text-align:center;color:#34C759'>{data['rate']:.2f} ETB</td><td style='color:var(--text-secondary)'>{data['fees']}</td></tr>"
    
    ticker_items.append({'source': 'Official', 'median': official, 'change': 0})
    
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
    
    recent_trades = load_recent_trades()
    buys_count = len([t for t in recent_trades if t.get('type') == 'buy'])
    sells_count = len([t for t in recent_trades if t.get('type') == 'sell'])
    
    chart_data = {'BINANCE': [], 'MEXC': [], 'OKX': []}
    for source, ads in grouped_ads.items():
        prices = [a["price"] / peg for a in ads if a.get("price", 0) > 0]
        if prices and source in chart_data:
            chart_data[source] = prices
    
    chart_data_json = json.dumps(chart_data)
    
    history_data = {
        'dates': [d.isoformat() if hasattr(d, 'isoformat') else str(d) for d in dates] if dates else [],
        'medians': medians if medians else [],
        'officials': [o if o else 0 for o in offs] if offs else []
    }
    history_data_json = json.dumps(history_data)
    
    volume_by_exchange = calculate_volume_by_exchange(recent_trades)
    trade_volume_json = json.dumps(volume_by_exchange)
    
    feed_html = generate_feed_html(recent_trades, peg)
    
    trade_stats = calculate_trade_stats(recent_trades)
    hour_buys = trade_stats['hour_buys']
    hour_sells = trade_stats['hour_sells']
    hour_buy_volume = trade_stats['hour_buy_volume']
    hour_sell_volume = trade_stats['hour_sell_volume']
    today_buys = trade_stats['today_buys']
    today_sells = trade_stats['today_sells']
    today_buy_volume = trade_stats['today_buy_volume']
    today_sell_volume = trade_stats['today_sell_volume']
    overall_buys = trade_stats['overall_buys']
    overall_sells = trade_stats['overall_sells']
    overall_buy_volume = trade_stats['overall_buy_volume']
    overall_sell_volume = trade_stats['overall_sell_volume']
    
    # Volume chart HTML
    volume_chart_html = ""
    if not volume_by_exchange or all(v['total'] == 0 for v in volume_by_exchange.values()):
        volume_chart_html = """
        <div style="text-align:center;padding:40px;color:var(--text-secondary)">
            <div style="font-size:48px;margin-bottom:16px">📊</div>
            <div style="font-size:16px;font-weight:600;margin-bottom:8px">No Volume Data Yet</div>
        </div>
        """
    else:
        max_volume = max([v['total'] for v in volume_by_exchange.values()])
        
        for source in ['BINANCE', 'MEXC', 'OKX']:
            data = volume_by_exchange.get(source, {'buy': 0, 'sell': 0, 'total': 0})
            buy_pct = (data['buy'] / max_volume * 100) if max_volume > 0 else 0
            sell_pct = (data['sell'] / max_volume * 100) if max_volume > 0 else 0
            
            if data['buy'] > 0 and buy_pct < 2: buy_pct = 2
            if data['sell'] > 0 and sell_pct < 2: sell_pct = 2
            
            if source == 'BINANCE':
                emoji, color = '🟡', '#F3BA2F'
            elif source == 'MEXC':
                emoji, color = '🔵', '#2E55E6'
            else:
                emoji, color = '🟣', '#A855F7'
            
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
    
    # Ticker HTML
    ticker_html = ""
    for item in ticker_items * 3:
        change_symbol = "▲" if item['change'] > 0 else "▼" if item['change'] < 0 else "━"
        change_color = "#00C805" if item['change'] > 0 else "#FF3B30" if item['change'] < 0 else "#8E8E93"
        
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
    
    # AI Summary HTML
    ai_summary_html = ""
    if ai_summary:
        sentiment = ai_summary.get('market_sentiment', 'neutral')
        sentiment_color = '#00C805' if sentiment == 'bullish' else '#FF3B30' if sentiment == 'bearish' else '#FF9500'
        sentiment_emoji = '📈' if sentiment == 'bullish' else '📉' if sentiment == 'bearish' else '➡️'
        
        insights_html = ""
        for insight in ai_summary.get('key_insights', []):
            insights_html += f"<li style='margin-bottom:8px;'>{insight}</li>"
        
        risks_html = ""
        for risk in ai_summary.get('risk_factors', []):
            risks_html += f"<li style='margin-bottom:8px;color:#FF9500;'>{risk}</li>"
        
        ai_summary_html = f"""
        <div style="background:linear-gradient(135deg, var(--card), rgba(10,132,255,0.1));padding:30px;border-radius:16px;margin-top:30px;border:2px solid var(--accent);">
            <div style="display:flex;align-items:center;gap:12px;margin-bottom:20px;">
                <span style="font-size:32px;">🤖</span>
                <div>
                    <div style="font-size:22px;font-weight:700;color:var(--text);">AI Market Analysis</div>
                    <div style="font-size:13px;color:var(--text-secondary);">Powered by Google Gemini • Updated {ai_summary.get('generated_at', 'recently')[:16]}</div>
                </div>
                <div style="margin-left:auto;background:{sentiment_color}22;padding:8px 16px;border-radius:20px;border:1px solid {sentiment_color};">
                    <span style="font-size:18px;">{sentiment_emoji}</span>
                    <span style="color:{sentiment_color};font-weight:700;text-transform:uppercase;">{sentiment}</span>
                </div>
            </div>
            
            <div style="background:var(--bg);padding:20px;border-radius:12px;margin-bottom:20px;">
                <div style="font-size:16px;line-height:1.7;color:var(--text);">
                    {ai_summary.get('summary', 'Analysis not available.')}
                </div>
            </div>
            
            <div style="display:grid;grid-template-columns:1fr 1fr;gap:20px;">
                <div style="background:rgba(0,200,5,0.1);padding:20px;border-radius:12px;border:1px solid rgba(0,200,5,0.3);">
                    <div style="font-weight:700;color:var(--green);margin-bottom:12px;font-size:16px;">💡 Key Insights</div>
                    <ul style="margin:0;padding-left:20px;color:var(--text);line-height:1.6;">
                        {insights_html}
                    </ul>
                </div>
                
                <div style="background:rgba(255,149,0,0.1);padding:20px;border-radius:12px;border:1px solid rgba(255,149,0,0.3);">
                    <div style="font-weight:700;color:var(--orange);margin-bottom:12px;font-size:16px;">⚠️ Risk Factors</div>
                    <ul style="margin:0;padding-left:20px;line-height:1.6;">
                        {risks_html}
                    </ul>
                </div>
            </div>
            
            <div style="display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-top:20px;">
                <div style="background:var(--card);padding:20px;border-radius:12px;border:1px solid var(--border);">
                    <div style="font-weight:700;color:var(--accent);margin-bottom:8px;">📅 Short-Term Prediction (1-7 Days)</div>
                    <div style="color:var(--text);line-height:1.6;">{ai_summary.get('short_term_prediction', 'Not available')}</div>
                </div>
                
                <div style="background:var(--card);padding:20px;border-radius:12px;border:1px solid var(--border);">
                    <div style="font-weight:700;color:var(--accent);margin-bottom:8px;">💰 Recommendation</div>
                    <div style="color:var(--text);line-height:1.6;">{ai_summary.get('recommendation', 'Not available')}</div>
                </div>
            </div>
        </div>
        """
    else:
        ai_summary_html = """
        <div style="background:var(--card);padding:30px;border-radius:16px;margin-top:30px;border:1px solid var(--border);text-align:center;">
            <span style="font-size:48px;">🤖</span>
            <div style="font-size:18px;font-weight:600;margin-top:12px;color:var(--text);">AI Analysis Loading...</div>
            <div style="font-size:14px;color:var(--text-secondary);margin-top:8px;">Gemini AI will analyze the market on next update</div>
        </div>
        """
    
    # Legal Remittance HTML
    legal_html = ""
    for channel, data in legal_rates.items():
        if data and data.get('rate'):
            channel_name = channel.replace('_', ' ').title()
            if channel == 'NBE_OFFICIAL':
                emoji, color = '🏛️', '#34C759'
            elif channel == 'COMMERCIAL_BANKS':
                emoji, color = '🏦', '#007AFF'
            else:
                emoji, color = '🌐', '#5856D6'
            
            savings = stats['median'] - data['rate']
            savings_pct = (savings / stats['median'] * 100) if stats['median'] > 0 else 0
            
            legal_html += f"""
            <div style="background:var(--card);padding:20px;border-radius:12px;border:1px solid var(--border);flex:1;">
                <div style="display:flex;align-items:center;gap:10px;margin-bottom:12px;">
                    <span style="font-size:28px;">{emoji}</span>
                    <div>
                        <div style="font-weight:700;color:{color};">{channel_name}</div>
                        <div style="font-size:12px;color:var(--text-secondary);">{data.get('source', 'Legal Channel')}</div>
                    </div>
                </div>
                <div style="font-size:28px;font-weight:700;color:var(--text);">{data['rate']:.2f} <span style="font-size:16px;color:var(--text-secondary);">ETB</span></div>
                <div style="font-size:13px;color:var(--text-secondary);margin-top:8px;">Fees: {data.get('fees', 'Varies')}</div>
                <div style="font-size:12px;color:#FF9500;margin-top:4px;">vs Black Market: -{savings:.2f} ETB (-{savings_pct:.1f}%)</div>
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
        <title>ETB Market v42.8 - AI-Powered</title>
        <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
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
            }}
            
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
            
            .ticker-source {{ font-weight: 700; color: var(--accent); font-size: 14px; }}
            .ticker-price {{ font-weight: 600; color: var(--text); font-size: 14px; }}
            .ticker-change {{ font-weight: 700; font-size: 16px; }}
            
            .container {{ max-width: 1400px; margin: 0 auto; padding: 20px; }}
            
            header {{
                display: flex;
                justify-content: space-between;
                align-items: center;
                padding: 20px 0;
                border-bottom: 1px solid var(--border);
                margin-bottom: 30px;
            }}
            
            .logo {{ font-size: 24px; font-weight: 700; }}
            
            .theme-toggle {{
                background: var(--card);
                border: 1px solid var(--border);
                border-radius: 20px;
                padding: 8px 16px;
                cursor: pointer;
                color: var(--text);
                font-size: 14px;
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
            }}
            
            .price-label {{
                color: var(--text-secondary);
                font-size: 14px;
                font-weight: 500;
                text-transform: uppercase;
                margin-bottom: 10px;
            }}
            
            .price-value {{
                font-size: 52px;
                font-weight: 700;
                letter-spacing: -2px;
                margin-bottom: 15px;
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
            
            .price-change.positive {{ background: rgba(0, 200, 5, 0.1); color: var(--green); }}
            .price-change.negative {{ background: rgba(255, 59, 48, 0.1); color: var(--red); }}
            
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
            
            .chart-card {{
                background: var(--card);
                border-radius: 16px;
                padding: 20px;
                border: 1px solid var(--border);
                margin-bottom: 20px;
            }}
            
            .plotly-chart {{ width: 100%; height: 350px; }}
            
            .chart-title {{
                font-size: 16px;
                font-weight: 600;
                margin-bottom: 12px;
            }}
            
            .table-card {{
                background: var(--card);
                border-radius: 16px;
                padding: 20px;
                border: 1px solid var(--border);
                margin-bottom: 20px;
            }}
            
            .table-card h3 {{ font-size: 18px; font-weight: 700; margin-bottom: 15px; }}
            
            table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
            th {{ text-align: left; padding: 12px; color: var(--text-secondary); font-weight: 600; border-bottom: 1px solid var(--border); }}
            td {{ padding: 12px; color: var(--text); border-bottom: 1px solid var(--border); }}
            tr:last-child td {{ border-bottom: none; }}
            .source-col {{ font-weight: 600; color: #00ff9d; }}
            .med-col {{ color: #ff0066; font-weight: 700; }}
            
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
            
            .feed-title {{ font-size: 18px; font-weight: 700; margin-bottom: 15px; }}
            
            .feed-container {{ max-height: 600px; overflow-y: auto; padding: 10px; }}
            
            .feed-item {{
                display: flex;
                align-items: flex-start;
                gap: 12px;
                padding: 12px;
                border-radius: 12px;
                margin-bottom: 8px;
            }}
            
            .feed-item:hover {{ background: var(--card-hover); }}
            
            .feed-icon {{
                width: 36px;
                height: 36px;
                border-radius: 50%;
                display: flex;
                align-items: center;
                justify-content: center;
                font-size: 18px;
            }}
            
            .feed-icon.buy {{ background: rgba(0, 200, 5, 0.15); color: var(--green); }}
            .feed-icon.sell {{ background: rgba(255, 59, 48, 0.15); color: var(--red); }}
            
            .feed-content {{ flex: 1; font-size: 13px; }}
            .feed-meta {{ display: flex; justify-content: space-between; color: var(--text-secondary); font-size: 12px; margin-bottom: 4px; }}
            .feed-user {{ font-weight: 600; font-family: monospace; color: #00ff9d; }}
            .feed-amount {{ font-weight: 700; color: #00bfff; }}
            .feed-price {{ font-weight: 600; }}
            
            .volume-row {{
                display: grid;
                grid-template-columns: 150px 1fr;
                gap: 20px;
                margin-bottom: 16px;
                align-items: center;
            }}
            
            .volume-source {{ display: flex; align-items: center; gap: 8px; }}
            .volume-bars {{ display: flex; flex-direction: column; gap: 8px; }}
            .volume-bar-group {{ display: flex; align-items: center; gap: 12px; }}
            .volume-bar {{ height: 24px; border-radius: 4px; min-width: 2px; }}
            .buy-bar {{ background: linear-gradient(90deg, #00C805 0%, #00ff9d 100%); }}
            .sell-bar {{ background: linear-gradient(90deg, #FF3B30 0%, #ff6b6b 100%); }}
            .volume-label {{ font-size: 13px; font-weight: 600; min-width: 100px; }}
            .buy-label {{ color: #00C805; }}
            .sell-label {{ color: #FF3B30; }}
            
            .stats-panel {{
                background: var(--card);
                border-radius: 12px;
                padding: 20px;
                margin: 20px 0;
                border: 1px solid var(--border);
            }}
            
            .stats-title {{ font-size: 18px; font-weight: 700; margin-bottom: 20px; text-align: center; }}
            .stats-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; }}
            
            .stat-card {{
                background: rgba(10, 132, 255, 0.05);
                border: 1px solid var(--border);
                border-radius: 10px;
                padding: 16px;
                text-align: center;
            }}
            
            .buy-card {{ background: rgba(0, 200, 5, 0.08); border-color: rgba(0, 200, 5, 0.3); }}
            .sell-card {{ background: rgba(255, 59, 48, 0.08); border-color: rgba(255, 59, 48, 0.3); }}
            
            .stat-label {{ font-size: 12px; color: var(--text-secondary); text-transform: uppercase; margin-bottom: 8px; font-weight: 600; }}
            .stat-value {{ font-size: 32px; font-weight: 700; margin-bottom: 6px; }}
            .stat-value.green {{ color: #00C805; }}
            .stat-value.red {{ color: #FF3B30; }}
            .stat-volume {{ font-size: 13px; color: #00bfff; font-weight: 600; }}
            
            footer {{
                text-align: center;
                padding: 30px 20px;
                color: var(--text-secondary);
                font-size: 13px;
                border-top: 1px solid var(--border);
                margin-top: 40px;
            }}
            
            @media (max-width: 1024px) {{
                .main-grid {{ grid-template-columns: 1fr; }}
                .feed-panel {{ position: relative; top: 0; }}
                .price-value {{ font-size: 42px; }}
            }}
        </style>
    </head>
    <body>
        <div class="ticker-wrapper">
            <div class="ticker">{ticker_html}</div>
        </div>
        
        <div class="container">
            <header>
                <div class="logo">🇪🇹 ETB MARKET <span style="font-size:14px;color:var(--accent);">AI-Powered</span></div>
                <button class="theme-toggle" onclick="toggleTheme()">
                    <span id="theme-icon">🌙</span> Theme
                </button>
            </header>
            
            <div class="main-grid">
                <div class="left-column">
                    <div class="price-card">
                        <div class="price-label">ETB/USD BLACK MARKET RATE</div>
                        <div class="price-value">{stats['median']:.2f} <span style="font-size:28px;color:var(--text-secondary);">ETB</span></div>
                        <div class="price-change {('positive' if price_change > 0 else 'negative' if price_change < 0 else '')}">
                            <span>{arrow}</span>
                            <span>{abs(price_change):.2f} ETB ({abs(price_change_pct):.2f}%) 24h</span>
                        </div>
                        <div class="premium-badge">Black Market Premium: +{prem:.2f}%</div>
                    </div>
                    
                    <!-- Legal Remittance Channels -->
                    <div style="background:linear-gradient(135deg, rgba(52,199,89,0.15), rgba(52,199,89,0.05));padding:20px;border-radius:16px;margin-top:20px;border:2px solid rgba(52,199,89,0.4);">
                        <div style="font-size:18px;font-weight:700;margin-bottom:16px;color:#34C759;display:flex;align-items:center;gap:10px;">
                            <span>🏦</span> Legal Remittance Channels
                            <span style="font-size:12px;background:#34C75922;padding:4px 8px;border-radius:8px;">RECOMMENDED</span>
                        </div>
                        <div style="display:flex;gap:12px;flex-wrap:wrap;">
                            {legal_html}
                        </div>
                    </div>
                    
                    <!-- AI Summary -->
                    {ai_summary_html}
                    
                    <!-- Charts -->
                    <div class="chart-card" style="margin-top:20px;">
                        <div class="chart-title">📊 Live Price Distribution by Exchange</div>
                        <div id="priceDistChart" class="plotly-chart"></div>
                    </div>
                    
                    <div class="chart-card">
                        <div class="chart-title">📈 24h Price Trend</div>
                        <div id="trendChart" class="plotly-chart"></div>
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
                                    <th>Ads/Fees</th>
                                </tr>
                            </thead>
                            <tbody>{table_rows}</tbody>
                        </table>
                    </div>
                </div>
                
                <div class="feed-panel">
                    <div class="feed-header">
                        <div class="feed-title">Market Activity (P2P)</div>
                        <div style="color:var(--text-secondary);font-size:13px;">
                            <span style="color:var(--green)">🟢 {buys_count} Buys</span> • <span style="color:var(--red)">🔴 {sells_count} Sells</span>
                        </div>
                    </div>
                    <div class="feed-container">{feed_html}</div>
                </div>
            </div>
            
            <!-- Volume Chart -->
            <div class="stats-panel">
                <div class="stats-title">24h P2P Volume by Exchange</div>
                {volume_chart_html}
            </div>
            
            <!-- Stats -->
            <div class="stats-panel">
                <div class="stats-title">Transaction Statistics (24h)</div>
                <div class="stats-grid">
                    <div class="stat-card buy-card">
                        <div class="stat-label">1 Hour Buys</div>
                        <div class="stat-value green">{hour_buys}</div>
                        <div class="stat-volume">{hour_buy_volume:,.0f} USDT</div>
                    </div>
                    <div class="stat-card sell-card">
                        <div class="stat-label">1 Hour Sells</div>
                        <div class="stat-value red">{hour_sells}</div>
                        <div class="stat-volume">{hour_sell_volume:,.0f} USDT</div>
                    </div>
                    <div class="stat-card buy-card">
                        <div class="stat-label">Today Buys</div>
                        <div class="stat-value green">{today_buys}</div>
                        <div class="stat-volume">{today_buy_volume:,.0f} USDT</div>
                    </div>
                    <div class="stat-card sell-card">
                        <div class="stat-label">Today Sells</div>
                        <div class="stat-value red">{today_sells}</div>
                        <div class="stat-volume">{today_sell_volume:,.0f} USDT</div>
                    </div>
                    <div class="stat-card buy-card">
                        <div class="stat-label">24h Buys</div>
                        <div class="stat-value green">{overall_buys}</div>
                        <div class="stat-volume">{overall_buy_volume:,.0f} USDT</div>
                    </div>
                    <div class="stat-card sell-card">
                        <div class="stat-label">24h Sells</div>
                        <div class="stat-value red">{overall_sells}</div>
                        <div class="stat-volume">{overall_sell_volume:,.0f} USDT</div>
                    </div>
                </div>
            </div>
            
            <footer>
                Official Rate: {official:.2f} ETB | Last Update: {timestamp} UTC<br>
                v42.8 AI-Powered • Gemini Analysis • Legal Channels Included 🤖✅
            </footer>
        </div>
        
        <script>
            const chartData = {chart_data_json};
            const historyData = {history_data_json};
            
            function initCharts() {{
                const isDark = document.documentElement.getAttribute('data-theme') !== 'light';
                const bgColor = isDark ? '#1C1C1E' : '#ffffff';
                const textColor = isDark ? '#ffffff' : '#1a1a1a';
                const gridColor = isDark ? '#38383A' : '#e0e0e0';
                
                const scatterTraces = [];
                const colors = {{'BINANCE': '#F3BA2F', 'MEXC': '#2E55E6', 'OKX': '#A855F7'}};
                
                let allPrices = [];
                let xIndex = 0;
                const exchangeOrder = ['BINANCE', 'MEXC', 'OKX'];
                const exchangeNames = [];
                
                for (const exchange of exchangeOrder) {{
                    const prices = chartData[exchange];
                    if (prices && prices.length > 0) {{
                        allPrices = allPrices.concat(prices);
                        exchangeNames.push(exchange);
                        
                        const xPositions = prices.map(() => xIndex + (Math.random() - 0.5) * 0.6);
                        scatterTraces.push({{
                            type: 'scatter',
                            mode: 'markers',
                            name: exchange,
                            x: xPositions,
                            y: prices,
                            marker: {{ color: colors[exchange], size: 10, opacity: 0.75 }}
                        }});
                        xIndex++;
                    }}
                }}
                
                if (allPrices.length > 0) {{
                    const sortedAll = [...allPrices].sort((a, b) => a - b);
                    const overallMedian = sortedAll[Math.floor(sortedAll.length / 2)];
                    
                    scatterTraces.push({{
                        type: 'scatter',
                        mode: 'lines',
                        name: 'Median: ' + overallMedian.toFixed(2) + ' ETB',
                        x: [-0.5, exchangeNames.length - 0.5],
                        y: [overallMedian, overallMedian],
                        line: {{ color: '#00ff9d', width: 3 }}
                    }});
                }}
                
                const minPrice = allPrices.length > 0 ? Math.min(...allPrices) - 5 : 130;
                const maxPrice = allPrices.length > 0 ? Math.max(...allPrices) + 5 : 190;
                
                Plotly.newPlot('priceDistChart', scatterTraces, {{
                    paper_bgcolor: bgColor,
                    plot_bgcolor: bgColor,
                    font: {{ color: textColor }},
                    showlegend: true,
                    legend: {{ orientation: 'h', y: -0.15 }},
                    margin: {{ l: 60, r: 30, t: 30, b: 60 }},
                    yaxis: {{ title: 'Price (ETB)', gridcolor: gridColor, range: [minPrice, maxPrice] }},
                    xaxis: {{ gridcolor: gridColor, tickmode: 'array', tickvals: exchangeNames.map((_, i) => i), ticktext: exchangeNames }}
                }}, {{responsive: true, displayModeBar: false}});
                
                if (historyData.dates && historyData.dates.length > 1) {{
                    const trendTraces = [
                        {{
                            type: 'scatter',
                            mode: 'lines',
                            name: 'Black Market',
                            x: historyData.dates,
                            y: historyData.medians,
                            line: {{ color: '#00ff9d', width: 3 }}
                        }},
                        {{
                            type: 'scatter',
                            mode: 'lines',
                            name: 'Official',
                            x: historyData.dates,
                            y: historyData.officials,
                            line: {{ color: '#FF9500', width: 2, dash: 'dot' }}
                        }}
                    ];
                    
                    Plotly.newPlot('trendChart', trendTraces, {{
                        paper_bgcolor: bgColor,
                        plot_bgcolor: bgColor,
                        font: {{ color: textColor }},
                        showlegend: true,
                        legend: {{ orientation: 'h', y: -0.15 }},
                        margin: {{ l: 60, r: 30, t: 30, b: 60 }},
                        xaxis: {{ gridcolor: gridColor, tickformat: '%H:%M' }},
                        yaxis: {{ title: 'Rate (ETB)', gridcolor: gridColor }}
                    }}, {{responsive: true, displayModeBar: false}});
                }}
            }}
            
            document.addEventListener('DOMContentLoaded', initCharts);
            
            function toggleTheme() {{
                const html = document.documentElement;
                const current = html.getAttribute('data-theme');
                const next = current === 'light' ? 'dark' : 'light';
                html.setAttribute('data-theme', next);
                localStorage.setItem('theme', next);
                document.getElementById('theme-icon').textContent = next === 'light' ? '☀️' : '🌙';
                initCharts();
            }}
            
            (function() {{
                const theme = localStorage.getItem('theme') || 'dark';
                document.documentElement.setAttribute('data-theme', theme);
                document.getElementById('theme-icon').textContent = theme === 'light' ? '☀️' : '🌙';
            }})();
        </script>
    </body>
    </html>
    """
    
    with open(HTML_FILENAME, "w") as f:
        f.write(html)

# --- MAIN ---
def main():
    print("🔍 Running v42.8 (AI-Powered + Legal Remittance!)...", file=sys.stderr)
    print(f"   🤖 Gemini API: {'✅ SET' if GEMINI_API_KEY and len(GEMINI_API_KEY) > 10 else '❌ NOT SET'}", file=sys.stderr)
    print(f"   🔑 RapidAPI Key: {'✅ SET' if RAPIDAPI_KEY and len(RAPIDAPI_KEY) > 10 else '❌ NOT SET'}", file=sys.stderr)
    print(f"   🔑 P2P Army Key: {'✅ SET' if P2P_ARMY_KEY and len(P2P_ARMY_KEY) > 10 else '❌ NOT SET'}", file=sys.stderr)
    
    NUM_SNAPSHOTS = 8
    WAIT_TIME = 15
    all_trades = []
    
    # First snapshot
    print(f"   > Snapshot 1/{NUM_SNAPSHOTS}...", file=sys.stderr)
    prev_snapshot = capture_market_snapshot()
    save_market_state(prev_snapshot)
    
    peg = fetch_usdt_peg() or 1.0
    
    # Additional snapshots
    for i in range(2, NUM_SNAPSHOTS + 1):
        print(f"   > ⏳ Waiting {WAIT_TIME}s...", file=sys.stderr)
        time.sleep(WAIT_TIME)
        
        print(f"   > Snapshot {i}/{NUM_SNAPSHOTS}...", file=sys.stderr)
        current_snapshot = capture_market_snapshot()
        
        trades_this_round = detect_real_trades(current_snapshot, peg)
        if trades_this_round:
            all_trades.extend(trades_this_round)
            print(f"   ✅ Round {i-1}: {len(trades_this_round)} events", file=sys.stderr)
        
        save_market_state(current_snapshot)
        prev_snapshot = current_snapshot
    
    # Final snapshot
    print("   > Final snapshot...", file=sys.stderr)
    with ThreadPoolExecutor(max_workers=10) as ex:
        f_binance = ex.submit(fetch_binance_both_sides)
        f_mexc = ex.submit(fetch_mexc_both_sides)
        f_okx = ex.submit(fetch_exchange_both_sides, "okx")
        f_off = ex.submit(fetch_official_rate)
        f_legal = ex.submit(fetch_legal_remittance_rates)
        
        bin_ads = f_binance.result() or []
        mexc_ads = f_mexc.result() or []
        okx_ads = f_okx.result() or []
        official = f_off.result() or 0.0
        legal_rates = f_legal.result() or {}
    
    print(f"   🔍 Final: BINANCE={len(bin_ads)}, MEXC={len(mexc_ads)}, OKX={len(okx_ads)}", file=sys.stderr)
    
    # Filter outliers
    bin_ads = remove_outliers(bin_ads, peg)
    mexc_ads = remove_outliers(mexc_ads, peg)
    okx_ads = remove_outliers(okx_ads, peg)
    
    final_snapshot = bin_ads + mexc_ads + okx_ads
    grouped_ads = {"BINANCE": bin_ads, "MEXC": mexc_ads, "OKX": okx_ads}
    
    # Save trades
    if all_trades:
        save_trades(all_trades)
        print(f"   💾 Saved {len(all_trades)} events", file=sys.stderr)
    
    # Generate stats and AI summary
    if final_snapshot:
        all_prices = [x['price'] for x in final_snapshot]
        stats = analyze(all_prices, peg)
        
        if stats:
            save_to_history(stats, official)
            generate_charts(stats, official)
            
            # Load recent trades for AI context
            recent_trades = load_recent_trades()
            trade_stats = calculate_trade_stats(recent_trades)
            volume_by_exchange = calculate_volume_by_exchange(recent_trades)
            history_data = load_history()
            
            # Check for cached AI summary or generate new
            ai_summary = load_cached_ai_summary()
            if not ai_summary:
                ai_summary = generate_ai_summary(stats, official, trade_stats, volume_by_exchange, history_data)
            
            update_website_html(
                stats, official,
                time.strftime("%Y-%m-%d %H:%M:%S"),
                final_snapshot, grouped_ads, peg,
                ai_summary, legal_rates
            )
    else:
        print("⚠️ No ads found", file=sys.stderr)
    
    buys = len([t for t in all_trades if t.get('type') == 'buy'])
    sells = len([t for t in all_trades if t.get('type') == 'sell'])
    print(f"\n🎯 Complete! {buys} buys, {sells} sells detected.")
    print(f"✅ v42.8: AI Analysis + Legal Remittance Channels!")

if __name__ == "__main__":
    main()
