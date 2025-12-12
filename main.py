#!/usr/bin/env python3
"""
🇪🇹 ETB Financial Terminal v42.9 (AI + Market Depth!)
- NEW: Gemini AI Analysis with Gap Explanation (why black market vs official differs)
- NEW: Price Trend with Premium % + Time filters (1H, 1D, 1W, ALL)
- NEW: Live Market Insight - Supply/Demand by Price with stacked exchange bars
- NEW: Black Market Drivers & Official Rate Factors in AI section
- NEW: Remitly, Western Union, Ria rates in ticker slider
- NEW: p2p.army fallback when RapidAPI fails (502 errors)
- REMOVED: Bybit (per user request)
- KEEP: Binance (RapidAPI → p2p.army), MEXC (RapidAPI → p2p.army), OKX (p2p.army)
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
import re
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

def fetch_remittance_rates():
    """Fetch estimated remittance rates for ticker display"""
    rates = {}
    
    try:
        # Get official NBE rate as base
        r = requests.get("https://open.er-api.com/v6/latest/USD", timeout=5)
        nbe_rate = r.json()["rates"]["ETB"]
        
        # Remittance services typically offer rates close to official + small margin
        # These are estimates - actual rates vary by amount and payment method
        rates['NBE_OFFICIAL'] = {
            'rate': nbe_rate,
            'name': 'NBE Official',
            'emoji': '🏛️',
            'color': '#34C759'
        }
        
        rates['WESTERN_UNION'] = {
            'rate': nbe_rate * 1.01,  # ~1% margin estimate
            'name': 'Western Union',
            'emoji': '💛',
            'color': '#FFCC00'
        }
        
        rates['REMITLY'] = {
            'rate': nbe_rate * 1.015,  # ~1.5% margin estimate
            'name': 'Remitly',
            'emoji': '💚',
            'color': '#00C805'
        }
        
        rates['RIA'] = {
            'rate': nbe_rate * 1.012,  # ~1.2% margin estimate
            'name': 'Ria',
            'emoji': '🧡',
            'color': '#FF6B00'
        }
        
        print(f"   💱 Remittance rates fetched (NBE base: {nbe_rate:.2f})", file=sys.stderr)
        
    except Exception as e:
        print(f"   ⚠️ Error fetching remittance rates: {e}", file=sys.stderr)
    
    return rates

def fetch_binance_rapidapi(side="SELL"):
    """Fetch Binance P2P ads using RapidAPI with p2p.army fallback"""
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
    use_fallback = False
    
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
            
            # Check for 502 or other server errors - use fallback
            if r.status_code in [502, 503, 500, 429]:
                print(f"   ⚠️ Binance RapidAPI error {r.status_code}, switching to p2p.army fallback...", file=sys.stderr)
                use_fallback = True
                break
            
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
                use_fallback = True
                break
                
        except Exception as e:
            print(f"   ❌ Binance connection error: {e}, trying p2p.army fallback...", file=sys.stderr)
            use_fallback = True
            break
    
    # If RapidAPI failed, use p2p.army fallback
    if use_fallback or len(all_ads) == 0:
        print(f"   🔄 Using p2p.army fallback for Binance {side}...", file=sys.stderr)
        fallback_ads = fetch_p2p_army_exchange("binance", side)
        if fallback_ads:
            return fallback_ads
    
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
    """Universal fetcher with p2p.army - used as primary for OKX and fallback for others"""
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
                    except Exception as e:
                        continue
        
        print(f"   {market.upper()} {side} (p2p.army): {len(ads)} ads", file=sys.stderr)
    except Exception as e:
        print(f"   {market.upper()} {side} error: {e}", file=sys.stderr)
    
    return ads

def fetch_mexc_rapidapi(side="SELL"):
    """Fetch MEXC P2P ads using RapidAPI with p2p.army fallback"""
    url = "https://mexc-p2p-api.p.rapidapi.com/mexc/p2p/search"
    ads = []
    use_fallback = False
    
    try:
        headers = {
            "X-RapidAPI-Key": RAPIDAPI_KEY,
            "X-RapidAPI-Host": "mexc-p2p-api.p.rapidapi.com",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
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
                    
                    # Check for server errors - use fallback
                    if r.status_code in [502, 503, 500]:
                        print(f"   ⚠️ MEXC RapidAPI error {r.status_code}, switching to p2p.army fallback...", file=sys.stderr)
                        use_fallback = True
                        break
                    
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
                    
                except Exception as e:
                    print(f"   ⚠️ MEXC request error: {e}", file=sys.stderr)
                    use_fallback = True
                    break
            
            if use_fallback:
                break
        
        # If RapidAPI failed, use p2p.army fallback
        if use_fallback or len(ads) == 0:
            print(f"   🔄 Using p2p.army fallback for MEXC {side}...", file=sys.stderr)
            fallback_ads = fetch_p2p_army_exchange("mexc", side)
            if fallback_ads:
                return fallback_ads
        
        print(f"   MEXC {side} (RapidAPI): {len(ads)} ads", file=sys.stderr)
    except Exception as e:
        print(f"   MEXC {side} error: {e}, trying p2p.army fallback...", file=sys.stderr)
        return fetch_p2p_army_exchange("mexc", side)
    
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

# --- GEMINI AI INTEGRATION ---
def generate_ai_summary(stats, official, trade_stats, volume_by_exchange, history_data):
    """Generate AI market analysis using Google Gemini API with forecasting"""
    
    print(f"   🤖 Starting AI Summary generation...", file=sys.stderr)
    print(f"   🔑 Gemini API Key: {'✅ SET (len=' + str(len(GEMINI_API_KEY)) + ')' if GEMINI_API_KEY else '❌ NOT SET'}", file=sys.stderr)
    
    if not GEMINI_API_KEY or GEMINI_API_KEY == "YOUR_KEY_HERE" or len(GEMINI_API_KEY) < 10:
        print("   ⚠️ Gemini API key not set or invalid, using fallback", file=sys.stderr)
        return create_fallback_summary(stats, official, trade_stats)
    
    try:
        black_market_rate = stats.get('median', 0)
        premium = ((black_market_rate - official) / official * 100) if official > 0 else 0
        
        dates, medians, q1s, q3s, offs = history_data if history_data else ([], [], [], [], [])
        
        trend_direction = "stable"
        trend_change = 0
        if len(medians) >= 2:
            trend_change = medians[-1] - medians[0] if medians else 0
            if trend_change > 2:
                trend_direction = "increasing"
            elif trend_change < -2:
                trend_direction = "decreasing"
        
        # Calculate volume trends
        total_buy = trade_stats.get('overall_buy_volume', 0)
        total_sell = trade_stats.get('overall_sell_volume', 0)
        buy_sell_ratio = total_buy / total_sell if total_sell > 0 else 1
        
        # Build comprehensive prompt with forecasting request
        prompt = f"""You are an expert Ethiopian financial market analyst specializing in ETB/USD exchange rates and remittance markets. Analyze the current market data and provide insights WITH FORECASTING.

CURRENT MARKET DATA (as of {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}):
- Black Market Rate: {black_market_rate:.2f} ETB per USD
- Official NBE Rate: {official:.2f} ETB per USD  
- Black Market Premium: {premium:.1f}%
- Price Range: {stats.get('min', 0):.2f} - {stats.get('max', 0):.2f} ETB
- 24h Trend: {trend_direction} ({'+' if trend_change > 0 else ''}{trend_change:.2f} ETB)
- Active P2P Ads: {stats.get('count', 0)}

TRADING ACTIVITY (24h):
- Buy Volume: ${total_buy:,.0f} USDT
- Sell Volume: ${total_sell:,.0f} USDT
- Buy/Sell Ratio: {buy_sell_ratio:.2f}
- Total Trades: {trade_stats.get('overall_buys', 0) + trade_stats.get('overall_sells', 0)}

MARKET CONTEXT:
- Ethiopia recently unified exchange rates (March 2024)
- IMF monitoring economic reforms
- Diaspora remittances are major USD source
- Foreign currency shortage affects businesses

Based on this data and your knowledge of Ethiopian economic conditions, provide analysis in this EXACT JSON format:
{{
    "market_sentiment": "bullish/bearish/neutral",
    "summary": "2-3 sentence market summary explaining current conditions",
    "key_insights": ["insight 1", "insight 2", "insight 3"],
    "black_market_drivers": ["factor driving black market rate up/down 1", "factor 2", "factor 3"],
    "official_rate_factors": ["factor affecting official NBE rate 1", "factor 2"],
    "gap_explanation": "Why is there a {premium:.1f}% gap between black market and official rate? Explain the key reasons.",
    "short_term_forecast": "Detailed 1-7 day price prediction with specific range",
    "medium_term_forecast": "1-4 week outlook based on trends",
    "risk_factors": ["risk 1", "risk 2"],
    "recommendation": "Specific advice for remittance senders",
    "confidence_level": "high/medium/low"
}}

Focus on actionable insights. Be specific about price forecasts. Explain what economic factors are driving the difference between black market and official rates."""

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
        
        print(f"   📡 Calling Gemini API...", file=sys.stderr)
        response = requests.post(url, json=payload, timeout=30)
        print(f"   📡 Gemini API Status: {response.status_code}", file=sys.stderr)
        
        if response.status_code == 200:
            data = response.json()
            
            if 'error' in data:
                print(f"   ❌ Gemini API returned error: {data['error']}", file=sys.stderr)
                return create_fallback_summary(stats, official, trade_stats)
            
            candidates = data.get('candidates', [])
            if not candidates:
                print(f"   ⚠️ No candidates in Gemini response", file=sys.stderr)
                return create_fallback_summary(stats, official, trade_stats)
            
            text = candidates[0].get('content', {}).get('parts', [{}])[0].get('text', '')
            print(f"   📝 Gemini response length: {len(text)} chars", file=sys.stderr)
            
            if not text:
                print(f"   ⚠️ Empty text in Gemini response", file=sys.stderr)
                return create_fallback_summary(stats, official, trade_stats)
            
            json_match = re.search(r'```json\s*([\s\S]*?)\s*```', text)
            if json_match:
                json_str = json_match.group(1)
            else:
                json_match = re.search(r'\{[\s\S]*\}', text)
                json_str = json_match.group() if json_match else None
            
            if json_str:
                try:
                    ai_data = json.loads(json_str)
                    ai_data['generated_at'] = datetime.datetime.now().isoformat()
                    ai_data['rate_at_generation'] = black_market_rate
                    
                    with open(AI_SUMMARY_FILE, 'w') as f:
                        json.dump(ai_data, f)
                    
                    print(f"   ✅ AI Summary generated successfully!", file=sys.stderr)
                    return ai_data
                except json.JSONDecodeError as je:
                    print(f"   ⚠️ JSON parse error: {je}", file=sys.stderr)
                    return create_fallback_summary(stats, official, trade_stats)
            else:
                print(f"   ⚠️ Could not find JSON in response", file=sys.stderr)
                return create_fallback_summary(stats, official, trade_stats)
        else:
            print(f"   ❌ Gemini API HTTP error: {response.status_code}", file=sys.stderr)
            return create_fallback_summary(stats, official, trade_stats)
            
    except requests.exceptions.Timeout:
        print(f"   ❌ Gemini API timeout (30s)", file=sys.stderr)
        return create_fallback_summary(stats, official, trade_stats)
    except requests.exceptions.RequestException as e:
        print(f"   ❌ Gemini API request error: {e}", file=sys.stderr)
        return create_fallback_summary(stats, official, trade_stats)
    except Exception as e:
        print(f"   ❌ AI Summary error: {type(e).__name__}: {e}", file=sys.stderr)
        return create_fallback_summary(stats, official, trade_stats)

def create_fallback_summary(stats, official, trade_stats):
    """Create a rule-based fallback summary when AI is unavailable"""
    print(f"   📋 Using fallback rule-based summary", file=sys.stderr)
    
    black_market_rate = stats.get('median', 0)
    premium = ((black_market_rate - official) / official * 100) if official > 0 else 0
    
    buy_vol = trade_stats.get('overall_buy_volume', 0)
    sell_vol = trade_stats.get('overall_sell_volume', 0)
    
    if buy_vol > sell_vol * 1.5:
        sentiment = "bullish"
        sentiment_text = "Strong buying pressure indicates demand for USDT/USD"
        forecast = f"Rate likely to increase to {black_market_rate + 2:.2f}-{black_market_rate + 5:.2f} ETB"
    elif sell_vol > buy_vol * 1.5:
        sentiment = "bearish"
        sentiment_text = "Strong selling pressure indicates USDT supply increase"
        forecast = f"Rate may decrease to {black_market_rate - 3:.2f}-{black_market_rate - 1:.2f} ETB"
    else:
        sentiment = "neutral"
        sentiment_text = "Balanced buy/sell activity with stable market conditions"
        forecast = f"Rate expected to stay within {black_market_rate - 2:.2f}-{black_market_rate + 2:.2f} ETB"
    
    return {
        "market_sentiment": sentiment,
        "summary": f"The ETB black market rate is currently {black_market_rate:.2f} ETB/USD, representing a {premium:.1f}% premium over the official rate of {official:.2f} ETB. {sentiment_text}.",
        "key_insights": [
            f"Black market premium: {premium:.1f}% above official rate",
            f"24h volume: ${buy_vol + sell_vol:,.0f} USDT traded",
            f"Market spread: {stats.get('min', 0):.2f} - {stats.get('max', 0):.2f} ETB"
        ],
        "black_market_drivers": [
            "High demand for USD from importers and businesses",
            "Limited forex availability through official channels",
            "Diaspora remittance preferences for better rates"
        ],
        "official_rate_factors": [
            "NBE monetary policy and forex reserves",
            "IMF program requirements and reform timeline"
        ],
        "gap_explanation": f"The {premium:.1f}% gap exists primarily due to foreign currency shortage in official banking channels, forcing businesses to seek USD through parallel markets at premium rates.",
        "short_term_forecast": forecast,
        "medium_term_forecast": "Market expected to remain volatile. Monitor NBE policy announcements for direction.",
        "risk_factors": [
            "Exchange rate volatility during policy changes",
            "P2P transaction counterparty risks"
        ],
        "recommendation": "For remittances, compare legal channel rates (Western Union, Remitly, Ria). Legal channels offer security despite lower rates.",
        "confidence_level": "medium",
        "generated_at": datetime.datetime.now().isoformat(),
        "rate_at_generation": black_market_rate,
        "is_fallback": True
    }

def load_cached_ai_summary():
    """Load cached AI summary if recent (within 1 hour)"""
    if not os.path.exists(AI_SUMMARY_FILE):
        print(f"   📋 No cached AI summary found", file=sys.stderr)
        return None
    
    try:
        with open(AI_SUMMARY_FILE, 'r') as f:
            data = json.load(f)
        
        generated_at = datetime.datetime.fromisoformat(data.get('generated_at', '2000-01-01'))
        age = datetime.datetime.now() - generated_at
        
        if age.total_seconds() < 3600:
            print(f"   📋 Using cached AI summary ({int(age.total_seconds()/60)}min old)", file=sys.stderr)
            return data
        else:
            print(f"   📋 Cached AI summary expired ({int(age.total_seconds()/60)}min old)", file=sys.stderr)
            return None
    except Exception as e:
        print(f"   ⚠️ Error loading cached summary: {e}", file=sys.stderr)
        return None

# --- MARKET SNAPSHOT ---
def capture_market_snapshot():
    """Capture market snapshot: Binance, MEXC, OKX (NO Bybit)"""
    with ThreadPoolExecutor(max_workers=6) as ex:
        f_binance = ex.submit(fetch_binance_both_sides)
        f_mexc = ex.submit(fetch_mexc_both_sides)
        f_okx = ex.submit(fetch_exchange_both_sides, "okx")
        f_peg = ex.submit(fetch_usdt_peg)
        
        binance_data = f_binance.result() or []
        mexc_data = f_mexc.result() or []
        okx_data = f_okx.result() or []
        peg = f_peg.result() or 1.0
        
        total = len(binance_data) + len(mexc_data) + len(okx_data)
        print(f"   📊 Collected {total} ads total (Binance, MEXC, OKX)", file=sys.stderr)
        
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
    """CONSERVATIVE TRADE DETECTION - PARTIAL FILLS ONLY"""
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
    
    print(f"\n   📊 DETECTION SUMMARY:", file=sys.stderr)
    print(f"   > Requests posted: {len(requests)}", file=sys.stderr)
    print(f"   > Trades detected: {len(trades)} ({len([t for t in trades if t['type']=='buy'])} buys 🟢, {len([t for t in trades if t['type']=='sell'])} sells 🔴)", file=sys.stderr)
    print(f"   > Checked: Binance={sources_checked.get('BINANCE', 0)}, MEXC={sources_checked.get('MEXC', 0)}, OKX={sources_checked.get('OKX', 0)}", file=sys.stderr)
    
    return trades + requests

def load_recent_trades():
    if not os.path.exists(TRADES_FILE):
        return []
    
    try:
        with open(TRADES_FILE, "r") as f:
            all_trades = json.load(f)
        
        cutoff = time.time() - (TRADE_RETENTION_MINUTES * 60)
        
        valid_trades = []
        for t in all_trades:
            if t.get("timestamp", 0) > cutoff and t.get("type") in ['buy', 'sell', 'request']:
                valid_trades.append(t)
        
        buys = len([t for t in valid_trades if t['type'] == 'buy'])
        sells = len([t for t in valid_trades if t['type'] == 'sell'])
        requests = len([t for t in valid_trades if t['type'] == 'request'])
        
        print(f"   > Loaded {len(valid_trades)} events from last 24h ({buys} buys, {sells} sells, {requests} requests)", file=sys.stderr)
        return valid_trades
    except Exception as e:
        print(f"   > Error loading trades: {e}", file=sys.stderr)
        return []

def save_trades(new_trades):
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
    
    if len(new_trades) != len(unique_new):
        print(f"   > Deduplication: {len(new_trades)} → {len(unique_new)} events", file=sys.stderr)
    
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

# --- STATISTICS CALCULATOR ---
def calculate_trade_stats(trades):
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

def calculate_market_depth_by_price(ads, peg):
    """Calculate market depth (supply/demand) by price level for stacked bar chart"""
    if not ads:
        return {'supply': [], 'demand': []}
    
    # Group ads by price bins and exchange
    supply_by_price = {}  # SELL ads = supply
    demand_by_price = {}  # BUY ads = demand
    
    for ad in ads:
        price = ad.get('price', 0) / peg
        vol = ad.get('available', 0)
        source = ad.get('source', 'Unknown')
        ad_type = ad.get('ad_type', 'SELL').upper()
        
        # Round to nearest integer for grouping
        price_bin = int(round(price))
        
        if ad_type in ['SELL', 'SELL_AD']:
            # Supply
            if price_bin not in supply_by_price:
                supply_by_price[price_bin] = {'BINANCE': 0, 'MEXC': 0, 'OKX': 0, 'total': 0}
            supply_by_price[price_bin][source] = supply_by_price[price_bin].get(source, 0) + vol
            supply_by_price[price_bin]['total'] += vol
        else:
            # Demand
            if price_bin not in demand_by_price:
                demand_by_price[price_bin] = {'BINANCE': 0, 'MEXC': 0, 'OKX': 0, 'total': 0}
            demand_by_price[price_bin][source] = demand_by_price[price_bin].get(source, 0) + vol
            demand_by_price[price_bin]['total'] += vol
    
    # Convert to sorted lists
    supply_list = []
    for price, data in sorted(supply_by_price.items()):
        supply_list.append({
            'price': price,
            'BINANCE': data.get('BINANCE', 0),
            'MEXC': data.get('MEXC', 0),
            'OKX': data.get('OKX', 0),
            'total': data['total']
        })
    
    demand_list = []
    for price, data in sorted(demand_by_price.items()):
        demand_list.append({
            'price': price,
            'BINANCE': data.get('BINANCE', 0),
            'MEXC': data.get('MEXC', 0),
            'OKX': data.get('OKX', 0),
            'total': data['total']
        })
    
    return {'supply': supply_list, 'demand': demand_list}

# --- HTML GENERATOR ---
def update_website_html(stats, official, timestamp, current_ads, grouped_ads, peg, ai_summary=None, remittance_rates=None):
    prem = ((stats["median"] - official) / official) * 100 if official else 0
    cache_buster = int(time.time())
    
    dates, medians, q1s, q3s, offs = load_history()
    price_change = 0
    price_change_pct = 0
    if len(medians) > 0:
        old_median = medians[0]
        price_change = stats["median"] - old_median
        price_change_pct = (price_change / old_median * 100) if old_median > 0 else 0
    
    # Calculate premiums for each historical point
    premiums = []
    for i in range(len(medians)):
        if i < len(offs) and offs[i] > 0:
            prem_val = ((medians[i] - offs[i]) / offs[i]) * 100
            premiums.append(prem_val)
        else:
            premiums.append(0)
    
    arrow = "↗" if price_change > 0 else "↘" if price_change < 0 else "→"
    change_color = "#00C805" if price_change > 0 else "#FF3B30" if price_change < 0 else "#8E8E93"
    
    # Source summary table (NO remittance rates here)
    table_rows = ""
    ticker_items = []
    
    for source, ads in grouped_ads.items():
        prices = [a["price"] for a in ads]
        s = analyze(prices, peg)
        if s:
            ticker_items.append({
                'source': source,
                'median': s['median'],
                'change': 0,
                'type': 'exchange'
            })
            
            table_rows += f"<tr><td class='source-col'>{source}</td><td>{s['min']:.2f}</td><td>{s['q1']:.2f}</td><td class='med-col'>{s['median']:.2f}</td><td>{s['q3']:.2f}</td><td>{s['max']:.2f}</td><td>{s['count']}</td></tr>"
        else:
            table_rows += f"<tr><td>{source}</td><td colspan='6' style='opacity:0.5'>No Data</td></tr>"
    
    # Add official rate to ticker
    ticker_items.append({
        'source': 'Official',
        'median': official,
        'change': 0,
        'type': 'official',
        'emoji': '💵',
        'color': '#34C759'
    })
    
    # Add remittance rates to ticker ONLY
    if remittance_rates:
        for key, data in remittance_rates.items():
            if key != 'NBE_OFFICIAL':  # Already have official
                ticker_items.append({
                    'source': data['name'],
                    'median': data['rate'],
                    'change': 0,
                    'type': 'remittance',
                    'emoji': data['emoji'],
                    'color': data['color']
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
    
    # Load recent trades
    recent_trades = load_recent_trades()
    buys_count = len([t for t in recent_trades if t.get('type') == 'buy'])
    sells_count = len([t for t in recent_trades if t.get('type') == 'sell'])
    
    # Chart data - only 3 exchanges now
    chart_data = {'BINANCE': [], 'MEXC': [], 'OKX': []}
    for source, ads in grouped_ads.items():
        prices = [a["price"] / peg for a in ads if a.get("price", 0) > 0]
        if prices and source in chart_data:
            chart_data[source] = prices
    
    chart_data_json = json.dumps(chart_data)
    
    # History data with premiums
    history_data = {
        'dates': [d.isoformat() if hasattr(d, 'isoformat') else str(d) for d in dates] if dates else [],
        'medians': medians if medians else [],
        'officials': [o if o else 0 for o in offs] if offs else [],
        'premiums': premiums
    }
    history_data_json = json.dumps(history_data)
    
    volume_by_exchange = calculate_volume_by_exchange(recent_trades)
    trade_volume_json = json.dumps(volume_by_exchange)
    
    # Calculate market depth by price for stacked chart
    market_depth = calculate_market_depth_by_price(current_ads, peg)
    market_depth_json = json.dumps(market_depth)
    
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
    week_buys = trade_stats['week_buys']
    week_sells = trade_stats['week_sells']
    week_buy_volume = trade_stats['week_buy_volume']
    week_sell_volume = trade_stats['week_sell_volume']
    overall_buys = trade_stats['overall_buys']
    overall_sells = trade_stats['overall_sells']
    overall_buy_volume = trade_stats['overall_buy_volume']
    overall_sell_volume = trade_stats['overall_sell_volume']
    
    # Generate ticker HTML with remittance rates
    ticker_html = ""
    for item in ticker_items * 3:
        change_symbol = "▲" if item['change'] > 0 else "▼" if item['change'] < 0 else "━"
        change_color = "#00C805" if item['change'] > 0 else "#FF3B30" if item['change'] < 0 else "#8E8E93"
        
        source_display = item['source']
        
        # Exchange colors
        if item.get('type') == 'exchange':
            if item['source'] == 'BINANCE':
                source_display = f"🟡 {item['source']}"
            elif item['source'] == 'MEXC':
                source_display = f"🔵 {item['source']}"
            elif item['source'] == 'OKX':
                source_display = f"🟣 {item['source']}"
        elif item.get('type') == 'official':
            source_display = f"💵 {item['source']}"
        elif item.get('type') == 'remittance':
            source_display = f"{item.get('emoji', '💱')} {item['source']}"
        
        # Color based on type
        if item.get('type') == 'remittance':
            price_color = item.get('color', '#34C759')
        else:
            price_color = 'var(--text)'
        
        ticker_html += f"""
        <div class="ticker-item">
            <span class="ticker-source">{source_display}</span>
            <span class="ticker-price" style="color:{price_color}">{item['median']:.2f} ETB</span>
            <span class="ticker-change" style="color:{change_color}">{change_symbol}</span>
        </div>
        """
    
    # AI Summary HTML at BOTTOM
    ai_summary_html = ""
    if ai_summary:
        sentiment = ai_summary.get('market_sentiment', 'neutral')
        sentiment_color = '#00C805' if sentiment == 'bullish' else '#FF3B30' if sentiment == 'bearish' else '#FF9500'
        sentiment_emoji = '📈' if sentiment == 'bullish' else '📉' if sentiment == 'bearish' else '➡️'
        
        is_fallback = ai_summary.get('is_fallback', False)
        source_text = "Rule-Based Analysis" if is_fallback else "Powered by Google Gemini AI"
        source_badge = '<span style="background:#FF950033;color:#FF9500;padding:2px 8px;border-radius:4px;font-size:11px;margin-left:8px;">FALLBACK</span>' if is_fallback else ''
        
        insights_html = ""
        for insight in ai_summary.get('key_insights', []):
            insights_html += f"<li style='margin-bottom:8px;'>{insight}</li>"
        
        risks_html = ""
        for risk in ai_summary.get('risk_factors', []):
            risks_html += f"<li style='margin-bottom:8px;color:#FF9500;'>{risk}</li>"
        
        # Black market drivers
        bm_drivers_html = ""
        for driver in ai_summary.get('black_market_drivers', []):
            bm_drivers_html += f"<li style='margin-bottom:8px;'>{driver}</li>"
        
        # Official rate factors
        official_factors_html = ""
        for factor in ai_summary.get('official_rate_factors', []):
            official_factors_html += f"<li style='margin-bottom:8px;'>{factor}</li>"
        
        gap_explanation = ai_summary.get('gap_explanation', 'No explanation available')
        
        # Get forecasts
        short_forecast = ai_summary.get('short_term_forecast', ai_summary.get('short_term_prediction', 'Not available'))
        medium_forecast = ai_summary.get('medium_term_forecast', 'Not available')
        confidence = ai_summary.get('confidence_level', 'medium')
        confidence_color = '#00C805' if confidence == 'high' else '#FF9500' if confidence == 'medium' else '#FF3B30'
        
        ai_summary_html = f"""
        <div style="background:linear-gradient(135deg, var(--card), rgba(10,132,255,0.1));padding:30px;border-radius:16px;margin-top:30px;border:2px solid var(--accent);">
            <div style="display:flex;align-items:center;gap:12px;margin-bottom:20px;flex-wrap:wrap;">
                <span style="font-size:32px;">🤖</span>
                <div>
                    <div style="font-size:22px;font-weight:700;color:var(--text);">AI Market Analysis & Forecast{source_badge}</div>
                    <div style="font-size:13px;color:var(--text-secondary);">{source_text} • {ai_summary.get('generated_at', 'recently')[:16]}</div>
                </div>
                <div style="margin-left:auto;display:flex;gap:10px;flex-wrap:wrap;">
                    <div style="background:{sentiment_color}22;padding:8px 16px;border-radius:20px;border:1px solid {sentiment_color};">
                        <span style="font-size:18px;">{sentiment_emoji}</span>
                        <span style="color:{sentiment_color};font-weight:700;text-transform:uppercase;">{sentiment}</span>
                    </div>
                    <div style="background:{confidence_color}22;padding:8px 16px;border-radius:20px;border:1px solid {confidence_color};">
                        <span style="color:{confidence_color};font-weight:600;">Confidence: {confidence.upper()}</span>
                    </div>
                </div>
            </div>
            
            <div style="background:var(--bg);padding:20px;border-radius:12px;margin-bottom:20px;">
                <div style="font-size:16px;line-height:1.7;color:var(--text);">
                    {ai_summary.get('summary', 'Analysis not available.')}
                </div>
            </div>
            
            <!-- WHY THE GAP SECTION -->
            <div style="background:linear-gradient(135deg, rgba(255,149,0,0.15), rgba(255,149,0,0.05));padding:20px;border-radius:12px;margin-bottom:20px;border:1px solid rgba(255,149,0,0.4);">
                <div style="font-weight:700;color:var(--orange);margin-bottom:12px;font-size:18px;">📊 Why the {prem:.1f}% Gap Between Black Market & Official Rate?</div>
                <div style="color:var(--text);line-height:1.7;font-size:15px;">{gap_explanation}</div>
            </div>
            
            <!-- DRIVERS SECTION -->
            <div style="display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-bottom:20px;">
                <div style="background:rgba(255,59,48,0.1);padding:20px;border-radius:12px;border:1px solid rgba(255,59,48,0.3);">
                    <div style="font-weight:700;color:var(--red);margin-bottom:12px;font-size:16px;">🔴 Black Market Drivers</div>
                    <ul style="margin:0;padding-left:20px;color:var(--text);line-height:1.6;">
                        {bm_drivers_html if bm_drivers_html else '<li>High USD demand from businesses</li><li>Limited forex in official channels</li>'}
                    </ul>
                </div>
                
                <div style="background:rgba(52,199,89,0.1);padding:20px;border-radius:12px;border:1px solid rgba(52,199,89,0.3);">
                    <div style="font-weight:700;color:#34C759;margin-bottom:12px;font-size:16px;">🏛️ Official Rate Factors</div>
                    <ul style="margin:0;padding-left:20px;color:var(--text);line-height:1.6;">
                        {official_factors_html if official_factors_html else '<li>NBE monetary policy</li><li>IMF program requirements</li>'}
                    </ul>
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
            
            <!-- FORECASTING SECTION -->
            <div style="display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-top:20px;">
                <div style="background:linear-gradient(135deg, rgba(10,132,255,0.15), rgba(10,132,255,0.05));padding:20px;border-radius:12px;border:1px solid rgba(10,132,255,0.4);">
                    <div style="font-weight:700;color:var(--accent);margin-bottom:8px;font-size:16px;">📅 Short-Term Forecast (1-7 Days)</div>
                    <div style="color:var(--text);line-height:1.6;">{short_forecast}</div>
                </div>
                
                <div style="background:linear-gradient(135deg, rgba(88,86,214,0.15), rgba(88,86,214,0.05));padding:20px;border-radius:12px;border:1px solid rgba(88,86,214,0.4);">
                    <div style="font-weight:700;color:#5856D6;margin-bottom:8px;font-size:16px;">📆 Medium-Term Outlook (1-4 Weeks)</div>
                    <div style="color:var(--text);line-height:1.6;">{medium_forecast}</div>
                </div>
            </div>
            
            <div style="background:var(--card);padding:20px;border-radius:12px;border:1px solid var(--border);margin-top:20px;">
                <div style="font-weight:700;color:var(--accent);margin-bottom:8px;">💰 Recommendation</div>
                <div style="color:var(--text);line-height:1.6;">{ai_summary.get('recommendation', 'Not available')}</div>
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
    
    html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <meta http-equiv="refresh" content="300">
        <title>ETB Market v42.9 - AI Powered + Remittance Rates</title>
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
                transition: background 0.3s ease;
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
                animation: scroll 50s linear infinite;
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
            
            .trend-btn {{
                background: transparent;
                border: 1px solid var(--border);
                color: var(--text-secondary);
                padding: 6px 14px;
                border-radius: 8px;
                cursor: pointer;
                font-size: 13px;
                font-weight: 600;
                transition: all 0.2s ease;
            }}
            
            .trend-btn:hover {{
                background: var(--card-hover);
                color: var(--text);
            }}
            
            .trend-btn.active {{
                background: var(--accent);
                color: white;
                border-color: var(--accent);
            }}
            
            .chart-card {{
                background: var(--card);
                border-radius: 16px;
                padding: 20px;
                border: 1px solid var(--border);
                margin-bottom: 20px;
            }}
            
            .plotly-chart {{
                width: 100%;
                height: 350px;
                border-radius: 12px;
            }}
            
            .chart-title {{
                font-size: 16px;
                font-weight: 600;
                color: var(--text);
                margin-bottom: 12px;
                display: flex;
                align-items: center;
                gap: 8px;
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
                color: #00ff9d;
            }}
            
            .med-col {{
                color: #ff0066;
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
            
            .feed-container {{
                max-height: 800px;
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
        <!-- TICKER WITH REMITTANCE RATES -->
        <div class="ticker-wrapper">
            <div class="ticker">
                {ticker_html}
            </div>
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
                    </div>
                    
                    <div class="chart-card">
                        <div class="chart-title">📊 Live Price Distribution by Exchange</div>
                        <div id="priceDistChart" class="plotly-chart"></div>
                    </div>
                    
                    <div class="chart-card">
                        <div class="chart-title">📈 Price Trend & Premium</div>
                        <div style="display:flex;gap:8px;margin-bottom:15px;flex-wrap:wrap;">
                            <button class="trend-btn active" data-trend="1h" onclick="filterTrend('1h')">1H</button>
                            <button class="trend-btn" data-trend="1d" onclick="filterTrend('1d')">1D</button>
                            <button class="trend-btn" data-trend="1w" onclick="filterTrend('1w')">1W</button>
                            <button class="trend-btn" data-trend="all" onclick="filterTrend('all')">ALL</button>
                        </div>
                        <div id="trendChart" class="plotly-chart"></div>
                    </div>
                    
                    <div class="table-card">
                        <h3>Market Summary by Exchange</h3>
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
                        </div>
                    </div>
                    <div class="feed-container" id="feedContainer">
                        {feed_html}
                    </div>
                </div>
            </div>
            
            <!-- Live Market Insight - Supply & Demand by Price -->
            <div class="chart-card" style="margin:20px 0;">
                <div class="chart-title">📊 Live Market Insight</div>
                <div style="display:grid;grid-template-columns:1fr 1fr;gap:30px;">
                    <!-- Supply (Sell Orders) -->
                    <div>
                        <div style="font-size:16px;font-weight:700;color:var(--green);margin-bottom:15px;">Total Market Supply (Sell Orders)</div>
                        <div style="display:grid;grid-template-columns:120px 80px 1fr;gap:8px;font-size:13px;color:var(--text-secondary);margin-bottom:10px;padding-bottom:8px;border-bottom:1px solid var(--border);">
                            <span>USD Supply</span>
                            <span>At Price</span>
                            <span>Volume by Exchange</span>
                        </div>
                        <div id="supplyChart" style="max-height:400px;overflow-y:auto;"></div>
                    </div>
                    
                    <!-- Demand (Buy Orders) -->
                    <div>
                        <div style="font-size:16px;font-weight:700;color:var(--red);margin-bottom:15px;">Total Market Demand (Buy Orders)</div>
                        <div style="display:grid;grid-template-columns:120px 80px 1fr;gap:8px;font-size:13px;color:var(--text-secondary);margin-bottom:10px;padding-bottom:8px;border-bottom:1px solid var(--border);">
                            <span>USD Demand</span>
                            <span>At Price</span>
                            <span>Volume by Exchange</span>
                        </div>
                        <div id="demandChart" style="max-height:400px;overflow-y:auto;"></div>
                    </div>
                </div>
                
                <!-- Legend -->
                <div style="display:flex;justify-content:center;gap:24px;margin-top:20px;padding-top:15px;border-top:1px solid var(--border);">
                    <div style="display:flex;align-items:center;gap:8px;">
                        <div style="width:16px;height:16px;background:#F3BA2F;border-radius:4px;"></div>
                        <span style="font-size:13px;">🟡 Binance</span>
                    </div>
                    <div style="display:flex;align-items:center;gap:8px;">
                        <div style="width:16px;height:16px;background:#2E55E6;border-radius:4px;"></div>
                        <span style="font-size:13px;">🔵 MEXC</span>
                    </div>
                    <div style="display:flex;align-items:center;gap:8px;">
                        <div style="width:16px;height:16px;background:#A855F7;border-radius:4px;"></div>
                        <span style="font-size:13px;">🟣 OKX</span>
                    </div>
                </div>
            </div>
            
            <!-- Transaction Statistics -->
            <div class="stats-panel">
                <div class="stats-title">Transaction Statistics (Within 24 hrs)</div>
                
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
                            <div class="stat-label">Overall (24h)</div>
                            <div class="stat-value green">{overall_buys}</div>
                            <div class="stat-volume">{overall_buy_volume:,.0f} USDT</div>
                        </div>
                    </div>
                </div>
                
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
                            <div class="stat-label">Overall (24h)</div>
                            <div class="stat-value red">{overall_sells}</div>
                            <div class="stat-volume">{overall_sell_volume:,.0f} USDT</div>
                        </div>
                    </div>
                </div>
            </div>
            
            <!-- AI ANALYSIS AT BOTTOM -->
            {ai_summary_html}
            
            <footer>
                Official Rate: {official:.2f} ETB | Last Update: {timestamp} UTC<br>
                v42.9 AI-Powered • Market Depth by Price • Premium Tracking 🤖📊
            </footer>
        </div>
        
        <script>
            const allTrades = {json.dumps(recent_trades)};
            let currentPeriod = 'live';
            let currentSource = 'all';
            let currentTrendPeriod = '1d';
            
            const chartData = {chart_data_json};
            const historyData = {history_data_json};
            const tradeVolume = {trade_volume_json};
            const marketDepth = {market_depth_json};
            
            // Render Market Depth (Supply/Demand by Price)
            function renderMarketDepth() {{
                const colors = {{
                    'BINANCE': '#F3BA2F',
                    'MEXC': '#2E55E6', 
                    'OKX': '#A855F7'
                }};
                
                // Render Supply (Sell Orders) - Green side
                const supplyContainer = document.getElementById('supplyChart');
                let supplyHtml = '';
                const supplyData = marketDepth.supply || [];
                const maxSupply = Math.max(...supplyData.map(d => d.total), 1);
                
                supplyData.slice(0, 15).forEach(item => {{
                    const binancePct = (item.BINANCE / maxSupply * 100) || 0;
                    const mexcPct = (item.MEXC / maxSupply * 100) || 0;
                    const okxPct = (item.OKX / maxSupply * 100) || 0;
                    
                    supplyHtml += `
                        <div style="display:grid;grid-template-columns:120px 80px 1fr;gap:8px;align-items:center;margin-bottom:8px;">
                            <span style="font-weight:600;color:var(--text);">$${{item.total.toLocaleString(undefined, {{maximumFractionDigits:0}})}}</span>
                            <span style="color:var(--green);font-weight:600;">${{item.price}} Br</span>
                            <div style="display:flex;height:20px;border-radius:4px;overflow:hidden;background:var(--border);">
                                ${{item.BINANCE > 0 ? `<div style="width:${{binancePct}}%;background:#F3BA2F;" title="Binance: $${{item.BINANCE.toLocaleString()}}"></div>` : ''}}
                                ${{item.MEXC > 0 ? `<div style="width:${{mexcPct}}%;background:#2E55E6;" title="MEXC: $${{item.MEXC.toLocaleString()}}"></div>` : ''}}
                                ${{item.OKX > 0 ? `<div style="width:${{okxPct}}%;background:#A855F7;" title="OKX: $${{item.OKX.toLocaleString()}}"></div>` : ''}}
                            </div>
                        </div>
                    `;
                }});
                
                if (supplyData.length === 0) {{
                    supplyHtml = '<div style="text-align:center;padding:20px;color:var(--text-secondary);">No supply data</div>';
                }}
                supplyContainer.innerHTML = supplyHtml;
                
                // Render Demand (Buy Orders) - Red side
                const demandContainer = document.getElementById('demandChart');
                let demandHtml = '';
                const demandData = marketDepth.demand || [];
                const maxDemand = Math.max(...demandData.map(d => d.total), 1);
                
                demandData.slice(0, 15).forEach(item => {{
                    const binancePct = (item.BINANCE / maxDemand * 100) || 0;
                    const mexcPct = (item.MEXC / maxDemand * 100) || 0;
                    const okxPct = (item.OKX / maxDemand * 100) || 0;
                    
                    demandHtml += `
                        <div style="display:grid;grid-template-columns:120px 80px 1fr;gap:8px;align-items:center;margin-bottom:8px;">
                            <span style="font-weight:600;color:var(--text);">$${{item.total.toLocaleString(undefined, {{maximumFractionDigits:0}})}}</span>
                            <span style="color:var(--red);font-weight:600;">${{item.price}} Br</span>
                            <div style="display:flex;height:20px;border-radius:4px;overflow:hidden;background:var(--border);">
                                ${{item.BINANCE > 0 ? `<div style="width:${{binancePct}}%;background:#F3BA2F;" title="Binance: $${{item.BINANCE.toLocaleString()}}"></div>` : ''}}
                                ${{item.MEXC > 0 ? `<div style="width:${{mexcPct}}%;background:#2E55E6;" title="MEXC: $${{item.MEXC.toLocaleString()}}"></div>` : ''}}
                                ${{item.OKX > 0 ? `<div style="width:${{okxPct}}%;background:#A855F7;" title="OKX: $${{item.OKX.toLocaleString()}}"></div>` : ''}}
                            </div>
                        </div>
                    `;
                }});
                
                if (demandData.length === 0) {{
                    demandHtml = '<div style="text-align:center;padding:20px;color:var(--text-secondary);">No demand data</div>';
                }}
                demandContainer.innerHTML = demandHtml;
            }}
            
            // Filter trend chart by time period
            function filterTrend(period) {{
                currentTrendPeriod = period;
                
                document.querySelectorAll('.trend-btn').forEach(btn => {{
                    btn.classList.remove('active');
                }});
                document.querySelector(`[data-trend="${{period}}"]`).classList.add('active');
                
                renderTrendChart(period);
            }}
            
            function renderTrendChart(period) {{
                const isDark = document.documentElement.getAttribute('data-theme') !== 'light';
                const bgColor = isDark ? '#1C1C1E' : '#ffffff';
                const textColor = isDark ? '#ffffff' : '#1a1a1a';
                const gridColor = isDark ? '#38383A' : '#e0e0e0';
                
                if (!historyData.dates || historyData.dates.length < 2) {{
                    document.getElementById('trendChart').innerHTML = '<div style="padding:60px;text-align:center;color:var(--text-secondary)"><div style="font-size:48px;margin-bottom:16px">📈</div><div>Collecting trend data...</div></div>';
                    return;
                }}
                
                // Filter data based on period
                let filteredDates = historyData.dates;
                let filteredMedians = historyData.medians;
                let filteredOfficials = historyData.officials;
                let filteredPremiums = historyData.premiums || [];
                
                const now = new Date();
                let cutoffTime;
                
                switch(period) {{
                    case '1h':
                        cutoffTime = new Date(now - 60 * 60 * 1000);
                        break;
                    case '1d':
                        cutoffTime = new Date(now - 24 * 60 * 60 * 1000);
                        break;
                    case '1w':
                        cutoffTime = new Date(now - 7 * 24 * 60 * 60 * 1000);
                        break;
                    case 'all':
                    default:
                        cutoffTime = new Date(0);
                }}
                
                // Filter arrays
                const indices = [];
                filteredDates.forEach((d, i) => {{
                    if (new Date(d) >= cutoffTime) indices.push(i);
                }});
                
                if (indices.length < 2) {{
                    document.getElementById('trendChart').innerHTML = '<div style="padding:60px;text-align:center;color:var(--text-secondary)"><div style="font-size:48px;margin-bottom:16px">📈</div><div>Not enough data for this period</div></div>';
                    return;
                }}
                
                const dates = indices.map(i => filteredDates[i]);
                const medians = indices.map(i => filteredMedians[i]);
                const officials = indices.map(i => filteredOfficials[i]);
                const premiums = indices.map(i => filteredPremiums[i] || 0);
                
                const lastIdx = medians.length - 1;
                const lastMedian = medians[lastIdx];
                const lastOfficial = officials[lastIdx] || 127;
                const lastPremium = premiums[lastIdx] || 0;
                
                const trendTraces = [];
                
                // Official rate (base line)
                if (officials && officials.some(v => v > 0)) {{
                    trendTraces.push({{
                        type: 'scatter',
                        mode: 'lines',
                        name: 'Official Rate',
                        x: dates,
                        y: officials,
                        line: {{ color: '#FF9500', width: 2, dash: 'dot' }},
                        hovertemplate: '<b>Official:</b> %{{y:.2f}} ETB<extra></extra>'
                    }});
                }}
                
                // Black market rate with fill
                trendTraces.push({{
                    type: 'scatter',
                    mode: 'lines',
                    name: 'Black Market Rate',
                    x: dates,
                    y: medians,
                    line: {{ color: '#00ff9d', width: 3 }},
                    fill: 'tonexty',
                    fillcolor: 'rgba(0, 255, 157, 0.15)',
                    hovertemplate: '<b>Black Market:</b> %{{y:.2f}} ETB<extra></extra>'
                }});
                
                // Premium on secondary axis
                trendTraces.push({{
                    type: 'scatter',
                    mode: 'lines+markers',
                    name: 'Premium %',
                    x: dates,
                    y: premiums,
                    line: {{ color: '#FF3B30', width: 2, dash: 'dash' }},
                    marker: {{ size: 4 }},
                    yaxis: 'y2',
                    hovertemplate: '<b>Premium:</b> %{{y:.1f}}%<extra></extra>'
                }});
                
                const allYValues = [...medians, ...officials.filter(v => v > 0)];
                const minY = Math.floor(Math.min(...allYValues) / 10) * 10 - 10;
                const maxY = Math.ceil(Math.max(...allYValues) / 10) * 10 + 20;
                
                const maxPremium = Math.max(...premiums) + 5;
                
                const trendLayout = {{
                    paper_bgcolor: bgColor,
                    plot_bgcolor: bgColor,
                    font: {{ color: textColor, family: '-apple-system, BlinkMacSystemFont, sans-serif' }},
                    showlegend: true,
                    legend: {{ orientation: 'h', y: -0.18 }},
                    margin: {{ l: 60, r: 60, t: 20, b: 70 }},
                    xaxis: {{
                        gridcolor: gridColor,
                        tickformat: period === '1h' ? '%H:%M' : '%m/%d %H:%M'
                    }},
                    yaxis: {{
                        title: 'Rate (ETB)',
                        gridcolor: gridColor,
                        zerolinecolor: gridColor,
                        range: [minY, maxY],
                        dtick: 10
                    }},
                    yaxis2: {{
                        title: 'Premium (%)',
                        overlaying: 'y',
                        side: 'right',
                        showgrid: false,
                        range: [0, maxPremium],
                        ticksuffix: '%'
                    }},
                    hovermode: 'x unified',
                    annotations: [
                        {{
                            x: dates[lastIdx],
                            y: lastMedian,
                            xanchor: 'left',
                            yanchor: 'middle',
                            text: '<b>' + lastMedian.toFixed(1) + '</b>',
                            font: {{ color: '#00ff9d', size: 12 }},
                            showarrow: false,
                            xshift: 10,
                            bgcolor: 'rgba(0,0,0,0.7)',
                            borderpad: 4
                        }},
                        {{
                            x: dates[lastIdx],
                            y: lastPremium,
                            xanchor: 'left',
                            yanchor: 'middle',
                            xref: 'x',
                            yref: 'y2',
                            text: '<b>' + lastPremium.toFixed(1) + '%</b>',
                            font: {{ color: '#FF3B30', size: 11 }},
                            showarrow: false,
                            xshift: 10,
                            bgcolor: 'rgba(0,0,0,0.7)',
                            borderpad: 4
                        }}
                    ]
                }};
                
                Plotly.newPlot('trendChart', trendTraces, trendLayout, {{responsive: true, displayModeBar: false}});
            }}
            
            function initCharts() {{
                const isDark = document.documentElement.getAttribute('data-theme') !== 'light';
                const bgColor = isDark ? '#1C1C1E' : '#ffffff';
                const textColor = isDark ? '#ffffff' : '#1a1a1a';
                const gridColor = isDark ? '#38383A' : '#e0e0e0';
                
                const scatterTraces = [];
                const colors = {{
                    'BINANCE': '#F3BA2F',
                    'MEXC': '#2E55E6', 
                    'OKX': '#A855F7'
                }};
                
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
                            marker: {{ 
                                color: colors[exchange] || '#00C805',
                                size: 10,
                                opacity: 0.75,
                                line: {{ color: 'rgba(255,255,255,0.5)', width: 1 }}
                            }},
                            hovertemplate: '<b>%{{y:.2f}} ETB</b><extra>' + exchange + '</extra>'
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
                        line: {{ color: '#00ff9d', width: 3, dash: 'solid' }},
                        hoverinfo: 'name'
                    }});
                }}
                
                const minPrice = allPrices.length > 0 ? Math.min(...allPrices) - 5 : 130;
                const maxPrice = allPrices.length > 0 ? Math.max(...allPrices) + 5 : 190;
                
                const scatterLayout = {{
                    paper_bgcolor: bgColor,
                    plot_bgcolor: bgColor,
                    font: {{ color: textColor, family: '-apple-system, BlinkMacSystemFont, sans-serif' }},
                    showlegend: true,
                    legend: {{ orientation: 'h', y: -0.15 }},
                    margin: {{ l: 60, r: 30, t: 30, b: 60 }},
                    yaxis: {{
                        title: 'Price (ETB)',
                        gridcolor: gridColor,
                        zerolinecolor: gridColor,
                        range: [minPrice, maxPrice],
                        dtick: 5
                    }},
                    xaxis: {{
                        gridcolor: gridColor,
                        tickmode: 'array',
                        tickvals: exchangeNames.map((_, i) => i),
                        ticktext: exchangeNames,
                        range: [-0.5, Math.max(exchangeNames.length - 0.5, 0.5)]
                    }}
                }};
                
                Plotly.newPlot('priceDistChart', scatterTraces, scatterLayout, {{responsive: true, displayModeBar: false}});
                
                // Render trend chart with default period
                renderTrendChart(currentTrendPeriod);
                
                // Render market depth
                renderMarketDepth();
            }}
            
            document.addEventListener('DOMContentLoaded', function() {{
                initCharts();
            }});
            
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
                    case 'live':
                    default: cutoff = 0;
                }}
                
                let filtered = allTrades.filter(t => {{
                    return t.timestamp > cutoff && 
                           (t.type === 'buy' || t.type === 'sell' || t.type === 'request');
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
                
                const sorted = trades.sort((a, b) => b.timestamp - a.timestamp);
                
                const html = sorted.map(trade => {{
                    const date = new Date(trade.timestamp * 1000);
                    const time = date.toLocaleTimeString('en-US', {{hour: '2-digit', minute: '2-digit'}});
                    const ageMin = Math.floor((Date.now() / 1000 - trade.timestamp) / 60);
                    const age = ageMin < 60 ? ageMin + 'm ago' : Math.floor(ageMin/60) + 'h ago';
                    
                    let icon, action, color;
                    
                    if (trade.type === 'request') {{
                        const requestType = trade.request_type || 'REQUEST';
                        const isBuyRequest = requestType.includes('BUY');
                        icon = isBuyRequest ? '➕' : '➖';
                        action = requestType;
                        color = isBuyRequest ? 'var(--green)' : 'var(--red)';
                    }} else {{
                        const isBuy = trade.type === 'buy';
                        icon = isBuy ? '↗' : '↘';
                        action = isBuy ? 'BOUGHT' : 'SOLD';
                        color = isBuy ? 'var(--green)' : 'var(--red)';
                    }}
                    
                    let sourceColor, sourceEmoji;
                    if (trade.source === 'BINANCE') {{
                        sourceColor = '#F3BA2F';
                        sourceEmoji = '🟡';
                    }} else if (trade.source === 'MEXC') {{
                        sourceColor = '#2E55E6';
                        sourceEmoji = '🔵';
                    }} else {{
                        sourceColor = '#A855F7';
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
    """Server-side initial feed rendering"""
    if not trades:
        return '<div style="padding:20px;text-align:center;color:var(--text-secondary)">Waiting for market activity...</div>'
    
    html = ""
    valid_count = 0
    
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
        
        valid_count += 1
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


# --- MAIN ---
def main():
    print("🔍 Running v42.9 (AI + Remittance Rates!)...", file=sys.stderr)
    print(f"   🤖 AI Analysis: Gemini with Forecasting", file=sys.stderr)
    print(f"   💱 Remittance: Western Union, Remitly, Ria in ticker", file=sys.stderr)
    print(f"   🔄 Fallback: p2p.army when RapidAPI fails", file=sys.stderr)
    print(f"   ❌ Bybit: REMOVED", file=sys.stderr)
    
    NUM_SNAPSHOTS = 8
    WAIT_TIME = 15
    all_trades = []
    
    print(f"   > Snapshot 1/{NUM_SNAPSHOTS}...", file=sys.stderr)
    prev_snapshot = capture_market_snapshot()
    save_market_state(prev_snapshot)
    print("   > Saved baseline snapshot", file=sys.stderr)
    
    peg = fetch_usdt_peg() or 1.0
    
    for i in range(2, NUM_SNAPSHOTS + 1):
        print(f"   > ⏳ Waiting {WAIT_TIME}s to catch trades...", file=sys.stderr)
        time.sleep(WAIT_TIME)
        
        print(f"   > Snapshot {i}/{NUM_SNAPSHOTS}...", file=sys.stderr)
        current_snapshot = capture_market_snapshot()
        
        trades_this_round = detect_real_trades(current_snapshot, peg)
        if trades_this_round:
            all_trades.extend(trades_this_round)
            print(f"   ✅ Round {i-1}: Detected {len(trades_this_round)} trades", file=sys.stderr)
        
        save_market_state(current_snapshot)
        prev_snapshot = current_snapshot
    
    # Final snapshot
    print("   > Final snapshot for display...", file=sys.stderr)
    with ThreadPoolExecutor(max_workers=6) as ex:
        f_binance = ex.submit(fetch_binance_both_sides)
        f_mexc = ex.submit(fetch_mexc_both_sides)
        f_okx = ex.submit(fetch_exchange_both_sides, "okx")
        f_off = ex.submit(fetch_official_rate)
        f_remittance = ex.submit(fetch_remittance_rates)
        
        bin_ads = f_binance.result() or []
        mexc_ads = f_mexc.result() or []
        okx_ads = f_okx.result() or []
        official = f_off.result() or 0.0
        remittance_rates = f_remittance.result() or {}
    
    print(f"   🔍 Final snapshot:", file=sys.stderr)
    print(f"      BINANCE: {len(bin_ads)} ads", file=sys.stderr)
    print(f"      MEXC: {len(mexc_ads)} ads", file=sys.stderr)
    print(f"      OKX: {len(okx_ads)} ads", file=sys.stderr)
    
    bin_ads = remove_outliers(bin_ads, peg)
    mexc_ads = remove_outliers(mexc_ads, peg)
    okx_ads = remove_outliers(okx_ads, peg)
    
    final_snapshot = bin_ads + mexc_ads + okx_ads
    grouped_ads = {"BINANCE": bin_ads, "MEXC": mexc_ads, "OKX": okx_ads}
    
    if all_trades:
        save_trades(all_trades)
        print(f"   💾 Saved {len(all_trades)} total trades", file=sys.stderr)
    
    if final_snapshot:
        all_prices = [x['price'] for x in final_snapshot]
        stats = analyze(all_prices, peg)
        
        if stats:
            save_to_history(stats, official)
            
            # Load history and trades for AI
            history_data = load_history()
            recent_trades = load_recent_trades()
            trade_stats = calculate_trade_stats(recent_trades)
            volume_by_exchange = calculate_volume_by_exchange(recent_trades)
            
            # Generate AI summary with forecasting
            ai_summary = load_cached_ai_summary()
            if not ai_summary:
                ai_summary = generate_ai_summary(stats, official, trade_stats, volume_by_exchange, history_data)
            
            if not ai_summary:
                print("   ⚠️ Using emergency fallback for AI", file=sys.stderr)
                ai_summary = create_fallback_summary(stats, official, trade_stats)
            
            # Generate HTML with AI summary and remittance rates
            update_website_html(
                stats, official,
                time.strftime("%Y-%m-%d %H:%M:%S"),
                final_snapshot, grouped_ads, peg,
                ai_summary=ai_summary,
                remittance_rates=remittance_rates
            )
    else:
        print("⚠️ No ads found", file=sys.stderr)
    
    buys = len([t for t in all_trades if t.get('type') == 'buy'])
    sells = len([t for t in all_trades if t.get('type') == 'sell'])
    print(f"\n🎯 TOTAL COVERAGE: {NUM_SNAPSHOTS} snapshots × {WAIT_TIME}s = {(NUM_SNAPSHOTS-1)*WAIT_TIME}s monitored")
    print(f"✅ Complete! Detected {buys} buys, {sells} sells this run.")


if __name__ == "__main__":
    main()
