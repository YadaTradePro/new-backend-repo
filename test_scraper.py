import json, time, logging, os, re, random
from typing import Dict, Any, List, Optional
import requests

TGJU_URL = "https://call5.tgju.org/ajax.json"
CACHE_PATH = "tgju_cache.json"
CACHE_TTL_SEC = 300  # 5 دقیقه

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s"
)
log = logging.getLogger("tgju")

# پیکربندی آیتم‌های هدف با نام‌های محتمل و الگوهای جستجو
TARGETS = [
    {
        "id": "dollar_free",
        "aliases": ["price_dollar_rl", "dollar_rl", "dollar_free"],
        "contains": ["دلار", "price_dollar", "dollar"],
    },
    {
        "id": "gold_18k",
        "aliases": ["tgju_gold_irg18", "gold_18k", "geram18"],
        "contains": ["طلای 18", "18k", "geram 18", "گرم ۱۸"],
    },
    {
        "id": "gold_24k",
        "aliases": ["gold_24k", "geram24"],
        "contains": ["طلای 24", "24k", "گرم ۲۴", "geram 24"],
        # اگر نبود از 18 عیار برآورد می‌کنیم
        "derive_from": "gold_18k",
        "derive_fn": lambda x: x * (24/18.0)
    },
    {
        "id": "mesghal",
        "aliases": ["mesghal"],
        "contains": ["مثقال"],
        # اختیاری: می‌شود از گرم 24 برآورد کرد (ولی بازار ایران ممکنه تفاوت کارمزدی داشته باشد)
        # "derive_from": "gold_24k",
        # "derive_fn": lambda x: x * 4.6083
    },
    {
        "id": "sekee",
        "aliases": ["sekee", "sekke", "sekkeh"],
        "contains": ["سکه امامی", "سکه طرح جدید", "emami", "coin emami"],
    },
]

def _num(s: Any) -> Optional[float]:
    """تبدیل امن رشته به عدد؛ اگر نشد None"""
    if s is None:
        return None
    if isinstance(s, (int, float)):
        return float(s)
    s = str(s)
    s = re.sub(r"[^\d\.\-]", "", s)  # فقط عدد/منفی/نقطه
    try:
        return float(s) if s != "" else None
    except ValueError:
        return None

def fetch_json_with_retry(url: str, retries=4, timeout=8) -> Dict[str, Any]:
    last_exc = None
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Python-Requests/2.x",
        "Accept": "application/json,text/plain;q=0.9,*/*;q=0.8",
        "Cache-Control": "no-cache",
    }
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_exc = e
            sleep_s = (2 ** (attempt - 1)) + random.random()
            log.warning("Fetch failed (attempt %s/%s): %s — retry in %.1fs",
                        attempt, retries, e, sleep_s)
            time.sleep(sleep_s)
    raise RuntimeError(f"Failed to fetch JSON after {retries} retries: {last_exc}")

def flatten_items(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """برخی کلیدهای بالا لیست‌هایی از آیتم‌ها هستند (last, tolerance_high, ...). همه را یکی می‌کنیم."""
    items = []
    for k, v in payload.items():
        if isinstance(v, list):
            items.extend([x for x in v if isinstance(x, dict)])
        elif isinstance(v, dict):
            # گاهی داده‌ها داخل یک دیکشنری دیگر هستند
            for vv in v.values():
                if isinstance(vv, list):
                    items.extend([x for x in vv if isinstance(x, dict)])
    return items

def match_item(items: List[Dict[str, Any]], cfg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """اول براساس name دقیق/aliases، بعد براساس contains در name/slug/title/title_en جستجو می‌کند."""
    aliases = set(a.lower() for a in cfg.get("aliases", []))
    contains = [c for c in cfg.get("contains", [])]

    # 1) تطبیق دقیق name
    for it in items:
        name = str(it.get("name", "")).lower()
        if name in aliases:
            return it

    # 2) جستجوی عبارت در name/slug/title/title_en
    def fields_str(it):
        return " || ".join([
            str(it.get("name", "")),
            str(it.get("slug", "")),
            str(it.get("title", "")),
            str(it.get("title_en", "")),
        ])

    for it in items:
        hay = fields_str(it)
        if any(c in hay for c in contains):
            return it

    return None

def load_cache() -> Dict[str, Any]:
    if not os.path.exists(CACHE_PATH):
        return {}
    try:
        with open(CACHE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_cache(cache: Dict[str, Any]) -> None:
    try:
        with open(CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log.warning("Failed to save cache: %s", e)

def now_ts() -> int:
    return int(time.time())

def is_fresh(ts: Optional[int], ttl: int) -> bool:
    return ts is not None and (now_ts() - ts) <= ttl

def extract_prices(payload: Dict[str, Any], use_cache=True) -> Dict[str, Any]:
    items = flatten_items(payload)
    cache = load_cache() if use_cache else {}
    out: Dict[str, Any] = {}

    # ابتدا تلاش برای یافتن مستقیم هر آیتم
    for cfg in TARGETS:
        item = match_item(items, cfg)
        if item:
            price = _num(item.get("p"))
            change = _num(item.get("d"))
            percent = _num(item.get("dp"))
            out[cfg["id"]] = {
                "title": item.get("title"),
                "name": item.get("name"),
                "price": price,
                "change": change,
                "percent": percent,
                "at": item.get("created_at") or item.get("t"),
                "source": "live"
            }
            log.debug("FOUND live '%s' (name=%s): %s", cfg["id"], item.get("name"), price)
            # کش را به‌روزرسانی کن
            cache[cfg["id"]] = {"ts": now_ts(), "data": out[cfg["id"]]}
        else:
            log.debug("MISSING '%s' in live payload.", cfg["id"])

    # سپس پرکردن جاهای خالی از کش
    for cfg in TARGETS:
        if cfg["id"] not in out and use_cache:
            cached = cache.get(cfg["id"])
            if cached and is_fresh(cached.get("ts"), CACHE_TTL_SEC):
                data = cached["data"]
                data = dict(data)  # copy
                data["source"] = "cache"
                out[cfg["id"]] = data
                log.info("Using cached '%s' (age=%ss).", cfg["id"], now_ts() - cached["ts"])

    # در نهایت، اگر هنوز نبود و امکان برآورد تعریف شده بود
    for cfg in TARGETS:
        if cfg["id"] not in out and cfg.get("derive_from") and cfg.get("derive_fn"):
            base_id = cfg["derive_from"]
            base = out.get(base_id)
            if base and base.get("price") is not None:
                derived_price = cfg["derive_fn"](base["price"])
                out[cfg["id"]] = {
                    "title": f"(estimated) {cfg['id']}",
                    "name": f"derived_from:{base_id}",
                    "price": round(derived_price),
                    "change": None,
                    "percent": None,
                    "at": None,
                    "source": f"derived_from_{base_id}"
                }
                log.info("Derived '%s' from '%s'.", cfg["id"], base_id)

    # ذخیره کش به‌روز
    if use_cache:
        save_cache(cache)

    return out

def get_prices() -> Dict[str, Any]:
    payload = fetch_json_with_retry(TGJU_URL, retries=4, timeout=8)
    return extract_prices(payload, use_cache=True)

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.DEBUG)  # برای دیباگ بیشتر
    data = get_prices()
    print(json.dumps(data, ensure_ascii=False, indent=2))