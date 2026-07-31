import requests
import threading
from collections import deque
from bs4 import BeautifulSoup


# ── Scrape free proxies from multiple sources ─────────────────────
def scrape_free_proxies() -> list[str]:
    proxies = []
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

    # ── Source 1: free-proxy-list.net ────────────────────────────
    try:
        r = requests.get("https://free-proxy-list.net/", headers=headers, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")
        table = soup.find("table")
        if table:
            for row in table.find_all("tr")[1:]:
                cols = row.find_all("td")
                if len(cols) >= 7:
                    ip      = cols[0].text.strip()
                    port    = cols[1].text.strip()
                    https   = cols[6].text.strip().lower()
                    scheme  = "https" if https == "yes" else "http"
                    proxies.append(f"{scheme}://{ip}:{port}")
        print(f"  [+] free-proxy-list.net     : {len(proxies)} proxies")
    except Exception as e:
        print(f"  [-] free-proxy-list.net failed: {e}")

    # ── Source 2: sslproxies.org ─────────────────────────────────
    count_before = len(proxies)
    try:
        r = requests.get("https://www.sslproxies.org/", headers=headers, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")
        table = soup.find("table")
        if table:
            for row in table.find_all("tr")[1:]:
                cols = row.find_all("td")
                if len(cols) >= 2:
                    ip   = cols[0].text.strip()
                    port = cols[1].text.strip()
                    proxies.append(f"https://{ip}:{port}")
        print(f"  [+] sslproxies.org          : {len(proxies) - count_before} proxies")
    except Exception as e:
        print(f"  [-] sslproxies.org failed: {e}")

    # ── Source 3: proxyscrape API ─────────────────────────────────
    count_before = len(proxies)
    try:
        url = (
            "https://api.proxyscrape.com/v2/?request=displayproxies"
            "&protocol=http&timeout=5000&country=all&ssl=all&anonymity=all"
        )
        r = requests.get(url, headers=headers, timeout=10)
        for line in r.text.strip().splitlines():
            line = line.strip()
            if ":" in line:
                proxies.append(f"http://{line}")
        print(f"  [+] proxyscrape API         : {len(proxies) - count_before} proxies")
    except Exception as e:
        print(f"  [-] proxyscrape API failed: {e}")

    # ── Source 4: geonode free-proxy list API ────────────────────
    count_before = len(proxies)
    try:
        url = (
            "https://proxylist.geonode.com/api/proxy-list"
            "?limit=100&page=1&sort_by=lastChecked&sort_type=desc&protocols=http,https"
        )
        r = requests.get(url, headers=headers, timeout=10)
        data = r.json()
        for entry in data.get("data", []):
            ip       = entry.get("ip", "")
            port     = entry.get("port", "")
            protocol = entry.get("protocols", ["http"])[0]
            if ip and port:
                proxies.append(f"{protocol}://{ip}:{port}")
        print(f"  [+] geonode API             : {len(proxies) - count_before} proxies")
    except Exception as e:
        print(f"  [-] geonode API failed: {e}")

    # ── Deduplicate ───────────────────────────────────────────────
    proxies = list(dict.fromkeys(proxies))
    print(f"\n  [*] Total unique proxies scraped: {len(proxies)}")
    return proxies


# ── ProxyPool ─────────────────────────────────────────────────────
class ProxyPool:
    def __init__(self, proxies: list[str], test_url="http://httpbin.org/ip", timeout=8):
        self._all     = proxies
        self._live    = deque()
        self._lock    = threading.Lock()
        self._test_url = test_url
        self._timeout  = timeout
        self._validate_all()

    def _validate_all(self):
        print(f"\n[*] Validating {len(self._all)} proxies in parallel...")
        threads = []
        for p in self._all:
            t = threading.Thread(target=self._check, args=(p,))
            t.daemon = True
            threads.append(t)
            t.start()
        for t in threads:
            t.join(timeout=self._timeout + 2)
        print(f"\n[*] {len(self._live)} / {len(self._all)} proxies passed validation.")
        if not self._live:
            raise RuntimeError("[!] No live proxies found. Try again later.")

    def _check(self, proxy_url: str):
        try:
            r = requests.get(
                self._test_url,
                proxies={"http": proxy_url, "https": proxy_url},
                timeout=self._timeout
            )
            if r.status_code == 200:
                with self._lock:
                    self._live.append(proxy_url)
                print(f"  [+] Live : {proxy_url}")
        except Exception:
            pass  # silently drop dead proxies

    def next(self) -> str:
        with self._lock:
            if not self._live:
                raise RuntimeError("[!] Proxy pool is empty. All proxies are dead.")
            proxy = self._live.popleft()
            self._live.append(proxy)
            return proxy

    def remove(self, proxy_url: str):
        with self._lock:
            try:
                self._live.remove(proxy_url)
                print(f"  [x] Removed dead proxy: {proxy_url} | Remaining: {len(self._live)}")
            except ValueError:
                pass

    def size(self) -> int:
        with self._lock:
            return len(self._live)

    def is_empty(self) -> bool:
        with self._lock:
            return len(self._live) == 0


# ── Session setup ─────────────────────────────────────────────────
S = requests.Session()
S.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "X-Requested-With": "XMLHttpRequest"
})

_current_proxy = {"url": None}


def set_new_proxy():
    _current_proxy["url"] = pool.next()
    S.proxies.update({
        "http":  _current_proxy["url"],
        "https": _current_proxy["url"]
    })
    print(f"  [*] Rotating IP -> {_current_proxy['url']}")


def report_bad_proxy():
    """Call inside except blocks when a proxy fails during actual use."""
    if _current_proxy["url"]:
        pool.remove(_current_proxy["url"])
    if pool.is_empty():
        raise RuntimeError("[!] All proxies exhausted.")
    set_new_proxy()


# ── Bootstrap ─────────────────────────────────────────────────────
print("[*] Scraping free proxies...")
raw_proxies = scrape_free_proxies()

pool = ProxyPool(raw_proxies)

set_new_proxy()
