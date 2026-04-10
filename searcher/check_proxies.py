"""
Proxy availability checker for monitoring-backend.
Fetches proxy list + tokens from ClickHouse,
tests each proxy via google.com or WB search endpoint.

Usage (inside container):
    python check_proxies.py                  # google.com, последовательно
    python check_proxies.py --concurrent     # google.com, все одновременно
    python check_proxies.py --search         # WB search с токеном, последовательно
    python check_proxies.py --search --concurrent  # WB search, все одновременно
"""
import asyncio
import sys
import httpx

from parser.db_config_loader import load_proxies_from_db, load_tokens_from_db, ProxyConfig
from settings import SEARCH_URL, logger


GOOGLE_URL = "https://www.google.com"
SEARCH_PARAMS = {
    "resultset": "catalog",
    "query": "джинсы",
    "limit": 10,
    "dest": -1257786,
    "page": 1,
    "ab_testing": "false",
    "appType": 64,
}
TIMEOUT = 5


def build_proxy_url(proxy: ProxyConfig) -> str:
    """Build httpx proxy URL with embedded auth."""
    url = proxy.proxy_url
    if not url.startswith("http://") and not url.startswith("https://"):
        url = f"http://{url}"
    if proxy.proxy_user and proxy.proxy_pass:
        url = url.replace("://", f"://{proxy.proxy_user}:{proxy.proxy_pass}@")
    return url


def check_single_proxy(proxy: ProxyConfig, test_url: str, headers: dict = None, params: dict = None) -> tuple[bool, str]:
    """Check single proxy (sync). Returns (ok, message)."""
    proxy_url = build_proxy_url(proxy)
    try:
        with httpx.Client(
            proxy=proxy_url,
            timeout=httpx.Timeout(TIMEOUT, connect=TIMEOUT),
        ) as client:
            resp = client.get(test_url, headers=headers, params=params)
            if test_url == SEARCH_URL:
                # For search, check products count
                try:
                    data = resp.json()
                    products = len(data.get("products", []))
                    return True, f"{resp.status_code} products={products} ({resp.elapsed.total_seconds():.2f}s)"
                except:
                    return True, f"{resp.status_code} (json parse error) ({resp.elapsed.total_seconds():.2f}s)"
            return True, f"{resp.status_code} ({resp.elapsed.total_seconds():.2f}s)"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


async def check_single_proxy_async(
    proxy: ProxyConfig, index: int, total: int,
    test_url: str, headers: dict = None, params: dict = None,
) -> tuple[str, bool, str]:
    """Check single proxy (async). Returns (proxy_url, ok, message)."""
    proxy_url = build_proxy_url(proxy)
    try:
        async with httpx.AsyncClient(
            proxy=proxy_url,
            timeout=httpx.Timeout(TIMEOUT, connect=TIMEOUT),
        ) as client:
            resp = await client.get(test_url, headers=headers, params=params)
            if test_url == SEARCH_URL:
                try:
                    data = resp.json()
                    products = len(data.get("products", []))
                    msg = f"{resp.status_code} products={products} ({resp.elapsed.total_seconds():.2f}s)"
                except:
                    msg = f"{resp.status_code} (json err) ({resp.elapsed.total_seconds():.2f}s)"
            else:
                msg = f"{resp.status_code} ({resp.elapsed.total_seconds():.2f}s)"
            print(f"[{index}/{total}] ✅ {proxy.proxy_url} — {msg}")
            return proxy.proxy_url, True, msg
    except Exception as e:
        msg = f"{type(e).__name__}: {e}"
        print(f"[{index}/{total}] ❌ {proxy.proxy_url} — {msg}")
        return proxy.proxy_url, False, msg


async def main():
    concurrent = "--concurrent" in sys.argv
    use_search = "--search" in sys.argv
    server = "monitoring"

    print(f"Fetching proxies for server '{server}' from ClickHouse...")
    proxies = await load_proxies_from_db(server)
    if not proxies:
        print("No proxies found. Exiting.")
        return

    # Setup test target
    test_url = GOOGLE_URL
    headers = None
    params = None

    if use_search:
        print("Loading tokens from ClickHouse...")
        tokens = await load_tokens_from_db(limit=1)
        if not tokens:
            print("No tokens found. Exiting.")
            return
        token = tokens[0]
        test_url = SEARCH_URL
        headers = {"Authorization": token}
        params = SEARCH_PARAMS
        print(f"Token: {token[:20]}...")

    mode = "CONCURRENT" if concurrent else "SEQUENTIAL"
    target = "WB Search" if use_search else "Google"
    print(f"Found {len(proxies)} proxies. User: {proxies[0].proxy_user or '(none)'}")
    print(f"Mode: {mode} | Target: {target} (timeout {TIMEOUT}s)\n")

    alive = []
    dead = []
    total = len(proxies)

    if concurrent:
        tasks = [
            check_single_proxy_async(proxy, i, total, test_url, headers, params)
            for i, proxy in enumerate(proxies, 1)
        ]
        results = await asyncio.gather(*tasks)
        for proxy_url, ok, msg in results:
            (alive if ok else dead).append(proxy_url)
    else:
        for i, proxy in enumerate(proxies, 1):
            ok, msg = check_single_proxy(proxy, test_url, headers, params)
            status = "✅" if ok else "❌"
            print(f"[{i}/{total}] {status} {proxy.proxy_url} — {msg}")
            (alive if ok else dead).append(proxy.proxy_url)

    # Summary
    print("\n" + "=" * 60)
    print(f"Server:  {server}")
    print(f"Target:  {target}")
    print(f"Total:   {total}")
    print(f"✅ Alive: {len(alive)}")
    print(f"❌ Dead:  {len(dead)}")
    print(f"Success: {len(alive)/total*100:.1f}%")

    if alive:
        print(f"\nWorking proxies ({len(alive)}):")
        for h in alive:
            print(f"  {h}")

    if dead:
        print(f"\nDead proxies ({len(dead)}):")
        for h in dead:
            print(f"  {h}")


if __name__ == "__main__":
    asyncio.run(main())
