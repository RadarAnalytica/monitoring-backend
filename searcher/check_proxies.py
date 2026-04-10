"""
Proxy availability checker for monitoring-backend.
Fetches proxy list from ClickHouse (radar.harvest_proxies),
tests each proxy via google.com, prints report.

Usage (inside container):
    python check_proxies.py
    python check_proxies.py --concurrent
    python check_proxies.py --server monitoring
"""
import asyncio
import sys
import httpx

from parser.db_config_loader import load_proxies_from_db, ProxyConfig
from settings import logger


TEST_URL = "https://www.google.com"
TIMEOUT = 5


def build_proxy_url(proxy: ProxyConfig) -> str:
    """Build httpx proxy URL with embedded auth."""
    url = proxy.proxy_url
    if not url.startswith("http://") and not url.startswith("https://"):
        url = f"http://{url}"
    if proxy.proxy_user and proxy.proxy_pass:
        url = url.replace("://", f"://{proxy.proxy_user}:{proxy.proxy_pass}@")
    return url


def check_single_proxy(proxy: ProxyConfig) -> tuple[bool, str]:
    """Check single proxy (sync). Returns (ok, message)."""
    proxy_url = build_proxy_url(proxy)
    try:
        with httpx.Client(
            proxy=proxy_url,
            timeout=httpx.Timeout(TIMEOUT, connect=TIMEOUT),
        ) as client:
            resp = client.get(TEST_URL)
            return True, f"{resp.status_code} ({resp.elapsed.total_seconds():.2f}s)"
    except httpx.ConnectTimeout:
        return False, "ConnectTimeout"
    except httpx.ConnectError:
        return False, "ConnectError"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


async def check_single_proxy_async(proxy: ProxyConfig, index: int, total: int) -> tuple[str, bool, str]:
    """Check single proxy (async). Returns (proxy_url, ok, message)."""
    proxy_url = build_proxy_url(proxy)
    try:
        async with httpx.AsyncClient(
            proxy=proxy_url,
            timeout=httpx.Timeout(TIMEOUT, connect=TIMEOUT),
        ) as client:
            resp = await client.get(TEST_URL)
            msg = f"{resp.status_code} ({resp.elapsed.total_seconds():.2f}s)"
            print(f"[{index}/{total}] ✅ {proxy.proxy_url} — {msg}")
            return proxy.proxy_url, True, msg
    except httpx.ConnectTimeout:
        print(f"[{index}/{total}] ❌ {proxy.proxy_url} — ConnectTimeout")
        return proxy.proxy_url, False, "ConnectTimeout"
    except httpx.ConnectError:
        print(f"[{index}/{total}] ❌ {proxy.proxy_url} — ConnectError")
        return proxy.proxy_url, False, "ConnectError"
    except Exception as e:
        msg = f"{type(e).__name__}: {e}"
        print(f"[{index}/{total}] ❌ {proxy.proxy_url} — {msg}")
        return proxy.proxy_url, False, msg


async def main():
    concurrent = "--concurrent" in sys.argv
    server = "monitoring"

    print(f"Fetching proxies for server '{server}' from ClickHouse...")

    proxies = await load_proxies_from_db(server)
    if not proxies:
        print("No proxies found. Exiting.")
        return

    mode = "CONCURRENT" if concurrent else "SEQUENTIAL"
    print(f"Found {len(proxies)} proxies. User: {proxies[0].proxy_user or '(none)'}")
    print(f"Mode: {mode} | Testing against {TEST_URL} (timeout {TIMEOUT}s)...\n")

    alive = []
    dead = []
    total = len(proxies)

    if concurrent:
        tasks = [
            check_single_proxy_async(proxy, i, total)
            for i, proxy in enumerate(proxies, 1)
        ]
        results = await asyncio.gather(*tasks)
        for proxy_url, ok, msg in results:
            if ok:
                alive.append(proxy_url)
            else:
                dead.append(proxy_url)
    else:
        for i, proxy in enumerate(proxies, 1):
            ok, msg = check_single_proxy(proxy)
            status = "✅" if ok else "❌"
            print(f"[{i}/{total}] {status} {proxy.proxy_url} — {msg}")
            if ok:
                alive.append(proxy.proxy_url)
            else:
                dead.append(proxy.proxy_url)

    # Summary
    print("\n" + "=" * 60)
    print(f"Server:  {server}")
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
