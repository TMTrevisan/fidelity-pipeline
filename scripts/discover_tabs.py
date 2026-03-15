#!/usr/bin/env python3
"""
Dynamic Chrome tab discovery for Fidelity research tabs.
Replaces hardcoded tab IDs with URL-based lookup.

Uses CDP /json endpoint (HTTP) as primary method, with WebSocket
CDP Target.getTargets as fallback.
"""
import asyncio
import json
import urllib.request

# Try HTTP CDP endpoint first (simpler, no WebSocket needed)
CDP_HTTP_URL = "http://localhost:9222/json"

# WebSocket relay fallback
RELAY_WS_BASE = "ws://127.0.0.1:18792/cdp"
RELAY_TOKEN = "84b5e26230b9a2a142ff5a386dae425f41b73554d302948d"

# URL patterns to match
ARGUS_URL = "Argus.asp"
ZACKS_URL = "ZacksResearch.asp"


def discover_tabs_http():
    """Query Chrome's /json endpoint to find tabs by URL pattern."""
    try:
        req = urllib.request.Request(CDP_HTTP_URL)
        with urllib.request.urlopen(req, timeout=5) as resp:
            tabs = json.loads(resp.read())

        argus_tab = None
        zacks_tab = None

        for t in tabs:
            url = t.get("url", "")
            tab_id = t.get("id", "")
            tab_type = t.get("type", "")

            # Only consider page-type tabs
            if tab_type != "page":
                continue

            if ARGUS_URL in url and not argus_tab:
                argus_tab = tab_id
            elif ZACKS_URL in url and not zacks_tab:
                zacks_tab = tab_id

        if argus_tab or zacks_tab:
            return {"argus": argus_tab, "zacks": zacks_tab}

    except Exception:
        pass

    return None


async def discover_tabs_websocket():
    """Query Chrome relay via WebSocket CDP for available tabs."""
    try:
        import websockets
    except ImportError:
        return {"argus": None, "zacks": None}

    ws_url = f"{RELAY_WS_BASE}?token={RELAY_TOKEN}"

    try:
        async with websockets.connect(ws_url, max_size=1*1024*1024) as ws:
            await ws.send(json.dumps({
                "id": 1,
                "method": "Target.getTargets",
                "params": {}
            }))

            while True:
                resp = json.loads(await ws.recv())
                if resp.get("id") == 1:
                    break

            targets = resp.get("result", {}).get("targetInfos", [])

            argus_tab = None
            zacks_tab = None

            for t in targets:
                url = t.get("url", "")
                target_id = t.get("targetId", "")
                if ARGUS_URL in url and not argus_tab:
                    argus_tab = target_id
                elif ZACKS_URL in url and not zacks_tab:
                    zacks_tab = target_id

            return {"argus": argus_tab, "zacks": zacks_tab}

    except Exception as e:
        print(f"WebSocket tab discovery failed: {e}")
        return {"argus": None, "zacks": None}


def get_tabs():
    """
    Discover Chrome tabs dynamically.
    Tries HTTP /json endpoint first, falls back to WebSocket CDP.
    """
    # Try HTTP endpoint first (simpler)
    result = discover_tabs_http()
    if result is not None:
        return result

    # Fallback to WebSocket CDP
    return asyncio.run(discover_tabs_websocket())


async def get_tabs_async():
    """Async version for use inside existing event loop."""
    result = discover_tabs_http()
    if result is not None:
        return result
    return await discover_tabs_websocket()


if __name__ == "__main__":
    tabs = get_tabs()
    print(f"Argus tab: {tabs['argus']}")
    print(f"Zacks tab: {tabs['zacks']}")
