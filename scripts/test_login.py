#!/usr/bin/env python3
"""Quick test of login handler - check if we're already logged in."""
import asyncio
import json
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from login_handler import attempt_login
from discover_tabs import get_tabs

async def test():
    # Discover tabs dynamically instead of hardcoding
    tabs = get_tabs()
    target = tabs.get('argus')
    
    if not target:
        print("No Argus tab found. Make sure Chrome is open with Fidelity research page.")
        return
    
    print(f"Testing login handler on Argus tab: {target[:8]}...")
    result = await attempt_login(target)
    print(json.dumps(result, indent=2))

asyncio.run(test())
