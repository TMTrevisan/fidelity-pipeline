#!/usr/bin/env python3
"""
Handle Fidelity login when session expires.
Uses Chrome relay with saved password autofill.
"""
import asyncio
import json
import os
import websockets

RELAY_WS = "ws://127.0.0.1:18792/cdp"
RELAY_TOKEN = "84b5e26230b9a2a142ff5a386dae425f41b73554d302948d"

# Login URLs
LOGIN_URL = "https://digital.fidelity.com/prgw/digital/login/fullPage"
# After login, Fidelity redirects to portfolio summary or research page
RESEARCH_URL = "https://research2.fidelity.com/fidelity/research/reports/release2/Research/Argus.asp"

# JS to trigger Chrome's saved password autofill and submit
LOGIN_JS = """
(async () => {
    // Wait for the login form to appear
    const waitFor = (selector, timeout = 10000) => new Promise((resolve, reject) => {
        const el = document.querySelector(selector);
        if (el) return resolve(el);
        const observer = new MutationObserver(() => {
            const el = document.querySelector(selector);
            if (el) { observer.disconnect(); resolve(el); }
        });
        observer.observe(document.body, {childList: true, subtree: true});
        setTimeout(() => { observer.disconnect(); reject(new Error('timeout: ' + selector)); }, timeout);
    });

    const results = {steps: []};

    try {
        // Fidelity login page selectors (multiple variants for resilience)
        const usernameSelectors = [
            '#userId-input',           // Primary Fidelity selector
            '#username',               // Fallback
            'input[name="userId"]',    // Fallback
            'input[name="username"]',  // Fallback
            'input[type="text"]',      // Generic fallback
            '#dom-username-input',     // Older Fidelity
        ];
        const passwordSelectors = [
            '#password-input',
            '#password', 
            'input[name="password"]',
            'input[type="password"]',
            '#dom-password-input',
        ];

        // Find username field
        let usernameField = null;
        for (const sel of usernameSelectors) {
            usernameField = document.querySelector(sel);
            if (usernameField) {
                results.steps.push('found username: ' + sel);
                break;
            }
        }

        // Find password field
        let passwordField = null;
        for (const sel of passwordSelectors) {
            passwordField = document.querySelector(sel);
            if (passwordField) {
                results.steps.push('found password: ' + sel);
                break;
            }
        }

        if (!usernameField) {
            // Check if we're already logged in (redirected past login)
            if (!document.querySelector('input[type="password"]')) {
                results.alreadyLoggedIn = true;
                results.steps.push('no login form found - possibly already logged in');
                return JSON.stringify(results);
            }
            results.error = 'username field not found';
            return JSON.stringify(results);
        }

        // Focus username field to trigger Chrome's saved password dropdown
        usernameField.focus();
        usernameField.click();
        await new Promise(r => setTimeout(r, 500));

        // Try to trigger Chrome autofill by dispatching events
        // Chrome autofills when it detects a form interaction
        usernameField.dispatchEvent(new Event('focus', {bubbles: true}));
        usernameField.dispatchEvent(new Event('input', {bubbles: true}));
        await new Promise(r => setTimeout(r, 1000));

        // Check if Chrome autofilled the username
        const usernameFilled = usernameField.value && usernameField.value.length > 0;
        results.usernameAutofilled = usernameFilled;
        results.steps.push('username autofilled: ' + usernameFilled);

        if (passwordField) {
            const passwordFilled = passwordField.value && passwordField.value.length > 0;
            results.passwordAutofilled = passwordFilled;
            results.steps.push('password autofilled: ' + passwordFilled);
        }

        // If autofill didn't work, we need the user to have previously saved credentials
        // Chrome should autofill on form focus if credentials are saved
        if (!usernameFilled) {
            // Try clicking on the field and waiting longer
            usernameField.focus();
            await new Promise(r => setTimeout(r, 2000));
            const filled2 = usernameField.value && usernameField.value.length > 0;
            results.steps.push('username filled after longer wait: ' + filled2);
        }

        // Find and click the login/submit button
        const submitSelectors = [
            '#dom-login-button',                    // Primary
            '#login-button',                        // Fallback
            'button[type="submit"]',                // Generic
            'button[data-testid="login-button"]',   // Test ID
            '.login-btn',                           // Class
            'button.primary',                       // Generic primary button
        ];

        let submitBtn = null;
        for (const sel of submitSelectors) {
            submitBtn = document.querySelector(sel);
            if (submitBtn) {
                results.steps.push('found submit: ' + sel);
                break;
            }
        }

        // Also try looking for any button with "Log In" text
        if (!submitBtn) {
            const buttons = document.querySelectorAll('button');
            for (const btn of buttons) {
                if (btn.textContent.toLowerCase().includes('log in') || 
                    btn.textContent.toLowerCase().includes('sign in')) {
                    submitBtn = btn;
                    results.steps.push('found submit by text: ' + btn.textContent.trim());
                    break;
                }
            }
        }

        if (submitBtn && (usernameFilled || document.querySelector('input[type="password"]')?.value)) {
            submitBtn.click();
            results.steps.push('clicked submit');
            results.submitted = true;

            // Wait for redirect (login success)
            await new Promise(r => setTimeout(r, 5000));
            results.finalUrl = window.location.href;
            results.loginSuccess = !window.location.href.includes('login');
        } else if (!submitBtn) {
            results.error = 'submit button not found';
        } else {
            results.error = 'credentials not filled, not submitting';
        }

    } catch (e) {
        results.error = e.message;
    }

    return JSON.stringify(results);
})()
"""


async def attempt_login(target_id):
    """Attempt to log into Fidelity using Chrome saved passwords."""
    ws_url = f"{RELAY_WS}?targetId={target_id}&token={RELAY_TOKEN}"
    
    async with websockets.connect(ws_url, max_size=10*1024*1024) as ws:
        msg_id = 0
        
        # Step 1: Navigate to login page
        msg_id += 1
        await ws.send(json.dumps({
            "id": msg_id,
            "method": "Page.navigate",
            "params": {"url": LOGIN_URL}
        }))
        
        while True:
            resp = json.loads(await ws.recv())
            if resp.get("id") == msg_id:
                break
        
        # Wait for page to load
        await asyncio.sleep(3)
        
        # Step 2: Execute login script
        msg_id += 1
        await ws.send(json.dumps({
            "id": msg_id,
            "method": "Runtime.evaluate",
            "params": {"expression": LOGIN_JS, "awaitPromise": True, "returnByValue": True}
        }))
        
        while True:
            resp = json.loads(await ws.recv())
            if resp.get("id") == msg_id:
                break
        
        result_str = resp.get("result", {}).get("result", {}).get("value")
        if result_str:
            return json.loads(result_str)
        return {"error": "no response from login script"}


async def navigate_to_research(target_id):
    """Navigate back to research page after successful login."""
    ws_url = f"{RELAY_WS}?targetId={target_id}&token={RELAY_TOKEN}"
    
    async with websockets.connect(ws_url, max_size=10*1024*1024) as ws:
        await ws.send(json.dumps({
            "id": 1,
            "method": "Page.navigate",
            "params": {"url": RESEARCH_URL}
        }))
        
        while True:
            resp = json.loads(await ws.recv())
            if resp.get("id") == 1:
                break
        
        await asyncio.sleep(3)
        return resp


def check_is_pdf(data):
    """Check if downloaded data is actually a PDF."""
    if isinstance(data, bytes):
        return data[:4] == b'%PDF'
    return False


def check_is_login_page(data):
    """Check if the response is a login page (session expired)."""
    if isinstance(data, bytes):
        text = data[:500].decode('utf-8', errors='ignore').lower()
    else:
        text = data[:500].lower()
    
    login_indicators = ['log in', 'sign in', 'login', 'user id', 'password', 
                        'authentication required', 'session expired']
    return any(ind in text for ind in login_indicators)


if __name__ == "__main__":
    import sys
    target = sys.argv[1] if len(sys.argv) > 1 else "400F7A2E213AD2A2423ED4CC7E856F4E"
    print("Attempting Fidelity login...")
    result = asyncio.run(attempt_login(target))
    print(json.dumps(result, indent=2))
