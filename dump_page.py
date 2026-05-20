"""Dump full HTML of a Lowes product page via AdsPower for DOM analysis."""
import asyncio
import os
import sys
from playwright.async_api import async_playwright
from adspower_helper import AdsPower
from config import ADSPOWER_API, ADSPOWER_PROFILE_ID, TIMEOUT

URL = sys.argv[1] if len(sys.argv) > 1 else "https://www.lowes.com/pd/DEWALT-20V-MAX-2-Tool-Brushless-Power-Tool-Combo-Kit-with-Soft-Case-2-Batteries-and-Charger-Included/5014148639"
OUT = "data/debug/sample_page.html"

async def main():
    ads = AdsPower(ADSPOWER_API, ADSPOWER_PROFILE_ID)
    ws = ads.start()
    pw = await async_playwright().start()
    browser = await pw.chromium.connect_over_cdp(ws)
    ctx = browser.contexts[0]
    page = ctx.pages[0] if ctx.pages else await ctx.new_page()

    print(f"Navigating to {URL}...")
    try:
        await page.goto(URL, wait_until="domcontentloaded", timeout=TIMEOUT)
    except Exception as e:
        print(f"Nav warning: {e}")

    await asyncio.sleep(8)

    # Click all accordion sections to expand them
    print("Expanding accordion sections...")
    accordion_btns = await page.query_selector_all('button[class*="accordion" i], [role="button"][aria-expanded="false"]')
    for btn in accordion_btns:
        try:
            text = await btn.text_content()
            if any(k in (text or "").lower() for k in ["overview", "specification", "feature", "review"]):
                await btn.click()
                await asyncio.sleep(1)
                print(f"  Expanded: {(text or '').strip()[:50]}")
        except Exception:
            pass

    await asyncio.sleep(3)

    # Dump full HTML
    html = await page.content()
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Saved {len(html)} bytes to {OUT}")

    # Also dump JSON-LD and key data
    data = await page.evaluate("""
        () => {
            const r = {};

            // JSON-LD
            const lds = document.querySelectorAll('script[type="application/ld+json"]');
            r.jsonLd = [...lds].map(s => { try { return JSON.parse(s.textContent); } catch(e) { return null; } }).filter(Boolean);

            // Accordion headers with expanded state
            const btns = document.querySelectorAll('button, [role="button"]');
            r.accordionBtns = [...btns].filter(b => {
                const t = (b.textContent || '').trim().toLowerCase();
                return ['overview','specification','feature','review','q&a','manual'].some(k => t.includes(k));
            }).map(b => ({
                text: b.textContent.trim().substring(0, 80),
                expanded: b.getAttribute('aria-expanded'),
                controls: b.getAttribute('aria-controls'),
                cls: (b.className || '').substring(0, 120),
                tag: b.tagName
            }));

            // Brand
            const brandEl = document.querySelector('a[href*="/brand/"]');
            r.brand = brandEl ? { text: brandEl.textContent.trim(), href: brandEl.href } : null;

            // Rating
            const ratingEls = document.querySelectorAll('[class*="rating" i], [class*="star" i]');
            r.ratings = [...ratingEls].slice(0, 5).map(el => ({
                cls: (el.className || '').substring(0, 100),
                text: el.textContent.trim().substring(0, 80),
                ariaLabel: el.getAttribute('aria-label')
            }));

            // Price - find dollar signs in leaf nodes
            const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
            const priceTexts = [];
            while (walker.nextNode()) {
                const t = walker.currentNode.textContent.trim();
                if (t.match(/^\$\d/) && t.length < 20) {
                    const p = walker.currentNode.parentElement;
                    priceTexts.push({
                        text: t,
                        parentTag: p?.tagName,
                        parentCls: (p?.className || '').substring(0, 100)
                    });
                }
            }
            r.priceLeaves = priceTexts.slice(0, 10);

            return r;
        }
    """)

    import json
    with open("data/debug/dom_analysis.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"Saved DOM analysis to data/debug/dom_analysis.json")

    # Print key findings
    print("\n=== KEY FINDINGS ===")
    if data.get("brand"):
        print(f"Brand: {data['brand']}")
    if data.get("jsonLd"):
        for ld in data["jsonLd"]:
            if isinstance(ld, dict):
                print(f"JSON-LD type: {ld.get('@type')}")
                for k in ['gtin', 'gtin12', 'gtin13', 'sku', 'mpn', 'brand']:
                    if ld.get(k):
                        print(f"  {k}: {ld[k]}")
    if data.get("accordionBtns"):
        print(f"\nAccordion sections found: {len(data['accordionBtns'])}")
        for b in data["accordionBtns"]:
            print(f"  [{b['expanded']}] {b['text'][:50]} -> controls={b['controls']}")
    if data.get("priceLeaves"):
        print(f"\nPrice elements:")
        for p in data["priceLeaves"]:
            print(f"  {p['text']} ({p['parentTag']}.{p['parentCls'][:40]})")

    await pw.stop()

asyncio.run(main())
