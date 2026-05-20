"""Smoke test for block_detector.detect_block — no Playwright needed.

We fake a `page` object whose `evaluate(expr)` returns pre-canned responses
based on which selector / which document field the production code asks for.
This lets us validate every detection path without a real browser.
"""
import asyncio

from block_detector import detect_block


class FakePage:
    def __init__(self, cf_dom_selector: str | None = None, title: str = "",
                 body: str = ""):
        self.cf_dom_selector = cf_dom_selector
        self.title = title
        self.body = body

    async def evaluate(self, expr: str):
        # The block_detector uses three eval shapes — distinguish by substring.
        if "challenge-form" in expr or "challenges.cloudflare" in expr:
            return self.cf_dom_selector
        if "document.title" in expr:
            return self.title
        if "innerText" in expr:
            return self.body[:4000]
        raise AssertionError(f"unexpected evaluate: {expr[:80]}")


async def main():
    cases: list[tuple[str, FakePage, str]] = [
        # (label, fake page, expected kind)
        ("clean page", FakePage(title="Power Tools - Lowes",
                                body="DEWALT drill ... add to cart"), "ok"),

        ("CF dom selector", FakePage(cf_dom_selector="#challenge-form",
                                     title="anything", body="anything"),
         "cloudflare"),

        ("CF title 'Just a moment'", FakePage(title="Just a moment...",
                                              body="Checking your browser before access"),
         "cloudflare"),

        ("CF title 'Attention Required'", FakePage(title="Attention Required! | Cloudflare",
                                                   body=""), "cloudflare"),

        ("Akamai AD standard", FakePage(
            title="Access Denied",
            body=("Access Denied You don't have permission to access "
                  "\"http://www.lowes.com/pl/power-tools/4294607842\" on this server. "
                  "Reference #18.b1d02e17.1778950810.5ae4ab89 "
                  "https://errors.edgesuite.net/18.b1d02e17.1778950810.5ae4ab89")),
         "access_denied"),

        ("Akamai AD short", FakePage(title="",
                                     body="Reference #99.abc1234.0.deadbeef"),
         "access_denied"),

        ("CF body only", FakePage(title="ok",
                                  body="please wait checking your browser ..."),
         "cloudflare"),

        ("Lowes 404 (should be ok, not AD)", FakePage(
            title="Page Not Found - Lowes.com",
            body="We couldn't find the page you requested."), "ok"),
    ]

    passed = 0
    for label, page, want in cases:
        result = await detect_block(page)  # type: ignore[arg-type]
        got = result["kind"]
        mark = "PASS" if got == want else "FAIL"
        print(f"  [{mark}] {label:35s} want={want:15s} got={got:15s} "
              f"signals={result['signals']}")
        if got == want:
            passed += 1

    print()
    if passed == len(cases):
        print(f"=== ALL {len(cases)} TESTS PASSED ===")
    else:
        print(f"=== {passed}/{len(cases)} passed ===")
        raise SystemExit(1)


if __name__ == "__main__":
    asyncio.run(main())
