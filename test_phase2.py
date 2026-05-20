"""Smoke test for Phase 2 completeness + merge. Run: python test_phase2.py"""
from completeness import check_completeness, merge_detail


def t(label, cond):
    mark = "PASS" if cond else "FAIL"
    print(f"  [{mark}] {label}")
    assert cond, label


def main():
    print("=== Phase 2: completeness ===")

    print("\n1) Empty/None")
    # Phase 5.5: required set is now {title, price, description, specifications}
    ok, miss = check_completeness(None)
    t("None => incomplete", not ok and len(miss) >= 4)
    ok, _ = check_completeness({})
    t("{} => incomplete", not ok)

    print("\n2) Fully complete sample (under new minimal rules)")
    complete = {
        "title": "DEWALT Impact Driver",
        "price": "$59.00",
        "description": "20V cordless impact driver kit",
        "specifications": {"Voltage": "20V"},  # 1 key is enough now (dict_min_1)
    }
    ok, miss = check_completeness(complete)
    t("complete sample passes", ok and miss == [])

    print("\n3) Missing detection — price + empty specs flagged")
    partial = dict(complete)
    partial["specifications"] = {}  # 0 keys → fails dict_min_1
    partial["price"] = ""
    ok, miss = check_completeness(partial)
    t("price + specs flagged", not ok and set(miss) >= {"price", "specifications"})

    print("\n3b) Images missing is OK (not required)")
    no_img = dict(complete)
    no_img["images"] = []
    ok, miss = check_completeness(no_img)
    t("missing images doesn't flag completeness", ok and miss == [])

    print("\n=== Phase 2: merge_detail ===")

    print("\n4) Static field preserved")
    old = {"title": "Old Title", "brand": "OldBrand"}
    new = {"title": "New Title", "brand": "NewBrand"}
    merged = merge_detail(old, new)
    t("old title kept", merged["title"] == "Old Title")
    t("old brand kept", merged["brand"] == "OldBrand")

    print("\n5) Static field filled when old empty")
    merged = merge_detail({"title": ""}, {"title": "New"})
    t("empty old filled by new", merged["title"] == "New")

    print("\n6) Dynamic field always updates")
    old = {"price": "$50.00", "rating": "4.0"}
    new = {"price": "$45.00", "rating": "4.5"}
    merged = merge_detail(old, new)
    t("price updated", merged["price"] == "$45.00")
    t("rating updated", merged["rating"] == "4.5")

    print("\n7) Dynamic field NOT overwritten by empty new")
    merged = merge_detail({"price": "$50.00"}, {"price": ""})
    t("empty new doesn't clobber", merged["price"] == "$50.00")

    print("\n8) Specifications key-by-key merge")
    old = {"specifications": {"A": "1", "B": "2"}}
    new = {"specifications": {"B": "X", "C": "3"}}
    merged = merge_detail(old, new)
    specs = merged["specifications"]
    t("old key A kept", specs["A"] == "1")
    t("old key B kept over new", specs["B"] == "2")
    t("new key C added", specs["C"] == "3")

    print("\n9) Images union + dedup, preserve order")
    old = {"images": ["a.jpg", "b.jpg"]}
    new = {"images": ["b.jpg", "c.jpg"]}
    merged = merge_detail(old, new)
    t("images merged in order", merged["images"] == ["a.jpg", "b.jpg", "c.jpg"])

    print("\n10) No old => returns new")
    merged = merge_detail(None, {"title": "X"})
    t("no old means new", merged == {"title": "X"})

    print("\n11) scraped_at always refreshed")
    merged = merge_detail(
        {"title": "T", "scraped_at": "2024-01-01"},
        {"title": "T2", "scraped_at": "2026-05-16"},
    )
    t("scraped_at refreshed", merged["scraped_at"] == "2026-05-16")

    print("\n12) Realistic scenario: refill missing description")
    # Phase 5.5: required = {title, price, description, specifications}.
    # An item missing description is incomplete; a refill scrape fills it.
    old = {
        "productId": "123", "title": "Drill", "brand": "DEWALT", "price": "$99",
        "modelNumber": "X1", "images": ["a.jpg"],
        "specifications": {"Voltage": "20V"},  # 1 key is enough now
        # description missing → incomplete
    }
    ok, miss = check_completeness(old)
    t("old is incomplete (description missing)", not ok and "description" in miss)

    new_scrape = {
        "productId": "123", "title": "Drill V2",  # title shouldn't be overwritten
        "brand": "DEWALT", "price": "$95",  # price WILL be updated (dynamic)
        "modelNumber": "X1", "images": ["a.jpg", "b.jpg"],
        "description": "Cordless 20V impact driver",
        "specifications": {
            "Voltage": "20V", "Weight": "5lb", "Length": "12in",
            "Material": "Steel", "Warranty": "5yr",
        },
    }
    merged = merge_detail(old, new_scrape)
    ok, miss = check_completeness(merged)
    t("merged is now complete", ok)
    t("title NOT overwritten (static)", merged["title"] == "Drill")
    t("price WAS updated (dynamic)", merged["price"] == "$95")
    t("description filled (static, was empty)", merged["description"] == "Cordless 20V impact driver")
    t("specs now has 5 keys", len(merged["specifications"]) == 5)
    t("images merged b.jpg added", "b.jpg" in merged["images"] and "a.jpg" in merged["images"])

    print("\n=== ALL TESTS PASSED ===")


if __name__ == "__main__":
    main()
