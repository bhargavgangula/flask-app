import asyncio
import pandas as pd
from playwright.async_api import async_playwright

async def get_category_from_gbp(url: str, p) -> str | None:
    browser = await p.chromium.launch(headless=True)
    page = await browser.new_page()
    try:
        await page.goto(url, timeout=60000)
        await page.wait_for_timeout(3000)

        candidate_selectors = [
            "button[data-item-id='category'] div",
            "div.YhemCb span",
            "span.Z3hnYe",
            "div[aria-label*='Category']",
        ]

        for sel in candidate_selectors:
            try:
                elem = await page.query_selector(sel)
                if elem:
                    text = (await elem.inner_text()).strip()
                    if text:
                        await browser.close()
                        return text
            except Exception:
                continue

        elems = await page.query_selector_all("span")
        for e in elems:
            txt = (await e.inner_text()).strip()
            if txt and len(txt) < 40 and not any(ch.isdigit() for ch in txt):
                await browser.close()
                return txt

    except Exception as e:
        print(f"Error for {url}: {e}")

    await browser.close()
    return None

async def main(input_excel="input.xlsx", output_excel="output.xlsx"):
    df = pd.read_excel(input_excel)

    async with async_playwright() as p:
        tasks = [get_category_from_gbp(url, p) for url in df["url"].dropna()]
        categories = await asyncio.gather(*tasks)

    df["category"] = categories
    df.to_excel(output_excel, index=False)
    print(f"✅ Results saved to {output_excel}")

if __name__ == "__main__":
    asyncio.run(main())
