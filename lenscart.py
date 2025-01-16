import asyncio
from playwright.async_api import async_playwright
import clickhouse_connect
from urllib.parse import urljoin

# Define the base URL for the website
BASE_URL = "https://www.lenskart.com"

async def fetch_page_content(url, page):
    try:
        await page.goto(url)
        await page.wait_for_selector('div.ProductContainer--1h5el3b')
        products = await page.query_selector_all('div.ProductContainer--1h5el3b')
        return products
    except Exception as e:
        print(f"Failed to fetch the webpage: {e}")
        return []

async def scrape_product_details(products):
    product_data = []
    for product in products:
        try:
            title_element = await product.query_selector('p.ProductTitle--xakon1')
            title = await title_element.inner_text() if title_element else "N/A"

            link_element = await product.query_selector('a')
            link = await link_element.get_attribute('href') if link_element else "N/A"

            # Ensure the link is absolute by joining with the base URL if it's relative
            full_link = urljoin(BASE_URL, link)

            img_element = await product.query_selector('img.ProductImage--irgn47')
            img_url = await img_element.get_attribute('src') if img_element else "N/A"

            rating_element = await product.query_selector('span.NumberedRatingSpan--kts3v6.cSQYCU')
            rating = await rating_element.inner_text() if rating_element else "N/A"

            num_reviews_element = await product.query_selector('span.NumberedRatingSpan--kts3v6.eQSVWf')
            num_reviews = await num_reviews_element.inner_text() if num_reviews_element else "N/A"

            price_element = await product.query_selector('div.OfferPrice--169iodc')
            price = await price_element.inner_text() if price_element else "N/A"

            old_price_element = await product.query_selector('span.Strikethrough--19jxslx')
            old_price = await old_price_element.inner_text() if old_price_element else "N/A"

            discount_element = await product.query_selector('h5.Title--19p6sp5')
            discount = await discount_element.inner_text() if discount_element else "N/A"

            product_data.append(
                (title, price, old_price, discount, rating, img_url, full_link)
            )

        except Exception as e:
            print(f"Error scraping product: {e}")

    return product_data

async def insert_into_clickhouse(data, table_name):
    try:
        client = clickhouse_connect.get_client(
            host="65.109.35.230",
            port=8123,
            username="default",
            password="abba1961937e58b8c1366842f2eada35dd5d354d",
            database="Dealwallet_qa"
        )
        columns = ["name", "price", "original_price", "discount", "ratings", "image", "product_link"]

        if data:
            client.insert(
                table_name,
                data,
                column_names=columns
            )
            print(f"Inserted {len(data)} records into the table '{table_name}'.")
        else:
            print("No data to insert.")
    except Exception as e:
        print(f"Failed to insert data into ClickHouse: {e}")

async def scrape(start_page, end_page):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)  # Use headless=True for no UI
        page = await browser.new_page()

        for page_number in range(start_page, end_page + 1):
            print(f"Scraping Page {page_number}...")
            url = f'https://www.lenskart.com/sunglasses/collections/premium-sunglasses.html?page={page_number}'

            products = await fetch_page_content(url, page)
            product_data = await scrape_product_details(products)

            if product_data:
                await insert_into_clickhouse(product_data, "products")

        await browser.close()

start_page = 1
end_page = 3  # You can adjust the range as needed

asyncio.run(scrape(start_page, end_page))
