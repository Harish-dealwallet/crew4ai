# from dagster import asset

# @asset
# def hello():
#     return "Hello, Dagster!"

# @asset
# def goodbye(hello):
#     # The "hello" asset is passed as an input to this asset
#     return f"Goodbye, Dagster! We just said: {hello}"

# @asset
# def number(hello):
#     # The "hello" asset is passed as an input to this asset
#     return len(hello)

# from dagster import asset
# from selenium import webdriver 
# from selenium.webdriver.chrome.service import Service 
# from selenium.webdriver.chrome.options import Options 
# from bs4 import BeautifulSoup 
# import time 
# from webdriver_manager.chrome import ChromeDriverManager 
# import uuid

# @asset
# def amazon_product_scraper():
#     # Set up Chrome options
#     chrome_options = Options() 
#     chrome_options.add_argument("--headless")   

#     # Initialize the WebDriver
#     driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

#     # URL to scrape
#     url = 'https://www.amazon.in/gcx/Gifts-for-Everyone/gfhz/?ref_=nav_cs_giftfinder&scrollState=eyJpdGVtSW5kZXgiOjAsInNjcm9sbE9mZnNldCI6MTMwLjU2MjV9&sectionManagerState=eyJzZWN0aW9uVHlwZUVuZEluZGV4Ijp7ImFtYWJvdCI6MH19' 
    
#     # Access the page
#     driver.get(url) 
#     time.sleep(20)  # Let the page load fully
#     soup = BeautifulSoup(driver.page_source, 'html.parser') 

#     # Extract product data
#     products = soup.find_all('div', class_='puis-card-container') 
#     data_to_insert = [] 

#     for product in products: 
#         id = str(uuid.uuid4()) 
#         name = product.find('span', class_='a-size-base-plus') 
#         name = name.get_text(strip=True) if name else 'N/A' 
        
#         link = product.find('a', class_='a-text-normal') 
#         product_link = link['href'] if link else 'N/A' 
        
#         img_tag = product.find('img', class_='s-image') 
#         img_url = img_tag['src'] if img_tag else 'N/A' 

#         price = product.find('span', class_='a-price-whole') 
#         price_text = price.text if price else 'N/A' 
        
#         original_price = product.find('span', class_='a-offscreen') 
#         original_price_text = original_price.text if original_price else 'N/A' 

#         # Append product data to the list
#         data_to_insert.append(( 
#             id, 
#             name, 
#             product_link, 
#             img_url, 
#             price_text, 
#             original_price_text, 
#         )) 

#     driver.quit()  # Don't forget to quit the driver after use!

#     # Return the scraped data
#     return data_to_insert





# from dagster import sensor, RunRequest, job, op
# import os

# # Define a simple operation that processes new files
# @op
# def process_new_files(context):
#     # This operation would process new files
#     context.log.info("Processing new files...")

# @job
# def process_files():
#     process_new_files()
    
# # Define a sensor that checks for new files in a directory
# @sensor(job=process_files)
# def file_system_sensor(context):
#     directory_path = "/path/to/directory"  # The directory you want to monitor
    
#     # List files in the directory
#     files_in_directory = os.listdir(directory_path)
    
#     # If there are files, trigger the pipeline
#     if files_in_directory:
#         yield RunRequest(run_key="new_files", tags={"source": "file_system"})
#     else:
#         context.log.info("No new files found.")





# from dagster import sensor, RunRequest, job, op
# import os

# # Define a simple operation that processes new files
# @op
# def process_new_files(context, directory_path: str):
#     context.log.info(f"Processing files in {directory_path}...")
#     try:
#         files_in_directory = os.listdir(directory_path)
#         context.log.info(f"Found {len(files_in_directory)} files in the directory.")
#         # Here you could add the actual file processing logic (e.g., read files, move them, etc.)
#     except FileNotFoundError:
#         context.log.error(f"Directory {directory_path} not found!")
#         raise

# @job
# def process_files():
#     process_new_files()

# # Define a sensor that checks for new files in a directory
# @sensor(job=process_files)
# def file_system_sensor(context):
#     directory_path = context.op_config["directory_path"]  # Fetch directory from config
#     context.log.info(f"Monitoring directory: {directory_path}")

#     try:
#         files_in_directory = os.listdir(directory_path)
#         if files_in_directory:
#             context.log.info(f"Found {len(files_in_directory)} new files.")
#             yield RunRequest(run_key="new_files", tags={"source": "file_system"})
#         else:
#             context.log.info("No new files found.")
#     except FileNotFoundError:
#         context.log.error(f"Directory {directory_path} not found!")



# from dagster import asset, schedule, job
# from datetime import datetime
# import uuid
# import os
# from dotenv import load_dotenv
# from playwright.sync_api import sync_playwright
# from bs4 import BeautifulSoup

# # Load environment variables from .env file
# load_dotenv()

# class PlaywrightSpider:
#     def __init__(self): 
#         self.browser = None 
#         self.page = None

#     def start(self): 
#         data = [] 
#         try: 
#             print("Starting Playwright...") 
#             with sync_playwright() as p: 
#                 self.browser = p.chromium.launch(headless=True) 
#                 self.page = self.browser.new_page() 
                
#                 # Open the Amazon homepage
#                 print("Opening the homepage...")
#                 self.page.goto("https://www.amazon.in/")
#                 print("Waiting for the page to load...") 

#                 # Wait for the hamburger menu to be visible before clicking
#                 self.page.locator('a#nav-hamburger-menu').wait_for(state="visible")
#                 print("Page loaded successfully.") 

#                 # Click on the hamburger menu (All dropdown)
#                 print("Waiting for the 'All' button to be clickable...")
#                 all_button = self.page.locator('a#nav-hamburger-menu')
#                 all_button.click()
#                 print("Clicked the 'All' button.")

#                 # Wait for "Mobiles, Computers" category to be clickable and click it
#                 print("Waiting for 'Mobiles, Computers' category to be clickable...")
#                 mobiles_computers_button = self.page.locator(
#                     '//li/a[@class="hmenu-item" and @data-menu-id="8"]/div[text()="Mobiles, Computers"]'
#                 ).first
#                 mobiles_computers_button.scroll_into_view_if_needed()
#                 mobiles_computers_button.wait_for(state="visible")
#                 mobiles_computers_button.click()
#                 print("Clicked 'Mobiles, Computers' category...")

#                 # URLs for different product categories
#                 urls = [
#                     "https://www.amazon.in/gp/browse.html?node=1389402031&ref_=nav_em_sbc_mobcomp_mobile_acc_0_2_8_3", 
#                     "https://www.amazon.in/gp/browse.html?node=1389409031&ref_=nav_em_sbc_mobcomp_mobile_covers_0_2_8_4",
#                     "https://www.amazon.in/gp/browse.html?node=1389425031&ref_=nav_em_sbc_mobcomp_scrn_protector_0_2_8_5",
#                     "https://www.amazon.in/gp/browse.html?node=6612025031&ref_=nav_em_sbc_mobcomp_powerbank_0_2_8_6",
#                     "https://www.amazon.in/gp/browse.html?node=1375458031&ref_=nav_em_sbc_mobcomp_tablets_0_2_8_8",
#                     "https://www.amazon.in/gp/browse.html?node=13773797031&ref_=nav_em_sbc_pc_smarthome_0_2_8_10",
#                     "https://www.amazon.in/gp/browse.html?node=2454172031&ref_=nav_em_sbc_mobcomp_office_0_2_8_11",
#                     "https://www.amazon.in/gp/browse.html?node=1375424031&ref_=nav_em_sbc_mobcomp_laptops_0_2_8_15",
#                     "https://www.amazon.in/gp/browse.html?node=1375393031&ref_=nav_em_sbc_mobcomp_pendrives_0_2_8_16",
#                     "https://www.amazon.in/gp/browse.html?node=1375443031&ref_=nav_em_sbc_mobcomp_printers_0_2_8_17",
#                     "https://www.amazon.in/gp/browse.html?node=1375427031&ref_=nav_em_pc_sbc_networking_0_2_8_18",
#                     "https://www.amazon.in/gp/browse.html?node=1375248031&ref_=nav_em_sbc_mobcomp_comp_acc_0_2_8_19",
#                     "https://www.amazon.in/gp/browse.html?node=1375425031&ref_=nav_em_sbc_mobcomp_monitors_0_2_8_21",
#                     "https://www.amazon.in/gp/browse.html?node=1375392031&ref_=nav_em_sbc_mobcomp_desktops_0_2_8_22",
#                 ]

#                 for url in urls: 
#                     print(f"Opening URL: {url}")  
#                     self.page.goto(url, wait_until="load", timeout=90000) 

#                     # Wait for "See all results" link to be clickable 
#                     print("Waiting for 'See all results' link...") 
#                     see_all_link = self.page.locator("//a[@id='apb-desktop-browse-search-see-all']") 
#                     see_all_link.wait_for(state="visible", timeout=90000)  # Wait for the link to be visible 
#                     href_link = see_all_link.get_attribute("href") 
#                     print(f"See all results href: {href_link}") 

#                     # Click the "See all results" link 
#                     print("Clicking 'See all results' link...") 
#                     see_all_link.click() 

#                     # Wait for the page to load completely after clicking the link 
#                     print("Waiting for product grid to load...") 
#                     self.page.wait_for_selector('div.s-main-slot', timeout=90000)  # Wait for the product grid to load 

#                     page_content = self.page.content() 
#                     soup = BeautifulSoup(page_content, "html.parser") 

#                     product_items = soup.select("div.s-widget-spacing-small") 
#                     print(f"Found {len(product_items)} product(s) on {url}.") 

#                     # Extract product data for each product 
#                     for product in product_items: 
#                         product_data = self.extract_product_data(product) 
#                         if product_data: 
#                             data.append(product_data) 
#                         print(f"Scraped product: {product_data['name']}") 

#         except Exception as e: 
#             print(f"An error occurred: {e}") 
#         finally: 
#             # Ensure the browser quits safely 
#             if self.browser: 
#                 try: 
#                     print("Closing the browser...")
#                     self.browser.close() 
#                 except Exception as e: 
#                     print(f"Error closing browser: {e}") 

#             # Return the scraped data
#             return data

#     def extract_product_data(self, product): 
#         """Extract product details from the HTML element.""" 
#         try: 
#             product_name = product.find("h2", class_="a-size-base-plus a-spacing-none a-color-base a-text-normal").text.strip() if product.find("h2", class_="a-size-base-plus a-spacing-none a-color-base a-text-normal") else "No Name" 
#             product_price = product.find("span", class_="a-price-whole").text.strip() if product.find("span", class_="a-price-whole") else "Price Not Available" 

#             original_price_tag = product.find('span', class_='a-price a-text-price', attrs={'data-a-strike': 'true'}) 
#             original_price = original_price_tag.find('span', class_='a-offscreen').text.strip() if original_price_tag else "Original Price Not Available" 

#             discount = product.find('span', string=lambda text: text and 'off' in text).text.strip() if product.find('span', string=lambda text: text and 'off' in text) else "Discount Not Available" 

#             rating = product.find("span", class_="a-icon-alt").text.strip() if product.find("span", class_="a-icon-alt") else "Ratings Not Available" 

#             image = product.find("img", class_="s-image")['src'] if product.find("img", class_="s-image") else "Image Not Available" 
#             if image and image.startswith("//"): 
#                 image = "https:" + image 

#             product_link = "https://www.amazon.in" + product.find("a", class_="a-link-normal")['href'] if product.find("a", class_="a-link-normal") else "Link Not Available" 

#             product_uuid = str(uuid.uuid4()) 

#             return { 
#                 "uuid": product_uuid, 
#                 "name": product_name, 
#                 "price": product_price, 
#                 "original_price": original_price, 
#                 "discount": discount, 
#                 "ratings": rating, 
#                 "image": image, 
#                 "product_link": product_link 
#             } 
#         except Exception as e: 
#             print(f"Error extracting product data: {e}") 
#             return None

# # Dagster asset definition
# @asset
# def amazon_product_data() -> list:
#     spider = PlaywrightSpider()
#     return spider.start()

# # Define a job that includes the asset
# @job
# def scrape_amazon_data():
#     amazon_product_data()

# # Schedule to run every day at midnight
# @schedule(cron_schedule="0 0 * * *", job=scrape_amazon_data)
# def daily_midnight_schedule(context):
#     """
#     This schedule triggers the scrape_amazon_data job every day at midnight.
#     """
#     return {}










# from dagster import asset, job, schedule
# from datetime import timedelta
# import uuid
# import os
# from bs4 import BeautifulSoup
# from playwright.sync_api import sync_playwright
# import meilisearch
# from dotenv import load_dotenv

# # Load environment variables from .env file
# load_dotenv()

# MEILISEARCH_URL = os.getenv('MEILISEARCH_URL')
# MEILISEARCH_API_KEY = os.getenv('MEILISEARCH_API_KEY')
# MEILISEARCH_INDEX_NAME = os.getenv('MEILISEARCH_INDEX_NAME')

# # Initialize Meilisearch client
# client = meilisearch.Client(MEILISEARCH_URL, MEILISEARCH_API_KEY)


# class PlaywrightSpider:

#     def __init__(self):
#         self.browser = None
#         self.page = None

#     def start(self):
#         data = []
#         try:
#             print("Starting Playwright...")
#             with sync_playwright() as p:
#                 self.browser = p.chromium.launch(headless=True)  # Run headless in production
#                 self.page = self.browser.new_page()

#                 # Open the Amazon homepage
#                 print("Opening the homepage...")
#                 self.page.goto("https://www.amazon.in/")
#                 print("Waiting for the page to load...")

#                 # Wait for the hamburger menu to be visible before clicking
#                 self.page.locator('a#nav-hamburger-menu').wait_for(state="visible")
#                 print("Page loaded successfully.")

#                 # Click on the hamburger menu (All dropdown)
#                 print("Waiting for the 'All' button to be clickable...")
#                 all_button = self.page.locator('a#nav-hamburger-menu')
#                 all_button.click()
#                 print("Clicked the 'All' button.")

#                 # URLs for different product categories
#                 urls = [
#                     "https://www.amazon.in/gp/browse.html?node=1389402031&ref_=nav_em_sbc_mobcomp_mobile_acc_0_2_8_3",
#                     "https://www.amazon.in/gp/browse.html?node=1389409031&ref_=nav_em_sbc_mobcomp_mobile_covers_0_2_8_4",
#                     "https://www.amazon.in/gp/browse.html?node=1389425031&ref_=nav_em_sbc_mobcomp_scrn_protector_0_2_8_5",
#                     "https://www.amazon.in/gp/browse.html?node=6612025031&ref_=nav_em_sbc_mobcomp_powerbank_0_2_8_6",
#                     # Add more URLs here as needed...
#                 ]

#                 for url in urls:
#                     print(f"Opening URL: {url}")
#                     self.page.goto(url, wait_until="load", timeout=90000)

#                     # Wait for "See all results" link to be clickable
#                     print("Waiting for 'See all results' link...")
#                     see_all_link = self.page.locator("//a[@id='apb-desktop-browse-search-see-all']")
#                     see_all_link.wait_for(state="visible", timeout=90000)
#                     href_link = see_all_link.get_attribute("href")
#                     print(f"See all results href: {href_link}")

#                     # Click the "See all results" link
#                     print("Clicking 'See all results' link...")
#                     see_all_link.click()

#                     # Wait for the page to load completely after clicking the link
#                     print("Waiting for product grid to load...")
#                     self.page.wait_for_selector('div.s-main-slot', timeout=90000)

#                     page_content = self.page.content()
#                     soup = BeautifulSoup(page_content, "html.parser")

#                     product_items = soup.select("div.s-widget-spacing-small")
#                     print(f"Found {len(product_items)} product(s) on {url}.")

#                     # Extract product data for each product
#                     for product in product_items:
#                         product_data = self.extract_product_data(product)
#                         if product_data:
#                             data.append(product_data)
#                         print(f"Scraped product: {product_data['name']}")

#         except Exception as e:
#             print(f"An error occurred: {e}")
#         finally:
#             # Ensure the browser quits safely
#             if self.browser:
#                 try:
#                     print("Closing the browser...")
#                     self.browser.close()
#                 except Exception as e:
#                     print(f"Error closing browser: {e}")

#             # Process and insert data into Meilisearch
#             if data:
#                 print("Data ready for Meilisearch insertion.")
#                 self.insert_into_meilisearch(data)
#             else:
#                 print("No data to process.")

#         return data

#     def extract_product_data(self, product):
#         """Extract product details from the HTML element."""
#         try:
#             product_name = product.find("h2", class_="a-size-base-plus a-spacing-none a-color-base a-text-normal").text.strip() if product.find("h2", class_="a-size-base-plus a-spacing-none a-color-base a-text-normal") else "No Name"
#             product_price = product.find("span", class_="a-price-whole").text.strip() if product.find("span", class_="a-price-whole") else "Price Not Available"
#             original_price_tag = product.find('span', class_='a-price a-text-price', attrs={'data-a-strike': 'true'})
#             original_price = original_price_tag.find('span', class_='a-offscreen').text.strip() if original_price_tag else "Original Price Not Available"
#             discount = product.find('span', string=lambda text: text and 'off' in text).text.strip() if product.find('span', string=lambda text: text and 'off' in text) else "Discount Not Available"
#             rating = product.find("span", class_="a-icon-alt").text.strip() if product.find("span", class_="a-icon-alt") else "Ratings Not Available"
#             image = product.find("img", class_="s-image")['src'] if product.find("img", class_="s-image") else "Image Not Available"
#             if image and image.startswith("//"):
#                 image = "https:" + image
#             product_link = "https://www.amazon.in" + product.find("a", class_="a-link-normal")['href'] if product.find("a", class_="a-link-normal") else "Link Not Available"
#             product_uuid = str(uuid.uuid4())

#             return {
#                 "uuid": product_uuid,
#                 "name": product_name,
#                 "price": product_price,
#                 "original_price": original_price,
#                 "discount": discount,
#                 "ratings": rating,
#                 "image": image,
#                 "product_link": product_link
#             }
#         except Exception as e:
#             print(f"Error extracting product data: {e}")
#             return None

#     def insert_into_meilisearch(self, data):
#         """Insert scraped data into Meilisearch."""
#         try:
#             index = client.index(MEILISEARCH_INDEX_NAME)
#             # Ensure index exists, create it if necessary
#             if not index.exists():
#                 client.create_index(MEILISEARCH_INDEX_NAME)
#             response = index.add_documents(data)
#             print(f"Inserted {len(data)} products into Meilisearch.")
#             print("Response:", response)
#         except Exception as e:
#             print(f"Error inserting into Meilisearch: {e}")


# # Dagster asset definition
# @asset
# def amazon_product_data() -> list:
#     spider = PlaywrightSpider()
#     return spider.start()


# # Define a job that includes the asset
# @job
# def scrape_amazon_data():
#     amazon_product_data()


# # Schedule to run every day at midnight
# @schedule(cron_schedule="0 0 * * *", job=scrape_amazon_data)
# def daily_midnight_schedule(context):
#     """
#     This schedule triggers the scrape_amazon_data job every day at midnight.
#     """
#     return {}



import uuid
import requests
from bs4 import BeautifulSoup
from dagster import asset, job, Field, Int
import clickhouse_connect

# ClickHouse connection details
CLICK_HOUSE_HOST = '65.109.35.230'
CLICK_HOUSE_PORT = 8123
CLICK_HOUSE_USER = 'default'
CLICK_HOUSE_PASSWORD = 'abba1961937e58b8c1366842f2eada35dd5d354d'
CLICK_HOUSE_DATABASE = 'Dealwallet_qa'
CLICK_HOUSE_TABLE = 'products'

# Establishing a connection to ClickHouse
client = clickhouse_connect.get_client(
    host=CLICK_HOUSE_HOST,
    port=CLICK_HOUSE_PORT,
    username=CLICK_HOUSE_USER,
    password=CLICK_HOUSE_PASSWORD,
    database=CLICK_HOUSE_DATABASE,
)

@asset(config_schema={"max_pages": Field(Int, default_value=10)})
def scrape_flipkart_and_insert_into_clickhouse(context):
    """
    Scrapes data from Flipkart website for mobile products and inserts the data into ClickHouse database.
    """
    max_pages = context.op_config["max_pages"]
    data_to_insert = []
    item_id = 1  # Initialize item ID

    # Loop through pages based on max_pages (defaults to 10)
    for page in range(1, max_pages + 1):
        url = f"https://www.flipkart.com/mobiles/mi~brand/pr?sid=tyy,4io&otracker=nmenu_sub_Electronics_0_Mi&page={page}"
        r = requests.get(url)
        soup = BeautifulSoup(r.content, "html.parser")

        # Extract relevant data using class names (ensure these are accurate)
        titles = soup.find_all('div', {'class': 'KzDlHZ'})
        prices = soup.find_all('div', {'class': 'Nx9bqj _4b5DiR'})
        actual_prices = soup.find_all('div', {'class': 'yRaY8j ZYYwLA'})
        discount_elements = soup.find_all('div', {'class': 'UkUFwK'})
        ratings = soup.find_all('div', {'class': 'XQDdHH'})
        images = soup.find_all('img', {'class': 'DByuf4'})
        links = soup.find_all('a', {'class': 'CGtC98'})

        # Combine the data into a structured format
        for i in range(min(len(titles), len(prices), len(actual_prices), len(discount_elements), len(ratings), len(images), len(links))):
            product_title = titles[i].get_text()
            product_price = prices[i].get_text()
            actual_price = actual_prices[i].get_text()
            product_discount = discount_elements[i].get_text()
            product_rating = ratings[i].get_text() if i < len(ratings) else "N/A"
            product_image = images[i]['src']
            product_link = "https://www.flipkart.com" + links[i]['href']

            product_id = str(uuid.uuid4())

            # Create a tuple for each item (id, name, price, original_price, discount, ratings, image, product_link)
            data_to_insert.append((product_id, product_title, product_price, actual_price, product_discount, product_rating, product_image, product_link))

            # Print the extracted data
            print(f"{item_id}. UUID: {product_id}")
            print(f"   Title: {product_title}")
            print(f"   Price: {product_price}")
            print(f"   Actual Price: {actual_price}")
            print(f"   Discount: {product_discount}")
            print(f"   Rating: {product_rating}")
            print(f"   Image: {product_image}")
            print(f"   Product Link: {product_link}")

            item_id += 1

    # Insert the data into the ClickHouse database
    if data_to_insert:
        try:
            client.insert(
                CLICK_HOUSE_TABLE,  # the table name
                data_to_insert,
                column_names=['id', 'name', 'price', 'original_price', 'discount', 'ratings', 'image', 'product_link']
            )
            print(f"Inserted {len(data_to_insert)} records into the table '{CLICK_HOUSE_TABLE}'.")
        except Exception as e:
            print(f"Failed to insert data into ClickHouse: {e}")
    else:
        print("No data to insert.")


# Define a job to run the asset
@job
def scrape_flipkart_job():
    scrape_flipkart_and_insert_into_clickhouse()

