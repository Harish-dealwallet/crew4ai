import os
import uuid
import crawl4ai
from crawl4ai import WebCrawler
from crawl4ai.extraction_strategy import LLMExtractionStrategy
from pydantic import BaseModel, Field
import clickhouse_connect
from datetime import datetime
import json

# Pydantic schema for OpenAI Model Fee (the extracted category data)
class OpenAIModelFee(BaseModel):
    category_name: str = Field(..., description="Name of the category.")
    url: str = Field(..., description="Link related to the category.")

# URL of the site to crawl
url = 'https://shoptheworld.in/'

# Initialize the WebCrawler and warmup
crawler = WebCrawler()
crawler.warmup()

# Run the web crawling process with extraction strategy
result = crawler.run(
    url=url,
    word_count_threshold=1,
    extraction_strategy=LLMExtractionStrategy(
        provider="gemini/gemini-1.5-pro",
        api_token=os.getenv("API_TOKEN"),  # Ensure the API token is set as an environment variable
        schema=OpenAIModelFee.schema(),
        extraction_type="schema",
        instruction="""
            From the crawled website content, extract only main categories. 
            Do not include items like ads, discounts, or under-priced categories (e.g., under 99, 199).
            Include the website link but not image links. Return them in list.
            Extract categories in sequence from the website: only include those which are currently available.
            The link should redirect to the complete product list of that category.
            If a category is irrelevant, such as "Can't find the product you're looking for?", do not include it.
        """
    ),
    bypass_cache=True,
)

# Extract and process the content
products = result.extracted_content
product = json.loads(products) if isinstance(products, str) else products

# ClickHouse client connection setup
def get_clickhouse_client():
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "65.109.35.230"),  # Use environment variables for security
        port=8123,
        username=os.getenv("CLICKHOUSE_USERNAME", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "abba1961937e58b8c1366842f2eada35dd5d354d"),
        database=os.getenv("CLICKHOUSE_DB", "Dealwallet_qa")
    )

# Insert the extracted data into ClickHouse
def insert_into_clickhouse(client, data):
    if not data:
        print("No data to insert.")
        return

    records = [
        (
            str(uuid.uuid4()),  # Generate a unique ID for each record
            record.get('category_name', 'unknown'),
            'Shop the World' + ' ' + record.get('category_name'),
            'none',  # Placeholder for the image
            record.get('url', ''),
            datetime.now(),
            datetime.now(),
            'none',  # Placeholder for subcategory name
            'none'   # Placeholder for subcategory link
        )
        for record in data
    ]

    try:
        client.insert(
            "categories",  # Ensure this table exists in your ClickHouse database
            records,  # Data to insert
            column_names=[
                "id", "category_name", "description", "image", 
                "category_link", "created_at", "updated_at", 
                "subcategory_name", "subcategory_link"
            ]
        )
        print("Data inserted successfully.")
    except Exception as e:
        print(f"Error during insertion: {e}")

# Main function to run the process
def main():
    try:
        client = get_clickhouse_client()
        insert_into_clickhouse(client, product)
    except Exception as e:
        print(f"Error during process execution: {e}")

# Run the script
if __name__ == "__main__":
    main()
