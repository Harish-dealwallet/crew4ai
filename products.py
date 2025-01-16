import asyncio
import os
from crawl4ai import AsyncWebCrawler
from pydantic import BaseModel, Field
from crawl4ai.extraction_strategy import LLMExtractionStrategy
from bs4 import BeautifulSoup
import requests

# Pydantic schema for OpenAI Model Fee (the extracted category data)
class OpenAIModelFee(BaseModel):
    category_name: str = Field(..., description="Name of the category.")
    url: str = Field(..., description="Link related to the category.")

# Function to scrape product details from a given subcategory URL
async def scrape_product_details(subcategory_url):
    # Use requests (or asyncio-based scraping) to fetch product details from the subcategory page
    response = requests.get(subcategory_url)
    soup = BeautifulSoup(response.content, "html.parser")
    
    products = []
    
    # Example selectors, you need to update them based on the website structure
    product_elements = soup.select('.product-card')  # Adjust the selector accordingly
    
    for product in product_elements:
        try:
            # Extract product details
            image_url = product.select_one('.product-image')['src']
            price = product.select_one('.price').text.strip()
            original_price = product.select_one('.original-price').text.strip() if product.select_one('.original-price') else price
            discount = product.select_one('.discount').text.strip() if product.select_one('.discount') else "0%"
            brand_name = product.select_one('.brand-name').text.strip()
            description = product.select_one('.product-description').text.strip() if product.select_one('.product-description') else "No description"
            ratings = product.select_one('.star-rating')['data-rating'] if product.select_one('.star-rating') else "No rating"
            product_url = product.select_one('a')['href']
            
            # Append the product details to the list
            products.append({
                "image_url": image_url,
                "price": price,
                "original_price": original_price,
                "discount": discount,
                "brand_name": brand_name,
                "description": description,
                "ratings": ratings,
                "product_url": product_url
            })
        except Exception as e:
            print(f"Error extracting data for a product: {e}")
    
    return products

async def main():
    # Web crawler setup
    async with AsyncWebCrawler(headless=True) as crawler:
        # Define extraction strategy to use the LLM (Language Learning Model) to extract categories
        extraction_strategy = LLMExtractionStrategy(
            provider="gemini/gemini-1.5-pro",
            api_token=os.getenv("API_TOKEN"),  # Ensure the API token is set as an environment variable
            schema=OpenAIModelFee.schema(),
            extraction_type="schema",
            instruction="""
                Extract all the main categories and their relevant subcategories under "Men" category.
                The extracted subcategories should include their names and the links to view products in those categories.
                Only include categories that are currently live and accessible.
                Exclude irrelevant items like "Contact Us", "Terms & Conditions", or categories with no products listed.
                The output should include:
                - category_name: The name of the subcategory.
                - url: The link to view the products in the subcategory.
            """
        )

        # Run the crawler with extraction strategy
        result = await crawler.arun(
            url="https://myntra.com",  # Replace with the main URL for Men category
            word_count_threshold=1,
            extraction_strategy=extraction_strategy,
            bypass_cache=True,
        )

        # Debugging: Print the structure of result.extracted_content to understand its format
        print("Extracted Content:", result.extracted_content)
        
        # Extract and process the content (subcategories under Men)
        subcategories = result.extracted_content

        # Check if subcategories is a list of dictionaries or a different structure
        if isinstance(subcategories, list):
            for subcategory in subcategories:
                if isinstance(subcategory, dict):
                    print(f"Scraping products from: {subcategory['category_name']} - {subcategory['url']}")
                    
                    # Scrape the product details from the subcategory URL
                    products = await scrape_product_details(subcategory['url'])

                    # Print product details
                    for product in products:
                        print(f"Product URL: {product['product_url']}")
                        print(f"Brand: {product['brand_name']}")
                        print(f"Price: {product['price']}")
                        print(f"Original Price: {product['original_price']}")
                        print(f"Discount: {product['discount']}")
                        print(f"Rating: {product['ratings']}")
                        print(f"Description: {product['description']}")
                        print(f"Image URL: {product['image_url']}")
                        print("-" * 40)
                else:
                    print("Subcategory is not a dictionary:", subcategory)
        else:
            print("Extracted content is not in the expected format.")

if __name__ == "__main__":
    # Run the main async function
    asyncio.run(main())
