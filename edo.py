import asyncio
import os
from crawl4ai import AsyncWebCrawler
from pydantic import BaseModel, Field
from crawl4ai.extraction_strategy import LLMExtractionStrategy

# Pydantic schema for OpenAI Model Fee (the extracted category data)
class OpenAIModelFee(BaseModel):
    category_name: str = Field(..., description="Name of the category.")
    url: str = Field(..., description="Link related to the category.")

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
                From the crawled website content, extract only main categories and subcategories. 
                Exclude irrelevant items like ads, discounts, or under-priced categories (e.g., under 99, 199).
                Include the website link but exclude image links. Return categories and subcategories in sequence.
                Only include categories that are currently available.
                If a category is irrelevant, such as "Can't find the product you're looking for?", do not include it.
            """
        )

        # Run the crawler with extraction strategy
        result = await crawler.arun(
            url="https://myntra.com",  # Replace with the actual URL you want to crawl
            word_count_threshold=1,
            extraction_strategy=extraction_strategy,
            bypass_cache=True,
        )

        # Extract and process the content (categories and subcategories)
        products = result.extracted_content

        # Debug: Print the structure of the extracted content
        print(products)

        # Print extracted categories and subcategories (if structure is correct)
        if isinstance(products, list):
            for product in products:
                # Assuming 'product' is a dictionary with the correct keys
                print(f"Category: {product['category_name']}, URL: {product['url']}")
        else:
            print("Error: Extracted content is not in the expected format")

if __name__ == "__main__":
    # Run the main async function
    asyncio.run(main())
