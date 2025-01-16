import os
import uuid
from crawl4ai import WebCrawler
from crawl4ai.extraction_strategy import LLMExtractionStrategy
from pydantic import BaseModel, Field
import clickhouse_connect
from datetime import datetime
import json


class OpenAIModelFee(BaseModel):
    category_name: str = Field(..., description="Name of the category.")
    category_link: str = Field(..., description="category_link which specifies complete subcategories ")
    image_link: str = Field(..., description="image link of category")
    description: str = Field(..., description="give the detailed description of the category")
    

url = 'https://www.snapdeal.com/'
crawler = WebCrawler()
#crawler.warmup()

result = crawler.run(
    url=url,
    word_count_threshold=1,
    extraction_strategy=LLMExtractionStrategy(
        provider="gemini/gemini-1.5-pro",
        api_token=os.getenv("API_TOKEN", "AIzaSyCk_kkw1VYpiuMxOXQwEP6CmdJpF-meK6E"),
        schema=OpenAIModelFee.schema(),
        extraction_type="schema",
        instruction="""From the crawled website content, extract only main categories. 
        Do not include items like ads, discounts, or under-priced categories (e.g., under 99, 199).
        Include the website link but not image links. return them in list
        Extract categories in sequence from the website: only include which are currently available.
        that link should redirect complete products of that category. Grocery & Gourmet getting non-relevant link.
        if it is there, include correct URL, or otherwise remove that category. extract all categories
        some categories showing this error Can't find the product you're looking for?
        don't include it. don't extract one category link for another category name.getting wrong url of groceries. once double check
        if not possible to take correct url , dont give that category

        category_name ="mobiles", category_link = "https://www.flipkart.com/mobiles-accessories/pr?sid=tyy&otracker=categorytree", image_link = "https://rukminim2.flixcart.com/flap/96/96/image/29327f40e9c4d26b.png?q=100"
        description = 'it belongs to the men's category'
        """
    ),
    bypass_cache=True,
)

products = result.extracted_content
product = json.loads(products) if isinstance(products, str) else products
print(f"Extracted categories: {product}")

# Extracting subcategories (assuming product contains the categories and category_link)
categories = []
for category in product:
    category_link = category.get("category_link")    
    categories.append(category_link)
print(f"categories found: {categories}")

for subcategories in categories:
    result = crawler.run(
        url=subcategories,
        word_count_threshold=1,
        extraction_strategy=LLMExtractionStrategy(
            provider="gemini/gemini-1.5-pro",
            api_token=os.getenv("API_TOKEN", "AIzaSyCk_kkw1VYpiuMxOXQwEP6CmdJpF-meK6E"),
            schema=OpenAIModelFee.schema(),
            extraction_type="schema",
            instruction="""
            extract all subcategories from the following main category url. extract complete category urls, only extract currentely available urls like if it is Footwear just give me sub categories 
            like slippers,sandals,sneakers and so on. dont extract other category links.  give subcategory name and subcategorylink. 
            """
        ),
        bypass_cache=True,
    )
   

prod = result.extracted_content
pro = json.loads(prod) if isinstance(prod, str) else prod
print(f"Extracted categories: {pro}")