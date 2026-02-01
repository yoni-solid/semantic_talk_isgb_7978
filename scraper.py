"""
Web scraper using Crawl4AI with LLM extraction strategy.
Scrapes products, books, and films with normalization of dimension data.
"""
import asyncio
import logging
import uuid
import json
import os
import re
from typing import List, Dict, Tuple, Optional
from datetime import datetime
import pandas as pd
from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, LLMConfig
from crawl4ai.extraction_strategy import LLMExtractionStrategy

from models import (
    Product, ProductCategory, ProductVariant, ProductReview, SimilarProduct,
    Book, Author, BookCategory, BookCategoryBridge,
    Film, Director, Actor, AwardCategory, FilmActorBridge, FilmAwardBridge
)

logger = logging.getLogger(__name__)


class WebScraper:
    """Web scraper with normalization capabilities"""
    
    def __init__(self):
        self.browser_config = BrowserConfig(
            enable_stealth=True,
            headless=True,
            verbose=False
        )
        
        # Storage for scraped data
        self.products: List[Product] = []
        self.product_categories: Dict[str, ProductCategory] = {}
        self.product_variants: List[ProductVariant] = []
        self.product_reviews: List[ProductReview] = []
        self.similar_products: List[SimilarProduct] = []
        
        self.books: List[Book] = []
        self.authors: Dict[str, Author] = {}
        self.book_categories: Dict[str, BookCategory] = {}
        self.book_category_bridges: List[BookCategoryBridge] = []
        
        self.films: List[Film] = []
        self.directors: Dict[str, Director] = {}
        self.actors: Dict[str, Actor] = {}
        self.award_categories: Dict[str, AwardCategory] = {}
        self.film_actor_bridges: List[FilmActorBridge] = []
        self.film_award_bridges: List[FilmAwardBridge] = []
        
        # OpenAI API key
        self.openai_api_key = os.getenv("OPENAI_API_KEY")
        if not self.openai_api_key:
            logger.warning("OPENAI_API_KEY not found in environment. LLM extraction may fail.")

    def _get_extraction_strategy(self, schema: dict, extraction_type: str = "schema"):
        """Create LLM extraction strategy"""
        if not self.openai_api_key:
            logger.warning("Cannot create LLMExtractionStrategy without OPENAI_API_KEY")
            return None
        
        llm_config = LLMConfig(
            provider="openai",
            api_token=self.openai_api_key
        )
        
        return LLMExtractionStrategy(
            llm_config=llm_config,
            schema=schema,
            extraction_type=extraction_type
        )
    
    def _extract_product_css(self, container) -> Optional[dict]:
        """Extract product data using CSS selectors (fast, no LLM tokens)"""
        try:
            # Try to extract name, price, category, product_id using CSS
            name_elem = container.select_one('h3, h2, .name, [class*="name"], a')
            name = name_elem.get_text(strip=True) if name_elem else ""
            
            price_elem = container.select_one('.price, [class*="price"], [class*="cost"]')
            price_text = price_elem.get_text(strip=True) if price_elem else "0"
            # Extract numeric price
            price_match = re.search(r'[\d.]+', price_text.replace(',', ''))
            price = float(price_match.group()) if price_match else 0.0
            
            category_elem = container.select_one('.category, [class*="category"], .tag, [class*="tag"]')
            category = category_elem.get_text(strip=True) if category_elem else "unknown"
            
            # Try to get product_id from link or data attribute
            link_elem = container.select_one('a[href*="/product/"]')
            product_id = ""
            if link_elem:
                href = link_elem.get('href', '')
                # Extract ID from URL like /product/123
                id_match = re.search(r'/product/([^/]+)', href)
                if id_match:
                    product_id = id_match.group(1)
            
            if not product_id:
                # Try data attributes
                product_id = container.get('data-id', container.get('data-product-id', str(uuid.uuid4())))
            
            if name:  # Only return if we got at least a name
                return {
                    "name": name,
                    "price": price,
                    "category": category,
                    "product_id": product_id
                }
        except Exception as e:
            logger.debug(f"CSS extraction failed: {e}")
        
        return None
    
    def _extract_book_css(self, container, base_url: str) -> Optional[dict]:
        """Extract book data using CSS selectors (fast, no LLM tokens)"""
        try:
            # Books.toscrape.com structure
            title_elem = container.select_one('h3 a, h3, .title, a[title]')
            title = title_elem.get('title') or title_elem.get_text(strip=True) if title_elem else ""
            
            price_elem = container.select_one('.price_color, .price, [class*="price"]')
            price_text = price_elem.get_text(strip=True) if price_elem else "£0.00"
            
            # Extract price (remove currency)
            price_match = re.search(r'[\d.]+', price_text.replace(',', ''))
            price = price_match.group() if price_match else "0.00"
            
            # Get book URL
            link_elem = container.select_one('h3 a, a[href*="/catalogue/"]')
            book_url = ""
            if link_elem:
                href = link_elem.get('href', '')
                if href.startswith('/'):
                    book_url = f"{base_url}{href}"
                else:
                    book_url = href
            
            # Availability
            avail_elem = container.select_one('.availability, [class*="availability"], .instock')
            availability = avail_elem.get_text(strip=True) if avail_elem else "In stock"
            
            # Rating (from class like "star-rating Three")
            rating_elem = container.select_one('.star-rating, [class*="star"]')
            rating = 3  # default
            if rating_elem:
                classes = ' '.join(rating_elem.get('class', []))
                if 'One' in classes or '1' in classes:
                    rating = 1
                elif 'Two' in classes or '2' in classes:
                    rating = 2
                elif 'Three' in classes or '3' in classes:
                    rating = 3
                elif 'Four' in classes or '4' in classes:
                    rating = 4
                elif 'Five' in classes or '5' in classes:
                    rating = 5
            
            # Author and categories will need to be scraped from detail page or use LLM
            # For now, return what we can get from listing
            if title:
                return {
                    "title": title,
                    "price": price,
                    "author": "Unknown",  # Will be filled from detail or LLM
                    "categories": [],  # Will be filled from detail or LLM
                    "availability": availability,
                    "rating": rating,
                    "description": "",  # Will be filled from detail
                    "book_url": book_url
                }
        except Exception as e:
            logger.debug(f"CSS extraction failed: {e}")
        
        return None
    
    async def _extract_items_with_llm(self, crawler: AsyncWebCrawler, html_sections: List[str], 
                                     item_schema: dict, item_type: str) -> List[dict]:
        """Extract structured data from HTML sections using LLM - hybrid approach"""
        if not self.openai_api_key:
            return []
        
        extraction_strategy = self._get_extraction_strategy(item_schema)
        if not extraction_strategy:
            return []
        
        extracted_items = []
        
        # Process items in parallel batches for efficiency
        import base64
        import re
        
        for idx, html_section in enumerate(html_sections):
            try:
                # Create a complete HTML document from the section
                full_html = f"""<!DOCTYPE html>
<html>
<head><title>{item_type} Item</title></head>
<body>{html_section}</body>
</html>"""
                
                # Encode as base64 data URL
                html_b64 = base64.b64encode(full_html.encode('utf-8')).decode('utf-8')
                data_url = f"data:text/html;base64,{html_b64}"
                
                result = await crawler.arun(
                    url=data_url,
                    config=CrawlerRunConfig(
                        extraction_strategy=extraction_strategy,
                        cache_mode="bypass"
                    )
                )
                
                if result.extracted_content:
                    if isinstance(result.extracted_content, str):
                        try:
                            data = json.loads(result.extracted_content)
                        except json.JSONDecodeError:
                            # Try to extract JSON from markdown/text
                            json_match = re.search(r'\{[^{}]*\}', result.extracted_content)
                            if json_match:
                                data = json.loads(json_match.group())
                            else:
                                continue
                    else:
                        data = result.extracted_content
                    
                    # Extract the item - handle various response formats
                    if isinstance(data, dict):
                        # Check if it matches our schema
                        schema_keys = set(item_schema.get("properties", {}).keys())
                        data_keys = set(data.keys())
                        
                        # If it has most of the required keys, use it
                        if len(schema_keys & data_keys) >= len(schema_keys) * 0.5:
                            extracted_items.append(data)
                        else:
                            # Try nested extraction
                            for value in data.values():
                                if isinstance(value, dict) and len(schema_keys & set(value.keys())) >= len(schema_keys) * 0.5:
                                    extracted_items.append(value)
                                    break
                    elif isinstance(data, list):
                        # Filter valid items
                        for item in data:
                            if isinstance(item, dict):
                                schema_keys = set(item_schema.get("properties", {}).keys())
                                if len(schema_keys & set(item.keys())) >= len(schema_keys) * 0.5:
                                    extracted_items.append(item)
                    
            except Exception as e:
                logger.debug(f"Error extracting {item_type} item {idx}: {str(e)}")
                continue
        
        return extracted_items

    async def scrape_products(self) -> None:
        """Scrape products from web-scraping.dev with pagination using hybrid approach"""
        logger.info("Starting products scraping from web-scraping.dev")
        
        base_url = "https://web-scraping.dev/products"
        page = 1
        max_pages = 50  # Scrape all available pages (website has ~6 pages, but set higher for safety)
        
        # Schema for individual product (not wrapped in array)
        product_item_schema = {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "price": {"type": "number"},
                "category": {"type": "string"},
                "product_id": {"type": "string"}
            }
        }
        
        try:
            async with AsyncWebCrawler(config=self.browser_config) as crawler:
                while page <= max_pages:
                    try:
                        url = f"{base_url}?page={page}" if page > 1 else base_url
                        logger.info(f"Scraping products page {page}: {url}")
                        
                        # Step 1: Get raw HTML without LLM extraction
                        result = await crawler.arun(
                            url=url,
                            config=CrawlerRunConfig(
                                cache_mode="bypass"
                            )
                        )
                        
                        if not result.html:
                            logger.warning(f"No HTML content from page {page}")
                            break
                        
                        # Step 2: Parse HTML and extract individual product sections using CSS selectors
                        soup = BeautifulSoup(result.html, 'html.parser')
                        
                        # Find product containers - adjust selector based on actual HTML structure
                        # Common patterns: article, div with class containing "product", etc.
                        product_containers = soup.select('article, .product, [class*="product"], .item, [class*="item"]')
                        
                        if not product_containers:
                            # Try alternative selectors
                            product_containers = soup.select('div:has(h3), div:has(a[href*="/product/"])')
                        
                        if not product_containers or len(product_containers) == 0:
                            logger.info(f"No products found on page {page}, stopping pagination")
                            break
                        
                        logger.info(f"Found {len(product_containers)} product containers on page {page}")
                        
                        # Step 3: Try CSS extraction first, then LLM for each item
                        products_data = []
                        for container in product_containers:
                            # Try CSS selector extraction first (fast, no tokens)
                            product_data = self._extract_product_css(container)
                            
                            # If CSS extraction failed or incomplete, use LLM
                            if not product_data or not product_data.get("name"):
                                html_section = str(container)
                                llm_results = await self._extract_items_with_llm(
                                    crawler, [html_section], product_item_schema, "product"
                                )
                                if llm_results:
                                    product_data = llm_results[0]
                            
                            if product_data:
                                products_data.append(product_data)
                        
                        if not products_data or len(products_data) == 0:
                            logger.info(f"No products extracted from page {page}, stopping pagination")
                            break
                        
                        logger.info(f"Extracted {len(products_data)} products from page {page}")
                        
                        # Process each product
                        for item in products_data:
                            product_id = item.get("product_id", str(uuid.uuid4()))
                            category_name = item.get("category", "unknown")
                            
                            # Normalize category
                            if category_name not in self.product_categories:
                                cat_cd = f"CAT_{len(self.product_categories) + 1:04d}"
                                self.product_categories[category_name] = ProductCategory(
                                    cat_cd=cat_cd,
                                    cat_nm=category_name
                                )
                            
                            cat_cd = self.product_categories[category_name].cat_cd
                            
                            # Create product
                            product = Product(
                                inv_id=product_id,
                                inv_nm=item.get("name", ""),
                                unit_prc=float(item.get("price", 0)),
                                cat_cd=cat_cd,
                                desc_txt="",  # Will be filled from detail page
                                lnk_id=str(uuid.uuid4()),
                                scrp_dt=datetime.now()
                            )
                            self.products.append(product)
                            
                            # Scrape detail page for variants, reviews, similar products
                            await self._scrape_product_detail(crawler, product_id, product.inv_id)
                        
                        page += 1
                        
                    except Exception as e:
                        logger.error(f"Error processing page {page}: {e}", exc_info=True)
                        break
                        
        except Exception as e:
            logger.error(f"Error scraping products: {e}", exc_info=True)
            raise
        
        logger.info(f"Successfully scraped {len(self.products)} products")

    async def _scrape_product_detail(self, crawler: AsyncWebCrawler, product_url_id: str, inv_id: str) -> None:
        """Scrape product detail page for variants, reviews, and similar products"""
        try:
            detail_url = f"https://web-scraping.dev/product/{product_url_id}"
            
            detail_schema = {
                "type": "object",
                "properties": {
                    "description": {"type": "string"},
                    "variants": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "size": {"type": "string"},
                                "flavor": {"type": "string"},
                                "price_modifier": {"type": "number"}
                            }
                        }
                    },
                    "reviews": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "rating": {"type": "integer"},
                                "text": {"type": "string"},
                                "reviewer": {"type": "string"}
                            }
                        }
                    },
                    "similar_products": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "product_id": {"type": "string"},
                                "name": {"type": "string"}
                            }
                        }
                    }
                }
            }
            
            extraction_strategy = self._get_extraction_strategy(detail_schema)
            if not extraction_strategy:
                return
            
            result = await crawler.arun(
                url=detail_url,
                config=CrawlerRunConfig(
                    extraction_strategy=extraction_strategy,
                    cache_mode="bypass"
                )
            )
            
            if not result.extracted_content:
                return
            
            # Handle both string and dict/list formats
            if isinstance(result.extracted_content, str):
                data = json.loads(result.extracted_content)
            else:
                data = result.extracted_content
            
            # If data is a list, take the first item or wrap it
            if isinstance(data, list):
                if len(data) > 0:
                    data = data[0]
                else:
                    return
            
            # Ensure data is a dict
            if not isinstance(data, dict):
                return
            
            # Update product description
            for product in self.products:
                if product.inv_id == inv_id:
                    product.desc_txt = data.get("description", "")
                    break
            
            # Process variants
            for idx, variant in enumerate(data.get("variants", [])):
                self.product_variants.append(ProductVariant(
                    var_id=f"{inv_id}_VAR_{idx}",
                    inv_id=inv_id,
                    sz_cd=variant.get("size", ""),
                    flv_cd=variant.get("flavor", ""),
                    prc_mod=float(variant.get("price_modifier", 0))
                ))
            
            # Process reviews
            for idx, review in enumerate(data.get("reviews", [])):
                self.product_reviews.append(ProductReview(
                    rev_id=f"{inv_id}_REV_{idx}",
                    inv_id=inv_id,
                    rtg_val=int(review.get("rating", 3)),
                    rev_txt=review.get("text", ""),
                    rev_nm=review.get("reviewer"),
                    rev_dt=datetime.now()
                ))
            
            # Process similar products
            for similar in data.get("similar_products", []):
                similar_inv_id = similar.get("product_id", str(uuid.uuid4()))
                self.similar_products.append(SimilarProduct(
                    sim_id=str(uuid.uuid4()),
                    inv_id=inv_id,
                    sim_inv_id=similar_inv_id,
                    sim_score=None
                ))
                
        except Exception as e:
            logger.warning(f"Error scraping product detail for {inv_id}: {e}")

    async def scrape_books(self) -> None:
        """Scrape books from books.toscrape.com with pagination using hybrid approach"""
        logger.info("Starting books scraping from books.toscrape.com")
        
        base_url = "https://books.toscrape.com"
        page = 1
        max_pages = 50  # Scrape all available pages (website has 50 pages with 1000 books total)
        
        # Schema for individual book (not wrapped in array)
        book_item_schema = {
            "type": "object",
            "properties": {
                "title": {"type": "string"},
                "price": {"type": "string"},
                "author": {"type": "string"},
                "categories": {"type": "array", "items": {"type": "string"}},
                "availability": {"type": "string"},
                "rating": {"type": "integer"},
                "description": {"type": "string"},
                "book_url": {"type": "string"}
            }
        }
        
        try:
            async with AsyncWebCrawler(config=self.browser_config) as crawler:
                while page <= max_pages:
                    try:
                        url = f"{base_url}/catalogue/page-{page}.html" if page > 1 else f"{base_url}/index.html"
                        logger.info(f"Scraping books page {page}: {url}")
                        
                        # Step 1: Get raw HTML without LLM extraction
                        result = await crawler.arun(
                            url=url,
                            config=CrawlerRunConfig(
                                cache_mode="bypass"
                            )
                        )
                        
                        if not result.html:
                            logger.warning(f"No HTML content from page {page}")
                            break
                        
                        # Step 2: Parse HTML and extract individual book sections
                        soup = BeautifulSoup(result.html, 'html.parser')
                        
                        # Books.toscrape.com uses article tags for each book
                        book_containers = soup.select('article.product_pod, article, .product_pod')
                        
                        if not book_containers:
                            # Try alternative selectors
                            book_containers = soup.select('li:has(h3), .product, [class*="product"]')
                        
                        if not book_containers or len(book_containers) == 0:
                            logger.info(f"No books found on page {page}, stopping pagination")
                            break
                        
                        logger.info(f"Found {len(book_containers)} book containers on page {page}")
                        
                        # Step 3: Try CSS extraction first, then LLM for each item
                        books_data = []
                        for container in book_containers:
                            # Try CSS selector extraction first (fast, no tokens)
                            book_data = self._extract_book_css(container, base_url)
                            
                            # If CSS extraction failed or incomplete, use LLM
                            if not book_data or not book_data.get("title"):
                                html_section = str(container)
                                llm_results = await self._extract_items_with_llm(
                                    crawler, [html_section], book_item_schema, "book"
                                )
                                if llm_results:
                                    book_data = llm_results[0]
                            
                            if book_data:
                                books_data.append(book_data)
                        
                        if not books_data or len(books_data) == 0:
                            logger.info(f"No books extracted from page {page}, stopping pagination")
                            break
                        
                        logger.info(f"Extracted {len(books_data)} books from page {page}")
                    
                        for item in books_data:
                            book_id = str(uuid.uuid4())
                            author_name = item.get("author", "Unknown")
                            categories = item.get("categories", [])
                            
                            # Normalize author
                            if author_name not in self.authors:
                                auth_cd = f"AUTH_{len(self.authors) + 1:04d}"
                                self.authors[author_name] = Author(
                                    auth_cd=auth_cd,
                                    auth_nm=author_name
                                )
                            
                            auth_cd = self.authors[author_name].auth_cd
                            
                            # Parse price (remove currency symbols)
                            price_str = item.get("price", "0").replace("£", "").replace("$", "").strip()
                            try:
                                price = float(price_str)
                            except ValueError:
                                price = 0.0
                            
                            # Parse rating (convert stars to number)
                            rating_str = item.get("rating", "0")
                            rating = 3  # default
                            if "One" in rating_str or "1" in rating_str:
                                rating = 1
                            elif "Two" in rating_str or "2" in rating_str:
                                rating = 2
                            elif "Three" in rating_str or "3" in rating_str:
                                rating = 3
                            elif "Four" in rating_str or "4" in rating_str:
                                rating = 4
                            elif "Five" in rating_str or "5" in rating_str:
                                rating = 5
                            
                            book = Book(
                                bk_id=book_id,
                                bk_ttl=item.get("title", ""),
                                unit_prc=price,
                                auth_cd=auth_cd,
                                avail_sts=item.get("availability", "Unknown"),
                                rtg_val=rating,
                                desc_txt=item.get("description", ""),
                                lnk_id=str(uuid.uuid4()),
                                scrp_dt=datetime.now()
                            )
                            self.books.append(book)
                            
                            # Normalize and link categories
                            for cat_name in categories:
                                if cat_name not in self.book_categories:
                                    bk_cat_cd = f"BK_CAT_{len(self.book_categories) + 1:04d}"
                                    self.book_categories[cat_name] = BookCategory(
                                        bk_cat_cd=bk_cat_cd,
                                        bk_cat_nm=cat_name
                                    )
                                
                                bk_cat_cd = self.book_categories[cat_name].bk_cat_cd
                                self.book_category_bridges.append(BookCategoryBridge(
                                    bk_id=book_id,
                                    bk_cat_cd=bk_cat_cd
                                ))
                        
                        page += 1
                        
                    except Exception as e:
                        logger.error(f"Error processing books page {page}: {e}", exc_info=True)
                        break
                        
        except Exception as e:
            logger.error(f"Error scraping books: {e}", exc_info=True)
            raise
        
        logger.info(f"Successfully scraped {len(self.books)} books")

    async def scrape_films(self) -> None:
        """Scrape films from scrapethissite.com AJAX page"""
        logger.info("Starting films scraping from scrapethissite.com")
        
        base_url = "https://www.scrapethissite.com/pages/ajax-javascript/"
        years = list(range(2010, 2016))  # Years 2010-2015 (6 years)
        
        try:
            async with AsyncWebCrawler(config=self.browser_config) as crawler:
                for year in years:
                    try:
                        logger.info(f"Scraping films for year {year}")
                        
                        # Use AJAX endpoint directly (skip main page to avoid timeout)
                        ajax_url = f"{base_url}?ajax=true&year={year}"
                        
                        # Step 1: Get raw HTML/JSON response
                        result = await crawler.arun(
                            url=ajax_url,
                            config=CrawlerRunConfig(
                                cache_mode="bypass"
                            )
                        )
                        
                        if not result.html and not result.markdown:
                            logger.warning(f"No content extracted for year {year}")
                            continue
                        
                        # Try to parse as JSON first (AJAX endpoints often return JSON)
                        films_data = []
                        try:
                            # Check if the content is JSON
                            content = result.html or result.markdown or ""
                            if content.strip().startswith('[') or content.strip().startswith('{'):
                                data = json.loads(content)
                                if isinstance(data, list):
                                    films_data = data
                                elif isinstance(data, dict):
                                    films_data = data.get("films", [])
                        except json.JSONDecodeError:
                            # Not JSON, try HTML parsing
                            soup = BeautifulSoup(result.html or result.markdown, 'html.parser')
                            # Films are in JSON format in the page, try to extract
                            # For scrapethissite, the data is in a code block or script tag
                            code_blocks = soup.find_all('code') or soup.find_all('pre')
                            for block in code_blocks:
                                try:
                                    data = json.loads(block.get_text())
                                    if isinstance(data, list):
                                        films_data = data
                                        break
                                except:
                                    continue
                    
                        # If still no data, try LLM extraction on the HTML
                        if not films_data:
                            film_item_schema = {
                                "type": "object",
                                "properties": {
                                    "title": {"type": "string"},
                                    "year": {"type": "integer"},
                                    "award_category": {"type": "string"},
                                    "director": {"type": "string"},
                                    "actors": {"type": "array", "items": {"type": "string"}}
                                }
                            }
                            
                            # Try extracting as a single section
                            html_sections = [result.html or result.markdown or ""]
                            films_data = await self._extract_items_with_llm(
                                crawler, html_sections, film_item_schema, "film"
                            )
                        
                        if not films_data:
                            logger.warning(f"No films found for year {year}")
                            continue
                    
                        logger.info(f"Extracted {len(films_data)} films for year {year}")
                        
                        for item in films_data:
                            # Handle case where item might be a dict with nested structure
                            if not isinstance(item, dict):
                                logger.warning(f"Skipping invalid film item: {type(item)}")
                                continue
                                
                            media_id = str(uuid.uuid4())
                            title = item.get("title", "Unknown")
                            director_name = item.get("director", "Unknown")
                            award_name = item.get("award_category", "Best Picture")  # Default to Best Picture
                            actors_list = item.get("actors", [])
                            
                            # If no director in data, try to infer or use default
                            if director_name == "Unknown" and "director" not in item:
                                director_name = "Unknown Director"
                            
                            # Normalize director
                            if director_name not in self.directors:
                                dir_cd = f"DIR_{len(self.directors) + 1:04d}"
                                self.directors[director_name] = Director(
                                    dir_cd=dir_cd,
                                    dir_nm=director_name
                                )
                            
                            dir_cd = self.directors[director_name].dir_cd
                            
                            # Normalize award category
                            if award_name not in self.award_categories:
                                awd_cat_cd = f"AWD_{len(self.award_categories) + 1:04d}"
                                self.award_categories[award_name] = AwardCategory(
                                    awd_cat_cd=awd_cat_cd,
                                    awd_nm=award_name,
                                    awd_typ=None
                                )
                            
                            awd_cat_cd = self.award_categories[award_name].awd_cat_cd
                            
                            film = Film(
                                media_id=media_id,
                                media_ttl=item.get("title", ""),
                                yr_val=int(item.get("year", year)),
                                awd_cat_cd=awd_cat_cd,
                                dir_cd=dir_cd,
                                scrp_dt=datetime.now()
                            )
                            self.films.append(film)
                            
                            # Normalize and link actors
                            for actor_name in actors_list:
                                if actor_name not in self.actors:
                                    perf_cd = f"PERF_{len(self.actors) + 1:04d}"
                                    self.actors[actor_name] = Actor(
                                        perf_cd=perf_cd,
                                        perf_nm=actor_name
                                    )
                                
                                perf_cd = self.actors[actor_name].perf_cd
                                self.film_actor_bridges.append(FilmActorBridge(
                                    media_id=media_id,
                                    perf_cd=perf_cd,
                                    role_nm=None
                                ))
                            
                            # Link award
                            self.film_award_bridges.append(FilmAwardBridge(
                                media_id=media_id,
                                awd_cat_cd=awd_cat_cd,
                                awd_yr=int(item.get("year", year))
                            ))
                    
                    except Exception as e:
                        logger.error(f"Error processing films for year {year}: {e}", exc_info=True)
                        continue
                        
        except Exception as e:
            logger.error(f"Error scraping films: {e}", exc_info=True)
            raise
        
        logger.info(f"Successfully scraped {len(self.films)} films")

    def to_dataframes(self) -> Dict[str, pd.DataFrame]:
        """Convert all scraped data to normalized DataFrames"""
        logger.info("Converting scraped data to DataFrames")
        
        dataframes = {}
        
        # Dimension tables
        if self.product_categories:
            dataframes["CAT_REF"] = pd.DataFrame([cat.model_dump() for cat in self.product_categories.values()])
        
        if self.book_categories:
            dataframes["BK_CAT_REF"] = pd.DataFrame([cat.model_dump() for cat in self.book_categories.values()])
        
        if self.authors:
            dataframes["AUTH_REF"] = pd.DataFrame([auth.model_dump() for auth in self.authors.values()])
        
        if self.directors:
            dataframes["DIR_REF"] = pd.DataFrame([dir.model_dump() for dir in self.directors.values()])
        
        if self.actors:
            dataframes["PERF_REF"] = pd.DataFrame([actor.model_dump() for actor in self.actors.values()])
        
        if self.award_categories:
            dataframes["AWARD_REF"] = pd.DataFrame([awd.model_dump() for awd in self.award_categories.values()])
        
        # Fact tables
        if self.products:
            dataframes["INV_MAST"] = pd.DataFrame([p.model_dump() for p in self.products])
        
        if self.books:
            dataframes["BK_CATALOG"] = pd.DataFrame([b.model_dump() for b in self.books])
        
        if self.films:
            dataframes["MEDIA_MAST"] = pd.DataFrame([f.model_dump() for f in self.films])
        
        # Bridge tables
        if self.book_category_bridges:
            dataframes["BK_CAT_XREF"] = pd.DataFrame([b.model_dump() for b in self.book_category_bridges])
        
        if self.film_actor_bridges:
            dataframes["MEDIA_PERF_XREF"] = pd.DataFrame([b.model_dump() for b in self.film_actor_bridges])
        
        if self.film_award_bridges:
            dataframes["MEDIA_AWD_XREF"] = pd.DataFrame([b.model_dump() for b in self.film_award_bridges])
        
        # Detail tables
        if self.product_variants:
            dataframes["INV_VAR"] = pd.DataFrame([v.model_dump() for v in self.product_variants])
        
        if self.product_reviews:
            dataframes["INV_REV"] = pd.DataFrame([r.model_dump() for r in self.product_reviews])
        
        if self.similar_products:
            dataframes["INV_SIM"] = pd.DataFrame([s.model_dump() for s in self.similar_products])
        
        logger.info(f"Created {len(dataframes)} DataFrames")
        for name, df in dataframes.items():
            logger.info(f"  {name}: {len(df)} rows")
        
        return dataframes

    def export_to_csv(self, output_dir: str = "data") -> Dict[str, str]:
        """Export all DataFrames to CSV files"""
        import os
        os.makedirs(output_dir, exist_ok=True)
        
        dataframes = self.to_dataframes()
        paths = {}
        
        for table_name, df in dataframes.items():
            file_path = f"{output_dir}/{table_name.lower()}.csv"
            df.to_csv(file_path, index=False)
            paths[table_name] = file_path
            logger.info(f"Exported {table_name} to {file_path}")
        
        return paths

    async def run_all(self) -> None:
        """Run all scrapers concurrently"""
        logger.info("Starting all scraping tasks concurrently")
        await asyncio.gather(
            self.scrape_products(),
            self.scrape_books(),
            self.scrape_films(),
            return_exceptions=True
        )
