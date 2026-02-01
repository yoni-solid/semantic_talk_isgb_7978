"""
Generate 200 unique business questions with SQL queries.
Each query is tested to ensure it returns at least one row.
"""
import json
import logging
from database import SnowflakeManager
from dotenv import load_dotenv
import os

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def generate_queries():
    """Generate 200 unique business questions with SQL queries"""
    
    queries = []
    
    # ========== PRODUCT/INVENTORY QUESTIONS (60 queries) ==========
    
    # Basic product queries
    queries.extend([
        {
            "id": 1,
            "category": "Products",
            "question": "What are all the products in our inventory?",
            "query": "SELECT inv_id, inv_nm, unit_prc, cat_cd FROM SUPPLY_CHAIN.INV_MAST ORDER BY inv_nm"
        },
        {
            "id": 2,
            "category": "Products",
            "question": "What is the total number of products in our inventory?",
            "query": "SELECT COUNT(*) as total_products FROM SUPPLY_CHAIN.INV_MAST"
        },
        {
            "id": 3,
            "category": "Products",
            "question": "What is the average price of all products?",
            "query": "SELECT AVG(unit_prc) as avg_price FROM SUPPLY_CHAIN.INV_MAST WHERE unit_prc IS NOT NULL"
        },
        {
            "id": 4,
            "category": "Products",
            "question": "What is the highest priced product?",
            "query": "SELECT inv_id, inv_nm, unit_prc FROM SUPPLY_CHAIN.INV_MAST WHERE unit_prc = (SELECT MAX(unit_prc) FROM SUPPLY_CHAIN.INV_MAST) LIMIT 1"
        },
        {
            "id": 5,
            "category": "Products",
            "question": "What is the lowest priced product?",
            "query": "SELECT inv_id, inv_nm, unit_prc FROM SUPPLY_CHAIN.INV_MAST WHERE unit_prc = (SELECT MIN(unit_prc) FROM SUPPLY_CHAIN.INV_MAST WHERE unit_prc > 0) LIMIT 1"
        },
        {
            "id": 6,
            "category": "Products",
            "question": "What products cost more than $50?",
            "query": "SELECT inv_id, inv_nm, unit_prc FROM SUPPLY_CHAIN.INV_MAST WHERE unit_prc > 50 ORDER BY unit_prc DESC"
        },
        {
            "id": 7,
            "category": "Products",
            "question": "What products cost less than $10?",
            "query": "SELECT inv_id, inv_nm, unit_prc FROM SUPPLY_CHAIN.INV_MAST WHERE unit_prc < 10 AND unit_prc > 0 ORDER BY unit_prc"
        },
        {
            "id": 8,
            "category": "Products",
            "question": "What is the price range of our products?",
            "query": "SELECT MIN(unit_prc) as min_price, MAX(unit_prc) as max_price, AVG(unit_prc) as avg_price FROM SUPPLY_CHAIN.INV_MAST WHERE unit_prc > 0"
        },
        {
            "id": 9,
            "category": "Products",
            "question": "How many products are in each price range?",
            "query": "SELECT CASE WHEN unit_prc < 10 THEN 'Under $10' WHEN unit_prc < 25 THEN '$10-$25' WHEN unit_prc < 50 THEN '$25-$50' WHEN unit_prc < 100 THEN '$50-$100' ELSE 'Over $100' END as price_range, COUNT(*) as product_count FROM SUPPLY_CHAIN.INV_MAST WHERE unit_prc > 0 GROUP BY price_range ORDER BY MIN(unit_prc)"
        },
        {
            "id": 10,
            "category": "Products",
            "question": "What are the top 10 most expensive products?",
            "query": "SELECT inv_id, inv_nm, unit_prc FROM SUPPLY_CHAIN.INV_MAST WHERE unit_prc > 0 ORDER BY unit_prc DESC LIMIT 10"
        },
    ])
    
    # Product categories
    queries.extend([
        {
            "id": 11,
            "category": "Products",
            "question": "What are all the product categories?",
            "query": "SELECT DISTINCT cat_cd, cat_nm FROM SUPPLY_CHAIN.CAT_REF ORDER BY cat_nm"
        },
        {
            "id": 12,
            "category": "Products",
            "question": "How many products are in each category?",
            "query": "SELECT c.cat_nm, COUNT(i.inv_id) as product_count FROM SUPPLY_CHAIN.CAT_REF c LEFT JOIN SUPPLY_CHAIN.INV_MAST i ON c.cat_cd = i.cat_cd GROUP BY c.cat_cd, c.cat_nm ORDER BY product_count DESC"
        },
        {
            "id": 13,
            "category": "Products",
            "question": "What is the average price by product category?",
            "query": "SELECT c.cat_nm, AVG(i.unit_prc) as avg_price, COUNT(i.inv_id) as product_count FROM SUPPLY_CHAIN.CAT_REF c LEFT JOIN SUPPLY_CHAIN.INV_MAST i ON c.cat_cd = i.cat_cd WHERE i.unit_prc > 0 GROUP BY c.cat_cd, c.cat_nm ORDER BY avg_price DESC"
        },
        {
            "id": 14,
            "category": "Products",
            "question": "What category has the most products?",
            "query": "SELECT c.cat_nm, COUNT(i.inv_id) as product_count FROM SUPPLY_CHAIN.CAT_REF c LEFT JOIN SUPPLY_CHAIN.INV_MAST i ON c.cat_cd = i.cat_cd GROUP BY c.cat_cd, c.cat_nm ORDER BY product_count DESC LIMIT 1"
        },
        {
            "id": 15,
            "category": "Products",
            "question": "What category has the highest average price?",
            "query": "SELECT c.cat_nm, AVG(i.unit_prc) as avg_price FROM SUPPLY_CHAIN.CAT_REF c LEFT JOIN SUPPLY_CHAIN.INV_MAST i ON c.cat_cd = i.cat_cd WHERE i.unit_prc > 0 GROUP BY c.cat_cd, c.cat_nm ORDER BY avg_price DESC LIMIT 1"
        },
        {
            "id": 16,
            "category": "Products",
            "question": "What products are in a specific category?",
            "query": "SELECT i.inv_id, i.inv_nm, i.unit_prc, c.cat_nm FROM SUPPLY_CHAIN.INV_MAST i JOIN SUPPLY_CHAIN.CAT_REF c ON i.cat_cd = c.cat_cd WHERE c.cat_nm = (SELECT cat_nm FROM SUPPLY_CHAIN.CAT_REF LIMIT 1) ORDER BY i.inv_nm"
        },
        {
            "id": 17,
            "category": "Products",
            "question": "What is the total value of inventory by category?",
            "query": "SELECT c.cat_nm, SUM(i.unit_prc) as total_value, COUNT(i.inv_id) as product_count FROM SUPPLY_CHAIN.CAT_REF c LEFT JOIN SUPPLY_CHAIN.INV_MAST i ON c.cat_cd = i.cat_cd WHERE i.unit_prc > 0 GROUP BY c.cat_cd, c.cat_nm ORDER BY total_value DESC"
        },
    ])
    
    # Product search and filtering
    queries.extend([
        {
            "id": 18,
            "category": "Products",
            "question": "What products have descriptions longer than 100 characters?",
            "query": "SELECT inv_id, inv_nm, LENGTH(desc_txt) as desc_length FROM SUPPLY_CHAIN.INV_MAST WHERE LENGTH(desc_txt) > 100 ORDER BY desc_length DESC"
        },
        {
            "id": 19,
            "category": "Products",
            "question": "What products were scraped in the last 30 days?",
            "query": "SELECT inv_id, inv_nm, scrp_dt FROM SUPPLY_CHAIN.INV_MAST WHERE scrp_dt >= DATEADD(day, -30, CURRENT_TIMESTAMP()) ORDER BY scrp_dt DESC"
        },
        {
            "id": 20,
            "category": "Products",
            "question": "How many products were added each day?",
            "query": "SELECT DATE(scrp_dt) as scrape_date, COUNT(*) as products_added FROM SUPPLY_CHAIN.INV_MAST GROUP BY DATE(scrp_dt) ORDER BY scrape_date DESC"
        },
        {
            "id": 21,
            "category": "Products",
            "question": "What products have no description?",
            "query": "SELECT inv_id, inv_nm, unit_prc FROM SUPPLY_CHAIN.INV_MAST WHERE desc_txt IS NULL OR LENGTH(TRIM(desc_txt)) = 0"
        },
        {
            "id": 22,
            "category": "Products",
            "question": "What products have duplicate names?",
            "query": "SELECT inv_nm, COUNT(*) as count FROM SUPPLY_CHAIN.INV_MAST GROUP BY inv_nm HAVING COUNT(*) > 1 ORDER BY count DESC"
        },
        {
            "id": 23,
            "category": "Products",
            "question": "What is the distribution of product prices?",
            "query": "SELECT CASE WHEN unit_prc = 0 THEN 'Free' WHEN unit_prc < 5 THEN '$0-$5' WHEN unit_prc < 10 THEN '$5-$10' WHEN unit_prc < 20 THEN '$10-$20' WHEN unit_prc < 50 THEN '$20-$50' WHEN unit_prc < 100 THEN '$50-$100' ELSE 'Over $100' END as price_bucket, COUNT(*) as count FROM SUPPLY_CHAIN.INV_MAST GROUP BY price_bucket ORDER BY MIN(unit_prc)"
        },
    ])
    
    # Product variants and details
    queries.extend([
        {
            "id": 24,
            "category": "Products",
            "question": "What products have variants?",
            "query": "SELECT DISTINCT i.inv_id, i.inv_nm FROM SUPPLY_CHAIN.INV_MAST i JOIN SUPPLY_CHAIN.INV_VAR v ON i.inv_id = v.inv_id"
        },
        {
            "id": 25,
            "category": "Products",
            "question": "What products have reviews?",
            "query": "SELECT DISTINCT i.inv_id, i.inv_nm, COUNT(r.rev_id) as review_count FROM SUPPLY_CHAIN.INV_MAST i JOIN SUPPLY_CHAIN.INV_REV r ON i.inv_id = r.inv_id GROUP BY i.inv_id, i.inv_nm ORDER BY review_count DESC"
        },
        {
            "id": 26,
            "category": "Products",
            "question": "What is the average rating for products with reviews?",
            "query": "SELECT i.inv_id, i.inv_nm, AVG(r.rtg_val) as avg_rating, COUNT(r.rev_id) as review_count FROM SUPPLY_CHAIN.INV_MAST i JOIN SUPPLY_CHAIN.INV_REV r ON i.inv_id = r.inv_id GROUP BY i.inv_id, i.inv_nm ORDER BY avg_rating DESC"
        },
        {
            "id": 27,
            "category": "Products",
            "question": "What products have similar products?",
            "query": "SELECT DISTINCT i.inv_id, i.inv_nm, COUNT(s.sim_id) as similar_count FROM SUPPLY_CHAIN.INV_MAST i JOIN SUPPLY_CHAIN.INV_SIM s ON i.inv_id = s.inv_id GROUP BY i.inv_id, i.inv_nm ORDER BY similar_count DESC"
        },
    ])
    
    # More product queries to reach 60
    queries.extend([
        {
            "id": 28,
            "category": "Products",
            "question": "What is the median price of products?",
            "query": "SELECT MEDIAN(unit_prc) as median_price FROM SUPPLY_CHAIN.INV_MAST WHERE unit_prc > 0"
        },
        {
            "id": 29,
            "category": "Products",
            "question": "What products are priced between $20 and $50?",
            "query": "SELECT inv_id, inv_nm, unit_prc FROM SUPPLY_CHAIN.INV_MAST WHERE unit_prc BETWEEN 20 AND 50 ORDER BY unit_prc"
        },
        {
            "id": 30,
            "category": "Products",
            "question": "What is the standard deviation of product prices?",
            "query": "SELECT STDDEV(unit_prc) as price_stddev FROM SUPPLY_CHAIN.INV_MAST WHERE unit_prc > 0"
        },
        {
            "id": 31,
            "category": "Products",
            "question": "What products have the longest names?",
            "query": "SELECT inv_id, inv_nm, LENGTH(inv_nm) as name_length FROM SUPPLY_CHAIN.INV_MAST ORDER BY name_length DESC LIMIT 10"
        },
        {
            "id": 32,
            "category": "Products",
            "question": "What percentage of products are in each category?",
            "query": "SELECT c.cat_nm, COUNT(i.inv_id) as product_count, ROUND(100.0 * COUNT(i.inv_id) / (SELECT COUNT(*) FROM SUPPLY_CHAIN.INV_MAST), 2) as percentage FROM SUPPLY_CHAIN.CAT_REF c LEFT JOIN SUPPLY_CHAIN.INV_MAST i ON c.cat_cd = i.cat_cd GROUP BY c.cat_cd, c.cat_nm ORDER BY percentage DESC"
        },
        {
            "id": 33,
            "category": "Products",
            "question": "What products were scraped most recently?",
            "query": "SELECT inv_id, inv_nm, scrp_dt FROM SUPPLY_CHAIN.INV_MAST ORDER BY scrp_dt DESC LIMIT 20"
        },
        {
            "id": 34,
            "category": "Products",
            "question": "What is the total inventory value?",
            "query": "SELECT SUM(unit_prc) as total_inventory_value, COUNT(*) as total_products FROM SUPPLY_CHAIN.INV_MAST WHERE unit_prc > 0"
        },
        {
            "id": 35,
            "category": "Products",
            "question": "What products have prices that are outliers (more than 2 standard deviations from mean)?",
            "query": "SELECT inv_id, inv_nm, unit_prc FROM SUPPLY_CHAIN.INV_MAST WHERE unit_prc > (SELECT AVG(unit_prc) + 2 * STDDEV(unit_prc) FROM SUPPLY_CHAIN.INV_MAST WHERE unit_prc > 0) ORDER BY unit_prc DESC"
        },
        {
            "id": 36,
            "category": "Products",
            "question": "What is the price distribution by quartiles?",
            "query": "SELECT PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY unit_prc) as q1, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY unit_prc) as median, PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY unit_prc) as q3 FROM SUPPLY_CHAIN.INV_MAST WHERE unit_prc > 0"
        },
        {
            "id": 37,
            "category": "Products",
            "question": "What products have no category assigned?",
            "query": "SELECT inv_id, inv_nm, cat_cd FROM SUPPLY_CHAIN.INV_MAST WHERE cat_cd IS NULL OR cat_cd NOT IN (SELECT cat_cd FROM SUPPLY_CHAIN.CAT_REF)"
        },
        {
            "id": 38,
            "category": "Products",
            "question": "What is the growth rate of products added over time?",
            "query": "SELECT DATE(scrp_dt) as date, COUNT(*) as products_added, LAG(COUNT(*)) OVER (ORDER BY DATE(scrp_dt)) as prev_day_count FROM SUPPLY_CHAIN.INV_MAST GROUP BY DATE(scrp_dt) ORDER BY date DESC"
        },
        {
            "id": 39,
            "category": "Products",
            "question": "What products share the same link ID?",
            "query": "SELECT lnk_id, COUNT(*) as product_count FROM SUPPLY_CHAIN.INV_MAST WHERE lnk_id IS NOT NULL GROUP BY lnk_id HAVING COUNT(*) > 1"
        },
        {
            "id": 40,
            "category": "Products",
            "question": "What is the cumulative count of products over time?",
            "query": "SELECT DATE(scrp_dt) as date, COUNT(*) as daily_count, SUM(COUNT(*)) OVER (ORDER BY DATE(scrp_dt)) as cumulative_count FROM SUPPLY_CHAIN.INV_MAST GROUP BY DATE(scrp_dt) ORDER BY date"
        },
    ])
    
    # More product queries (41-60)
    queries.extend([
        {
            "id": 41,
            "category": "Products",
            "question": "What products have prices that are exactly at the average?",
            "query": "SELECT inv_id, inv_nm, unit_prc FROM SUPPLY_CHAIN.INV_MAST WHERE ABS(unit_prc - (SELECT AVG(unit_prc) FROM SUPPLY_CHAIN.INV_MAST WHERE unit_prc > 0)) < 0.01"
        },
        {
            "id": 42,
            "category": "Products",
            "question": "What is the mode (most common) price?",
            "query": "SELECT unit_prc, COUNT(*) as frequency FROM SUPPLY_CHAIN.INV_MAST WHERE unit_prc > 0 GROUP BY unit_prc ORDER BY frequency DESC LIMIT 1"
        },
        {
            "id": 43,
            "category": "Products",
            "question": "What products have names starting with 'A'?",
            "query": "SELECT inv_id, inv_nm, unit_prc FROM SUPPLY_CHAIN.INV_MAST WHERE UPPER(inv_nm) LIKE 'A%' ORDER BY inv_nm"
        },
        {
            "id": 44,
            "category": "Products",
            "question": "What products have names containing 'pro'?",
            "query": "SELECT inv_id, inv_nm, unit_prc FROM SUPPLY_CHAIN.INV_MAST WHERE UPPER(inv_nm) LIKE '%PRO%' ORDER BY inv_nm"
        },
        {
            "id": 45,
            "category": "Products",
            "question": "What is the price variance by category?",
            "query": "SELECT c.cat_nm, VARIANCE(i.unit_prc) as price_variance FROM SUPPLY_CHAIN.CAT_REF c LEFT JOIN SUPPLY_CHAIN.INV_MAST i ON c.cat_cd = i.cat_cd WHERE i.unit_prc > 0 GROUP BY c.cat_cd, c.cat_nm ORDER BY price_variance DESC"
        },
        {
            "id": 46,
            "category": "Products",
            "question": "What products have the same price?",
            "query": "SELECT unit_prc, COUNT(*) as product_count, LISTAGG(inv_nm, ', ') WITHIN GROUP (ORDER BY inv_nm) as products FROM SUPPLY_CHAIN.INV_MAST WHERE unit_prc > 0 GROUP BY unit_prc HAVING COUNT(*) > 1 ORDER BY product_count DESC"
        },
        {
            "id": 47,
            "category": "Products",
            "question": "What is the price difference between highest and lowest product?",
            "query": "SELECT MAX(unit_prc) - MIN(unit_prc) as price_range FROM SUPPLY_CHAIN.INV_MAST WHERE unit_prc > 0"
        },
        {
            "id": 48,
            "category": "Products",
            "question": "What products are in the top 5% by price?",
            "query": "SELECT inv_id, inv_nm, unit_prc FROM SUPPLY_CHAIN.INV_MAST WHERE unit_prc >= (SELECT PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY unit_prc) FROM SUPPLY_CHAIN.INV_MAST WHERE unit_prc > 0) ORDER BY unit_prc DESC"
        },
        {
            "id": 49,
            "category": "Products",
            "question": "What products are in the bottom 5% by price?",
            "query": "SELECT inv_id, inv_nm, unit_prc FROM SUPPLY_CHAIN.INV_MAST WHERE unit_prc > 0 AND unit_prc <= (SELECT PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY unit_prc) FROM SUPPLY_CHAIN.INV_MAST WHERE unit_prc > 0) ORDER BY unit_prc"
        },
        {
            "id": 50,
            "category": "Products",
            "question": "What is the running total of product prices when sorted by price?",
            "query": "SELECT inv_id, inv_nm, unit_prc, SUM(unit_prc) OVER (ORDER BY unit_prc) as running_total FROM SUPPLY_CHAIN.INV_MAST WHERE unit_prc > 0 ORDER BY unit_prc LIMIT 20"
        },
        {
            "id": 51,
            "category": "Products",
            "question": "What products have prices that are perfect squares?",
            "query": "SELECT inv_id, inv_nm, unit_prc FROM SUPPLY_CHAIN.INV_MAST WHERE unit_prc > 0 AND SQRT(unit_prc) = FLOOR(SQRT(unit_prc)) ORDER BY unit_prc"
        },
        {
            "id": 52,
            "category": "Products",
            "question": "What is the coefficient of variation for product prices?",
            "query": "SELECT STDDEV(unit_prc) / AVG(unit_prc) as coefficient_of_variation FROM SUPPLY_CHAIN.INV_MAST WHERE unit_prc > 0"
        },
        {
            "id": 53,
            "category": "Products",
            "question": "What products were added on weekends?",
            "query": "SELECT inv_id, inv_nm, scrp_dt, DAYNAME(scrp_dt) as day_name FROM SUPPLY_CHAIN.INV_MAST WHERE DAYOFWEEK(scrp_dt) IN (0, 6) ORDER BY scrp_dt DESC"
        },
        {
            "id": 54,
            "category": "Products",
            "question": "What is the average price difference between consecutive products?",
            "query": "SELECT AVG(ABS(unit_prc - LAG(unit_prc) OVER (ORDER BY unit_prc))) as avg_price_diff FROM (SELECT unit_prc FROM SUPPLY_CHAIN.INV_MAST WHERE unit_prc > 0 ORDER BY unit_prc)"
        },
        {
            "id": 55,
            "category": "Products",
            "question": "What products have descriptions with more than 50 words?",
            "query": "SELECT inv_id, inv_nm, LENGTH(desc_txt) - LENGTH(REPLACE(desc_txt, ' ', '')) + 1 as word_count FROM SUPPLY_CHAIN.INV_MAST WHERE desc_txt IS NOT NULL AND LENGTH(desc_txt) > 0 AND LENGTH(desc_txt) - LENGTH(REPLACE(desc_txt, ' ', '')) + 1 > 50 ORDER BY word_count DESC"
        },
        {
            "id": 56,
            "category": "Products",
            "question": "What is the price rank of each product?",
            "query": "SELECT inv_id, inv_nm, unit_prc, RANK() OVER (ORDER BY unit_prc DESC) as price_rank FROM SUPPLY_CHAIN.INV_MAST WHERE unit_prc > 0 ORDER BY price_rank LIMIT 20"
        },
        {
            "id": 57,
            "category": "Products",
            "question": "What products have prices that are multiples of 5?",
            "query": "SELECT inv_id, inv_nm, unit_prc FROM SUPPLY_CHAIN.INV_MAST WHERE unit_prc > 0 AND MOD(unit_prc, 5) = 0 ORDER BY unit_prc"
        },
        {
            "id": 58,
            "category": "Products",
            "question": "What is the price density distribution?",
            "query": "SELECT FLOOR(unit_prc / 10) * 10 as price_bucket, COUNT(*) as count, ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage FROM SUPPLY_CHAIN.INV_MAST WHERE unit_prc > 0 GROUP BY price_bucket ORDER BY price_bucket"
        },
        {
            "id": 59,
            "category": "Products",
            "question": "What products have the highest price-to-name-length ratio?",
            "query": "SELECT inv_id, inv_nm, unit_prc, LENGTH(inv_nm) as name_length, unit_prc / NULLIF(LENGTH(inv_nm), 0) as price_per_char FROM SUPPLY_CHAIN.INV_MAST WHERE unit_prc > 0 AND LENGTH(inv_nm) > 0 ORDER BY price_per_char DESC LIMIT 10"
        },
        {
            "id": 60,
            "category": "Products",
            "question": "What is the interquartile range of product prices?",
            "query": "SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY unit_prc) - PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY unit_prc) as iqr FROM SUPPLY_CHAIN.INV_MAST WHERE unit_prc > 0"
        },
    ])
    
    # ========== BOOK QUESTIONS (61-120) ==========
    # Due to the large number of queries (140 remaining), I'll create them programmatically
    # This is more efficient than listing each one individually
    
    # Book queries will be added here - see queries_remaining.py for full definitions
    # For now, adding key book queries
    
    # Basic book queries
    book_queries = [
        ("What are all the books in our catalog?", "SELECT bk_id, bk_ttl, unit_prc, auth_cd, rtg_val FROM SUPPLY_CHAIN.BK_CATALOG ORDER BY bk_ttl"),
        ("What is the total number of books?", "SELECT COUNT(*) as total_books FROM SUPPLY_CHAIN.BK_CATALOG"),
        ("What is the average price of books?", "SELECT AVG(unit_prc) as avg_price FROM SUPPLY_CHAIN.BK_CATALOG WHERE unit_prc IS NOT NULL"),
        ("What is the most expensive book?", "SELECT bk_id, bk_ttl, unit_prc FROM SUPPLY_CHAIN.BK_CATALOG WHERE unit_prc = (SELECT MAX(unit_prc) FROM SUPPLY_CHAIN.BK_CATALOG) LIMIT 1"),
        ("What is the cheapest book?", "SELECT bk_id, bk_ttl, unit_prc FROM SUPPLY_CHAIN.BK_CATALOG WHERE unit_prc = (SELECT MIN(unit_prc) FROM SUPPLY_CHAIN.BK_CATALOG WHERE unit_prc > 0) LIMIT 1"),
        ("What books cost more than £20?", "SELECT bk_id, bk_ttl, unit_prc FROM SUPPLY_CHAIN.BK_CATALOG WHERE unit_prc > 20 ORDER BY unit_prc DESC"),
        ("What books cost less than £5?", "SELECT bk_id, bk_ttl, unit_prc FROM SUPPLY_CHAIN.BK_CATALOG WHERE unit_prc < 5 AND unit_prc > 0 ORDER BY unit_prc"),
        ("What is the price range of books?", "SELECT MIN(unit_prc) as min_price, MAX(unit_prc) as max_price, AVG(unit_prc) as avg_price FROM SUPPLY_CHAIN.BK_CATALOG WHERE unit_prc > 0"),
        ("What are the top 10 most expensive books?", "SELECT bk_id, bk_ttl, unit_prc FROM SUPPLY_CHAIN.BK_CATALOG WHERE unit_prc > 0 ORDER BY unit_prc DESC LIMIT 10"),
        ("What are the top 10 cheapest books?", "SELECT bk_id, bk_ttl, unit_prc FROM SUPPLY_CHAIN.BK_CATALOG WHERE unit_prc > 0 ORDER BY unit_prc LIMIT 10"),
        ("What are all the authors?", "SELECT DISTINCT auth_cd, auth_nm FROM SUPPLY_CHAIN.AUTH_REF ORDER BY auth_nm"),
        ("How many books does each author have?", "SELECT a.auth_nm, COUNT(b.bk_id) as book_count FROM SUPPLY_CHAIN.AUTH_REF a LEFT JOIN SUPPLY_CHAIN.BK_CATALOG b ON a.auth_cd = b.auth_cd GROUP BY a.auth_cd, a.auth_nm ORDER BY book_count DESC"),
        ("What author has the most books?", "SELECT a.auth_nm, COUNT(b.bk_id) as book_count FROM SUPPLY_CHAIN.AUTH_REF a LEFT JOIN SUPPLY_CHAIN.BK_CATALOG b ON a.auth_cd = b.auth_cd GROUP BY a.auth_cd, a.auth_nm ORDER BY book_count DESC LIMIT 1"),
        ("What is the average price by author?", "SELECT a.auth_nm, AVG(b.unit_prc) as avg_price, COUNT(b.bk_id) as book_count FROM SUPPLY_CHAIN.AUTH_REF a LEFT JOIN SUPPLY_CHAIN.BK_CATALOG b ON a.auth_cd = b.auth_cd WHERE b.unit_prc > 0 GROUP BY a.auth_cd, a.auth_nm ORDER BY avg_price DESC"),
        ("What author has the highest average book price?", "SELECT a.auth_nm, AVG(b.unit_prc) as avg_price FROM SUPPLY_CHAIN.AUTH_REF a LEFT JOIN SUPPLY_CHAIN.BK_CATALOG b ON a.auth_cd = b.auth_cd WHERE b.unit_prc > 0 GROUP BY a.auth_cd, a.auth_nm ORDER BY avg_price DESC LIMIT 1"),
        ("What books are by a specific author?", "SELECT b.bk_id, b.bk_ttl, b.unit_prc, a.auth_nm FROM SUPPLY_CHAIN.BK_CATALOG b JOIN SUPPLY_CHAIN.AUTH_REF a ON b.auth_cd = a.auth_cd WHERE a.auth_nm = (SELECT auth_nm FROM SUPPLY_CHAIN.AUTH_REF LIMIT 1) ORDER BY b.bk_ttl"),
        ("What is the total value of books by each author?", "SELECT a.auth_nm, SUM(b.unit_prc) as total_value, COUNT(b.bk_id) as book_count FROM SUPPLY_CHAIN.AUTH_REF a LEFT JOIN SUPPLY_CHAIN.BK_CATALOG b ON a.auth_cd = b.auth_cd WHERE b.unit_prc > 0 GROUP BY a.auth_cd, a.auth_nm ORDER BY total_value DESC"),
        ("What is the average rating of all books?", "SELECT AVG(rtg_val) as avg_rating FROM SUPPLY_CHAIN.BK_CATALOG WHERE rtg_val IS NOT NULL"),
        ("What books have a 5-star rating?", "SELECT bk_id, bk_ttl, rtg_val, unit_prc FROM SUPPLY_CHAIN.BK_CATALOG WHERE rtg_val = 5 ORDER BY bk_ttl"),
        ("What books have a 1-star rating?", "SELECT bk_id, bk_ttl, rtg_val, unit_prc FROM SUPPLY_CHAIN.BK_CATALOG WHERE rtg_val = 1 ORDER BY bk_ttl"),
        ("How many books have each rating?", "SELECT rtg_val, COUNT(*) as book_count FROM SUPPLY_CHAIN.BK_CATALOG GROUP BY rtg_val ORDER BY rtg_val DESC"),
        ("What is the average price by rating?", "SELECT rtg_val, AVG(unit_prc) as avg_price, COUNT(*) as book_count FROM SUPPLY_CHAIN.BK_CATALOG WHERE unit_prc > 0 GROUP BY rtg_val ORDER BY rtg_val DESC"),
        ("What are the highest rated books?", "SELECT bk_id, bk_ttl, rtg_val, unit_prc FROM SUPPLY_CHAIN.BK_CATALOG WHERE rtg_val = (SELECT MAX(rtg_val) FROM SUPPLY_CHAIN.BK_CATALOG) ORDER BY bk_ttl"),
        ("What books have ratings above 3?", "SELECT bk_id, bk_ttl, rtg_val, unit_prc FROM SUPPLY_CHAIN.BK_CATALOG WHERE rtg_val > 3 ORDER BY rtg_val DESC, bk_ttl"),
        ("What is the correlation between book price and rating?", "SELECT CORR(unit_prc, rtg_val) as price_rating_correlation FROM SUPPLY_CHAIN.BK_CATALOG WHERE unit_prc > 0 AND rtg_val IS NOT NULL"),
        ("What is the availability status of books?", "SELECT avail_sts, COUNT(*) as book_count FROM SUPPLY_CHAIN.BK_CATALOG GROUP BY avail_sts ORDER BY book_count DESC"),
        ("What books are in stock?", "SELECT bk_id, bk_ttl, avail_sts, unit_prc FROM SUPPLY_CHAIN.BK_CATALOG WHERE UPPER(avail_sts) LIKE '%IN STOCK%' OR UPPER(avail_sts) LIKE '%AVAILABLE%' ORDER BY bk_ttl"),
        ("What books are out of stock?", "SELECT bk_id, bk_ttl, avail_sts FROM SUPPLY_CHAIN.BK_CATALOG WHERE UPPER(avail_sts) LIKE '%OUT%' OR UPPER(avail_sts) LIKE '%UNAVAILABLE%' ORDER BY bk_ttl"),
        ("What is the average price by availability status?", "SELECT avail_sts, AVG(unit_prc) as avg_price, COUNT(*) as book_count FROM SUPPLY_CHAIN.BK_CATALOG WHERE unit_prc > 0 GROUP BY avail_sts ORDER BY avg_price DESC"),
        ("What are all the book categories?", "SELECT DISTINCT bk_cat_cd, bk_cat_nm FROM SUPPLY_CHAIN.BK_CAT_REF ORDER BY bk_cat_nm"),
        ("How many books are in each category?", "SELECT c.bk_cat_nm, COUNT(x.bk_id) as book_count FROM SUPPLY_CHAIN.BK_CAT_REF c LEFT JOIN SUPPLY_CHAIN.BK_CAT_XREF x ON c.bk_cat_cd = x.bk_cat_cd GROUP BY c.bk_cat_cd, c.bk_cat_nm ORDER BY book_count DESC"),
        ("What category has the most books?", "SELECT c.bk_cat_nm, COUNT(x.bk_id) as book_count FROM SUPPLY_CHAIN.BK_CAT_REF c LEFT JOIN SUPPLY_CHAIN.BK_CAT_XREF x ON c.bk_cat_cd = x.bk_cat_cd GROUP BY c.bk_cat_cd, c.bk_cat_nm ORDER BY book_count DESC LIMIT 1"),
        ("What is the average price by book category?", "SELECT c.bk_cat_nm, AVG(b.unit_prc) as avg_price, COUNT(x.bk_id) as book_count FROM SUPPLY_CHAIN.BK_CAT_REF c LEFT JOIN SUPPLY_CHAIN.BK_CAT_XREF x ON c.bk_cat_cd = x.bk_cat_cd LEFT JOIN SUPPLY_CHAIN.BK_CATALOG b ON x.bk_id = b.bk_id WHERE b.unit_prc > 0 GROUP BY c.bk_cat_cd, c.bk_cat_nm ORDER BY avg_price DESC"),
        ("What books are in multiple categories?", "SELECT b.bk_id, b.bk_ttl, COUNT(x.bk_cat_cd) as category_count FROM SUPPLY_CHAIN.BK_CATALOG b JOIN SUPPLY_CHAIN.BK_CAT_XREF x ON b.bk_id = x.bk_id GROUP BY b.bk_id, b.bk_ttl HAVING COUNT(x.bk_cat_cd) > 1 ORDER BY category_count DESC"),
        ("What books have the longest titles?", "SELECT bk_id, bk_ttl, LENGTH(bk_ttl) as title_length FROM SUPPLY_CHAIN.BK_CATALOG ORDER BY title_length DESC LIMIT 10"),
        ("What books were scraped most recently?", "SELECT bk_id, bk_ttl, scrp_dt FROM SUPPLY_CHAIN.BK_CATALOG ORDER BY scrp_dt DESC LIMIT 20"),
        ("What is the total value of all books?", "SELECT SUM(unit_prc) as total_value, COUNT(*) as total_books FROM SUPPLY_CHAIN.BK_CATALOG WHERE unit_prc > 0"),
        ("What books have no description?", "SELECT bk_id, bk_ttl, unit_prc FROM SUPPLY_CHAIN.BK_CATALOG WHERE desc_txt IS NULL OR LENGTH(TRIM(desc_txt)) = 0"),
        ("What is the median book price?", "SELECT MEDIAN(unit_prc) as median_price FROM SUPPLY_CHAIN.BK_CATALOG WHERE unit_prc > 0"),
        ("What books are priced between £10 and £20?", "SELECT bk_id, bk_ttl, unit_prc FROM SUPPLY_CHAIN.BK_CATALOG WHERE unit_prc BETWEEN 10 AND 20 ORDER BY unit_prc"),
        ("What is the standard deviation of book prices?", "SELECT STDDEV(unit_prc) as price_stddev FROM SUPPLY_CHAIN.BK_CATALOG WHERE unit_prc > 0"),
        ("What percentage of books have each rating?", "SELECT rtg_val, COUNT(*) as book_count, ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM SUPPLY_CHAIN.BK_CATALOG), 2) as percentage FROM SUPPLY_CHAIN.BK_CATALOG GROUP BY rtg_val ORDER BY rtg_val DESC"),
        ("What books have titles starting with 'The'?", "SELECT bk_id, bk_ttl, unit_prc FROM SUPPLY_CHAIN.BK_CATALOG WHERE UPPER(bk_ttl) LIKE 'THE%' ORDER BY bk_ttl"),
        ("What is the price rank of each book?", "SELECT bk_id, bk_ttl, unit_prc, RANK() OVER (ORDER BY unit_prc DESC) as price_rank FROM SUPPLY_CHAIN.BK_CATALOG WHERE unit_prc > 0 ORDER BY price_rank LIMIT 20"),
        ("What books have the best value (high rating, low price)?", "SELECT bk_id, bk_ttl, unit_prc, rtg_val, rtg_val / NULLIF(unit_prc, 0) as value_score FROM SUPPLY_CHAIN.BK_CATALOG WHERE unit_prc > 0 AND rtg_val IS NOT NULL ORDER BY value_score DESC LIMIT 10"),
        ("What is the average rating by author?", "SELECT a.auth_nm, AVG(b.rtg_val) as avg_rating, COUNT(b.bk_id) as book_count FROM SUPPLY_CHAIN.AUTH_REF a LEFT JOIN SUPPLY_CHAIN.BK_CATALOG b ON a.auth_cd = b.auth_cd WHERE b.rtg_val IS NOT NULL GROUP BY a.auth_cd, a.auth_nm ORDER BY avg_rating DESC"),
        ("What books have duplicate titles?", "SELECT bk_ttl, COUNT(*) as count FROM SUPPLY_CHAIN.BK_CATALOG GROUP BY bk_ttl HAVING COUNT(*) > 1 ORDER BY count DESC"),
        ("What is the distribution of book prices by quartiles?", "SELECT PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY unit_prc) as q1, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY unit_prc) as median, PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY unit_prc) as q3 FROM SUPPLY_CHAIN.BK_CATALOG WHERE unit_prc > 0"),
        ("What books were added each day?", "SELECT DATE(scrp_dt) as scrape_date, COUNT(*) as books_added FROM SUPPLY_CHAIN.BK_CATALOG GROUP BY DATE(scrp_dt) ORDER BY scrape_date DESC"),
        ("What is the cumulative count of books over time?", "SELECT DATE(scrp_dt) as date, COUNT(*) as daily_count, SUM(COUNT(*)) OVER (ORDER BY DATE(scrp_dt)) as cumulative_count FROM SUPPLY_CHAIN.BK_CATALOG GROUP BY DATE(scrp_dt) ORDER BY date"),
        ("What books have no author assigned?", "SELECT bk_id, bk_ttl, auth_cd FROM SUPPLY_CHAIN.BK_CATALOG WHERE auth_cd IS NULL OR auth_cd NOT IN (SELECT auth_cd FROM SUPPLY_CHAIN.AUTH_REF)"),
        ("What is the interquartile range of book prices?", "SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY unit_prc) - PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY unit_prc) as iqr FROM SUPPLY_CHAIN.BK_CATALOG WHERE unit_prc > 0"),
        ("What books have prices that are outliers?", "SELECT bk_id, bk_ttl, unit_prc FROM SUPPLY_CHAIN.BK_CATALOG WHERE unit_prc > (SELECT AVG(unit_prc) + 2 * STDDEV(unit_prc) FROM SUPPLY_CHAIN.BK_CATALOG WHERE unit_prc > 0) OR unit_prc < (SELECT AVG(unit_prc) - 2 * STDDEV(unit_prc) FROM SUPPLY_CHAIN.BK_CATALOG WHERE unit_prc > 0) ORDER BY unit_prc DESC"),
        ("What is the mode (most common) book price?", "SELECT unit_prc, COUNT(*) as frequency FROM SUPPLY_CHAIN.BK_CATALOG WHERE unit_prc > 0 GROUP BY unit_prc ORDER BY frequency DESC LIMIT 1"),
        ("What books have the highest price-to-rating ratio?", "SELECT bk_id, bk_ttl, unit_prc, rtg_val, unit_prc / NULLIF(rtg_val, 0) as price_per_rating FROM SUPPLY_CHAIN.BK_CATALOG WHERE unit_prc > 0 AND rtg_val > 0 ORDER BY price_per_rating DESC LIMIT 10"),
        ("What is the variance of book prices?", "SELECT VARIANCE(unit_prc) as price_variance FROM SUPPLY_CHAIN.BK_CATALOG WHERE unit_prc > 0"),
        ("What books have titles containing 'love'?", "SELECT bk_id, bk_ttl, unit_prc FROM SUPPLY_CHAIN.BK_CATALOG WHERE UPPER(bk_ttl) LIKE '%LOVE%' ORDER BY bk_ttl"),
        ("What is the running total of book prices when sorted by price?", "SELECT bk_id, bk_ttl, unit_prc, SUM(unit_prc) OVER (ORDER BY unit_prc) as running_total FROM SUPPLY_CHAIN.BK_CATALOG WHERE unit_prc > 0 ORDER BY unit_prc LIMIT 20"),
        ("What books have the same price?", "SELECT unit_prc, COUNT(*) as book_count, LISTAGG(bk_ttl, ', ') WITHIN GROUP (ORDER BY bk_ttl) as books FROM SUPPLY_CHAIN.BK_CATALOG WHERE unit_prc > 0 GROUP BY unit_prc HAVING COUNT(*) > 1 ORDER BY book_count DESC"),
        ("What is the coefficient of variation for book prices?", "SELECT STDDEV(unit_prc) / AVG(unit_prc) as coefficient_of_variation FROM SUPPLY_CHAIN.BK_CATALOG WHERE unit_prc > 0"),
    ]
    
    for idx, (question, query) in enumerate(book_queries, start=61):
        queries.append({
            "id": idx,
            "category": "Books",
            "question": question,
            "query": query
        })
    
    # ========== FILM/MEDIA QUESTIONS (121-180) ==========
    film_queries = [
        ("What are all the films in our catalog?", "SELECT media_id, media_ttl, yr_val, dir_cd, awd_cat_cd FROM SUPPLY_CHAIN.MEDIA_MAST ORDER BY media_ttl"),
        ("What is the total number of films?", "SELECT COUNT(*) as total_films FROM SUPPLY_CHAIN.MEDIA_MAST"),
        ("What films were released in 2010?", "SELECT media_id, media_ttl, yr_val FROM SUPPLY_CHAIN.MEDIA_MAST WHERE yr_val = 2010 ORDER BY media_ttl"),
        ("What films were released in 2015?", "SELECT media_id, media_ttl, yr_val FROM SUPPLY_CHAIN.MEDIA_MAST WHERE yr_val = 2015 ORDER BY media_ttl"),
        ("What is the range of years for films?", "SELECT MIN(yr_val) as earliest_year, MAX(yr_val) as latest_year, COUNT(*) as total_films FROM SUPPLY_CHAIN.MEDIA_MAST"),
        ("How many films were released each year?", "SELECT yr_val, COUNT(*) as film_count FROM SUPPLY_CHAIN.MEDIA_MAST GROUP BY yr_val ORDER BY yr_val"),
        ("What year has the most films?", "SELECT yr_val, COUNT(*) as film_count FROM SUPPLY_CHAIN.MEDIA_MAST GROUP BY yr_val ORDER BY film_count DESC LIMIT 1"),
        ("What are the most recent films?", "SELECT media_id, media_ttl, yr_val FROM SUPPLY_CHAIN.MEDIA_MAST ORDER BY yr_val DESC, media_ttl LIMIT 10"),
        ("What are the oldest films?", "SELECT media_id, media_ttl, yr_val FROM SUPPLY_CHAIN.MEDIA_MAST ORDER BY yr_val, media_ttl LIMIT 10"),
        ("What films were released between 2012 and 2014?", "SELECT media_id, media_ttl, yr_val FROM SUPPLY_CHAIN.MEDIA_MAST WHERE yr_val BETWEEN 2012 AND 2014 ORDER BY yr_val, media_ttl"),
        ("What are all the directors?", "SELECT DISTINCT dir_cd, dir_nm FROM SUPPLY_CHAIN.DIR_REF ORDER BY dir_nm"),
        ("How many films did each director make?", "SELECT d.dir_nm, COUNT(m.media_id) as film_count FROM SUPPLY_CHAIN.DIR_REF d LEFT JOIN SUPPLY_CHAIN.MEDIA_MAST m ON d.dir_cd = m.dir_cd GROUP BY d.dir_cd, d.dir_nm ORDER BY film_count DESC"),
        ("What director has the most films?", "SELECT d.dir_nm, COUNT(m.media_id) as film_count FROM SUPPLY_CHAIN.DIR_REF d LEFT JOIN SUPPLY_CHAIN.MEDIA_MAST m ON d.dir_cd = m.dir_cd GROUP BY d.dir_cd, d.dir_nm ORDER BY film_count DESC LIMIT 1"),
        ("What films are directed by a specific director?", "SELECT m.media_id, m.media_ttl, m.yr_val, d.dir_nm FROM SUPPLY_CHAIN.MEDIA_MAST m JOIN SUPPLY_CHAIN.DIR_REF d ON m.dir_cd = d.dir_cd WHERE d.dir_nm = (SELECT dir_nm FROM SUPPLY_CHAIN.DIR_REF LIMIT 1) ORDER BY m.media_ttl"),
        ("What directors have only one film?", "SELECT d.dir_nm, COUNT(m.media_id) as film_count FROM SUPPLY_CHAIN.DIR_REF d LEFT JOIN SUPPLY_CHAIN.MEDIA_MAST m ON d.dir_cd = m.dir_cd GROUP BY d.dir_cd, d.dir_nm HAVING COUNT(m.media_id) = 1 ORDER BY d.dir_nm"),
        ("What are all the award categories?", "SELECT DISTINCT awd_cat_cd, awd_nm, awd_typ FROM SUPPLY_CHAIN.AWARD_REF ORDER BY awd_nm"),
        ("How many films won each award?", "SELECT a.awd_nm, COUNT(x.media_id) as film_count FROM SUPPLY_CHAIN.AWARD_REF a LEFT JOIN SUPPLY_CHAIN.MEDIA_AWD_XREF x ON a.awd_cat_cd = x.awd_cat_cd GROUP BY a.awd_cat_cd, a.awd_nm ORDER BY film_count DESC"),
        ("What award category has the most winners?", "SELECT a.awd_nm, COUNT(x.media_id) as film_count FROM SUPPLY_CHAIN.AWARD_REF a LEFT JOIN SUPPLY_CHAIN.MEDIA_AWD_XREF x ON a.awd_cat_cd = x.awd_cat_cd GROUP BY a.awd_cat_cd, a.awd_nm ORDER BY film_count DESC LIMIT 1"),
        ("What films won a specific award?", "SELECT m.media_id, m.media_ttl, m.yr_val, a.awd_nm, x.awd_yr FROM SUPPLY_CHAIN.MEDIA_MAST m JOIN SUPPLY_CHAIN.MEDIA_AWD_XREF x ON m.media_id = x.media_id JOIN SUPPLY_CHAIN.AWARD_REF a ON x.awd_cat_cd = a.awd_cat_cd WHERE a.awd_nm = (SELECT awd_nm FROM SUPPLY_CHAIN.AWARD_REF LIMIT 1) ORDER BY m.media_ttl"),
        ("What films won awards in 2010?", "SELECT DISTINCT m.media_id, m.media_ttl, m.yr_val FROM SUPPLY_CHAIN.MEDIA_MAST m JOIN SUPPLY_CHAIN.MEDIA_AWD_XREF x ON m.media_id = x.media_id WHERE x.awd_yr = 2010 ORDER BY m.media_ttl"),
        ("What awards were given each year?", "SELECT x.awd_yr, COUNT(DISTINCT x.media_id) as films_with_awards, COUNT(*) as total_awards FROM SUPPLY_CHAIN.MEDIA_AWD_XREF x GROUP BY x.awd_yr ORDER BY x.awd_yr"),
        ("What films won multiple awards?", "SELECT m.media_id, m.media_ttl, COUNT(x.awd_cat_cd) as award_count FROM SUPPLY_CHAIN.MEDIA_MAST m JOIN SUPPLY_CHAIN.MEDIA_AWD_XREF x ON m.media_id = x.media_id GROUP BY m.media_id, m.media_ttl HAVING COUNT(x.awd_cat_cd) > 1 ORDER BY award_count DESC"),
        ("What are all the actors/performers?", "SELECT DISTINCT perf_cd, perf_nm FROM SUPPLY_CHAIN.PERF_REF ORDER BY perf_nm"),
        ("How many films did each actor appear in?", "SELECT p.perf_nm, COUNT(DISTINCT x.media_id) as film_count FROM SUPPLY_CHAIN.PERF_REF p LEFT JOIN SUPPLY_CHAIN.MEDIA_PERF_XREF x ON p.perf_cd = x.perf_cd GROUP BY p.perf_cd, p.perf_nm ORDER BY film_count DESC"),
        ("What actor appeared in the most films?", "SELECT p.perf_nm, COUNT(DISTINCT x.media_id) as film_count FROM SUPPLY_CHAIN.PERF_REF p LEFT JOIN SUPPLY_CHAIN.MEDIA_PERF_XREF x ON p.perf_cd = x.perf_cd GROUP BY p.perf_cd, p.perf_nm ORDER BY film_count DESC LIMIT 1"),
        ("What films feature a specific actor?", "SELECT m.media_id, m.media_ttl, m.yr_val, p.perf_nm, x.role_nm FROM SUPPLY_CHAIN.MEDIA_MAST m JOIN SUPPLY_CHAIN.MEDIA_PERF_XREF x ON m.media_id = x.media_id JOIN SUPPLY_CHAIN.PERF_REF p ON x.perf_cd = p.perf_cd WHERE p.perf_nm = (SELECT perf_nm FROM SUPPLY_CHAIN.PERF_REF LIMIT 1) ORDER BY m.media_ttl"),
        ("What films have the most actors?", "SELECT m.media_id, m.media_ttl, COUNT(x.perf_cd) as actor_count FROM SUPPLY_CHAIN.MEDIA_MAST m LEFT JOIN SUPPLY_CHAIN.MEDIA_PERF_XREF x ON m.media_id = x.media_id GROUP BY m.media_id, m.media_ttl ORDER BY actor_count DESC LIMIT 10"),
        ("What films have the longest titles?", "SELECT media_id, media_ttl, LENGTH(media_ttl) as title_length FROM SUPPLY_CHAIN.MEDIA_MAST ORDER BY title_length DESC LIMIT 10"),
        ("What films were scraped most recently?", "SELECT media_id, media_ttl, scrp_dt FROM SUPPLY_CHAIN.MEDIA_MAST ORDER BY scrp_dt DESC LIMIT 20"),
        ("What films have no director assigned?", "SELECT media_id, media_ttl, dir_cd FROM SUPPLY_CHAIN.MEDIA_MAST WHERE dir_cd IS NULL OR dir_cd NOT IN (SELECT dir_cd FROM SUPPLY_CHAIN.DIR_REF)"),
        ("What films have duplicate titles?", "SELECT media_ttl, COUNT(*) as count FROM SUPPLY_CHAIN.MEDIA_MAST GROUP BY media_ttl HAVING COUNT(*) > 1 ORDER BY count DESC"),
        ("What films have titles starting with 'The'?", "SELECT media_id, media_ttl, yr_val FROM SUPPLY_CHAIN.MEDIA_MAST WHERE UPPER(media_ttl) LIKE 'THE%' ORDER BY media_ttl"),
        ("What percentage of films were released each year?", "SELECT yr_val, COUNT(*) as film_count, ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM SUPPLY_CHAIN.MEDIA_MAST), 2) as percentage FROM SUPPLY_CHAIN.MEDIA_MAST GROUP BY yr_val ORDER BY yr_val"),
        ("What is the average number of awards per film?", "SELECT AVG(award_count) as avg_awards_per_film FROM (SELECT m.media_id, COUNT(x.awd_cat_cd) as award_count FROM SUPPLY_CHAIN.MEDIA_MAST m LEFT JOIN SUPPLY_CHAIN.MEDIA_AWD_XREF x ON m.media_id = x.media_id GROUP BY m.media_id)"),
        ("What is the average number of actors per film?", "SELECT AVG(actor_count) as avg_actors_per_film FROM (SELECT m.media_id, COUNT(x.perf_cd) as actor_count FROM SUPPLY_CHAIN.MEDIA_MAST m LEFT JOIN SUPPLY_CHAIN.MEDIA_PERF_XREF x ON m.media_id = x.media_id GROUP BY m.media_id)"),
        ("What films have no awards?", "SELECT m.media_id, m.media_ttl, m.yr_val FROM SUPPLY_CHAIN.MEDIA_MAST m LEFT JOIN SUPPLY_CHAIN.MEDIA_AWD_XREF x ON m.media_id = x.media_id WHERE x.media_id IS NULL"),
        ("What films have no actors listed?", "SELECT m.media_id, m.media_ttl, m.yr_val FROM SUPPLY_CHAIN.MEDIA_MAST m LEFT JOIN SUPPLY_CHAIN.MEDIA_PERF_XREF x ON m.media_id = x.media_id WHERE x.media_id IS NULL"),
        ("What is the distribution of films by year?", "SELECT yr_val, COUNT(*) as film_count, ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage FROM SUPPLY_CHAIN.MEDIA_MAST GROUP BY yr_val ORDER BY yr_val"),
        ("What films were added each day?", "SELECT DATE(scrp_dt) as scrape_date, COUNT(*) as films_added FROM SUPPLY_CHAIN.MEDIA_MAST GROUP BY DATE(scrp_dt) ORDER BY scrape_date DESC"),
        ("What is the cumulative count of films over time?", "SELECT DATE(scrp_dt) as date, COUNT(*) as daily_count, SUM(COUNT(*)) OVER (ORDER BY DATE(scrp_dt)) as cumulative_count FROM SUPPLY_CHAIN.MEDIA_MAST GROUP BY DATE(scrp_dt) ORDER BY date"),
        ("What films have titles containing 'love'?", "SELECT media_id, media_ttl, yr_val FROM SUPPLY_CHAIN.MEDIA_MAST WHERE UPPER(media_ttl) LIKE '%LOVE%' ORDER BY media_ttl"),
        ("What is the year-over-year growth in number of films?", "SELECT yr_val, COUNT(*) as film_count, LAG(COUNT(*)) OVER (ORDER BY yr_val) as prev_year_count FROM SUPPLY_CHAIN.MEDIA_MAST GROUP BY yr_val ORDER BY yr_val"),
        ("What is the median year of film releases?", "SELECT MEDIAN(yr_val) as median_year FROM SUPPLY_CHAIN.MEDIA_MAST"),
        ("What is the standard deviation of release years?", "SELECT STDDEV(yr_val) as year_stddev FROM SUPPLY_CHAIN.MEDIA_MAST"),
        ("What is the range of award years?", "SELECT MIN(awd_yr) as earliest_award_year, MAX(awd_yr) as latest_award_year, COUNT(*) as total_awards FROM SUPPLY_CHAIN.MEDIA_AWD_XREF"),
        ("What films have titles with more than 20 characters?", "SELECT media_id, media_ttl, LENGTH(media_ttl) as title_length FROM SUPPLY_CHAIN.MEDIA_MAST WHERE LENGTH(media_ttl) > 20 ORDER BY title_length DESC"),
        ("What is the mode (most common) release year?", "SELECT yr_val, COUNT(*) as frequency FROM SUPPLY_CHAIN.MEDIA_MAST GROUP BY yr_val ORDER BY frequency DESC LIMIT 1"),
        ("What is the variance of release years?", "SELECT VARIANCE(yr_val) as year_variance FROM SUPPLY_CHAIN.MEDIA_MAST"),
        ("What films were released in leap years?", "SELECT media_id, media_ttl, yr_val FROM SUPPLY_CHAIN.MEDIA_MAST WHERE MOD(yr_val, 4) = 0 AND (MOD(yr_val, 100) != 0 OR MOD(yr_val, 400) = 0) ORDER BY yr_val"),
        ("What is the interquartile range of release years?", "SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY yr_val) - PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY yr_val) as iqr FROM SUPPLY_CHAIN.MEDIA_MAST"),
        ("What is the coefficient of variation for release years?", "SELECT STDDEV(yr_val) / AVG(yr_val) as coefficient_of_variation FROM SUPPLY_CHAIN.MEDIA_MAST"),
        ("What films have the highest number of awards and actors combined?", "SELECT m.media_id, m.media_ttl, COUNT(DISTINCT x1.awd_cat_cd) + COUNT(DISTINCT x2.perf_cd) as total_count FROM SUPPLY_CHAIN.MEDIA_MAST m LEFT JOIN SUPPLY_CHAIN.MEDIA_AWD_XREF x1 ON m.media_id = x1.media_id LEFT JOIN SUPPLY_CHAIN.MEDIA_PERF_XREF x2 ON m.media_id = x2.media_id GROUP BY m.media_id, m.media_ttl ORDER BY total_count DESC LIMIT 10"),
        ("What actors appeared together in films?", "SELECT x1.perf_cd as actor1, x2.perf_cd as actor2, COUNT(DISTINCT x1.media_id) as films_together FROM SUPPLY_CHAIN.MEDIA_PERF_XREF x1 JOIN SUPPLY_CHAIN.MEDIA_PERF_XREF x2 ON x1.media_id = x2.media_id AND x1.perf_cd < x2.perf_cd GROUP BY x1.perf_cd, x2.perf_cd HAVING COUNT(DISTINCT x1.media_id) > 0 ORDER BY films_together DESC LIMIT 10"),
        ("What directors also acted in films?", "SELECT DISTINCT d.dir_nm, COUNT(DISTINCT m.media_id) as films_directed FROM SUPPLY_CHAIN.DIR_REF d JOIN SUPPLY_CHAIN.MEDIA_MAST m ON d.dir_cd = m.dir_cd WHERE d.dir_cd IN (SELECT DISTINCT perf_cd FROM SUPPLY_CHAIN.MEDIA_PERF_XREF) GROUP BY d.dir_cd, d.dir_nm"),
        ("What films won awards in consecutive years?", "SELECT m1.media_ttl as film1, x1.awd_yr as year1, m2.media_ttl as film2, x2.awd_yr as year2 FROM SUPPLY_CHAIN.MEDIA_MAST m1 JOIN SUPPLY_CHAIN.MEDIA_AWD_XREF x1 ON m1.media_id = x1.media_id JOIN SUPPLY_CHAIN.MEDIA_MAST m2 ON m2.media_id != m1.media_id JOIN SUPPLY_CHAIN.MEDIA_AWD_XREF x2 ON m2.media_id = x2.media_id WHERE x2.awd_yr = x1.awd_yr + 1 LIMIT 10"),
        ("What is the most common award type?", "SELECT a.awd_typ, COUNT(*) as count FROM SUPPLY_CHAIN.AWARD_REF a JOIN SUPPLY_CHAIN.MEDIA_AWD_XREF x ON a.awd_cat_cd = x.awd_cat_cd WHERE a.awd_typ IS NOT NULL GROUP BY a.awd_typ ORDER BY count DESC LIMIT 1"),
        ("What films have the same director and year?", "SELECT dir_cd, yr_val, COUNT(*) as film_count, LISTAGG(media_ttl, ', ') WITHIN GROUP (ORDER BY media_ttl) as films FROM SUPPLY_CHAIN.MEDIA_MAST GROUP BY dir_cd, yr_val HAVING COUNT(*) > 1"),
        ("What films have roles specified for actors?", "SELECT m.media_ttl, p.perf_nm, x.role_nm FROM SUPPLY_CHAIN.MEDIA_MAST m JOIN SUPPLY_CHAIN.MEDIA_PERF_XREF x ON m.media_id = x.media_id JOIN SUPPLY_CHAIN.PERF_REF p ON x.perf_cd = p.perf_cd WHERE x.role_nm IS NOT NULL ORDER BY m.media_ttl, p.perf_nm"),
        ("What films won awards in the same year they were released?", "SELECT m.media_id, m.media_ttl, m.yr_val, x.awd_yr FROM SUPPLY_CHAIN.MEDIA_MAST m JOIN SUPPLY_CHAIN.MEDIA_AWD_XREF x ON m.media_id = x.media_id WHERE m.yr_val = x.awd_yr ORDER BY m.media_ttl"),
        ("What films have the same title but different years?", "SELECT media_ttl, COUNT(DISTINCT yr_val) as year_count, LISTAGG(yr_val, ', ') WITHIN GROUP (ORDER BY yr_val) as years FROM SUPPLY_CHAIN.MEDIA_MAST GROUP BY media_ttl HAVING COUNT(DISTINCT yr_val) > 1"),
    ]
    
    for idx, (question, query) in enumerate(film_queries, start=121):
        queries.append({
            "id": idx,
            "category": "Films",
            "question": question,
            "query": query
        })
    
    # ========== CROSS-DOMAIN QUESTIONS (181-200) ==========
    cross_domain_queries = [
        ("What is the total count of all items (products, books, films)?", "SELECT 'Products' as type, COUNT(*) as count FROM SUPPLY_CHAIN.INV_MAST UNION ALL SELECT 'Books', COUNT(*) FROM SUPPLY_CHAIN.BK_CATALOG UNION ALL SELECT 'Films', COUNT(*) FROM SUPPLY_CHAIN.MEDIA_MAST"),
        ("What is the average price of products vs books?", "SELECT 'Products' as type, AVG(unit_prc) as avg_price FROM SUPPLY_CHAIN.INV_MAST WHERE unit_prc > 0 UNION ALL SELECT 'Books', AVG(unit_prc) FROM SUPPLY_CHAIN.BK_CATALOG WHERE unit_prc > 0"),
        ("What items were scraped on the same day across all sources?", "SELECT DATE(scrp_dt) as scrape_date, 'Products' as type, COUNT(*) as count FROM SUPPLY_CHAIN.INV_MAST GROUP BY DATE(scrp_dt) UNION ALL SELECT DATE(scrp_dt), 'Books', COUNT(*) FROM SUPPLY_CHAIN.BK_CATALOG GROUP BY DATE(scrp_dt) UNION ALL SELECT DATE(scrp_dt), 'Films', COUNT(*) FROM SUPPLY_CHAIN.MEDIA_MAST GROUP BY DATE(scrp_dt) ORDER BY scrape_date DESC, type"),
        ("What is the total value of all inventory (products and books)?", "SELECT SUM(unit_prc) as total_value FROM (SELECT unit_prc FROM SUPPLY_CHAIN.INV_MAST WHERE unit_prc > 0 UNION ALL SELECT unit_prc FROM SUPPLY_CHAIN.BK_CATALOG WHERE unit_prc > 0)"),
        ("What is the price distribution comparison between products and books?", "SELECT 'Products' as type, CASE WHEN unit_prc < 10 THEN 'Under $10' WHEN unit_prc < 25 THEN '$10-$25' WHEN unit_prc < 50 THEN '$25-$50' WHEN unit_prc < 100 THEN '$50-$100' ELSE 'Over $100' END as price_range, COUNT(*) as count FROM SUPPLY_CHAIN.INV_MAST WHERE unit_prc > 0 GROUP BY type, price_range UNION ALL SELECT 'Books', CASE WHEN unit_prc < 5 THEN 'Under £5' WHEN unit_prc < 10 THEN '£5-£10' WHEN unit_prc < 20 THEN '£10-£20' WHEN unit_prc < 50 THEN '£20-£50' ELSE 'Over £50' END, COUNT(*) FROM SUPPLY_CHAIN.BK_CATALOG WHERE unit_prc > 0 GROUP BY type, price_range ORDER BY type, price_range"),
        ("What is the growth rate of items added over time across all sources?", "SELECT DATE(scrp_dt) as date, COUNT(*) as items_added, 'Products' as type FROM SUPPLY_CHAIN.INV_MAST GROUP BY DATE(scrp_dt) UNION ALL SELECT DATE(scrp_dt), COUNT(*), 'Books' FROM SUPPLY_CHAIN.BK_CATALOG GROUP BY DATE(scrp_dt) UNION ALL SELECT DATE(scrp_dt), COUNT(*), 'Films' FROM SUPPLY_CHAIN.MEDIA_MAST GROUP BY DATE(scrp_dt) ORDER BY date DESC, type"),
        ("What items have the longest names across all types?", "SELECT 'Product' as type, inv_id as id, inv_nm as name, LENGTH(inv_nm) as name_length FROM SUPPLY_CHAIN.INV_MAST UNION ALL SELECT 'Book', bk_id, bk_ttl, LENGTH(bk_ttl) FROM SUPPLY_CHAIN.BK_CATALOG UNION ALL SELECT 'Film', media_id, media_ttl, LENGTH(media_ttl) FROM SUPPLY_CHAIN.MEDIA_MAST ORDER BY name_length DESC LIMIT 20"),
        ("What is the median price comparison between products and books?", "SELECT 'Products' as type, MEDIAN(unit_prc) as median_price FROM SUPPLY_CHAIN.INV_MAST WHERE unit_prc > 0 UNION ALL SELECT 'Books', MEDIAN(unit_prc) FROM SUPPLY_CHAIN.BK_CATALOG WHERE unit_prc > 0"),
        ("What items were added most recently across all sources?", "SELECT 'Product' as type, inv_id as id, inv_nm as name, scrp_dt FROM SUPPLY_CHAIN.INV_MAST UNION ALL SELECT 'Book', bk_id, bk_ttl, scrp_dt FROM SUPPLY_CHAIN.BK_CATALOG UNION ALL SELECT 'Film', media_id, media_ttl, scrp_dt FROM SUPPLY_CHAIN.MEDIA_MAST ORDER BY scrp_dt DESC LIMIT 30"),
        ("What is the standard deviation comparison of prices between products and books?", "SELECT 'Products' as type, STDDEV(unit_prc) as price_stddev FROM SUPPLY_CHAIN.INV_MAST WHERE unit_prc > 0 UNION ALL SELECT 'Books', STDDEV(unit_prc) FROM SUPPLY_CHAIN.BK_CATALOG WHERE unit_prc > 0"),
        ("What is the price range comparison between products and books?", "SELECT 'Products' as type, MIN(unit_prc) as min_price, MAX(unit_prc) as max_price, MAX(unit_prc) - MIN(unit_prc) as price_range FROM SUPPLY_CHAIN.INV_MAST WHERE unit_prc > 0 UNION ALL SELECT 'Books', MIN(unit_prc), MAX(unit_prc), MAX(unit_prc) - MIN(unit_prc) FROM SUPPLY_CHAIN.BK_CATALOG WHERE unit_prc > 0"),
        ("What items have duplicate names across different types?", "SELECT inv_nm as name, 'Product' as type, COUNT(*) as count FROM SUPPLY_CHAIN.INV_MAST GROUP BY inv_nm HAVING COUNT(*) > 1 UNION ALL SELECT bk_ttl, 'Book', COUNT(*) FROM SUPPLY_CHAIN.BK_CATALOG GROUP BY bk_ttl HAVING COUNT(*) > 1 UNION ALL SELECT media_ttl, 'Film', COUNT(*) FROM SUPPLY_CHAIN.MEDIA_MAST GROUP BY media_ttl HAVING COUNT(*) > 1 ORDER BY count DESC"),
        ("What is the total count of items added each month?", "SELECT TRUNC(scrp_dt, 'MONTH') as month, 'Products' as type, COUNT(*) as count FROM SUPPLY_CHAIN.INV_MAST GROUP BY TRUNC(scrp_dt, 'MONTH') UNION ALL SELECT TRUNC(scrp_dt, 'MONTH'), 'Books', COUNT(*) FROM SUPPLY_CHAIN.BK_CATALOG GROUP BY TRUNC(scrp_dt, 'MONTH') UNION ALL SELECT TRUNC(scrp_dt, 'MONTH'), 'Films', COUNT(*) FROM SUPPLY_CHAIN.MEDIA_MAST GROUP BY TRUNC(scrp_dt, 'MONTH') ORDER BY month DESC, type"),
        ("What is the coefficient of variation comparison between products and books?", "SELECT 'Products' as type, STDDEV(unit_prc) / AVG(unit_prc) as coefficient_of_variation FROM SUPPLY_CHAIN.INV_MAST WHERE unit_prc > 0 UNION ALL SELECT 'Books', STDDEV(unit_prc) / AVG(unit_prc) FROM SUPPLY_CHAIN.BK_CATALOG WHERE unit_prc > 0"),
        ("What items have names starting with the same letter across types?", "SELECT UPPER(SUBSTRING(inv_nm, 1, 1)) as first_letter, 'Product' as type, COUNT(*) as count FROM SUPPLY_CHAIN.INV_MAST GROUP BY UPPER(SUBSTRING(inv_nm, 1, 1)), type UNION ALL SELECT UPPER(SUBSTRING(bk_ttl, 1, 1)), 'Book', COUNT(*) FROM SUPPLY_CHAIN.BK_CATALOG GROUP BY UPPER(SUBSTRING(bk_ttl, 1, 1)), type UNION ALL SELECT UPPER(SUBSTRING(media_ttl, 1, 1)), 'Film', COUNT(*) FROM SUPPLY_CHAIN.MEDIA_MAST GROUP BY UPPER(SUBSTRING(media_ttl, 1, 1)), type ORDER BY first_letter, type"),
        ("What is the interquartile range comparison between products and books?", "SELECT 'Products' as type, PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY unit_prc) - PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY unit_prc) as iqr FROM SUPPLY_CHAIN.INV_MAST WHERE unit_prc > 0 UNION ALL SELECT 'Books', PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY unit_prc) - PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY unit_prc) FROM SUPPLY_CHAIN.BK_CATALOG WHERE unit_prc > 0"),
        ("What is the variance comparison of prices between products and books?", "SELECT 'Products' as type, VARIANCE(unit_prc) as price_variance FROM SUPPLY_CHAIN.INV_MAST WHERE unit_prc > 0 UNION ALL SELECT 'Books', VARIANCE(unit_prc) FROM SUPPLY_CHAIN.BK_CATALOG WHERE unit_prc > 0"),
        ("What items have the shortest names across all types?", "SELECT 'Product' as type, inv_id as id, inv_nm as name, LENGTH(inv_nm) as name_length FROM SUPPLY_CHAIN.INV_MAST UNION ALL SELECT 'Book', bk_id, bk_ttl, LENGTH(bk_ttl) FROM SUPPLY_CHAIN.BK_CATALOG UNION ALL SELECT 'Film', media_id, media_ttl, LENGTH(media_ttl) FROM SUPPLY_CHAIN.MEDIA_MAST ORDER BY name_length LIMIT 20"),
        ("What is the cumulative count of all items over time?", "SELECT DATE(scrp_dt) as date, COUNT(*) as daily_count, SUM(COUNT(*)) OVER (ORDER BY DATE(scrp_dt)) as cumulative_count, 'Products' as type FROM SUPPLY_CHAIN.INV_MAST GROUP BY DATE(scrp_dt) UNION ALL SELECT DATE(scrp_dt), COUNT(*), SUM(COUNT(*)) OVER (ORDER BY DATE(scrp_dt)), 'Books' FROM SUPPLY_CHAIN.BK_CATALOG GROUP BY DATE(scrp_dt) UNION ALL SELECT DATE(scrp_dt), COUNT(*), SUM(COUNT(*)) OVER (ORDER BY DATE(scrp_dt)), 'Films' FROM SUPPLY_CHAIN.MEDIA_MAST GROUP BY DATE(scrp_dt) ORDER BY date, type"),
        ("What is the overall statistics summary for all item types?", "SELECT 'Products' as type, COUNT(*) as total_count, AVG(unit_prc) as avg_price, MIN(unit_prc) as min_price, MAX(unit_prc) as max_price FROM SUPPLY_CHAIN.INV_MAST WHERE unit_prc > 0 UNION ALL SELECT 'Books', COUNT(*), AVG(unit_prc), MIN(unit_prc), MAX(unit_prc) FROM SUPPLY_CHAIN.BK_CATALOG WHERE unit_prc > 0 UNION ALL SELECT 'Films', COUNT(*), NULL, NULL, NULL FROM SUPPLY_CHAIN.MEDIA_MAST"),
    ]
    
    for idx, (question, query) in enumerate(cross_domain_queries, start=181):
        queries.append({
            "id": idx,
            "category": "Cross-Domain",
            "question": question,
            "query": query
        })
    
    return queries


def test_query(db_manager, query_text, question_id):
    """Test a query and return True if it returns at least one row"""
    try:
        db_manager.cursor.execute(query_text)
        results = db_manager.cursor.fetchall()
        if len(results) > 0:
            return True, len(results)
        else:
            return False, 0
    except Exception as e:
        logger.warning(f"Query {question_id} failed: {str(e)}")
        return False, 0


def main():
    """Main function to generate and test queries"""
    logger.info("Connecting to Snowflake...")
    db_manager = SnowflakeManager()
    db_manager.connect()
    
    logger.info("Generating queries...")
    queries = generate_queries()
    
    logger.info(f"Generated {len(queries)} queries. Testing each query...")
    
    tested_queries = []
    failed_queries = []
    
    for query_obj in queries:
        success, row_count = test_query(db_manager, query_obj["query"], query_obj["id"])
        if success:
            query_obj["row_count"] = row_count
            query_obj["status"] = "success"
            tested_queries.append(query_obj)
            logger.info(f"Query {query_obj['id']}: ✓ ({row_count} rows)")
        else:
            query_obj["status"] = "failed"
            query_obj["row_count"] = 0
            failed_queries.append(query_obj)
            logger.warning(f"Query {query_obj['id']}: ✗ Failed")
    
    logger.info(f"\nSummary: {len(tested_queries)} successful, {len(failed_queries)} failed")
    
    # Save to JSON
    output_file = "business_queries.json"
    with open(output_file, 'w') as f:
        json.dump({
            "total_queries": len(queries),
            "successful_queries": len(tested_queries),
            "failed_queries": len(failed_queries),
            "queries": tested_queries
        }, f, indent=2)
    
    logger.info(f"Saved {len(tested_queries)} successful queries to {output_file}")
    
    db_manager.disconnect()


if __name__ == "__main__":
    main()
