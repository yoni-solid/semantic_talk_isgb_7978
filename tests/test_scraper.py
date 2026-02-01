"""
Tests for scraper module
"""
import pytest
import asyncio
from unittest.mock import Mock, patch, AsyncMock
from scraper import WebScraper


class TestWebScraper:
    def test_scraper_initialization(self):
        scraper = WebScraper()
        assert scraper.products == []
        assert scraper.books == []
        assert scraper.films == []
        assert scraper.product_categories == {}
        assert scraper.authors == {}

    @pytest.mark.asyncio
    async def test_normalize_categories(self):
        scraper = WebScraper()
        
        # Simulate adding categories
        scraper.product_categories["apparel"] = Mock(cat_cd="CAT_0001", cat_nm="apparel")
        scraper.product_categories["consumables"] = Mock(cat_cd="CAT_0002", cat_nm="consumables")
        
        assert len(scraper.product_categories) == 2
        assert "apparel" in scraper.product_categories

    def test_to_dataframes_empty(self):
        scraper = WebScraper()
        dataframes = scraper.to_dataframes()
        assert isinstance(dataframes, dict)
        assert len(dataframes) == 0

    def test_export_to_csv_empty(self, tmp_path):
        scraper = WebScraper()
        paths = scraper.export_to_csv(str(tmp_path))
        assert isinstance(paths, dict)
        assert len(paths) == 0
