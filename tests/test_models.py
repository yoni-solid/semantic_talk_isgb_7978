"""
Tests for Pydantic models
"""
import pytest
from datetime import datetime
from models import (
    Product, ProductCategory, ProductVariant, ProductReview, SimilarProduct,
    Book, Author, BookCategory, BookCategoryBridge,
    Film, Director, Actor, AwardCategory, FilmActorBridge, FilmAwardBridge
)


class TestProductCategory:
    def test_valid_product_category(self):
        cat = ProductCategory(cat_cd="CAT_0001", cat_nm="apparel")
        assert cat.cat_cd == "CAT_0001"
        assert cat.cat_nm == "apparel"

    def test_product_category_required_fields(self):
        with pytest.raises(Exception):
            ProductCategory(cat_cd="CAT_0001")  # Missing cat_nm


class TestProduct:
    def test_valid_product(self):
        product = Product(
            inv_id="INV_001",
            inv_nm="Test Product",
            unit_prc=10.99,
            cat_cd="CAT_0001",
            desc_txt="Test description"
        )
        assert product.inv_id == "INV_001"
        assert product.inv_nm == "Test Product"
        assert product.unit_prc == 10.99
        assert product.cat_cd == "CAT_0001"

    def test_product_optional_fields(self):
        product = Product(
            inv_id="INV_001",
            inv_nm="Test",
            unit_prc=10.0,
            cat_cd="CAT_0001",
            desc_txt="Test",
            lnk_id="LINK_123"
        )
        assert product.lnk_id == "LINK_123"


class TestBook:
    def test_valid_book(self):
        book = Book(
            bk_id="BK_001",
            bk_ttl="Test Book",
            unit_prc=15.50,
            auth_cd="AUTH_0001",
            avail_sts="In stock",
            rtg_val=4,
            desc_txt="Test description"
        )
        assert book.bk_id == "BK_001"
        assert book.rtg_val == 4

    def test_book_rating_validation(self):
        with pytest.raises(Exception):
            Book(
                bk_id="BK_001",
                bk_ttl="Test",
                unit_prc=10.0,
                auth_cd="AUTH_0001",
                avail_sts="In stock",
                rtg_val=6,  # Invalid: must be 1-5
                desc_txt="Test"
            )


class TestFilm:
    def test_valid_film(self):
        film = Film(
            media_id="MEDIA_001",
            media_ttl="Test Film",
            yr_val=2020,
            awd_cat_cd="AWD_0001",
            dir_cd="DIR_0001"
        )
        assert film.media_id == "MEDIA_001"
        assert film.yr_val == 2020


class TestProductVariant:
    def test_valid_variant(self):
        variant = ProductVariant(
            var_id="VAR_001",
            inv_id="INV_001",
            sz_cd="small",
            flv_cd="orange",
            prc_mod=0.0
        )
        assert variant.var_id == "VAR_001"
        assert variant.sz_cd == "small"


class TestProductReview:
    def test_valid_review(self):
        review = ProductReview(
            rev_id="REV_001",
            inv_id="INV_001",
            rtg_val=5,
            rev_txt="Great product!"
        )
        assert review.rtg_val == 5
        assert review.rev_txt == "Great product!"


class TestBookCategoryBridge:
    def test_valid_bridge(self):
        bridge = BookCategoryBridge(
            bk_id="BK_001",
            bk_cat_cd="BK_CAT_0001"
        )
        assert bridge.bk_id == "BK_001"
        assert bridge.bk_cat_cd == "BK_CAT_0001"
