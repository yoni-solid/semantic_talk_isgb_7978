"""
Pydantic models for data validation and normalization.
Maps to non-obvious table names to mimic real-world messy environments.
"""
from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field


# ============ Dimension Models ============

class ProductCategory(BaseModel):
    """Maps to CAT_REF table"""
    cat_cd: str = Field(..., description="Category code")
    cat_nm: str = Field(..., description="Category name")


class BookCategory(BaseModel):
    """Maps to BK_CAT_REF table"""
    bk_cat_cd: str = Field(..., description="Book category code")
    bk_cat_nm: str = Field(..., description="Book category name")


class Author(BaseModel):
    """Maps to AUTH_REF table"""
    auth_cd: str = Field(..., description="Author code")
    auth_nm: str = Field(..., description="Author name")


class Director(BaseModel):
    """Maps to DIR_REF table"""
    dir_cd: str = Field(..., description="Director code")
    dir_nm: str = Field(..., description="Director name")


class Actor(BaseModel):
    """Maps to PERF_REF table"""
    perf_cd: str = Field(..., description="Performer/actor code")
    perf_nm: str = Field(..., description="Performer/actor name")


class AwardCategory(BaseModel):
    """Maps to AWARD_REF table"""
    awd_cat_cd: str = Field(..., description="Award category code")
    awd_nm: str = Field(..., description="Award name")
    awd_typ: Optional[str] = Field(None, description="Award type")


# ============ Fact Models ============

class Product(BaseModel):
    """Maps to INV_MAST table"""
    inv_id: str = Field(..., description="Inventory ID")
    inv_nm: str = Field(..., description="Inventory name")
    unit_prc: float = Field(..., description="Unit price")
    cat_cd: str = Field(..., description="Category code (references CAT_REF, no FK constraint)")
    desc_txt: str = Field(..., description="Description text")
    lnk_id: Optional[str] = Field(None, description="Link ID")
    scrp_dt: datetime = Field(default_factory=datetime.now, description="Scraped date")


class Book(BaseModel):
    """Maps to BK_CATALOG table"""
    bk_id: str = Field(..., description="Book ID")
    bk_ttl: str = Field(..., description="Book title")
    unit_prc: float = Field(..., description="Unit price")
    auth_cd: str = Field(..., description="Author code (references AUTH_REF, no FK constraint)")
    avail_sts: str = Field(..., description="Availability status")
    rtg_val: int = Field(..., ge=1, le=5, description="Rating value (1-5)")
    desc_txt: str = Field(..., description="Description text")
    lnk_id: Optional[str] = Field(None, description="Link ID")
    scrp_dt: datetime = Field(default_factory=datetime.now, description="Scraped date")


class Film(BaseModel):
    """Maps to MEDIA_MAST table"""
    media_id: str = Field(..., description="Media ID")
    media_ttl: str = Field(..., description="Media title")
    yr_val: int = Field(..., description="Year value")
    awd_cat_cd: str = Field(..., description="Award category code (references AWARD_REF, no FK constraint)")
    dir_cd: str = Field(..., description="Director code (references DIR_REF, no FK constraint)")
    scrp_dt: datetime = Field(default_factory=datetime.now, description="Scraped date")


# ============ Bridge/Junction Models ============

class BookCategoryBridge(BaseModel):
    """Maps to BK_CAT_XREF table"""
    bk_id: str = Field(..., description="Book ID")
    bk_cat_cd: str = Field(..., description="Book category code")


class FilmActorBridge(BaseModel):
    """Maps to MEDIA_PERF_XREF table"""
    media_id: str = Field(..., description="Media ID")
    perf_cd: str = Field(..., description="Performer code")
    role_nm: Optional[str] = Field(None, description="Role name")


class FilmAwardBridge(BaseModel):
    """Maps to MEDIA_AWD_XREF table"""
    media_id: str = Field(..., description="Media ID")
    awd_cat_cd: str = Field(..., description="Award category code")
    awd_yr: int = Field(..., description="Award year")


# ============ Detail Models ============

class ProductVariant(BaseModel):
    """Maps to INV_VAR table"""
    var_id: str = Field(..., description="Variant ID")
    inv_id: str = Field(..., description="Inventory ID")
    sz_cd: str = Field(..., description="Size code")
    flv_cd: str = Field(..., description="Flavor code")
    prc_mod: float = Field(..., description="Price modifier")


class ProductReview(BaseModel):
    """Maps to INV_REV table"""
    rev_id: str = Field(..., description="Review ID")
    inv_id: str = Field(..., description="Inventory ID")
    rtg_val: int = Field(..., ge=1, le=5, description="Rating value (1-5)")
    rev_txt: str = Field(..., description="Review text")
    rev_nm: Optional[str] = Field(None, description="Reviewer name")
    rev_dt: Optional[datetime] = Field(None, description="Review date")


class SimilarProduct(BaseModel):
    """Maps to INV_SIM table"""
    sim_id: str = Field(..., description="Similar ID")
    inv_id: str = Field(..., description="Inventory ID")
    sim_inv_id: str = Field(..., description="Similar inventory ID")
    sim_score: Optional[float] = Field(None, description="Similarity score")
