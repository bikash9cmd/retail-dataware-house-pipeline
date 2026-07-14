"""Pydantic response models for the metrics API."""
from pydantic import BaseModel


class DailySales(BaseModel):
    full_date: str
    year: int
    month_name: str
    order_count: int
    units_sold: int
    net_revenue: float
    gross_margin: float


class TopProduct(BaseModel):
    product_id: str
    product_name: str
    category: str
    units_sold: int
    net_revenue: float
    revenue_rank: int


class CustomerSegment(BaseModel):
    customer_segment: str
    country: str
    customer_count: int
    order_count: int
    net_revenue: float
    revenue_per_customer: float


class StorePerformance(BaseModel):
    store_id: str
    store_name: str
    city: str
    country: str
    order_count: int
    net_revenue: float


class HealthCheck(BaseModel):
    status: str
    database_mode: str
