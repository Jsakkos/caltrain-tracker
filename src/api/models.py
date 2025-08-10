"""
Pydantic models for API requests and responses.
"""
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from datetime import datetime, date

class TrainLocationBase(BaseModel):
    """Base model for train location data."""
    trip_id: str
    stop_id: str
    vehicle_lat: float
    vehicle_lon: float

class TrainLocationCreate(TrainLocationBase):
    """Model for creating train location data."""
    timestamp: datetime

class TrainLocation(TrainLocationBase):
    """Model for train location data with ID."""
    id: int
    timestamp: datetime

    class Config:
        from_attributes = True

class ArrivalDataBase(BaseModel):
    """Base model for arrival data."""
    trip_id: str
    stop_id: str
    scheduled_arrival: datetime
    actual_arrival: datetime
    delay_minutes: float
    is_delayed: bool
    delay_severity: str
    commute_period: str

class ArrivalData(ArrivalDataBase):
    """Model for arrival data with ID."""
    id: int
    date: date

    class Config:
        from_attributes = True

class StopBase(BaseModel):
    """Base model for stop data."""
    stop_id: str
    stop_name: str
    stop_lat: float
    stop_lon: float
    parent_station: Optional[str] = None

class Stop(StopBase):
    """Model for stop data."""
    class Config:
        from_attributes = True

class SummaryStats(BaseModel):
    """Model for summary statistics."""
    on_time_performance: float
    total_trips: int
    on_time_trips: int
    best_train: Dict[str, Any]
    worst_train: Dict[str, Any]
    best_stop: Dict[str, Any]
    worst_stop: Dict[str, Any]
    date_range: Dict[str, str]

class DelayStatsByDate(BaseModel):
    """Model for delay statistics by date."""
    date: date
    on_time_percentage: float
    minor_delay_percentage: float
    major_delay_percentage: float

class TrainPerformance(BaseModel):
    """Model for train performance metrics."""
    trip_id: str
    avg_delay: float
    on_time_percentage: float
    total_arrivals: int

class StopPerformance(BaseModel):
    """Model for stop performance metrics."""
    stop_id: str
    stop_name: str
    avg_delay: float
    on_time_percentage: float
    total_arrivals: int

class CommutePeriodStats(BaseModel):
    """Model for commute period statistics."""
    commute_period: str
    on_time_percentage: float
    minor_delay_percentage: float
    major_delay_percentage: float
    total_trips: int
