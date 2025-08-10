"""
SQLAlchemy models for train data.
"""
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, ForeignKey, Index
from sqlalchemy.orm import relationship
from datetime import datetime

from src.db.database import Base

class TrainLocation(Base):
    """Train location data recorded from GTFS real-time feed."""
    __tablename__ = "train_locations"

    id = Column(Integer, primary_key=True, index=True)
    trip_id = Column(String, index=True)
    stop_id = Column(String, index=True)
    vehicle_lat = Column(Float)
    vehicle_lon = Column(Float)
    timestamp = Column(DateTime, default=datetime.utcnow)
    
    # Add index on composite columns to speed up queries
    __table_args__ = (
        Index('idx_trip_stop_timestamp', 'trip_id', 'stop_id', 'timestamp', unique=True),
    )

class Stop(Base):
    """Caltrain stop data from GTFS static feed."""
    __tablename__ = "stops"

    stop_id = Column(String, primary_key=True)
    stop_name = Column(String)
    stop_lat = Column(Float)
    stop_lon = Column(Float)
    parent_station = Column(String, nullable=True)

class Trip(Base):
    """Trip information from GTFS static feed."""
    __tablename__ = "trips"

    trip_id = Column(String, primary_key=True)
    route_id = Column(String, index=True)
    service_id = Column(String, index=True)
    trip_headsign = Column(String, nullable=True)
    direction_id = Column(Integer, nullable=True)
    
class StopTime(Base):
    """Scheduled stop times from GTFS static feed."""
    __tablename__ = "stop_times"

    id = Column(Integer, primary_key=True, index=True)
    trip_id = Column(String, ForeignKey("trips.trip_id"), index=True)
    stop_id = Column(String, ForeignKey("stops.stop_id"), index=True)
    arrival_time = Column(String)
    departure_time = Column(String)
    stop_sequence = Column(Integer)

    # Define relationships
    trip = relationship("Trip", backref="stop_times")
    stop = relationship("Stop", backref="stop_times")
    
    __table_args__ = (
        Index('idx_trip_stop', 'trip_id', 'stop_id'),
    )

class ArrivalData(Base):
    """Processed arrival data with calculated metrics."""
    __tablename__ = "arrival_data"

    id = Column(Integer, primary_key=True, index=True)
    trip_id = Column(String, index=True)
    stop_id = Column(String, index=True)
    scheduled_arrival = Column(DateTime, index=True)
    actual_arrival = Column(DateTime, index=True)
    delay_minutes = Column(Float)
    is_delayed = Column(Boolean, default=False)
    delay_severity = Column(String)
    commute_period = Column(String)
    date = Column(DateTime, index=True)
    
    __table_args__ = (
        Index('idx_arrival_trip_stop_date', 'trip_id', 'stop_id', 'date', unique=True),
    )
