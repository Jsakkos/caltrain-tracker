"""
SQLAlchemy models for versioned GTFS schedule data.

These models store static GTFS schedule information with version tracking,
enabling historical analysis of schedule changes over time.
"""
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, ForeignKey, Index, Date, Text
from sqlalchemy.orm import relationship
from datetime import datetime

from src.db.database import Base


class GtfsScheduleVersion(Base):
    """Master record for a GTFS schedule version.
    
    Each time a new schedule is published, a new version record is created.
    All schedule data (stops, routes, trips, etc.) is linked to a version.
    """
    __tablename__ = "gtfs_schedule_versions"

    id = Column(Integer, primary_key=True, index=True)
    feed_version = Column(String, unique=True, index=True, nullable=False)  # e.g., "20251107"
    feed_start_date = Column(String, nullable=True)  # YYYYMMDD from feed_info.txt
    feed_end_date = Column(String, nullable=True)    # YYYYMMDD from feed_info.txt
    feed_publisher_name = Column(String, nullable=True)
    feed_publisher_url = Column(String, nullable=True)
    feed_lang = Column(String, nullable=True)
    ingested_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    is_active = Column(Boolean, default=True)  # Latest version is marked active
    
    # Relationships
    agencies = relationship("GtfsAgency", back_populates="schedule_version", cascade="all, delete-orphan")
    stops = relationship("GtfsStop", back_populates="schedule_version", cascade="all, delete-orphan")
    routes = relationship("GtfsRoute", back_populates="schedule_version", cascade="all, delete-orphan")
    trips = relationship("GtfsTrip", back_populates="schedule_version", cascade="all, delete-orphan")
    stop_times = relationship("GtfsStopTime", back_populates="schedule_version", cascade="all, delete-orphan")
    calendars = relationship("GtfsCalendar", back_populates="schedule_version", cascade="all, delete-orphan")
    calendar_dates = relationship("GtfsCalendarDate", back_populates="schedule_version", cascade="all, delete-orphan")


class GtfsAgency(Base):
    """Agency information from agency.txt."""
    __tablename__ = "gtfs_agencies"

    id = Column(Integer, primary_key=True, index=True)
    schedule_version_id = Column(Integer, ForeignKey("gtfs_schedule_versions.id"), nullable=False, index=True)
    agency_id = Column(String, nullable=False)
    agency_name = Column(String, nullable=False)
    agency_url = Column(String, nullable=True)
    agency_timezone = Column(String, nullable=True)
    agency_lang = Column(String, nullable=True)
    agency_phone = Column(String, nullable=True)
    
    schedule_version = relationship("GtfsScheduleVersion", back_populates="agencies")
    
    __table_args__ = (
        Index('idx_agency_version_id', 'schedule_version_id', 'agency_id'),
    )


class GtfsStop(Base):
    """Stop/station information from stops.txt."""
    __tablename__ = "gtfs_stops"

    id = Column(Integer, primary_key=True, index=True)
    schedule_version_id = Column(Integer, ForeignKey("gtfs_schedule_versions.id"), nullable=False, index=True)
    stop_id = Column(String, nullable=False, index=True)
    stop_code = Column(String, nullable=True)
    stop_name = Column(String, nullable=False)
    stop_lat = Column(Float, nullable=True)
    stop_lon = Column(Float, nullable=True)
    zone_id = Column(String, nullable=True)
    stop_url = Column(String, nullable=True)
    location_type = Column(Integer, nullable=True)  # 0=stop, 1=station, 2=entrance
    parent_station = Column(String, nullable=True)
    stop_timezone = Column(String, nullable=True)
    wheelchair_boarding = Column(Integer, nullable=True)
    platform_code = Column(String, nullable=True)
    
    schedule_version = relationship("GtfsScheduleVersion", back_populates="stops")
    
    __table_args__ = (
        Index('idx_stop_version_id', 'schedule_version_id', 'stop_id'),
    )


class GtfsRoute(Base):
    """Route information from routes.txt."""
    __tablename__ = "gtfs_routes"

    id = Column(Integer, primary_key=True, index=True)
    schedule_version_id = Column(Integer, ForeignKey("gtfs_schedule_versions.id"), nullable=False, index=True)
    route_id = Column(String, nullable=False, index=True)
    agency_id = Column(String, nullable=True)
    route_short_name = Column(String, nullable=True)
    route_long_name = Column(String, nullable=True)
    route_desc = Column(String, nullable=True)
    route_type = Column(Integer, nullable=True)  # 2 = rail
    route_url = Column(String, nullable=True)
    route_color = Column(String, nullable=True)
    route_text_color = Column(String, nullable=True)
    
    schedule_version = relationship("GtfsScheduleVersion", back_populates="routes")
    
    __table_args__ = (
        Index('idx_route_version_id', 'schedule_version_id', 'route_id'),
    )


class GtfsTrip(Base):
    """Trip information from trips.txt."""
    __tablename__ = "gtfs_trips"

    id = Column(Integer, primary_key=True, index=True)
    schedule_version_id = Column(Integer, ForeignKey("gtfs_schedule_versions.id"), nullable=False, index=True)
    trip_id = Column(String, nullable=False, index=True)
    route_id = Column(String, nullable=False, index=True)
    service_id = Column(String, nullable=False, index=True)
    trip_headsign = Column(String, nullable=True)
    trip_short_name = Column(String, nullable=True)
    direction_id = Column(Integer, nullable=True)
    block_id = Column(String, nullable=True)
    shape_id = Column(String, nullable=True)
    wheelchair_accessible = Column(Integer, nullable=True)
    bikes_allowed = Column(Integer, nullable=True)
    
    schedule_version = relationship("GtfsScheduleVersion", back_populates="trips")
    
    __table_args__ = (
        Index('idx_trip_version_id', 'schedule_version_id', 'trip_id'),
    )


class GtfsStopTime(Base):
    """Stop time information from stop_times.txt."""
    __tablename__ = "gtfs_stop_times"

    id = Column(Integer, primary_key=True, index=True)
    schedule_version_id = Column(Integer, ForeignKey("gtfs_schedule_versions.id"), nullable=False, index=True)
    trip_id = Column(String, nullable=False, index=True)
    stop_id = Column(String, nullable=False, index=True)
    arrival_time = Column(String, nullable=True)  # HH:MM:SS format (can exceed 24:00)
    departure_time = Column(String, nullable=True)
    stop_sequence = Column(Integer, nullable=False)
    stop_headsign = Column(String, nullable=True)
    pickup_type = Column(Integer, nullable=True)
    drop_off_type = Column(Integer, nullable=True)
    shape_dist_traveled = Column(Float, nullable=True)
    timepoint = Column(Integer, nullable=True)
    
    schedule_version = relationship("GtfsScheduleVersion", back_populates="stop_times")
    
    __table_args__ = (
        Index('idx_stoptime_version_trip', 'schedule_version_id', 'trip_id'),
        Index('idx_stoptime_version_stop', 'schedule_version_id', 'stop_id'),
    )


class GtfsCalendar(Base):
    """Service calendar from calendar.txt."""
    __tablename__ = "gtfs_calendars"

    id = Column(Integer, primary_key=True, index=True)
    schedule_version_id = Column(Integer, ForeignKey("gtfs_schedule_versions.id"), nullable=False, index=True)
    service_id = Column(String, nullable=False, index=True)
    monday = Column(Integer, nullable=False, default=0)
    tuesday = Column(Integer, nullable=False, default=0)
    wednesday = Column(Integer, nullable=False, default=0)
    thursday = Column(Integer, nullable=False, default=0)
    friday = Column(Integer, nullable=False, default=0)
    saturday = Column(Integer, nullable=False, default=0)
    sunday = Column(Integer, nullable=False, default=0)
    start_date = Column(String, nullable=False)  # YYYYMMDD
    end_date = Column(String, nullable=False)    # YYYYMMDD
    
    schedule_version = relationship("GtfsScheduleVersion", back_populates="calendars")
    
    __table_args__ = (
        Index('idx_calendar_version_service', 'schedule_version_id', 'service_id'),
    )


class GtfsCalendarDate(Base):
    """Calendar exceptions from calendar_dates.txt."""
    __tablename__ = "gtfs_calendar_dates"

    id = Column(Integer, primary_key=True, index=True)
    schedule_version_id = Column(Integer, ForeignKey("gtfs_schedule_versions.id"), nullable=False, index=True)
    service_id = Column(String, nullable=False, index=True)
    date = Column(String, nullable=False)  # YYYYMMDD
    exception_type = Column(Integer, nullable=False)  # 1=added, 2=removed
    
    schedule_version = relationship("GtfsScheduleVersion", back_populates="calendar_dates")
    
    __table_args__ = (
        Index('idx_calendardate_version_service', 'schedule_version_id', 'service_id'),
    )
