"""
FastAPI routes for the Caltrain Tracker API.
"""
import logging
from typing import List, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import func, select, desc
import json
import os
import csv
from datetime import datetime, date, timedelta

from src.db.database import get_db
from src.models.train_data import TrainLocation as DBTrainLocation
from src.models.train_data import ArrivalData as DBArrivalData
from src.models.train_data import Stop as DBStop
from src.config import GTFS_DATA_PATH
from src.api.models import (
    TrainLocation, ArrivalData, Stop, SummaryStats,
    DelayStatsByDate, TrainPerformance, StopPerformance, 
    CommutePeriodStats
)
from src.config import STATIC_CONTENT_PATH

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/health", response_model=Dict[str, str])
async def health_check():
    """Health check endpoint."""
    return {"status": "ok", "timestamp": datetime.now().isoformat()}

@router.get("/train-locations", response_model=List[TrainLocation])
async def get_train_locations(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    trip_id: str = None,
    start_date: date = None,
    end_date: date = None,
    db: Session = Depends(get_db)
):
    """
    Get train location data with optional filtering.
    """
    query = select(DBTrainLocation)
    
    # Apply filters if provided
    if trip_id:
        query = query.filter(DBTrainLocation.trip_id == trip_id)
    if start_date:
        start_datetime = datetime.combine(start_date, datetime.min.time())
        query = query.filter(DBTrainLocation.timestamp >= start_datetime)
    if end_date:
        end_datetime = datetime.combine(end_date, datetime.max.time())
        query = query.filter(DBTrainLocation.timestamp <= end_datetime)
    
    # Apply pagination
    query = query.order_by(desc(DBTrainLocation.timestamp)).offset(skip).limit(limit)
    
    result = db.execute(query)
    locations = result.scalars().all()
    
    return locations

@router.get("/arrival-data", response_model=List[ArrivalData])
async def get_arrival_data(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    trip_id: str = None,
    stop_id: str = None,
    date_from: date = None,
    date_to: date = None,
    is_delayed: bool = None,
    commute_period: str = None,
    db: Session = Depends(get_db)
):
    """
    Get processed arrival data with optional filtering.
    """
    query = select(DBArrivalData)
    
    # Apply filters if provided
    if trip_id:
        query = query.filter(DBArrivalData.trip_id == trip_id)
    if stop_id:
        query = query.filter(DBArrivalData.stop_id == stop_id)
    if date_from:
        query = query.filter(DBArrivalData.date >= date_from)
    if date_to:
        query = query.filter(DBArrivalData.date <= date_to)
    if is_delayed is not None:
        query = query.filter(DBArrivalData.is_delayed == is_delayed)
    if commute_period:
        query = query.filter(DBArrivalData.commute_period == commute_period)
    
    # Apply pagination
    query = query.order_by(desc(DBArrivalData.actual_arrival)).offset(skip).limit(limit)
    
    result = db.execute(query)
    arrivals = result.scalars().all()
    
    return arrivals

@router.get("/stops", response_model=List[Stop])
async def get_stops(db: Session = Depends(get_db)):
    """
    Get all stops data. If no stops in database, loads from GTFS file.
    """
    query = select(DBStop)
    result = db.execute(query)
    stops = result.scalars().all()
    
    # If no stops in database, load from GTFS file
    if not stops:
        stops = load_stops_from_gtfs()
    
    return stops

def load_stops_from_gtfs() -> List[Stop]:
    """
    Load stops data from GTFS stops.txt file.
    Returns a list of Stop objects.
    """
    stops_file = os.path.join(GTFS_DATA_PATH, 'stops.txt')
    stops = []
    
    try:
        with open(stops_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Create Stop object
                stop = Stop(
                    stop_id=row['stop_id'],
                    stop_name=row['stop_name'],
                    stop_lat=float(row['stop_lat']),
                    stop_lon=float(row['stop_lon']),
                    parent_station=row.get('parent_station') if row.get('parent_station') else None
                )
                stops.append(stop)
        
        logger.info(f"Loaded {len(stops)} stops from GTFS file")
        return stops
    except Exception as e:
        logger.error(f"Error loading stops from GTFS file: {e}")
        return []

@router.get("/summary", response_model=SummaryStats)
async def get_summary_stats():
    """
    Get summary statistics for the Caltrain performance.
    """
    # Load from the pre-generated JSON file
    summary_path = os.path.join(STATIC_CONTENT_PATH, 'data', 'summary_stats.json')
    
    try:
        with open(summary_path, 'r') as f:
            summary = json.load(f)
        return summary
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.error(f"Error loading summary stats: {e}")
        raise HTTPException(status_code=404, detail="Summary statistics not found or invalid")

@router.get("/delay-stats-by-date", response_model=List[DelayStatsByDate])
async def get_delay_stats_by_date(
    date_from: date = None,
    date_to: date = None,
    db: Session = Depends(get_db)
):
    """
    Get delay statistics aggregated by date.
    """
    query = select(
        DBArrivalData.date,
        func.avg(
            func.case(
                (DBArrivalData.delay_severity == 'On Time', 1),
                else_=0
            )
        ).label('on_time_percentage'),
        func.avg(
            func.case(
                (DBArrivalData.delay_severity == 'Minor', 1),
                else_=0
            )
        ).label('minor_delay_percentage'),
        func.avg(
            func.case(
                (DBArrivalData.delay_severity == 'Major', 1),
                else_=0
            )
        ).label('major_delay_percentage')
    ).group_by(DBArrivalData.date)
    
    # Apply date filters if provided
    if date_from:
        query = query.filter(DBArrivalData.date >= date_from)
    if date_to:
        query = query.filter(DBArrivalData.date <= date_to)
    
    result = db.execute(query)
    stats = []
    
    for row in result:
        stats.append({
            'date': row.date,
            'on_time_percentage': row.on_time_percentage * 100,
            'minor_delay_percentage': row.minor_delay_percentage * 100,
            'major_delay_percentage': row.major_delay_percentage * 100
        })
    
    return stats

@router.get("/train-performance", response_model=List[TrainPerformance])
async def get_train_performance(
    top: int = Query(10, ge=1, le=100),
    order_by: str = Query("avg_delay", regex="^(avg_delay|on_time_percentage)$"),
    ascending: bool = Query(True),
    db: Session = Depends(get_db)
):
    """
    Get performance metrics for trains, ordered by specified metric.
    """
    query = select(
        DBArrivalData.trip_id,
        func.avg(DBArrivalData.delay_minutes).label('avg_delay'),
        func.avg(
            func.case(
                (DBArrivalData.is_delayed == False, 1),
                else_=0
            )
        ).label('on_time_percentage'),
        func.count(DBArrivalData.id).label('total_arrivals')
    ).group_by(DBArrivalData.trip_id)
    
    # Apply ordering
    if order_by == "avg_delay":
        query = query.order_by(func.avg(DBArrivalData.delay_minutes).asc() if ascending else func.avg(DBArrivalData.delay_minutes).desc())
    else:  # on_time_percentage
        on_time_expr = func.avg(func.case((DBArrivalData.is_delayed == False, 1), else_=0))
        query = query.order_by(on_time_expr.desc() if ascending else on_time_expr.asc())
    
    # Apply limit
    query = query.limit(top)
    
    result = db.execute(query)
    trains = []
    
    for row in result:
        trains.append({
            'trip_id': row.trip_id,
            'avg_delay': row.avg_delay,
            'on_time_percentage': row.on_time_percentage * 100,
            'total_arrivals': row.total_arrivals
        })
    
    return trains

@router.get("/stop-performance", response_model=List[StopPerformance])
async def get_stop_performance(
    top: int = Query(10, ge=1, le=100),
    order_by: str = Query("avg_delay", regex="^(avg_delay|on_time_percentage)$"),
    ascending: bool = Query(True),
    db: Session = Depends(get_db)
):
    """
    Get performance metrics for stops, ordered by specified metric.
    """
    # Join with stops to get stop names
    query = select(
        DBArrivalData.stop_id,
        DBStop.stop_name,
        func.avg(DBArrivalData.delay_minutes).label('avg_delay'),
        func.avg(
            func.case(
                (DBArrivalData.is_delayed == False, 1),
                else_=0
            )
        ).label('on_time_percentage'),
        func.count(DBArrivalData.id).label('total_arrivals')
    ).join(
        DBStop, DBArrivalData.stop_id == DBStop.stop_id
    ).group_by(DBArrivalData.stop_id, DBStop.stop_name)
    
    # Apply ordering
    if order_by == "avg_delay":
        query = query.order_by(func.avg(DBArrivalData.delay_minutes).asc() if ascending else func.avg(DBArrivalData.delay_minutes).desc())
    else:  # on_time_percentage
        on_time_expr = func.avg(func.case((DBArrivalData.is_delayed == False, 1), else_=0))
        query = query.order_by(on_time_expr.desc() if ascending else on_time_expr.asc())
    
    # Apply limit
    query = query.limit(top)
    
    result = db.execute(query)
    stops = []
    
    for row in result:
        stops.append({
            'stop_id': row.stop_id,
            'stop_name': row.stop_name,
            'avg_delay': row.avg_delay,
            'on_time_percentage': row.on_time_percentage * 100,
            'total_arrivals': row.total_arrivals
        })
    
    return stops

@router.get("/commute-period-stats", response_model=List[CommutePeriodStats])
async def get_commute_period_stats(
    date_from: date = None,
    date_to: date = None,
    db: Session = Depends(get_db)
):
    """
    Get statistics by commute period (Morning, Evening, Other, Weekend).
    """
    query = select(
        DBArrivalData.commute_period,
        func.avg(
            func.case(
                (DBArrivalData.delay_severity == 'On Time', 1),
                else_=0
            )
        ).label('on_time_percentage'),
        func.avg(
            func.case(
                (DBArrivalData.delay_severity == 'Minor', 1),
                else_=0
            )
        ).label('minor_delay_percentage'),
        func.avg(
            func.case(
                (DBArrivalData.delay_severity == 'Major', 1),
                else_=0
            )
        ).label('major_delay_percentage'),
        func.count(DBArrivalData.id).label('total_trips')
    ).group_by(DBArrivalData.commute_period)
    
    # Apply date filters if provided
    if date_from:
        query = query.filter(DBArrivalData.date >= date_from)
    if date_to:
        query = query.filter(DBArrivalData.date <= date_to)
    
    result = db.execute(query)
    stats = []
    
    for row in result:
        stats.append({
            'commute_period': row.commute_period,
            'on_time_percentage': row.on_time_percentage * 100,
            'minor_delay_percentage': row.minor_delay_percentage * 100,
            'major_delay_percentage': row.major_delay_percentage * 100,
            'total_trips': row.total_trips
        })
    
    return stats
