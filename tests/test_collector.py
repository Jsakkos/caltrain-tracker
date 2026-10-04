from src.collector import parse_vehicles, to_local_text


def activity(vehicle="168", recorded="2026-09-14T04:07:49Z", lat="37.5", lon="-122.3",
             monitored="70012", onward=None):
    journey = {
        "VehicleRef": vehicle,
        "VehicleLocation": {"Latitude": lat, "Longitude": lon},
    }
    if monitored is not None:
        journey["MonitoredCall"] = {"StopPointRef": monitored}
    if onward is not None:
        journey["OnwardCalls"] = {"OnwardCall": [{"StopPointRef": s} for s in onward]}
    return {"RecordedAtTime": recorded, "MonitoredVehicleJourney": journey}


def payload(*activities):
    return {"Siri": {"ServiceDelivery": {"VehicleMonitoringDelivery": {"VehicleActivity": list(activities)}}}}


def test_timestamp_matches_stored_format_in_local_time():
    # Existing rows: '2026-09-14 21:07:49.000000' (PDT wall time, microseconds).
    assert to_local_text("2026-09-15T04:07:49Z") == "2026-09-14 21:07:49.000000"
    assert to_local_text("2026-09-14T21:07:49-07:00") == "2026-09-14 21:07:49.000000"


def test_timestamp_uses_standard_time_in_winter():
    assert to_local_text("2026-01-15T20:00:00Z") == "2026-01-15 12:00:00.000000"


def test_parses_vehicle_with_monitored_call():
    rows = parse_vehicles(payload(activity()))
    assert rows == [("168", "70012", 37.5, -122.3, "2026-09-13 21:07:49.000000")]


def test_falls_back_to_first_onward_call():
    rows = parse_vehicles(payload(activity(monitored=None, onward=["70021", "70031"])))
    assert rows[0][1] == "70021"


def test_unknown_stop_when_no_calls():
    rows = parse_vehicles(payload(activity(monitored=None)))
    assert rows[0][1] == "unknown"


def test_skips_malformed_activity_and_keeps_the_rest():
    bad = {"RecordedAtTime": "2026-09-14T04:07:49Z", "MonitoredVehicleJourney": {"VehicleRef": "1"}}
    rows = parse_vehicles(payload(bad, activity(vehicle="170")))
    assert [r[0] for r in rows] == ["170"]


def test_no_vehicles_overnight():
    empty = {"Siri": {"ServiceDelivery": {"VehicleMonitoringDelivery": {}}}}
    assert parse_vehicles(empty) == []
