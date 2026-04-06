# Incident Analysis — Design Spec

## Context

The Caltrain dashboard tracks on-time performance with aggregate metrics (daily/weekly/monthly breakdowns, station rankings, commute analysis). What's missing is the ability to examine **individual bad days** — when a train was severely delayed, what did the disruption look like, and did it cascade to other trains?

This feature adds an **Incident Analysis** section to the dashboard: an auto-detected gallery of the worst disruptions, each expandable to reveal a Marey (space-time) diagram showing every train's trajectory that day. The human eye can instantly spot anomalies — stopped trains appear as flat horizontal sections, cascading delays show as converging lines.

## Data Pipeline

### New Prefect task: `detect_incidents()`

Added to `src/flows/data_processing.py`, called after `generate_dashboard_data()` in `process_data_flow()`.

**Stage 1 — Identify candidate incident dates**

Scan `processed_arrivals` DataFrame for trains with `delay_minutes >= 15`. Group by date to find:
- Tier 2 (System): dates with 3+ distinct trains delayed ≥15 min
- Tier 1 (Train): individual train-days with delay ≥15 min, not already part of a Tier 2 date

Rank by severity score: `sum(delay_minutes_across_affected_trains) × num_affected_trains`. Keep top 20-30.

**Stage 2 — Build trajectories for incident dates**

For each incident date, query raw GPS from `train_locations` table in SQLite for all trains active that day.

Project each GPS point onto the route:
1. Load `shapes.txt` as a polyline of `(lat, lon, shape_dist_traveled)` points
2. For each GPS point, find the nearest point on the shape polyline (minimum haversine distance)
3. Read the `shape_dist_traveled` value → this is the train's distance along the route (km)
4. Result per train: `[(timestamp, distance_km)]` sorted by timestamp

**Stage 3 — Classify trajectories**

For each train on an incident date:
- `is_anomalous`: true if this train had delay ≥15 min
- `is_cascading`: true if this train was delayed ≥10 min AND a different anomalous train departed the same station earlier that day (suggesting the first train's hold caused this one's delay)
- All other trains: normal (rendered as background context)

Identify the root cause: the anomalous train with the earliest large delay on that day, and the station where it was held. A "hold" is detected as a flat section in the trajectory: 5+ consecutive GPS points (≥5 minutes) where the distance traveled changes by less than 0.3 km total, and the train is within 0.5 km of a known station. The nearest station to the flat section center is reported as `root_cause_station`.

**Output files** (written to `static/data/`):

`incidents.json`:
```json
[
  {
    "id": "2025-12-15-system",
    "date": "2025-12-15",
    "tier": 2,
    "summary": "System Disruption — 5 trains delayed",
    "trains_affected": 5,
    "max_delay_min": 35,
    "avg_delay_min": 28,
    "on_time_pct": 42,
    "severity_score": 700,
    "root_cause_train": "312",
    "root_cause_station": "San Mateo"
  }
]
```

`incident_trajectories.json`:
```json
{
  "2025-12-15-system": {
    "incident_id": "2025-12-15-system",
    "stations": [
      { "name": "San Francisco", "distance": 0.0 },
      { "name": "22nd Street", "distance": 2.8 },
      ...
      { "name": "San Jose Diridon", "distance": 77.4 }
    ],
    "trajectories": [
      {
        "trip_id": "312",
        "direction": "SB",
        "points": [
          { "time": "07:15:00", "distance": 0.0 },
          { "time": "07:16:00", "distance": 0.8 },
          ...
        ],
        "is_anomalous": true,
        "is_cascading": false,
        "max_delay_min": 35
      },
      {
        "trip_id": "314",
        "direction": "SB",
        "points": [...],
        "is_anomalous": false,
        "is_cascading": true,
        "max_delay_min": 18
      },
      ...
    ]
  }
}
```

Estimated size: ~20 incidents × ~30 trains × ~80 points ≈ 48K data points → ~1-2 MB JSON.

### Pipeline integration

```
process_data_flow()
  ├── load_data()
  ├── process_arrival_data()
  ├── generate_summary_stats()
  ├── generate_dashboard_data()
  └── detect_incidents()          ← NEW
```

`export_to_website.py` already copies all `static/data/*.json` — no changes needed.

### Key utility: GPS → route distance projection

New function in `src/utils/geo_utils.py`:

```python
def project_to_route(lat, lon, shape_points):
    """Project a GPS point onto the route polyline.
    
    Args:
        lat, lon: GPS coordinates
        shape_points: list of (lat, lon, dist_traveled) from shapes.txt
    
    Returns:
        distance_km along route (float)
    """
```

Uses existing `haversine()` to find nearest shape point, returns its `shape_dist_traveled` value. For accuracy, interpolates between the two nearest shape points.

## Frontend Components

### `incident-analysis.tsx` — Gallery + Accordion

**Props:** `{ incidents: Incident[] }`

**State:**
- `expandedId: string | null` — which incident card is expanded
- `tierFilter: "all" | 1 | 2` — filter pills

**Rendering:**
- Section header: "Incident Analysis" with AlertTriangle icon
- Tier filter pills: All / System (red) / Train (yellow)
- List of `IncidentCard` components, filtered by tier
- Expanded card renders `<MareyDiagram>` inside

**IncidentCard anatomy:**
- Left edge: colored bar (red for Tier 2, yellow for Tier 1)
- Title: date + summary
- Stats row: trains affected, max delay, on-time rate
- One-line root cause description
- Expand/collapse chevron

### `marey-diagram.tsx` — SVG Space-Time Visualization

**Props:** `{ detail: IncidentDetail }`

**This is a custom SVG, not Recharts** (Recharts doesn't support space-time diagrams).

**Axes:**
- X: time of day, computed from the min/max timestamps across all trajectories for the incident, with 30min padding. Tick marks every 30 min.
- Y: distance along route (km), with station names as tick labels from `detail.stations[]`. SF at top, San Jose at bottom.

**Rendering layers (bottom to top):**
1. Station grid lines (horizontal, `stroke: #e5e7eb` / `dark: #334155`)
2. Time grid lines (vertical, dashed, every 30 min)
3. Normal trains — thin polylines (`stroke-width: 1`, blue, `opacity: 0.3`). Darker blue for SB, lighter for NB.
4. Cascading trains — medium polylines (`stroke-width: 2`, orange, `opacity: 0.6`)
5. Anomalous trains — thick polylines (`stroke-width: 3`, red) + circles at each GPS point (`r: 3`)
6. Annotation callout — positioned at the flat section of the primary anomaly train. Shows: train ID, hold duration, station name, cascading count.

**Interactivity:**
- Hover any train line → tooltip with trip_id, direction, delay info
- Hover anomaly dots → exact timestamp and station proximity
- Annotation callout always visible for the root cause train

**Responsive:** Fixed aspect ratio container. On mobile (<640px), horizontally scrollable with a minimum width of 600px to keep the diagram readable.

**Dark mode:** Dark background by default (`bg-slate-900`). Light mode: `bg-white` with darker grid lines. Train line colors remain consistent in both modes.

### Legend

Rendered below the SVG:
- Blue line = Normal train
- Red line + dots = Primary delay
- Orange line = Cascading delay
- "Flat sections indicate stopped trains"

## Data Layer

### `api.ts` — New interfaces

```typescript
interface Incident {
  id: string;
  date: string;
  tier: 1 | 2;
  summary: string;
  trains_affected: number;
  max_delay_min: number;
  avg_delay_min: number;
  on_time_pct: number;
  severity_score: number;
  root_cause_train?: string;
  root_cause_station?: string;
}

interface TrajectoryPoint {
  time: string;       // "HH:MM:SS"
  distance: number;   // km along route
}

interface TrainTrajectory {
  trip_id: string;
  direction: "NB" | "SB";
  points: TrajectoryPoint[];
  is_anomalous: boolean;
  is_cascading: boolean;
  max_delay_min: number;
}

interface IncidentDetail {
  incident_id: string;
  stations: { name: string; distance: number }[];
  trajectories: TrainTrajectory[];
}
```

New loaders: `getStaticIncidents()`, `getStaticIncidentTrajectories()`

### `useCaltrain.ts` — New hooks

- `useCaltrainIncidents()` → fetches `incidents.json`
- `useCaltrainIncidentDetail(id: string)` → fetches `incident_trajectories.json`, returns the entry matching `id`

### Dashboard page integration

New section placed between CommuteAnalysis and StationRankings:

```tsx
{incidents && incidents.length > 0 && (
  <IncidentAnalysis incidents={incidents} trajectories={trajectoryData} />
)}
```

## Files to Create / Modify

| File | Action | Description |
|------|--------|-------------|
| `src/flows/data_processing.py` | Modify | Add `detect_incidents()` task |
| `src/utils/geo_utils.py` | Modify | Add `project_to_route()` function |
| `~/website-deploy/src/components/caltrain/incident-analysis.tsx` | Create | Gallery + accordion component |
| `~/website-deploy/src/components/caltrain/marey-diagram.tsx` | Create | Custom SVG Marey diagram |
| `~/website-deploy/src/lib/api.ts` | Modify | Add Incident interfaces + loaders |
| `~/website-deploy/src/hooks/useCaltrain.ts` | Modify | Add incident hooks |
| `~/website-deploy/src/components/caltrain/index.ts` | Modify | Add exports |
| `~/website-deploy/src/app/projects/caltrain-tracker/dashboard/page.tsx` | Modify | Add IncidentAnalysis section |

## Verification

1. **Pipeline:** Run `detect_incidents()` manually against current data. Verify `incidents.json` and `incident_trajectories.json` appear in `static/data/` with reasonable content (incidents detected, trajectories have GPS points).
2. **GPS projection:** Spot-check a few trajectory points — the distance values should monotonically increase for southbound trains and decrease for northbound. Station distances should match known Caltrain line distances (~77km SF to SJ).
3. **Export:** Run `export_to_website.py`, verify new JSON files land in `~/website-deploy/public/data/caltrain/`.
4. **Frontend:** Run `npm run dev` in website-deploy, navigate to dashboard. Verify:
   - Incident gallery renders with tier badges
   - Filter pills work (All / System / Train)
   - Clicking an incident card expands the Marey diagram
   - Normal trains appear as thin blue diagonals
   - Anomalous trains appear as thick red with GPS dots
   - Annotation callout shows on root cause train
   - Hover tooltips work on train lines
   - Dark mode renders correctly
   - Mobile: diagram is horizontally scrollable
5. **End-to-end:** Process data → export → verify dashboard shows real incidents with real trajectories.
