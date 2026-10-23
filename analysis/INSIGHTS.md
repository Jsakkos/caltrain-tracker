# Caltrain delay stories: ranked candidates

Fourteen months of arrivals (2025-08-10 → 2026-10-22), re-derived from the raw 511 pings
with signed delay and a per-arrival quality score, scored against the timetable that was
actually in effect each day, and validated against 511's own independent arrival records
(r = 0.96, 97% within two minutes, 436k matched arrivals). Evidence lives in the four
notebooks in this folder; the clean subset and its reasons are in `01_data_quality` and
`README.md`.

Unless stated otherwise every number is: **weekdays, non-origin stops, bad-feed days
masked, "late" = more than 4 minutes behind schedule**. Delay precision is one minute,
so differences under ~1 minute in a single arrival are noise; aggregates over thousands
of arrivals are fine.

Ratings are 1–5: **S**urprising (would a regular rider guess it?), **R**obust (sample
size, confounders, agreement with the independent 511 record), **V**isual (can one chart
carry it?).

---

## 1. Once a train is ten minutes late, it stays late to the end of the line  — S4 R5 V5

**Finding.** After a train first reaches ≥ 10 minutes late, its delay over the next ten
stops has a median change of 0 to +1 minute; 40% get two or more minutes worse, 8%
recover two or more minutes, and 0.3% get back under the 4-minute threshold.

**Evidence.** 743 weekday trips that crossed 10 minutes at a non-origin stop. Fan chart in
`03_propagation §4`. Northbound Express/Limited are the worst (median +2 min five stops
later; 55% worsen); southbound Express is the only group with any recovery (27%, but n = 15).

**Caveats.** The destination stop is never observed, so "the end of the line" is the
penultimate stop. Schedule slack exists (see #9) but is ~1 minute per padded segment,
far less than a 10-minute delay. Tells you nothing about whether Caltrain *could* recover,
only that it does not.

**Visual.** A fan chart: x = stops after going 10 min late, y = change in delay, median
line with 25–75 and 10–90 bands; annotate "0.3% back on time". Alternative: a spaghetti
plot of 100 random late trips, all flat or rising.

## 2. Strikes freeze the whole railroad for an hour, wreck three hours of departures, and the expresses are what gets cut  — S3 R4 V5

**Finding.** Twelve times in 380 clean days (about once every five weeks) four or more
trains froze at the same minute with no station to stop at, for 20 to 131 minutes. Every
one hit both directions. Trains scheduled to depart in the hour before the stop finished
a median 48 minutes late; those departing one to two hours *after* it, 27 minutes; only
after three hours do departures run normally. Those twelve windows hold 31% of every
arrival more than 15 minutes late in the whole record. On the nine days with three or
more never-run trips, 18% of scheduled Express trains and 9% of Limiteds were dropped,
against 1% of Locals.

**Evidence.** `05_network_stops`. Detector A: ≥ 10 consecutive identical positions (not
terminal parking) for ≥ 4 trains at once, confirmed by ≥ 5 trains ≥ 15 min late in the
next four hours (13 candidates failed that test and are sub-day feed freezes). Detector B:
scheduled trips that never appear while the feed is alive for other trains around their
departure. Three events match public reports: 2025-10-24 (Caltrain Alerts: major delays,
all trains), 2026-02-03 (pedestrian fatality at Palo Alto; our hold cluster is Palo Alto),
2026-09-02 (vehicle strike at Burlingame; our cluster is Burlingame–Hillsdale–San Bruno).
Caltrain's own monthly reports list trespasser strikes as the largest delay cause and
report 7–33 cancellations a month; our never-ran count finds half to two thirds of those.
The 2025-11-06 time-distance diagram shows the mechanism in one picture: the line
closes near Palo Alto at 15:55, within minutes trains are parked from Redwood City to
Sunnyvale in both directions, and every train that enters the segment for the next two
hours leaves it 60–90 minutes late.

**Caveats.** The feed never says *why* the line stopped; "strike" is inferred from the
shape and from the three reported cases. Bad-feed days are excluded, so incidents on
those days are invisible, and a stop that holds trains at the terminals is not detected.
"Never ran" cannot see trains terminated mid-run, and some cancellation clusters (Nov
18–19 2025, the same expresses two days running) look like equipment shortages rather
than incidents. Caltrain counts lateness at the final stop; we count it at the
penultimate one, so our late-train totals run higher than theirs.

**Visual.** The time-distance (Marey) diagram of one day, every train a line coloured by
delay, the frozen window shaded. Then the ripple chart: end-of-trip delay against
departure time relative to the stop, showing the three-hour shadow. A small table of the
twelve events (date, where, how long, trains hit, cancelled, hours to normal) and a bar of
cancellation rate by service type complete it. This is also the natural static frame of an
animation (below).

## 3. The least reliable trains on the line are the northbound expresses  — S3 R5 V5

**Finding.** Train 523 (17:22 Express NB) arrives more than four minutes late at 22nd
Street on half its runs; 511 (08:22 Express NB) on 37%. Express NB is late 16% of the
time in the AM peak and 15% in the PM peak, versus 4–6% for Express SB and 6–11% for
Local NB. 23 of the 25 least reliable weekday trains are northbound.

**Evidence.** League table of 111 train numbers with ≥ 80 clean weekday runs (≈ 250 each),
Wilson intervals, `02_when_where_which §5–6`. Service-type comparison within peak period
and direction so time of day is not the confounder.

**Caveats.** "Last observed stop" for SF-bound trains is 22nd Street, which is also where
NB delay peaks (#4), so the ranking partly measures the SF approach. The mechanism is in
#4: an express cannot overtake the local ~10 minutes ahead of it.

**Visual.** Horizontal bar league table (top/bottom 10) coloured by service type with
error bars; or a grouped bar of late share by service × direction × peak.

## 4. Knock-on delay is gated by headway, and expresses inherit the local ahead  — S3 R5 V5

**Finding.** When the train ahead is 10–20 minutes late at a stop, the next train is late
88% of the time if it is scheduled ≤ 10 minutes behind, 66% at 11–20 minutes, and 34% at
21–30 minutes. At 22nd Street NB an Express whose leader is > 6 minutes late is late 88%
of the time; a Local in the same situation 38% (locals have 25+ minutes of headway).

**Evidence.** 321k consecutive same-direction pairs at the same platform, `03_propagation
§3`. Cell sizes shown on the heatmap (smallest cell n = 91).

**Caveats.** Descriptive, not causal: a shared cause (e.g. a line-wide slow order) also
makes consecutive trains late. The headway effect is the part that is hard to explain
any other way. Delay of the leader is measured at the same stop, so "inherits" means
"arrives late at the same place", not "was held behind".

**Visual.** The 5 × 4 heatmap (leader delay × headway → % followers late) with n in each
cell. A second panel: Express vs Local late share at 22nd St by leader state.

## 5. Northbound delay is made on the final approach into San Francisco  — S4 R4 V4

**Finding.** The single largest time sink on the line is South San Francisco → 22nd
Street northbound for trains that skip Bayshore: +1.3 minutes on average, and Express
trains gain ≥ 2 minutes there on 43% of runs. 28% of all northbound "first went late"
events happen at 22nd Street or Bayshore. Locals, with 2.5 more scheduled minutes through
Bayshore, *recover* on the same stretch.

**Evidence.** Segment accrual over 313k consecutive-stop pairs and 1,900 NB onset events,
`03_propagation §1–2`. 511's observations show the same end-of-line pattern (22nd St NB
mean delay 2.8 min in both sources).

**Caveats.** The approach into SF (the 22nd St tunnel and the 4th & King throat) is also
where trains slow down, so a stale position could bias an arrival late; but the effect is
specific to skip-stop trains on the same stretch where locals recover, which a measurement
artefact would not produce. Whether it is the 7.5-minute allowance being too tight or
queuing at the terminal cannot be separated with position data alone.

**Visual.** A line map: stations on the x-axis, bar per segment coloured blue/orange for
time recovered/lost, NB and SB panels; highlight SSF → 22nd St. Or a "where trains first
go late" mirrored bar chart (NB up, SB down).

## 6. Northbound trains accumulate twice the delay of southbound, every month  — S3 R5 V4

**Finding.** Scheduled run times are symmetric (78 min NB vs 77 min SB for a 22-stop
local), yet at the last observed stops NB trains average 2.2 minutes late and 14% late
versus 1.4 minutes and 9% SB. The gap is present in all 15 months and in all three
service types.

**Evidence.** 69k end-of-trip arrivals by month and direction, `02_when_where_which §4`;
per-segment accrual by hour shows NB losing time at every hour of the day while SB is
roughly flat.

**Caveats.** Part of this is #5 (the SF approach) and part is that NB trains run through
the peak-direction crowding in the morning. The after-midnight and SF-terminal arrivals,
which would add SB evening lateness, are unobservable, so the asymmetry may be slightly
overstated.

**Visual.** Two lines (NB blue, SB orange) of end-of-trip late share by month; or the
along-the-trip mean-delay curves per direction and service.

## 7. Saturday is the least reliable day to ride  — S5 R3 V4

**Finding.** Pooled, 10.7% of Saturday arrivals are late versus 6.7% on weekdays and 7.1%
on Sundays. The typical Saturday is only slightly worse (median day 6.7% late vs 5.7%),
but one Saturday in five has more than 15% of arrivals late, versus one weekday in
fourteen. Both directions suffer equally on Saturdays, unlike weekdays.

**Evidence.** 52 Saturdays, 56 Sundays, 272 weekdays; per-day distribution in
`02_when_where_which §2`. Worst Saturdays: 2025-10-04 (39% late), 2026-05-23 (38%),
2026-01-03 (33%).

**Caveats.** Driven by the tail, so a few more or fewer bad Saturdays would move the
pooled number a lot. The pattern (both directions, late share rising through the
afternoon, onsets shifted to the northern Peninsula and the weekend-only Broadway stop)
is consistent with weekend track work, but no work-schedule data was joined to confirm.
Weekend schedules have 66 trips, so per-hour sample sizes are a third of weekday ones.

**Visual.** Violin/strip plot of per-day late share for weekday vs Sunday vs Saturday
with the 15% line marked; or the hour × day-of-week heatmap where Saturday afternoon
lights up.

## 8. A handful of days carry most of the severe delay, and each takes an afternoon to clear  — S2 R5 V5

**Finding.** The median service day has two arrivals more than 15 minutes late. Ten days
(2.6% of days) account for a third of all such arrivals, and 38 days for two thirds. On
those days the system goes from normal to a median delay of 20–60 minutes within one
hour and takes a median of four hours (range 1–6) to get back under 15% late.

**Evidence.** 380 clean days, Pareto curve and hourly profiles of the six worst days,
`03_propagation §5`. Worst day: Thursday 2025-11-06, 37 trains ≥ 15 min late, median
delay above 60 minutes from 17:00 to 19:00.

**Caveats.** Cause of each incident is not in the data (no ServiceAlerts join). The
recovery statistic is computed on the 20 worst days only (18 had a > 50% hour).

**Visual.** Small multiples of % late by hour for the six worst days (already drawn), or
a calendar heatmap of daily severity with the worst days labelled. The Pareto curve is a
good inset.

## 9. The timetable's whole-minute rounding builds in a one-minute sawtooth  — S4 R3 V4

**Finding.** Median delay by station alternates between ~+1 and ~0 minutes at adjacent
stops. Two-minute hops (San Mateo → Hayward Park SB, Belmont → San Carlos SB, Hillsdale →
San Mateo NB) are consistently run in three minutes; four-minute hops (Hillsdale →
Belmont SB, San Carlos → Redwood City SB, Burlingame → Millbrae NB) are run in three, so
trains arrive a minute "late" at one station and on time at the next.

**Evidence.** 7–12k clean arrivals per station; 511's independent `stop_observations`
show the same sign at the same stations for most of the line (`02_when_where_which §7`).

**Caveats.** Magnitudes are at the one-minute precision floor and depend on the arrival
detector: 511's record disagrees with ours by 1–1.5 minutes at Burlingame NB and San
Carlos SB, so the *pattern* is robust but individual station values are not. None of this
affects the 4-minute on-time measure; it is a story about timetable craft, not service.

**Visual.** Median delay vs station as a line per direction, with the 511 series dashed
alongside so the agreement is visible; annotate the 2-minute hops.

## 10. The weekday shape: two peaks, a midday trough, and the PM peak northbound is the worst hour  — S1 R5 V5

**Finding.** Late share is 2–4% at 05:00, 11% for NB at 08:00, back to 4%/2% at noon,
and 12.5% for NB at 17:00–18:00. Southbound peaks later and lower (8.5% at 17:00, still
8% at 20:00–21:00).

**Evidence.** 398k weekday arrivals, 95% intervals narrower than the line width,
`02_when_where_which §1`.

**Caveats.** None beyond the clean-subset rules. Expected, but it anchors every other
story and the hour × day-of-week heatmap is the single most legible chart in the set.

**Visual.** Hour × direction line chart with intervals, or the hour × day-of-week heatmap.

## 11. Hot days run late; rainy days do not  — S4 R2 V3

**Finding.** Weekdays with a maximum temperature ≥ 28 °C are late 9.0% of the time versus
6.5% otherwise, and +1.9 points above their own month's average; days with ≥ 5 mm of
rain are no different from dry days (7.5% vs 6.6%, within noise; correlation with daily
late share 0.02).

**Evidence.** Open-Meteo daily archive near SFO joined to 271 weekday totals,
`02_when_where_which §8`.

**Caveats.** Only 18 hot days, and 2026-09-09 (36.5 °C, the tenth-worst day in the record)
carries a lot of the effect; residual correlation with temperature is 0.11. Caltrain does
issue heat-related speed restrictions, which is a plausible mechanism, but this is
"suggestive", not "established". Rain's null result is more solid (72 rainy weekdays).

**Visual.** Scatter of daily late share (minus month mean) vs max temperature, hot days
highlighted; a second small panel for rain showing nothing.

---

## Forecasting and anomaly detection: what simple baselines achieve

Reported because you asked for honest numbers, not because they are a "story".
Hold-out: 47 clean weekdays from 2026-07-01 (176k origin/target stop pairs).

| Predict delay *h* stops ahead | schedule (0) | persistence | train history | persistence + median accrual profile |
|---|---|---|---|---|
| h = 1, MAE (min) | 1.47 | 0.62 | 1.26 | **0.45** |
| h = 5 | 1.61 | 0.98 | 1.37 | **0.78** |
| h = 8 | 1.67 | 1.12 | 1.42 | **0.95** |

* Knowing the train's current delay is worth far more than its history; persistence
  alone removes 40% of the schedule's error at five stops ahead.
* Adding the per-train median accrual profile (the sawtooth and the SF approach, learned
  on the training period) removes another 20%. This is the baseline anything learned
  must beat.
* Adding the leader's excess delay over the headway fits a coefficient of 0.04 and changes
  nothing measurable: by the time a follower is stuck, its own current delay already says so.
* Classification "late in 5 stops?": precision 0.86, recall 0.64 at base rate 7%. The
  misses are delays that *start* after the prediction, which position data cannot see.
* Ranking is identical in each hold-out month, including the calm July/August and the
  rough September.

Anomaly rule ("> 50% of arrivals in the last 60 scheduled minutes late, ≥ 20 arrivals"):
catches 19 of the 20 worst days, fires on 13% of other days (mostly real short
disruptions), typically 30–60 minutes after the incident's first late arrivals. At 70%:
14 of 20, 2% false alarms. Details in `04_baselines`.

## Next phase: animating incidents

Everything a frame needs is in the dataset: one position per train per minute, the route
projection used for the diagrams above, and the delay at each train's last observed stop.
Notebook 05's detector gives the days worth animating and the minute to start from. The
open questions for that phase are design ones (map vs route axis, how to show held trains
and cancelled trips, how to mark the unknown cause), not data ones, except that ServiceAlerts
is not collected, so the cause can only be captioned by hand.

## Looked at and set aside

* **Event days.** No Stanford-stop service appears in any archive, no extra trips show up
  on game days, and there is no ridership signal in position data, so Giants/49ers/Stanford
  days cannot be identified from the data itself. Possible with an external fixture list.
* **The February 2026 timetable change** extended all locals to Tamien (one more stop,
  +5–6 minutes); the SF–San Jose portion did not change, so there is no before/after
  experiment on padding. Note it as a context marker only.
* **Monthly trend.** No secular trend; Jul–Aug 2026 were the calmest months (4% late),
  Sep–Oct 2026 worse, but those months are scored against a published feed rather than a
  monthly archive and have few clean days. Not a story.
* **Feed-quality episodes as a story about data honesty.** Unmasked, the Oct 10–17 2026
  window would show as the worst week of the year (10% late, 40% fewer arrivals) and the
  Jul 28–Aug 16 window as the best fortnight (4% late). Worth a sidebar on the site,
  not a headline.
* **South County** (Gilroy) trains run *early* (median −0.2 min NB) but with 6 trains and
  ~1k arrivals per station, too thin for more than a footnote.
