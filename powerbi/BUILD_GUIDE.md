# Build the "Game State" dashboard in Power BI — a first-timer's guide

**What you'll build:** a 4-page Power BI report that answers one question —
*how does the scoreline change the way teams play?* — using every event from all
64 matches of the 2022 FIFA World Cup.

**The headline you'll prove:** teams protecting a one-goal lead give up the ball,
drop their press deeper and counter more — they take fewer but better shots, yet
concede **30% more xG per 90** than when the game is level.

**Time:** about 4–6 hours spread over a few sittings. Each part ends with a
✅ **Checkpoint** — a number you should see. If yours matches, move on.

> 📘 **Learning boxes** like this one explain *why*, not just *what*. They're the
> things interviewers ask about, so don't skip them.

---

## Part 0 — Setup (15 min)

1. Install **Power BI Desktop** (free) from the Microsoft Store on Windows.
   The Store version updates itself, which matters because menus move between releases.
2. Download this repository: on GitHub, **Code → Download ZIP**, and unzip it somewhere
   permanent such as `Documents\statsbomb-elt-pipeline`.
   Power BI remembers file paths, so don't move the folder after you load the data.
3. Open Power BI Desktop → **Blank report**. Save it immediately as
   `powerbi\game_state_dashboard.pbix`, and save often (Ctrl+S).
4. **Turn off Auto date/time:** File → Options and settings → Options →
   *Current file* → Data load → untick **Auto date/time**.

> 📘 Auto date/time silently creates a hidden calendar table for every date column.
> In a real model you control your own dimensions — turning it off is a habit
> that signals you know what you're doing.

### What's in `powerbi/data/`

These CSVs were produced by this repo's pipeline (extract → PySpark → dbt → `make powerbi-export`).

| File | Rows | Grain (one row per…) | Role |
|---|---|---|---|
| `fct_team_game_state.csv` | 326 | match × team × game state | **Fact** — minutes, shots, xG, passes, pressures… |
| `fct_shots.csv` | 1,453 | shot | **Fact** — location, xG, outcome, game state |
| `dim_game_state.csv` | 5 | game state | **Dimension** — Losing by 2+ … Winning by 2+, plus colours |
| `dim_team.csv` | 32 | team | **Dimension** |
| `dim_match.csv` | 64 | match | **Dimension** — label, stage, date |
| `dim_player.csv` | 428 | player | **Dimension** — players who took a shot |

**Game state** = the score difference *from that team's point of view* at the moment
something happened. A shot that scores is taken while "Drawing"; everything after it
happens while "Winning by 1" (and, for the opponent, "Losing by 1").

---

## Part 1 — Load and shape the data (30 min)

1. **Home → Get data → Text/CSV** → pick `fct_team_game_state.csv` → **Transform Data**
   (not *Load*). This opens **Power Query**, the data-shaping tool.
2. In Power Query: **Home → New Source → Text/CSV** to add the other five files.
   You now have six queries in the left pane.
3. Check the **data type** icon at the top of each column (`123` = whole number,
   `1.2` = decimal, `ABC` = text, calendar = date, ✓✗ = True/False). Fix any that
   are wrong by clicking the icon:

   | Columns | Type |
   |---|---|
   | every `*_key` column **except** `game_state_key`, plus `event_id` | Text |
   | `match_id`, `game_state_key`, counts (`shots`, `passes`, …), `minute`, `period` | Whole number |
   | `xg`, `xg_conceded`, `minutes_in_state`, `pressure_x_sum`, `location_x/y`, `shot_distance_to_goal` | Decimal number |
   | `is_home`, `went_to_extra_time`, `went_to_penalties`, `shot_first_time`, `is_under_pressure` | True/False |
   | `match_date` | Date |
   | `is_goal`, `is_on_target` | Whole number (they're 0/1 so you can SUM them) |

   If decimals arrive as huge whole numbers (e.g. `15666` instead of `1.5666`), your
   Windows region uses a decimal comma: right-click the column → **Change Type → Using Locale…**
   → Decimal Number, English (United States).

4. In `fct_shots`, add a **minute bucket** for a "when do shots happen" chart:
   **Add Column → Conditional Column** named `minute_bucket`:
   `minute < 15 → "0–15"`, `< 30 → "15–30"`, `< 45 → "30–45"`, `< 60 → "45–60"`,
   `< 75 → "60–75"`, `< 90 → "75–90"`, otherwise `"90+"`.
5. **Home → Close & Apply.**

> 📘 **Power Query vs DAX:** Power Query (M language) *shapes* data before it's loaded —
> types, cleaning, new columns that don't depend on filters. DAX *calculates* on top of the
> loaded model and reacts to slicers. Rule of thumb: if the answer never changes when a user
> clicks a slicer, do it in Power Query (or upstream in dbt, as this project does).

✅ **Checkpoint:** in **Table view** (left rail, grid icon), `fct_shots` shows 1,453 rows
and `fct_team_game_state` 326 rows (row count is at the bottom-left).

---

## Part 2 — Build the star schema (30 min)

Open **Model view** (left rail, the three-connected-boxes icon).

1. Power BI may have guessed some relationships. **Delete them all** (click a line → Delete)
   — you'll build the right ones yourself.
2. Drag column onto column to create these relationships. In each one the dimension is
   the "1" side and the fact is the "many" (`*`) side:

   | From (fact, many) | To (dimension, one) |
   |---|---|
   | `fct_team_game_state[game_state_key]` | `dim_game_state[game_state_key]` |
   | `fct_team_game_state[team_key]` | `dim_team[team_key]` |
   | `fct_team_game_state[match_id]` | `dim_match[match_id]` |
   | `fct_shots[game_state_key]` | `dim_game_state[game_state_key]` |
   | `fct_shots[team_key]` | `dim_team[team_key]` |
   | `fct_shots[match_id]` | `dim_match[match_id]` |
   | `fct_shots[player_key]` | `dim_player[player_key]` |

   Double-click each line and confirm **Cardinality: Many to one (\*:1)** and
   **Cross filter direction: Single**. Leave `opponent_team_key` unconnected.

3. Arrange it as a star: dimensions on the outside, the two facts in the middle.
   **Take a screenshot of this** — it goes in your README and portfolio.

4. **Sort by column** so states never appear alphabetically ("Drawing, Losing, Winning"):
   in Table view select `dim_game_state[game_state]` → **Column tools → Sort by column →
   `game_state_sort`**. Do the same for `game_state_detail` → `game_state_detail_sort`,
   and `dim_match[competition_stage_name]` → `stage_sort`.

5. **Hide the plumbing:** in Model view right-click → *Hide in report view* for every
   `*_key`, `*_sort`, `*_color`, `event_id` and `match_id` column. Report builders
   should only see fields that mean something.

6. **Make a measures table:** Home → **Enter data** → name it `_Measures` → Load.
   You'll put every measure here.

> 📘 **Why a star schema?** Facts hold numbers at a fixed grain; dimensions hold the
> descriptive attributes you slice by. Filters flow *from* the one side *to* the many side,
> so selecting "Japan" in `dim_team` filters both fact tables at once. Two facts can share
> dimensions ("conformed dimensions") — that's how one slicer drives both the shot map and
> the possession charts. Bidirectional filters and many-to-many relationships are where
> wrong numbers come from; you don't need either here.

---

## Part 3 — DAX measures (60–90 min)

Select `_Measures` → **Home → New measure**, type the formula in the bar, press Enter.
**Each formula below is a separate measure** — a block with five lines means five
*New measure* clicks. Multi-line formulas (with `VAR`) are one measure; press
Shift+Enter for new lines inside the formula bar. Set formats in **Measure tools** (% for percentages, 2 decimals for xG).

### 3a. The denominator

```dax
Minutes = SUM ( fct_team_game_state[minutes_in_state] )
```

```dax
Share of Time =
DIVIDE ( [Minutes], CALCULATE ( [Minutes], REMOVEFILTERS ( dim_game_state ) ) )
```

> 📘 **Why "per 90"?** A team that led for 80 minutes will obviously have more shots
> while leading. Comparing raw totals is meaningless; you compare **rates per 90 minutes
> in that state**. This is the single most important idea in the dashboard.
>
> 📘 **`CALCULATE`** is the heart of DAX: it evaluates an expression under *modified*
> filters. `REMOVEFILTERS(dim_game_state)` says "ignore whichever game state this row or
> slicer is showing" — giving you the total to divide by. Learn this one well.

### 3b. Attacking and defending rates

```dax
Shots per 90 = DIVIDE ( SUM ( fct_team_game_state[shots] ), [Minutes] ) * 90
xG per 90 = DIVIDE ( SUM ( fct_team_game_state[xg] ), [Minutes] ) * 90
xG Conceded per 90 = DIVIDE ( SUM ( fct_team_game_state[xg_conceded] ), [Minutes] ) * 90
xG Difference per 90 = [xG per 90] - [xG Conceded per 90]
xG per Shot = DIVIDE ( SUM ( fct_team_game_state[xg] ), SUM ( fct_team_game_state[shots] ) )
```

### 3c. Style of play

```dax
Possession % =
VAR ours = SUM ( fct_team_game_state[passes] )
VAR theirs = SUM ( fct_team_game_state[opponent_passes] )
RETURN DIVIDE ( ours, ours + theirs )

Pass Completion % =
DIVIDE ( SUM ( fct_team_game_state[passes_completed] ), SUM ( fct_team_game_state[passes] ) )

Long Ball % =
DIVIDE ( SUM ( fct_team_game_state[long_passes] ), SUM ( fct_team_game_state[passes] ) )

Pressing Height =
DIVIDE ( SUM ( fct_team_game_state[pressure_x_sum] ), SUM ( fct_team_game_state[pressures] ) )

PPDA =
DIVIDE (
    SUM ( fct_team_game_state[opponent_passes_own_60pct] ),
    SUM ( fct_team_game_state[def_actions_opp_60pct] )
)
```

> 📘 **Never average an average.** The fact table stores `pressure_x_sum` and `pressures`,
> not "average pressure height". If you averaged per-row averages, a 2-minute spell with
> 3 pressures would count as much as an 80-minute spell with 120. Summing numerator and
> denominator separately, then dividing, is always correct at every level of the visual.
>
> 📘 **`VAR … RETURN`** stores an intermediate result. It makes formulas readable and
> avoids computing the same thing twice.
>
> **Metric definitions:** *Possession %* is a team's share of the passes made in that
> state (a standard proxy). *Pressing Height* is the average x-coordinate of a team's
> pressures (0 = own goal line, 120 = opponent's; higher = pressing further up).
> *PPDA* (passes per defensive action) is opponent passes in their own 60% of the pitch
> divided by our tackles + interceptions + fouls there — **lower = more intense press**.

### 3d. Comparisons for the KPI cards

```dax
Possession % (Drawing) = CALCULATE ( [Possession %], dim_game_state[game_state] = "Drawing" )
Possession vs Drawing (pts) = ( [Possession %] - [Possession % (Drawing)] ) * 100

Pressing Height (Drawing) = CALCULATE ( [Pressing Height], dim_game_state[game_state] = "Drawing" )
Pressing Height vs Drawing = [Pressing Height] - [Pressing Height (Drawing)]

xG Conceded per 90 (Drawing) = CALCULATE ( [xG Conceded per 90], dim_game_state[game_state] = "Drawing" )
xG Conceded vs Drawing % = DIVIDE ( [xG Conceded per 90], [xG Conceded per 90 (Drawing)] ) - 1

xG per Shot (Drawing) = CALCULATE ( [xG per Shot], dim_game_state[game_state] = "Drawing" )
xG per Shot vs Drawing % = DIVIDE ( [xG per Shot], [xG per Shot (Drawing)] ) - 1
```

> 📘 `CALCULATE([measure], column = "value")` **replaces** any existing filter on that
> column. So even when a card is filtered to "Winning by 1", the "(Drawing)" measure
> still looks at Drawing — that's what makes "vs Drawing" work.

### 3e. Shot measures

```dax
Shots = COUNTROWS ( fct_shots )
Shot Goals = SUM ( fct_shots[is_goal] )
Shot xG = SUM ( fct_shots[xg] )
Goals minus xG = [Shot Goals] - [Shot xG]
Counter-attack Shot % =
DIVIDE ( CALCULATE ( [Shots], fct_shots[play_pattern_name] = "From Counter" ), [Shots] )
```

### 3f. For the team page

```dax
Possession % (Winning) = CALCULATE ( [Possession %], dim_game_state[game_state] = "Winning" )
Share of Time Leading = CALCULATE ( [Share of Time], dim_game_state[game_state] = "Winning" )

Team Page Title =
"How " & SELECTEDVALUE ( dim_team[team_name], "all 32 teams" ) & " play by scoreline"
```

> 📘 `SELECTEDVALUE` returns the single selected value, or the fallback if zero or many
> are selected — the standard way to make dynamic titles.

✅ **Checkpoint:** drop a **Table** visual on a scratch page with
`dim_game_state[game_state_detail]` and the measures below. You should see:

| game_state_detail | Share of Time | Possession % | Pressing Height | xG Conceded per 90 | xG per Shot |
|---|---|---|---|---|---|
| Losing by 2+ | 8.6% | 46.3% | 56.8 | 1.34 | 0.096 |
| Losing by 1 | 15.6% | 55.8% | 58.6 | 1.06 | 0.116 |
| Drawing | 51.7% | 50.0% | 57.0 | 0.97 | 0.098 |
| Winning by 1 | 15.6% | 44.2% | 53.8 | 1.26 | 0.125 |
| Winning by 2+ | 8.6% | 53.7% | 57.0 | 0.88 | 0.129 |

Also: `Shots` = 1,453, `Shot Goals` = 169, `Shot xG` ≈ 155.9.
(172 tournament goals − 3 own goals = 169 goals from shots.) Delete the scratch page after.

If your numbers are off, the usual culprits are a wrong data type (Part 1, step 3) or a
relationship pointing the wrong way (Part 2).

---

## Part 4 — Theme and colour (10 min)

**View → Themes → Browse for themes →** `powerbi/game_state_theme.json`.

The colour rule for the whole report: **red = losing, grey = drawing, blue = winning**,
darker = bigger margin. These colours live *in the data* (`dim_game_state[game_state_color]`
and `[game_state_detail_color]`), and you apply them with conditional formatting:

> On a bar/column chart: Format → **Columns → Color → fx** → *Format style:* **Field value**
> → *What field should we base this on?* `game_state_detail_color` (or `game_state_color`)
> → *Summarization:* First.

> 📘 **Colour follows the entity.** "Winning by 1" is the same blue on every page, whatever
> chart it's in and whatever is filtered. Storing the colour in the dimension guarantees it.
> The palette is a diverging red–grey–blue scale checked for colour-blind separation; the
> light grey is low-contrast on purpose (it means "nothing happening"), so always turn
> **Data labels** on so values don't rely on colour alone.

---

## Part 5 — Build the pages

Canvas: **Format page → Canvas settings → 16:9**. Keep a 24px margin around the edge and
align visuals with **Format → Align**. One message per page; the page title states it.

### Page 1 — "The Story"

```
┌─────────────────────────────────────────────────────────────────────────┐
│ Holding a lead changes how teams play — and not for the better          │
│ FIFA World Cup 2022 · 64 matches · 234k events · StatsBomb open data   │
├───────────────┬───────────────┬───────────────┬─────────────────────────┤
│ Possession    │ Pressing      │ xG conceded   │ xG per shot             │
│ 44.2%         │ 53.8          │ 1.26 / 90     │ 0.125                   │
│ −5.8 pts      │ −3.3 yds      │ +30%          │ +27%                    │
│      (all four: "Winning by 1" vs "Drawing")                            │
├──────────────────────────────────┬──────────────────────────────────────┤
│ Possession % by game state       │ Pressing height by game state        │
├──────────────────────────────────┼──────────────────────────────────────┤
│ xG conceded per 90 by game state │ xG per shot by game state            │
├──────────────────────────────────┴──────────────────────────────────────┤
│ Share of time in each state  [■■■|■■■■■|■■■■■■■■■■■■■■■■|■■■■■|■■■]    │
└─────────────────────────────────────────────────────────────────────────┘
```

1. **Title:** Insert → **Text box**. Title in 20pt semibold, subtitle 11pt grey.
2. **KPI cards (×4):** Insert → **Card (new)**. Values: `Possession %`.
   In the Filters pane, drag `game_state_detail` into *Filters on this visual* → tick only
   **Winning by 1**. Under Format → **Reference labels**, add `Possession vs Drawing (pts)`
   with the title "vs Drawing". Repeat for `Pressing Height` / `Pressing Height vs Drawing`,
   `xG Conceded per 90` / `xG Conceded vs Drawing %`, `xG per Shot` / `xG per Shot vs Drawing %`.
   *(If your version has no Reference labels, put the "vs" measure in a second, smaller card underneath.)*
3. **Four column charts**, one measure each: X-axis `game_state_detail`, Y-axis the measure.
   Colour with conditional formatting from `game_state_detail_color` (Part 4). Data labels on;
   Y-axis title off; gridlines faint.
4. **Share of time:** **100% stacked bar chart**, Legend = `game_state_detail`,
   X = `Share of Time`. Set each legend colour manually to match the palette:
   `#9E1B2B`, `#D6604D`, `#ADADAD`, `#4A90C8`, `#1F5294`.

> 📘 **Why four charts and not one chart with four lines?** The measures have different
> units (%, pitch-yards, xG). Putting them on one chart forces a second y-axis, which
> lets the designer make any two lines appear to cross. Small multiples — same X axis,
> one measure each — are honest and easier to read.

✅ **Checkpoint:** the four cards read **44.2% · 53.8 · 1.26 · 0.125**, with
**−5.8 pts · −3.3 · +30% · +27%**.

### Page 2 — "Where the shots come from"

1. **Shot map:** Insert → **Scatter chart**.
   - X-axis: `location_x` (set to **Don't summarize**) · Y-axis: `location_y` (**Don't summarize**)
   - Values / Details: `event_id` (this makes one dot per shot) · Size: `xg` · Legend: `game_state`
   - Format → **X-axis:** range min 60, max 120, values off, title off. **Y-axis:** min 0, max 80,
     values off, title off. Gridlines off.
   - Format → **Plot area background → Image:** `powerbi/assets/pitch_half.png`, *Image fit:* **Fit**,
     transparency 0%.
   - Make the visual roughly **3 wide : 4 tall** (e.g. 450 × 600 px) and move the legend to the top,
     so the plot area matches the image. Check alignment: the penalties should sit on the penalty spot.
   - Legend colours: Losing `#D6604D`, Drawing `#8E8E8E`, Winning `#2E78B7`. Markers ≥ 8px.
2. **Slicers** in one row above the map: `game_state` (Format → Slicer settings → Style: **Tile**),
   `dim_team[team_name]` (Dropdown), `shot_body_part_name` (Dropdown).
3. **Bar chart:** Y = `play_pattern_name`, X = `Shots`. Next to it, a card with `Counter-attack Shot %`.
   Click "Winning" on the slicer and watch it jump from 2.9% (drawing) to **6.7%**.
4. **Column chart:** X = `minute_bucket`, Y = `Shots`, Legend = `game_state`.
5. **Table:** `player_display_name`, `Shots`, `Shot Goals`, `Shot xG`, `Goals minus xG`, sorted by
   Shot Goals. Add a **data bar** to `Goals minus xG` (Format → Cell elements).

> 📘 **Tooltips:** hover over a shot — Power BI shows the fields on the visual. Drag
> `player_display_name`, `minute`, `shot_outcome_name` and `match_label` into the **Tooltips**
> well so every dot tells its story.

### Page 3 — "Team explorer"

1. **Title:** a card (or text box with a *value* from a measure) showing `Team Page Title`.
2. **Slicer:** `dim_team[team_name]`, Dropdown.
3. **Matrix:** Rows `team_name` · Columns `game_state` · Values `Possession %`.
   Format → Cell elements → **Background colour → fx → Gradient**, diverging:
   min `#D6604D`, centre `#F0F0EE` at 50%, max `#2E78B7`.
   Add `Minutes` as a second value to show how much evidence each cell has.
4. **"Who changes most?" scatter:** Values = `team_name` · X = `Possession % (Drawing)` ·
   Y = `Possession % (Winning)` · Size = `Share of Time Leading`. Turn on **category labels**.
   Add an **X = Y reference line** mentally: teams below the diagonal sit back when they lead.
5. **Annotation:** a text box pointing at Japan: *"Japan beat Germany and Spain with 19% of the
   ball while ahead."*

✅ **Checkpoint:** select **Japan** → Possession % by state reads Losing 35.1%, Drawing 40.7%,
Winning **18.9%**. Select **Argentina** → Share of Time Leading ≈ **51%** (the most of any team).

### Page 4 — "Methodology"

A text-only page — this is what makes the project credible:

- **Data:** StatsBomb Open Data, FIFA World Cup 2022, all 64 matches, ~234k events.
- **Pipeline:** Python extract → PySpark → dbt (DuckDB / Athena) with 190+ data tests →
  CSV → Power BI. Insert the image `docs/images/dbt-lineage.png`.
- **Definitions:** game state, per-90, Possession %, Pressing Height, PPDA (from Part 3).
- **Data quality:** shoot-outs excluded; own goals credited to the benefiting team; game-state
  totals reconcile to official scores, shots and xG (dbt tests); events ordered by clock time
  because a few source events are indexed out of order.
- **Limitations — say these out loud:**
  1. *Selection effect:* better teams lead more often, so "Winning" is partly "being good".
     The patterns are associations, not proof that sitting back *causes* anything.
  2. *Sample size:* 64 matches; "Losing by 2+" covers ~9.5 match-hours in total. Show `Minutes`
     next to rates.
  3. *Possession* is estimated from pass share, not ball-in-play time.
- **Credit:** "Data: StatsBomb Open Data" — required by StatsBomb's licence for public work.

---

## Part 6 — Polish checklist

- [ ] Every page title states the finding, not the chart type ("Leaders press 3 yards deeper", not "Pressing chart")
- [ ] Same colour = same game state on every page
- [ ] No visual has a second Y-axis
- [ ] Units in titles (“per 90”, “%”, “pitch yards”)
- [ ] Data labels on bars; gridlines faint or off
- [ ] Slicers in one row at the top of the page they control
- [ ] Hidden key/sort/colour columns; measures in `_Measures` with folders
      (Model view → select measures → *Display folder*: `Attack`, `Style`, `Comparisons`, `Shots`)
- [ ] **View → Mobile layout** — drag the cards and one chart onto the phone canvas
- [ ] **Edit interactions** (Format → Edit interactions): make the KPI cards on page 1 *not* respond to the column charts

---

## Part 7 — Make it resume-ready

1. **Commit the file:** save `powerbi/game_state_dashboard.pbix` and push it.
2. **Screenshots:** export each page (File → Export → **Export to PDF**, or Windows Snipping Tool)
   into `docs/images/powerbi-*.png` and add them to the main README. Recruiters look at images,
   not `.pbix` files. A 20-second GIF of clicking the slicers (ScreenToGif is free) is even better.
3. **Publishing online:** *Publish to web* needs a work/school Microsoft account and an admin who
   allows it — personal Gmail/Outlook accounts can't sign up for the Power BI Service. If you have a
   student account, publish there and link it; otherwise the screenshots + PDF + `.pbix` are enough.
4. **Resume bullets** (edit to taste):
   - *Built an end-to-end sports analytics platform (Airflow, PySpark, dbt, Terraform/AWS) and a
     Power BI dashboard analysing how scorelines change team behaviour across 234k World Cup 2022 events.*
   - *Designed a game-state star schema in dbt with reconciliation tests guaranteeing per-state
     shots, goals and xG tie back to official match totals.*
   - *Found that teams protecting a one-goal lead conceded 30% more xG per 90 despite producing
     the highest-quality chances (xG/shot +27%); modelled rate metrics in DAX (per-90, PPDA, pressing height).*
5. **Be ready to answer:**
   - *Why per 90?* → time in state differs hugely; raw totals mislead.
   - *Why does the fact store sums, not averages?* → additivity (Part 3c).
   - *Does sitting back cause you to concede?* → not proven; selection effect, small sample (Page 4).
   - *How do you know the numbers are right?* → dbt reconciliation tests + the checkpoint table.
   - *What would you add next?* → more tournaments (Euro 2024, Women's World Cup 2023) for a bigger
     sample; a model controlling for team strength.

---

## Going further (optional, once it's done)

- **Field parameters:** Modeling → New parameter → *Fields*, containing the 4 style measures, so
  one chart can switch between them with a slicer.
- **Bookmarks:** a "Show only Winning by 1" button on page 1.
- **Add a tournament:** in the repo run `make pipeline COMPETITIONS=43:106,55:282 MAX_MATCHES=0`
  (World Cup 2022 + Euro 2024) and `make powerbi-export`, then **Home → Refresh** in Power BI.
  Add `competition_key` back to the export to slice by tournament.
- **Tooltip page:** a small page (Page information → *Allow use as tooltip*) with a mini shot map,
  assigned as the tooltip of the matrix on page 3.
