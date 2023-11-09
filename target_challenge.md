# Target Challenge
## Silicon Slayers

### 1. Target
Our target is the number of people attending church for each census tract. We start with filtering the data frame down to just Churches by filtering by location name. We also take out the temples by using their specific addresses. Next we take our newly filtered data and organize it by census tract. We can do this because the data gives us the home census tract for each visitor at the church buildings. After that we calculate Sunday attendance by multiplying the total visitors for each census tract by the ratio of Sunday visits compared to the rest of the week. Finally we verify our results by making sure we have 298 tracts(the number of census tracts in Idaho) and graphing our results.

### 2. Pseudocode
- Load data and packages
- Filter places for LDS churches with reg ex 
- Filter out the Temples
- Join Filter places table to patterns table
- Isolate the month from the date_range_start column and create a month column
- Select needed columns and explode the visitor_home_aggregation
- Create scaled visitors column(logic is tract_visitors * (normalized_visits_by_state_scaling/raw_visit_counts))
- Collect the ratio of Sunday visits to the other weekdays and multiply that number to our new scaled visitors
- Group by tract and month and sum total scaled visitors
- Plot and explore!

### 3. Diagram of tables and columns used to build the feature
```

Church Schema: 
root
 |-- placekey: string (nullable = true)
 |-- location_name: string (nullable = true)


Patterns Schema: 
root
 |-- placekey: string (nullable = true)
 |-- date_range_start: string (nullable = true)
 |-- raw_visit_counts: double (nullable = true)
 |-- popularity_by_day: map (nullable = true)
 |    |-- key: string
 |    |-- value: integer (valueContainsNull = true)
 |-- visitor_home_aggregation: map (nullable = true)
 |    |-- key: string
 |    |-- value: integer (valueContainsNull = true)
 |-- normalized_visits_by_state_scaling: double (nullable = true)
```

### 4. Code Snippet of Data Wrangling
```python
""" 
Assumptions: 
The ratio of Sunday to other days is proportionate for different tracts. 
The state scaling ratio is accurate. 
"""
# Filter to just churches
church_patterns = patterns.join(churches, on='placekey', how='leftsemi')

# visitor_home_aggregation
home_agg = church_patterns.select(
    "*", 
    # Explodes the map of census tracts and visitor counts
    F.explode(
        F.col('visitor_home_aggregation')
        ).alias('tract', 'tract_visitors'),    
    # Gets the state scaling ratio
    (F.col('normalized_visits_by_state_scaling')/F.col('raw_visit_counts')).alias('state_scaling'),
    # Multiplies tract visitors by the state scaling ratio to get a more accurate total estimate
    (F.col('tract_visitors') * F.col('state_scaling')).alias('tract_visitors_scaled')
)

home_agg = home_agg.select(
    "*",
    # Gets the ratio of Sunday visits compared to all days of the week visits
    (F.expr(
        """popularity_by_day['Sunday'] / 
        (popularity_by_day['Monday'] 
        + popularity_by_day['Tuesday']
        + popularity_by_day['Wednesday']
        + popularity_by_day['Thursday']
        + popularity_by_day['Friday']
        + popularity_by_day['Saturday']
        + popularity_by_day['Sunday']
           )""")).alias('sunday_ratio'),
    # Calculates the Sunday visits by census tract
    (F.col('tract_visitors_scaled')*F.col('sunday_ratio')).alias('sunday_visitors')
    
)

home_agg.select('tract', 
                F.month(home_agg['date_range_start']).alias('month'),
                'tract_visitors', 
                'state_scaling', 
                'tract_visitors_scaled', 
                'sunday_ratio', 
                'sunday_visitors').display()
```

### 5. Visualizations
<img src = '/files/Screenshot_2023_10_31_at_8_33_14_PM.png'>
<img src ='/files/newplot__1_.png'>



### 6. Display of the first five rows of your feature table used in the visualizations
| **tract**   | **sum_sunday_visitors** |
|-------------|-------------------------|
| 16001000100 | 38.46542862168799       |
| 16001000201 | 22.925532081082704      |
| 16001000202 | 66.53805417020122       |
| 16001000302 | 51.699551779091514      |
| 16001000303 | 17.566378663817087      |

