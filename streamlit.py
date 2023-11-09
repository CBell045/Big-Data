# Use this file to follow along with the demo
import polars as pl
import plotly.express as px
import pyarrow.parquet as pq

# STEP 1)
# Documentation - https://docs.streamlit.io/library/api-reference
# Cheat Sheet - https://docs.streamlit.io/library/cheatsheet

import streamlit as st
st.title('Hello Big Data')



# STEP 2)
# read in our dataset
dat = pl.from_arrow(pq.read_table("target.parquet"))
dat = dat.with_columns(pl.col("tractcode").cast(pl.Utf8))



# STEP 3)
# add the percent_active column
dat = dat.with_columns(
    percent_active = (
        pl.when(pl.col("population") == 0)
            .then(0)
            .otherwise(pl.col("target") / pl.col("population") * 100)
    )
)

# STEP 4)
# define our tract groupings 
rexburg_tracts = ["16065950100", "16065950200", "16065950400", "16065950301", "16065950500", "16065950302"]
courd_tracts = ["16055000402", "16055000401", "16055001200", "16055000900"]

# created filtered dataframes for each tract grouping 
filtered_rexburg = dat.filter(pl.col("tractcode").is_in(rexburg_tracts))
filtered_courd = dat.filter(pl.col("tractcode").is_in(courd_tracts))

print(dat)



# STEP 5)
# describe the data
description = dat.select("target").describe()
mean = round(description.select("target")[2].item(), 2)
std = round(description.select("target")[3].item(), 2)
min = round(description.select("target")[4].item(), 2)
max = round(description.select("target")[8].item(), 2)

print(description.transpose(include_header=False, column_names="describe"))
print(f'mean: {mean}\nstd: {std}\nmin: {min}\nmax: {max}')



# STEP 6)
# show histograms
fig1 = px.histogram(filtered_rexburg.sort(by=pl.col("target"), descending=True).head(10), 
                    x="tractcode", 
                    y="target",
                    color_discrete_sequence=['#EA6633'])
fig1.update_layout(title=f"Count of Active Members per Rexburg Tract",
                    xaxis_type="category",
                    bargap=0.2,
                    xaxis_title="Tract Code",
                    yaxis_title="Active Members")

fig2 = px.histogram(filtered_courd.sort(by=pl.col("percent_active"), descending=True).head(10), 
                    x="tractcode", 
                    y="percent_active",
                    color_discrete_sequence=['#33B6EA'])
fig2.update_layout(title=f"Percent of Active Members per Coeur d'Alene Tract",
                    xaxis_type="category",
                    bargap=0.2,
                    xaxis_title="Tract Code",
                    yaxis_title="Percentage of Active LDS Members")
fig2.update_yaxes(range=[0, 100])

fig1.show()
fig2.show()