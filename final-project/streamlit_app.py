import streamlit as st
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import plotly.express as px

st.set_page_config(layout="wide")
df1 = pd.read_csv("final-project/all_trains1.csv")
df1['Execution'] = 1
df2 = pd.read_csv("all_trains2.csv")
df2['Execution'] = 2
df3 = pd.read_csv("all_trains3.csv")
df3['Execution'] = 3

df = pd.concat([df1, df2, df3])

df["Diff"] = df["Diff"].round()
df = df[~df["Line"].isin(["--", "No"])]

st.sidebar.header("Filter Options")


line_options = sorted(df["Line"].dropna().unique())
location_options = sorted(df["LocationName"].dropna().unique())
min_range = df["Min"].apply(lambda x: int(x) if str(x).isdigit() else None).dropna()

selected_line = st.sidebar.selectbox("Select Rail Line", ["All"] + line_options)
selected_location = st.sidebar.selectbox("Select Station Location", ["All"] + location_options)
min_slider = st.sidebar.slider("Estimated Minutes Away", int(min_range.min()), int(min_range.max()), (int(min_range.min()), int(min_range.max())))

peak_filter = st.sidebar.radio("Peak Hours?", options=["All", "Peak Hours", "Off Peak"])
filtered_df = df.copy()

plot_color = "#ff4b4b"
title_color = "All Lines"
title_location = "All Stations"
if selected_line != "All":
    filtered_df = filtered_df[filtered_df["Line"] == selected_line]
    if selected_line=="BL":
        plot_color = "#0279c1"
        title_color = "Blue Line"
    elif selected_line=="OR":
        plot_color = "#f78e1d"
        title_color = "Orange Line"
    elif selected_line=="SV":
        plot_color = "#d8d8d8"
        title_color = "Silver Line"
    elif selected_line=="YL":
        plot_color = "#ffdd04"
        title_color = "Yellow Line"
    elif selected_line=="GR":
        plot_color = "#01a94f"
        title_color = "Green Line"
    elif selected_line=="RD":
        plot_color = "#ee4135"
        title_color = "Red Line"
if selected_location != "All":
    filtered_df = filtered_df[filtered_df["LocationName"] == selected_location]
    title_location = selected_location

filtered_df = filtered_df[filtered_df["Min"].apply(lambda x: str(x).isdigit())]
filtered_df["Min"] = filtered_df["Min"].astype(int)
filtered_df = filtered_df[
    (filtered_df["Min"] >= min_slider[0]) & (filtered_df["Min"] <= min_slider[1])
]

if peak_filter == "Peak Hours":
    filtered_df = filtered_df[filtered_df["Peak"] == True]
elif peak_filter == "Off Peak":
    filtered_df = filtered_df[filtered_df["Peak"] == False]

arrivals_df = filtered_df.dropna(subset=["BRD_Time"]).copy()
arrivals_df["BRD_Time"] = pd.to_datetime(arrivals_df["BRD_Time"])
arrivals_df = arrivals_df.drop_duplicates(
    subset=["Line", "LocationName", "Destination", "BRD_Time"]
)

arrivals_df = arrivals_df.sort_values(by=["Line", "LocationName", "Destination", "BRD_Time"])

arrivals_df["GapMin"] = (
    arrivals_df.groupby(["Line", "LocationName", "Destination", "Execution"])["BRD_Time"]
    .diff()
    .dt.total_seconds()
    .div(60)
)

arrivals_df = arrivals_df.dropna(subset=["GapMin"])

st.title("WMATA Rail Punctuality Analysis")

st_col1, st_col2 = st.columns(2)

with st_col1:
    hist_fig = px.histogram(
        filtered_df,
        x="Diff",
        nbins=int(filtered_df["Diff"].max() - filtered_df["Diff"].min()) + 1,
        title="Distribution of Prediction Error - " + title_color + " - " + title_location,
        labels={"Diff": "Actual Arrival minus Predicted Arrival (minutes)", "count": "Count"},
        color_discrete_sequence=[plot_color],
        histnorm="probability"
    )
    hist_fig.update_layout(bargap=0.05,height=350, margin=dict(t=30, b=30),yaxis_title="Proportion")
    st.plotly_chart(hist_fig, use_container_width=True)

gap_filtered = arrivals_df.copy()
if selected_line != "All":
    gap_filtered = gap_filtered[gap_filtered["Line"] == selected_line]
if selected_location != "All":
    gap_filtered = gap_filtered[gap_filtered["LocationName"] == selected_location]

with st_col2:
    gap_fig = px.histogram(
        gap_filtered,
        x="GapMin",
        nbins=30,
        title="Time Between Train Arrivals - " + title_color + " - " + title_location,
        labels={"GapMin": "Time Between Trains (minutes)"},
        color_discrete_sequence=[plot_color],
        histnorm="probability"
    )
    gap_fig.update_layout(
        bargap=0.05,
        height=350,
        margin=dict(t=30, b=30),
        xaxis_title="Gap (minutes)",
        yaxis_title="Proportion"
    )
    st.plotly_chart(gap_fig, use_container_width=True)

avg_diff = filtered_df.groupby("Min", as_index=False)["Diff"].mean()

box_fig = px.box(
    filtered_df,
    x="Min",
    y="Diff",
    points="outliers",
    title= "Prediction Error Box Plots by Estimated Minutes Out - " + title_color + " - " + title_location,
    labels={"Min": "Estimated Minutes Out", "Diff": "Prediction Error (minutes)"},
    color_discrete_sequence=[plot_color]
)

box_fig.update_yaxes(
    zeroline=True,
    zerolinewidth=1,
    zerolinecolor="black"
)
box_fig.update_layout(height=350, margin=dict(t=30, b=30))
st.plotly_chart(box_fig, use_container_width=True)

