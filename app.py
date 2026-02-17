import streamlit as st
import pandas as pd
import plotly.express as px

st.set_page_config(
    page_title='NYC Taxi Dashboard',
    page_icon='🚕',
    layout='wide'
)

#Loading Data... 
@st.cache_data
def load_data():
    import requests
    import os

    os.makedirs('data/raw', exist_ok=True)

    zone_path = 'data/raw/taxi_zone_lookup.csv'
    if not os.path.exists(zone_path):
        url = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv'
        r = requests.get(url)
        with open(zone_path, 'wb') as f:
            f.write(r.content)

    
    parquet_path = 'data/raw/taxi_cleaned.parquet'
    if not os.path.exists(parquet_path):
        url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'
        r = requests.get(url, stream=True)
        with open('data/raw/yellow_tripdata_2024-01.parquet', 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    
        df = pd.read_parquet('data/raw/yellow_tripdata_2024-01.parquet')
        df = df.dropna(subset=['tpep_pickup_datetime', 'tpep_dropoff_datetime',
                               'PULocationID', 'DOLocationID', 'fare_amount'])
        df = df[df['trip_distance'] > 0]
        df = df[df['fare_amount'] > 0]
        df = df[df['fare_amount'] <= 500]
        df = df[df['tpep_dropoff_datetime'] > df['tpep_pickup_datetime']]
        df = df[(df['tpep_pickup_datetime'] >= '2024-01-01') &
                (df['tpep_pickup_datetime'] < '2024-02-01')]

        
        df['trip_duration_minutes'] = (
            (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime'])
            .dt.total_seconds() / 60
        )
        df['trip_speed_mph'] = (
            df['trip_distance'] / (df['trip_duration_minutes'] / 60)
        ).where(df['trip_duration_minutes'] > 0, other=0)
        df['pickup_hour'] = df['tpep_pickup_datetime'].dt.hour
        df['pickup_day_of_week'] = df['tpep_pickup_datetime'].dt.day_name()

        df.to_parquet(parquet_path, index=False)

    df = pd.read_parquet(parquet_path)
    zones = pd.read_csv(zone_path)
    return df, zones

df, zones = load_data()

#Payment type labels
payment_labels = {1: 'Credit Card', 2: 'Cash', 3: 'No Charge', 4: 'Dispute', 5: 'Unknown'}
df['payment_label'] = df['payment_type'].map(payment_labels).fillna('Other')

#Title & Introduction
st.title('🚕 NYC Yellow Taxi Trip Dashboard')
st.markdown("""
This dashboard explores **2.8 million NYC Yellow Taxi trips** from January 2024.
Use the filters in the sidebar to explore patterns in fares, trip distances, pickup zones and more.
""")

#Sidebar Filters
st.sidebar.header('Filters')

#Date range selector
min_date = df['tpep_pickup_datetime'].dt.date.min()
max_date = df['tpep_pickup_datetime'].dt.date.max()
date_range = st.sidebar.date_input(
    'Date Range',
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

#Hour range slider
hour_range = st.sidebar.slider(
    'Hour Range',
    min_value=0,
    max_value=23,
    value=(0, 23)
)

#Payment type (allows to select multiple!)
payment_options = df['payment_label'].unique().tolist()
selected_payments = st.sidebar.multiselect(
    'Payment Type',
    options=payment_options,
    default=payment_options
)

#Applying Filters...
if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date = end_date = date_range[0]

filtered_df = df[
    (df['tpep_pickup_datetime'].dt.date >= start_date) &
    (df['tpep_pickup_datetime'].dt.date <= end_date) &
    (df['pickup_hour'] >= hour_range[0]) &
    (df['pickup_hour'] <= hour_range[1]) &
    (df['payment_label'].isin(selected_payments))
]

#Key Metrics!
st.subheader('Key Metrics')
col1, col2, col3, col4, col5 = st.columns(5)
col1.metric('Total Trips', f'{len(filtered_df):,}')
col2.metric('Avg Fare', f'${filtered_df["fare_amount"].mean():.2f}')
col3.metric('Total Revenue', f'${filtered_df["total_amount"].sum():,.0f}')
col4.metric('Avg Distance', f'{filtered_df["trip_distance"].mean():.2f} mi')
col5.metric('Avg Duration', f'{filtered_df["trip_duration_minutes"].mean():.1f} min')

st.markdown('---')

#Tabs
tab1, tab2, tab3 = st.tabs(['Zones & Fares', 'Time Patterns', 'Payment & Distance'])


#Tab 1: Zones & Fares
with tab1:

    #Chart 1: Top 10 pickup zones
    st.subheader('Top 10 Pickup Zones by Trip Count')
    top_zones = (
        filtered_df.merge(zones, left_on='PULocationID', right_on='LocationID')
        .groupby('Zone')
        .size()
        .reset_index(name='total_trips')
        .sort_values('total_trips', ascending=False)
        .head(10)
    )
    fig1 = px.bar(
        top_zones,
        x='total_trips',
        y='Zone',
        orientation='h',
        title='Top 10 Busiest Pickup Zones',
        labels={'total_trips': 'Number of Trips', 'Zone': 'Pickup Zone'},
        color='total_trips',
        color_continuous_scale='Blues'
    )
    fig1.update_layout(yaxis={'categoryorder': 'total ascending'})
    st.plotly_chart(fig1, width='stretch')
    st.markdown("""
    Midtown and Upper East Side dominate pickup activity, reflecting dense office and residential 
    populations. JFK Airport ranks highly due to consistent airport taxi demand throughout the day,
    even outside of typical commute hours.
    """)

    #Chart 2: Average fare by hour
    st.subheader('Average Fare by Hour of Day')
    fare_by_hour = (
        filtered_df.groupby('pickup_hour')['fare_amount']
        .mean()
        .reset_index()
        .rename(columns={'fare_amount': 'avg_fare'})
    )
    fig2 = px.line(
        fare_by_hour,
        x='pickup_hour',
        y='avg_fare',
        title='Average Fare by Hour of Day',
        labels={'pickup_hour': 'Hour of Day', 'avg_fare': 'Average Fare ($)'},
        markers=True
    )
    st.plotly_chart(fig2, width='stretch')
    st.markdown("""
    Fares peak in the early morning hours (4-6 AM), likely reflecting longer airport and 
    cross-borough trips when traffic is light and drivers cover more distance. Evening rush hour 
    (5-7 PM) shows relatively lower fares despite high demand, suggesting shorter congested trips.
    """)


#Tab 2: Time Patterns
with tab2:

    #Chart 3: Heatmap of trips by day and hour
    st.subheader('Trip Volume by Day of Week and Hour')
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    heatmap_data = (
        filtered_df.groupby(['pickup_day_of_week', 'pickup_hour'])
        .size()
        .reset_index(name='trip_count')
    )
    heatmap_data['pickup_day_of_week'] = pd.Categorical(
        heatmap_data['pickup_day_of_week'], categories=day_order, ordered=True
    )
    heatmap_pivot = heatmap_data.pivot(
        index='pickup_day_of_week', columns='pickup_hour', values='trip_count'
    )
    fig3 = px.imshow(
        heatmap_pivot,
        title='Trips by Day of Week and Hour',
        labels={'x': 'Hour of Day', 'y': 'Day of Week', 'color': 'Trip Count'},
        color_continuous_scale='Blues',
        aspect='auto'
    )
    st.plotly_chart(fig3, width='stretch')
    st.markdown("""
    Weekday evenings (5-8 PM) show the highest trip concentration, driven by after-work commuters. 
    Friday and Saturday nights stand out with elevated late-night activity (10 PM - 2 AM), 
    reflecting NYC's nightlife. Early morning weekday trips (6-9 AM) also show strong demand.
    """)

#Tab 3: Payment & Distance
with tab3:

    col1, col2 = st.columns(2)

    #Chart 4: Payment type breakdown
    with col1:
        st.subheader('Payment Type Breakdown')
        payment_counts = (
            filtered_df.groupby('payment_label')
            .size()
            .reset_index(name='count')
        )
        fig4 = px.pie(
            payment_counts,
            names='payment_label',
            values='count',
            title='Breakdown of Payment Types',
            color_discrete_sequence=px.colors.sequential.Blues_r
        )
        st.plotly_chart(fig4, width='stretch')
        st.markdown("""
        Credit card payments dominate at over 80%, reflecting the industry-wide shift toward 
        cashless transactions. Cash usage at ~15% is notable given NYC's push toward digital payments,
        suggesting some riders still prefer or require cash.
        """)

    #Chart 5: Trip distance distribution
    with col2:
        st.subheader('Distribution of Trip Distances')
        fig5 = px.histogram(
            filtered_df[filtered_df['trip_distance'] <= 20],
            x='trip_distance',
            nbins=50,
            title='Trip Distance Distribution',
            labels={'trip_distance': 'Trip Distance (miles)', 'count': 'Number of Trips'},
            color_discrete_sequence=['#1f77b4']
        )
        st.plotly_chart(fig5, width='stretch')
        st.markdown("""
        The vast majority of NYC taxi trips are under 5 miles, confirming that taxis primarily 
        serve short inner-city journeys. The sharp dropoff after 2 miles reflects the dense, 
        walkable nature of Manhattan where most pickups occur.
        """)
