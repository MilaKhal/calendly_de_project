import streamlit as st
import pandas as pd
from pyathena import connect
import altair as alt


st.set_page_config(layout="wide")  # 👈 must be here, before any st.* calls

# --- Athena Query Wrapper ---
def query_athena(sql):
    conn = connect(
        s3_staging_dir=st.secrets["s3_staging_dir"],
        region_name=st.secrets["aws_region"],
        aws_access_key_id=st.secrets["aws_access_key_id"],
        aws_secret_access_key=st.secrets["aws_secret_access_key"]
    )
    return pd.read_sql(sql, conn)
# --- Cached Queries (refresh daily) ---
@st.cache_data(ttl=86400)
def get_cost_per_lead(): #horizontal bar chart by channel
    return query_athena("""
    SELECT channel, 
        ROUND(SUM(spend)/COUNT(event_id),0) AS cost_per_lead
        FROM calendly_project.calendly_events
        INNER JOIN calendly_project.event_type_channels  USING (event_type)
        INNER JOIN calendly_project.daily_spends ON channel_name = channel
        GROUP BY 1
        ORDER BY 2                
                        
    """)

@st.cache_data(ttl=86400)
def get_daily_leads_by_channel(): #line chart by date, colors by channel
    return query_athena("""
        SELECT channel, 
        DATE(booking_datetime) AS booking_date,
        COUNT(event_id) AS n_events
        FROM calendly_project.calendly_events
        INNER JOIN calendly_project.event_type_channels  USING (event_type)
        INNER JOIN calendly_project.daily_spends ON channel_name = channel
        GROUP BY 1,2
        ORDER BY 1
        
    """)

@st.cache_data(ttl=86400)
def get_channel_leaderboard(): # table
    return query_athena("""
    SELECT channel,
    ROUND(SUM(spend),0) AS spend,
    COUNT(event_id) AS bookings,
    ROUND(SUM(spend)/COUNT(event_id),0) AS cost_per_lead
    FROM calendly_project.calendly_events
    INNER JOIN calendly_project.event_type_channels  USING (event_type)
    INNER JOIN calendly_project.daily_spends ON channel_name = channel
    GROUP BY 1
    ORDER BY 2 
    """)

@st.cache_data(ttl=86400)
def get_bookings_by_day(): # bar chart by day of week
    return query_athena("""
    SELECT 
    format_datetime(booking_datetime, 'EEEE') AS booking_day,
    COUNT(event_id) AS bookings
    FROM calendly_project.calendly_events
    INNER JOIN calendly_project.event_type_channels  USING (event_type)
    INNER JOIN calendly_project.daily_spends ON channel_name = channel
    GROUP BY 1, day_of_week(booking_datetime)
    ORDER BY day_of_week(booking_datetime)  
    """)

@st.cache_data(ttl=86400)
def get_bookings_by_hour(): # line chart 
    return query_athena("""
        SELECT 
        hour(booking_datetime) AS booking_hour,
        COUNT(event_id) AS bookings
        FROM calendly_project.calendly_events
        INNER JOIN calendly_project.event_type_channels  USING (event_type)
        INNER JOIN calendly_project.daily_spends ON channel_name = channel
        GROUP BY 1
        ORDER BY 1
    """)

@st.cache_data(ttl=86400)
def get_meeting_by_day(): # bar chart by day of week
    return query_athena("""
        SELECT 
        format_datetime(event_datetime, 'EEEE') AS meeting_day,
        COUNT(event_id) AS bookings
        FROM calendly_project.calendly_events
        INNER JOIN calendly_project.event_type_channels  USING (event_type)
        INNER JOIN calendly_project.daily_spends ON channel_name = channel
        GROUP BY 1, day_of_week(event_datetime)
        ORDER BY day_of_week(event_datetime)  
    """)

@st.cache_data(ttl=86400)
def get_meeting_by_hour(): # bar chart by day of week
    return query_athena("""
        SELECT 
        hour(event_datetime) AS event_hour,
        COUNT(event_id) AS bookings
        FROM calendly_project.calendly_events
        INNER JOIN calendly_project.event_type_channels  USING (event_type)
        INNER JOIN calendly_project.daily_spends ON channel_name = channel
        GROUP BY 1
        ORDER BY 1
    """)
@st.cache_data(ttl=86400)
def get_meetings_by_employee_per_day(): # horizontal bar chart by employee
    return query_athena("""
        SELECT 
        employee_name,
        count(DISTINCT event_id)/count(DISTINCT(DATE(event_datetime))) AS meetings_per_day
        FROM calendly_project.calendly_events
        INNER JOIN calendly_project.event_memberships  USING (event_id)
        INNER JOIN calendly_project.employees USING(employee_id)
        GROUP BY 1
        ORDER BY 2 DESC
    """)

# --- App UI ---
st.title("📊 Calendly Project Dashboard ")
st.markdown("Data updates every 24 hours. Click below to refresh manually if needed.")

if st.button("🔄 Refresh Data Now"):
    st.cache_data.clear()
    st.success("Cache cleared! Please rerun to fetch fresh data.")

# --- Cost per Lead (Horizontal Bar) + Daily Leads by Channel (Line Chart) ---
col1, col2 = st.columns(2)

with col1:
    st.subheader("💰 Cost per Lead by Channel")
    df_cpl = get_cost_per_lead()
    chart_cpl = alt.Chart(df_cpl).mark_bar().encode(
        x=alt.X("cost_per_lead:Q", title="Cost per Lead"),
        y=alt.Y("channel:N", sort="-x", title="Channel"),
        tooltip=["channel", "cost_per_lead"]
    ).properties(height=300)
    st.altair_chart(chart_cpl, use_container_width=True)

with col2:
    st.subheader("📈 Daily Leads by Channel")
    df_daily_leads = get_daily_leads_by_channel()
    chart_daily = alt.Chart(df_daily_leads).mark_line(point=True).encode(
        x=alt.X("booking_date:T", title="Date"),
        y=alt.Y("n_events:Q", title="Number of Events"),
        color=alt.Color("channel:N", title="Channel"),
        tooltip=["booking_date", "channel", "n_events"]
    ).properties(height=300)
    st.altair_chart(chart_daily, use_container_width=True)


# --- Channel Leaderboard (Table) ---
st.subheader("🏆 Channel Leaderboard")
df_leaderboard = get_channel_leaderboard()
st.dataframe(df_leaderboard, use_container_width=True)


# --- Bookings by Day (Bar) + Bookings by Hour (Line) ---
col3, col4 = st.columns(2)

with col3:
    st.subheader("📅 Bookings by Day of Week")
    df_by_day = get_bookings_by_day()
    chart_day = alt.Chart(df_by_day).mark_bar().encode(
        x=alt.X("booking_day:N", title="Day of Week", sort=["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]),
        y=alt.Y("bookings:Q", title="Bookings"),
        tooltip=["booking_day", "bookings"]
    ).properties(height=300)
    st.altair_chart(chart_day, use_container_width=True)

with col4:
    st.subheader("⏰ Bookings by Hour")
    df_by_hour = get_bookings_by_hour()
    chart_hour = alt.Chart(df_by_hour).mark_line(point=True).encode(
        x=alt.X("booking_hour:O", title="Hour (24h)"),
        y=alt.Y("bookings:Q", title="Bookings"),
        tooltip=["booking_hour", "bookings"]
    ).properties(height=300)
    st.altair_chart(chart_hour, use_container_width=True)


# --- Meetings by Day (Bar) + Meetings by Hour (line) ---
col5, col6 = st.columns(2)

with col5:
    st.subheader("📅 Meetings by Day of Week")
    df_meeting_day = get_meeting_by_day()
    chart_meet_day = alt.Chart(df_meeting_day).mark_bar().encode(
        x=alt.X("meeting_day:N", title="Day of Week", sort=["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]),
        y=alt.Y("bookings:Q", title="Meetings"),
        tooltip=["meeting_day", "bookings"]
    ).properties(height=300)
    st.altair_chart(chart_meet_day, use_container_width=True)

with col6:
    st.subheader("⏰ Meetings by Hour")
    df_meeting_hour = get_meeting_by_hour()
    chart_meet_hour = alt.Chart(df_meeting_hour).mark_line(point=True).encode(
        x=alt.X("event_hour:O", title="Hour (24h)"),
        y=alt.Y("bookings:Q", title="Meetings"),
        tooltip=["event_hour", "bookings"]
    ).properties(height=300)
    st.altair_chart(chart_meet_hour, use_container_width=True)


# --- Meetings per Day per Employee ---
st.subheader("👩‍💼 Average Meetings per Day per Employee")
df_meetings_emp = get_meetings_by_employee_per_day()
chart_emp = alt.Chart(df_meetings_emp).mark_bar().encode(
    x=alt.X("meetings_per_day:Q", title="Meetings per Day"),
    y=alt.Y("employee_name:N", sort="-x", title="Employee"),
    tooltip=["employee_name", "meetings_per_day"]
).properties(height=400)
st.altair_chart(chart_emp, use_container_width=True)
