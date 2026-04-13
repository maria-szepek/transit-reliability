import streamlit as st
import requests
import pandas as pd
import psycopg2

API_URL = "http://localhost:8000/routes/reliable"


# STRUCTURE 

st.markdown("""
<style>
div[data-testid="stMetric"] {
    text-align: center;
}
</style>
""", unsafe_allow_html=True)

st.set_page_config(layout="wide")
st.title("Transit Reliability System")

tab1, tab2 = st.tabs(["Route Planner", "Analytics"])

# TAB 1 ROUTING APPLICATION

# with tab1:

#     st.header("Route Planner")

#     origin = st.text_input("From")
#     destination = st.text_input("To")

#     if st.button("Search"):
#         response = requests.get(
#             "http://api:8000/routes/reliable",
#             params={
#                 "from_place": origin,
#                 "to_place": destination
#             }
#         )

#         routes = response.json()

#         for r in routes:
#             st.write(r)

with tab1:
    st.header("Route Planner")

    origin = st.text_input("From", "Times Square")
    destination = st.text_input("To", "Central Park")

    if st.button("Search routes"):
        response = requests.get(
            API_URL,
            params={
                "from_place": origin,
                "to_place": destination
            }
        )

        routes = response.json()

        for r in routes:
            badge = []
            if r["recommended"]:
                badge.append("⭐ Recommended")
            if r["fastest"]:
                badge.append("⚡ Fastest")

            badge_text = " | ".join(badge)

            st.subheader(f"Rank {r['rank']}  {badge_text}")

            col1, col2, col3 = st.columns(3)

            col1.metric("Duration", f"{r['duration_min']} min")
            col2.metric("Transfers", r["transfers"])
            col3.metric(
                "Reliability",
                f"{float(r['reliability_score']):.1f}"
            )

            if r["explanation"]:
                st.caption(r["explanation"])

            st.markdown("**Route:**")

            for i, leg in enumerate(r["legs"]):
                st.write(
                    f"{i+1}. **{leg['line']}** "
                    f"{leg['from']} → {leg['to']}"
                )

            st.divider()

# TAB 2 ANALYTICS DASHBOARD WITH 2 TILES 

with tab2:

    st.header("Analytics Dashboard")

    col1, col2 = st.columns(2)

    # TILE 1 STATIC GTFS ANALYTICS

    with col1:
        st.subheader("Trips per Route (Static GTFS)")

        conn = psycopg2.connect(
            host="localhost",  # host="postgres",
            database="transit",
            user="transit",
            password="transit"
        )

        query = """
            SELECT route_id, COUNT(*) as trips
            FROM raw.trips
            GROUP BY route_id
            LIMIT 50
        """

        df = pd.read_sql(query, conn)

        st.bar_chart(
            df.set_index("route_id"),
            use_container_width=True
        )

    # TILE 2 REALTIME GTFS ANALYTICS

    with col2:
        st.subheader("Realtime Reliability Over Time")

        query = """
            SELECT
                window_end,
                AVG(avg_abs_prediction_drift_seconds) as drift  -- AVG(1 / (1 + avg_abs_prediction_drift_seconds)) as reliability
            FROM analytics.realtime_stop_reliability
            GROUP BY window_end
            ORDER BY window_end
            LIMIT 200
        """

        df = pd.read_sql(query, conn)

        if not df.empty:
            df["window_end"] = pd.to_datetime(df["window_end"])
            st.line_chart(
                df.set_index("window_end"),
                use_container_width=True
            )
        else:
            st.info("No realtime data yet — wait for Flink stream.")