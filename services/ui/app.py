# Streamlit UI for route search and lightweight static/realtime analytics views.

import os

import streamlit as st
import requests
import pandas as pd
import psycopg2

API_URL = os.getenv("API_URL", "http://localhost:8000/routes/reliable")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_DB = os.getenv("POSTGRES_DB", "transit")
POSTGRES_USER = os.getenv("POSTGRES_USER", "transit")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "transit")


# STRUCTURE 

st.markdown("""
<style>
.route-planner {
    max-width: 920px;
}

.route-card {
    border: 1px solid #e5e7eb;
    border-radius: 8px;
    padding: 1rem 1.1rem;
    margin: 1rem 0;
    background: #ffffff;
}

.route-card h3 {
    margin-top: 0;
}

.route-explanation {
    color: #64748b;
    font-size: 0.9rem;
    margin: 0.5rem 0 0.8rem;
}

.route-leg {
    margin: 0.25rem 0;
}

div[data-testid="stMetric"] {
    background: #f8fafc;
    border: 1px solid #e5e7eb;
    border-radius: 8px;
    padding: 0.65rem 0.75rem;
}
</style>
""", unsafe_allow_html=True)

st.set_page_config(layout="wide")
st.title("Transit Reliability System")

tab1, tab2 = st.tabs(["Route Planner", "Analytics"])

# TAB 1 ROUTING APPLICATION

with tab1:
    st.markdown('<div class="route-planner">', unsafe_allow_html=True)
    st.header("Route Planner")

    with st.form("route_search"):
        origin = st.text_input("From", "Times Square")
        destination = st.text_input("To", "Central Park")
        submitted = st.form_submit_button("Search routes")

    if submitted:
        response = requests.get(
            API_URL,
            params={
                "from_place": origin,
                "to_place": destination
            },
            timeout=60,
        )

        if response.status_code != 200:
            try:
                detail = response.json().get("detail", response.text)
            except requests.JSONDecodeError:
                detail = response.text

            st.error(f"Route search failed: {detail}")
            st.stop()

        routes = response.json()

        if not isinstance(routes, list):
            st.error(f"Unexpected API response: {routes}")
            st.stop()

        if not routes:
            st.info("No routes found for this search.")
            st.stop()

        for r in routes:
            badge = []
            if r["recommended"]:
                badge.append("⭐ Recommended")
            if r["fastest"]:
                badge.append("⚡ Fastest")

            badge_text = " | ".join(badge)

            title = f"Rank {r['rank']}"
            if badge_text:
                title = f"{title} · {badge_text}"

            st.markdown('<div class="route-card">', unsafe_allow_html=True)
            st.markdown(f"### {title}")

            col1, col2, col3, _ = st.columns([1, 1, 1.2, 1.8])

            col1.metric("Duration", f"{r['duration_min']} min")
            col2.metric("Transfers", r["transfers"])
            col3.metric(
                "Reliability",
                f"{float(r['reliability_score']):.1f}"
            )

            if r["explanation"]:
                st.markdown(
                    f'<div class="route-explanation">{r["explanation"]}</div>',
                    unsafe_allow_html=True,
                )

            st.markdown("**Route:**")

            for i, leg in enumerate(r["legs"]):
                st.markdown(
                    '<div class="route-leg">'
                    f'{i+1}. <strong>{leg["line"]}</strong> '
                    f'{leg["from"]} → {leg["to"]}'
                    '</div>',
                    unsafe_allow_html=True,
                )

            st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("</div>", unsafe_allow_html=True)

# TAB 2 ANALYTICS DASHBOARD WITH 2 TILES 

with tab2:

    st.header("Analytics Dashboard")

    col1, col2 = st.columns(2)

    # TILE 1 STATIC GTFS ANALYTICS

    with col1:
        st.subheader("Trips per Route (Static GTFS)")

        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
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
