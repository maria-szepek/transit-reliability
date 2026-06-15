-- Estimates scheduled transfer risk for platform-to-platform transfer opportunities.
-- A transfer opportunity is modeled as arriving at one stop/platform and catching
-- the next departure on another route within the same station/parent station.
{{ config(materialized='table') }}

with events as (

    select
        t.route_id,
        st.trip_id,
        st.stop_id,
        coalesce(nullif(s.parent_station, ''), st.stop_id) as station_id,
        st.arrival_seconds,
        st.departure_seconds
    from {{ ref('stg_stop_times') }} st
    join {{ ref('stg_trips') }} t using (trip_id)
    left join {{ ref('stg_stops') }} s using (stop_id)

),

candidate_transfers as (

    select
        arriving.route_id as route_id,
        arriving.route_id as from_route_id,
        departing.route_id as to_route_id,
        arriving.stop_id as from_stop_id,
        departing.stop_id as to_stop_id,
        arriving.station_id,
        arriving.trip_id as from_trip_id,
        departing.trip_id as to_trip_id,
        arriving.arrival_seconds,
        departing.departure_seconds,
        (departing.departure_seconds - arriving.arrival_seconds) / 60.0 as buffer_minutes,

        row_number() over (
            partition by
                arriving.trip_id,
                arriving.stop_id,
                arriving.arrival_seconds,
                departing.route_id,
                departing.stop_id
            order by departing.departure_seconds
        ) as transfer_rank
    from events arriving
    join events departing
        on arriving.station_id = departing.station_id
       and arriving.route_id != departing.route_id
       and departing.departure_seconds > arriving.arrival_seconds
       and departing.departure_seconds - arriving.arrival_seconds <= 3600

),

with_transfer_rules as (

    select
        c.route_id,
        c.from_route_id,
        c.to_route_id,
        c.from_stop_id,
        c.to_stop_id,
        c.station_id,
        c.from_trip_id,
        c.to_trip_id,
        c.buffer_minutes,
        t.transfer_type,
        coalesce(t.min_transfer_time / 60.0, 3) as required_minutes
    from candidate_transfers c
    left join {{ ref('stg_transfers') }} t
        on c.from_stop_id = t.from_stop_id
       and c.to_stop_id = t.to_stop_id
    where c.transfer_rank = 1
      and coalesce(t.transfer_type, 0) != 3

),

final as (

    select
        route_id,
        from_route_id,
        to_route_id,
        from_stop_id,
        to_stop_id,
        station_id,
        from_trip_id,
        to_trip_id,
        buffer_minutes,
        required_minutes,
        transfer_type,
        buffer_minutes - required_minutes as slack_minutes
    from with_transfer_rules
    where buffer_minutes between 0 and 60

)

select
    route_id,
    from_route_id,
    to_route_id,
    from_stop_id,
    to_stop_id,
    station_id,
    from_trip_id,
    to_trip_id,
    buffer_minutes,
    required_minutes,
    slack_minutes,

    -- high score: high risk / low score: low risk
    case
        when slack_minutes < 0 then 0.9
        when slack_minutes < 2 then 0.7
        when slack_minutes < 5 then 0.4
        else 0.1
    end as risk_score

from final
