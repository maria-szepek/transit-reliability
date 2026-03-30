-- prob(delay > buffer)
-- builds on assumptions for the buffer amount of minutes transfer
{{ config(materialized='table') }}

with stop_times as (

    select
        trip_id,
        stop_id,
        arrival_time,
        departure_time
    from {{ ref('stg_stop_times') }}

),

trips as (

    select
        trip_id,
        route_id
    from {{ ref('stg_trips') }}

),

joined as (

    select
        t.route_id,
        st.trip_id,
        st.stop_id,

        -- convert arrival once
        (
            split_part(st.arrival_time, ':', 1)::int * 3600 +
            split_part(st.arrival_time, ':', 2)::int * 60 +
            split_part(st.arrival_time, ':', 3)::int
        ) as arrival_seconds,

        -- convert departure once
        (
            split_part(st.departure_time, ':', 1)::int * 3600 +
            split_part(st.departure_time, ':', 2)::int * 60 +
            split_part(st.departure_time, ':', 3)::int
        ) as departure_seconds

    from stop_times st
    join trips t
        on st.trip_id = t.trip_id

),

ordered as (

    select
        *,
        lead(departure_seconds) over (
            partition by stop_id
            order by arrival_seconds
        ) as next_departure_seconds,

        lead(route_id) over (
            partition by stop_id
            order by arrival_seconds
        ) as next_route_id

    from joined

),

pairs as (

    select
        route_id,
        stop_id as transfer_stop,
        next_departure_seconds,
        arrival_seconds
    from ordered
    where next_departure_seconds is not null
      and route_id != next_route_id   -- critical perf + logic filter

),

buffer as (

    select
        route_id,
        transfer_stop,
        (next_departure_seconds - arrival_seconds) / 60.0 as buffer_minutes
    from pairs
),

with_transfers as (

    select
        b.route_id,
        b.transfer_stop,
        b.buffer_minutes,
        coalesce(t.min_transfer_time / 60.0, 3) as required_minutes
    from buffer b
    left join {{ ref('stg_transfers') }} t
        on b.transfer_stop = t.from_stop_id
       and b.transfer_stop = t.to_stop_id
),

final as (

    select
        route_id,
        transfer_stop,
        buffer_minutes,
        required_minutes,
        buffer_minutes - required_minutes as slack_minutes
    from with_transfers
    where buffer_minutes between 0 and 60
)

select
    route_id,
    transfer_stop,
    buffer_minutes,
    required_minutes,
    slack_minutes,

    case
        when slack_minutes < 0 then 0.9
        when slack_minutes < 2 then 0.7
        when slack_minutes < 5 then 0.4
        else 0.1
    end as risk_score

from final