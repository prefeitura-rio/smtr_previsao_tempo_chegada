with Dates as (
    select data
    from UNNEST(
        GENERATE_DATE_ARRAY(DATE('2023-01-01'), CURRENT_DATE(), INTERVAL 1 DAY)
    ) as data
),

-- lendo cada tabela necessária do GTFS e dando join com datas,
-- para ter uma linha por data e não por intervalo

Stops as (
    select data, stop_id, stop_lat, stop_lon, feed_start_date, feed_end_date
    from `rj-smtr.gtfs.stops`
        inner join Dates 
            on (Dates.data between feed_start_date and feed_end_date) or
            (feed_end_date is null and Dates.data >= feed_start_date)
    where data between "2024-01-26" and "2024-03-27"
)

select * from Stops