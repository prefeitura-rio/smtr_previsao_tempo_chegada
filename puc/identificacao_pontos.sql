-- placeholders são {date} e {feed_start}

with Stops as (
    select stop_id, stop_lat, stop_lon
    from `rj-smtr.gtfs.stops`
    where feed_start_date = {feed_start}
),

StopTimes as (
    select stop_id, trip_id
    from `rj-smtr.gtfs.stop_times`
    where feed_start_date = {feed_start}
),

Trips as (
    select trip_id, route_id, shape_id
    from `rj-smtr.gtfs.trips`
    where feed_start_date = {feed_start}
),

ShapesGeom as (
    select shape_id, shape, start_pt, end_pt
    from `rj-smtr.gtfs.shapes_geom`
    where feed_start_date = {feed_start}
),

Routes as (
    select route_id, route_short_name as servico
    from `rj-smtr.gtfs.routes`
    where feed_start_date = {feed_start}
),
        
-- fazendo joins para linkar serviços e pontos
        
TripStops as (
    select distinct route_id, shape_id, stop_id
    from Trips
        left join StopTimes using(trip_id)
),

GTFSStops as (
    select stop_id, shape_id, servico, stop_lat, stop_lon from TripStops
    left join Routes using(route_id)
    left join Stops using(stop_id)
),
        
-- agora para linkar serviços e shapes
        
TripShapes as (
    select distinct route_id, shape_id
    from Trips
    left join ShapesGeom using(shape_id)
),

GTFSShapes as (
    select shape_id, servico, shape, start_pt, end_pt from TripShapes
        left join Routes using(route_id)
        left join ShapesGeom using(shape_id)
),
        
-- lendo a base de gps
        
GPS as (
    select timestamp_gps, data, hora, servico, latitude, longitude, flag_em_movimento,
        tipo_parada, flag_trajeto_correto, velocidade_instantanea, velocidade_estimada_10_min,
        distancia, flag_em_operacao, id_veiculo
    from `rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo`
    where data = {date}
        and flag_em_operacao = TRUE
    order by servico, id_veiculo, timestamp_gps
),
        
-- join do gps com shapes para identificar viagens
        
GPSShapes as (
    select *,
        ST_GEOGPOINT(longitude, latitude) posicao_veiculo_geo
    from GPS
        left join GTFSShapes using(servico)
),
        
-- computando status da viagem
        
GPSStatus as (
    select *,
        case
            when ST_DWITHIN(posicao_veiculo_geo, start_pt, 500)
                then 'start'
            when ST_DWITHIN(posicao_veiculo_geo, end_pt, 500)
                then 'end'
            when ST_DWITHIN(posicao_veiculo_geo, shape, 500)
                then 'middle'
            else 'out'
        end status_viagem
    from GPSShapes
),
        
-- marcando início e fim das viagens
        
GPSTrips as (
    select *,
        string_agg(status_viagem, "") over (
           partition by id_veiculo, shape_id
           order by id_veiculo, shape_id, timestamp_gps
           rows between current row and 1 following) = 'startmiddle' starts,
        string_agg(status_viagem, "") over (
           partition by id_veiculo, shape_id
           order by id_veiculo, shape_id, timestamp_gps
           rows between 1 preceding and current row) = 'endend' ended
    from GPSStatus
),
        
-- criando id viagem quando viagem começa
        
GPSTripsId as (
    select *,
        case
           when starts = true
               then ROW_NUMBER() over(order by (select 1))
           when ended = true
               then 0
           else null
        end pre_id_viagem
    from GPSTrips
),
        
-- estendendo id_viagem de cima para baixo
        
GPSTripsId2 as (
    select *,
        count(pre_id_viagem) over(
            partition by id_veiculo, shape_id
            order by id_veiculo, shape_id, timestamp_gps)
        as _grp
    from GPSTripsId
),
        
GPSTripsId3 as (
    select *,
        FIRST_VALUE(pre_id_viagem) over(
            partition by id_veiculo, shape_id, _grp
            order by id_veiculo, shape_id, timestamp_gps)
        as id_viagem
    from GPSTripsId2
),
        
-- apenas mantendo viagens que fazem sentido
        
GPSTripsId4 as (
    select * from GPSTripsId3
    where id_viagem is not null and id_viagem != 0
),
        
-- identificando pontos de onibus
        
GPSStops as (
    select *,
        ST_GEOGPOINT(stop_lon, stop_lat) stop_geo
    from GPSTripsId4
        left join GTFSStops using(servico, shape_id)
),
        
-- calculando distancias
        
GPSStops2 as (
    select *,
        ST_DISTANCE(posicao_veiculo_geo, stop_geo) as distancia_ponto
    from GPSStops
),
        
-- mantendo o ponto mais próximo
        
GPSStopsClosest as (
    select id_veiculo, shape_id, timestamp_gps, MIN(distancia_ponto) as distancia_ponto
    from GPSStops2
    group by id_veiculo, shape_id, timestamp_gps
),
        
GPSStops3 as (
    select *
    from GPSStops2
        inner join GPSStopsClosest using(distancia_ponto, id_veiculo, shape_id, timestamp_gps)
),
        
-- calculando qual o ponto, se há algum
        
GPSStops4 as (
    select *,
        case
           when distancia_ponto <= 100
               then stop_id
           else 'none'
        end stop
    from GPSStops3
),
        
-- ponto anterior
        
GPSStops5 as (
    select *,
        LAG(stop) over(
            partition by id_veiculo, shape_id
            order by id_veiculo, shape_id, timestamp_gps)
        as previous_stop
    from GPSStops4
),
        
-- calculando tempo até o ponto seguinte
        
GPSStops6 as (
    select *,
        case
           when stop != previous_stop and stop != 'none'
               then timestamp_gps
           else null
        end pre_arrival_time
    from GPSStops5
),
        
-- estendendo de baixo para cima
        
GPSStops7 as (
    select *,
        count(pre_arrival_time) over(
            partition by id_veiculo, shape_id
            order by id_veiculo, shape_id, timestamp_gps)
        as _grp2
    from GPSStops6
),
        
GPSStops8 as (
    select *,
        LAST_VALUE(pre_arrival_time) over(
            partition by id_veiculo, shape_id, _grp2
            order by id_veiculo, shape_id, timestamp_gps)
        as pre_arrival_time2
    from GPSStops7
        ),
        
-- quando está no ponto, tempo de chegada é o seguinte
        
GPSStops9 as (
    select *,
        LEAD(pre_arrival_time2) over(
            partition by id_veiculo, shape_id
            order by id_veiculo, shape_id, timestamp_gps)
        as lead_arrival_time
    from GPSStops8
    where pre_arrival_time2 is not null
),
        
GPSStops10 as (
    select *,
        case
           when pre_arrival_time2 = timestamp_gps
               then lead_arrival_time
           else pre_arrival_time2
        end pre_arrival_time3
    from GPSStops9
),
        
GPSStops11 as (
    select *,
        DATETIME_DIFF(pre_arrival_time3, timestamp_gps, MICROSECOND)/(60 * 1000000) as arrival_time
    from GPSStops10
)
        
select timestamp_gps, data, hora, servico, latitude, longitude, flag_em_movimento,
    tipo_parada, flag_trajeto_correto, velocidade_instantanea, velocidade_estimada_10_min,
    distancia, flag_em_operacao, id_veiculo, id_viagem, stop, arrival_time
from GPSStops11