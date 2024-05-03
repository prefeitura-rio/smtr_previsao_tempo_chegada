-- placeholders são {start_date} e {end_date}

-- criando uma table auxiliar com datas

with Dates as (
    select data
    from UNNEST(
        GENERATE_DATE_ARRAY(DATE('2023-01-01'), CURRENT_DATE(), INTERVAL 1 DAY)
    ) as data
),

-- lendo cada tabela necessária do GTFS e dando join com datas,
-- para ter uma linha por data e não por intervalo

Stops as (
    select data, stop_id, stop_lat, stop_lon
    from `rj-smtr.gtfs.stops` s
        inner join Dates
            on (Dates.data between feed_start_date and feed_end_date) or
                (feed_end_date is null and Dates.data >= feed_start_date)
    where data between {start_date} and {end_date}
),

StopTimes as (
    select data, stop_id, trip_id, stop_sequence, shape_dist_traveled as dist_traveled_stop
    from `rj-smtr.gtfs.stop_times`
        inner join Dates
            on (Dates.data between feed_start_date and feed_end_date) or
                (feed_end_date is null and Dates.data >= feed_start_date)
    where data between {start_date} and {end_date}
),

Trips as (
    select data, trip_id, route_id, shape_id
    from `rj-smtr.gtfs.trips`
        inner join Dates
            on (Dates.data between feed_start_date and feed_end_date) or
                (feed_end_date is null and Dates.data >= feed_start_date)
    where data between {start_date} and {end_date}
),

Shapes as (
    select data, shape_id, shape_pt_lat, shape_pt_lon, shape_dist_traveled as dist_traveled_shape
    from `rj-smtr.gtfs.shapes` s
        inner join Dates
            on (Dates.data between feed_start_date and feed_end_date) or
                (feed_end_date is null and Dates.data >= feed_start_date)
    where data between {start_date} and {end_date}
),

ShapesGeom as (
    select data, shape_id, shape, start_pt, end_pt
    from `rj-smtr.gtfs.shapes_geom`
        inner join Dates
            on (Dates.data between feed_start_date and feed_end_date) or
                (feed_end_date is null and Dates.data >= feed_start_date)
    where data between {start_date} and {end_date}
),

Routes as (
    select data, route_id, route_short_name as servico
    from `rj-smtr.gtfs.routes`
        inner join Dates
            on (Dates.data between feed_start_date and feed_end_date) or
                (feed_end_date is null and Dates.data >= feed_start_date)
    where data between {start_date} and {end_date}
),
        
-- fazendo joins para linkar serviços e pontos
        
TripStops as (
    select distinct data, route_id, shape_id, stop_id, stop_sequence, dist_traveled_stop
    from Trips
        left join StopTimes using(trip_id, data)
),

GTFSStops as (
    select distinct data, stop_id, shape_id, servico, stop_lat, stop_lon, stop_sequence, dist_traveled_stop
    from TripStops
        left join Routes using(route_id, data)
        left join Stops using(stop_id, data)
),
        
-- agora para linkar serviços e shapes
        
TripShapes as (
    select distinct data, route_id, shape_id
    from Trips
        left join ShapesGeom using(shape_id, data)
),

GTFSServShapes as (
    select distinct data, shape_id, servico
    from TripShapes
        left join Routes using(route_id, data)
        
),

GTFSShapes as (
    select data, shape_id, servico, shape, start_pt, end_pt
    from GTFSServShapes
        left join ShapesGeom using(shape_id, data)
),
        
-- lendo a base de gps
        
GPS as (
    select timestamp_gps, data, hora, servico, latitude, longitude, flag_em_movimento,
        tipo_parada, flag_trajeto_correto, velocidade_instantanea, velocidade_estimada_10_min,
        distancia, flag_em_operacao, id_veiculo
    from `rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo`
    where data between {start_date} and {end_date}
        and flag_em_operacao = TRUE
),
        
-- join do gps com shapes para identificar viagens
        
GPSShapes as (
    select *,
        ST_GEOGPOINT(longitude, latitude) posicao_veiculo_geo
    from GPS
        left join GTFSShapes using(servico, data)
),
        
-- computando status da viagem
        
GPSStatus as (
    select *,
        ST_DISTANCE(posicao_veiculo_geo, shape) as sq_distance_shape, 
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
        
GPSTripStatus as (
    select * except(status_viagem),
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
    select * except(starts, ended),
        case
           when starts = true
               then CONCAT(id_veiculo, shape_id, timestamp_gps)
           when ended = true
               then "ended"
           else null
        end id_viagem
    from GPSTripStatus
),
        
-- estendendo id_viagem de cima para baixo
        
GPSTripsIdFill as (
    select * except(id_viagem),
        LAST_VALUE(id_viagem IGNORE NULLS) over(
            partition by id_veiculo, shape_id
            order by id_veiculo, shape_id, timestamp_gps
            rows between unbounded preceding and current row)
        as id_viagem
    from GPSTripsId
),        
        
-- apenas mantendo viagens que fazem sentido
        
GPSTrips as (
    select * 
    from GPSTripsIdFill
    where id_viagem is not null and id_viagem != "ended"
),

-- se uma observação é ambígua, removo

AuxShapesAmbig as (
    select id_veiculo, timestamp_gps, COUNT(shape_id) as count_obs
    from GPSTrips
    group by id_veiculo, timestamp_gps
),

GPSShapesDesem as (
    select * except(count_obs)
    from GPSTrips
        inner join AuxShapesAmbig using(id_veiculo, timestamp_gps)
    where count_obs = 1
),

-- identificando pontos de onibus
        
GPSStops as (
    select *,
        ST_GEOGPOINT(stop_lon, stop_lat) stop_geo
    from GPSShapesDesem
        left join GTFSStops using(data, servico, shape_id)
),
        
-- calculando distancias
        
GPSStopsDist as (
    select *,
        ST_DISTANCE(posicao_veiculo_geo, stop_geo) as distancia_ponto
    from GPSStops
),
        
-- mantendo o ponto mais próximo
        
AuxStopsClosest as (
    select id_veiculo, shape_id, timestamp_gps, MIN(distancia_ponto) as distancia_ponto
    from GPSStopsDist
    group by id_veiculo, shape_id, timestamp_gps
),
        
GPSStopsClosest as (
    select *
    from GPSStopsDist
        inner join AuxStopsClosest using(distancia_ponto, id_veiculo, shape_id, timestamp_gps)
),
        
-- calculando qual o ponto, se há algum
        
GPSStopsId as (
    select *,
        case
           when distancia_ponto <= 50
               then stop_id
           else 'none'
        end stop
    from GPSStopsClosest
),
        
-- ponto anterior
        
GPSStopsLag as (
    select *,
        LAG(stop) over(
            partition by id_veiculo, shape_id
            order by id_veiculo, shape_id, timestamp_gps)
        as previous_stop
    from GPSStopsId
),
        
-- calculando tempo até o ponto seguinte
        
GPSStopsTime as (
    select *,
        case
           when stop != previous_stop and stop != 'none'
               then timestamp_gps
           else null
        end pre_arrival_time
    from GPSStopsLag
),
        
-- estendendo de baixo para cima
        
GPSStopsFill as (
    select *,
        FIRST_VALUE(pre_arrival_time IGNORE NULLS) over(
            partition by id_veiculo, shape_id
            order by id_veiculo, shape_id, timestamp_gps
            rows between current row and unbounded following)
        as pre_arrival_time2
    from GPSStopsTime
),
        
-- quando está no ponto, tempo de chegada é o seguinte
        
GPSStopsLead as (
    select *,
        LEAD(pre_arrival_time2) over(
            partition by id_veiculo, shape_id
            order by id_veiculo, shape_id, timestamp_gps)
        as lead_arrival_time
    from GPSStopsFill
    where pre_arrival_time2 is not null
),

-- mas se o ônibus fica muito tempo parado, o lead ainda vai ser o mesmo
-- crio uma variável que só não é null quando o ponto muda

GPSStopsChange as (
    select * except(lead_arrival_time),
        case
            when pre_arrival_time2 != lead_arrival_time
                then lead_arrival_time
            else null
        end lead_arrival_time
    from GPSStopsLead
),

-- estendo essa variável de baixo pra cima:

GPSArrivalFill as (
    select * except(lead_arrival_time),
        FIRST_VALUE(lead_arrival_time IGNORE NULLS) over(
            partition by id_veiculo, shape_id
            order by id_veiculo, shape_id, timestamp_gps
            rows between current row and unbounded following)
        as lead_arrival_time
    from GPSStopsChange
),

GPSStops10 as (
    select *,
        case
           when pre_arrival_time2 = timestamp_gps
               then lead_arrival_time
           else pre_arrival_time2
        end pre_arrival_time3
    from GPSArrivalFill
),
        
GPSStops11 as (
    select * except(stop_lon, stop_lat),
        DATETIME_DIFF(pre_arrival_time3, timestamp_gps, MICROSECOND)/(60 * 1000000) as arrival_time
    from GPSStops10
),
        
-- calculando distância viajada ao longo do shape
GPSStops12 as (
    select *,
    ST_GEOGPOINT(shape_pt_lon, shape_pt_lat) shape_pt_geo
    from GPSStops11
        left join Shapes using(data, shape_id)
),

-- calculando distância até o ponto seguinte
  
GPSStops15 as (
    select timestamp_gps, g.data as data, hora, g.servico as servico, latitude,
        longitude, flag_em_movimento, g.stop_sequence as stop_sequence,
        tipo_parada, flag_trajeto_correto, velocidade_instantanea, velocidade_estimada_10_min,
        id_veiculo, id_viagem, stop, arrival_time, dist_traveled_shape,
        s.dist_traveled_stop as dist_traveled_next_stop
    from GPSStops12 g
        left join GTFSStops s
            on g.data = s.data and g.servico = s.servico and g.shape_id = s.shape_id and g.stop_sequence = s.stop_sequence - 1
),
      
GPSStops16 as (
    select *,
        dist_traveled_next_stop - dist_traveled_shape as dist_next_stop
    from GPSStops15
)

-- chamando a base completa
        
select * from GPSStops16 
    where dist_next_stop > 0 and dist_next_stop < 1000
        and arrival_time > 0 and arrival_time < 60
        and (tipo_parada is null or tipo_parada != "garagem")