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
    select distinct data, shape_id, route_id, ST_ASBINARY(shape) as shape, ST_ASBINARY(start_pt) as start_pt, ST_ASBINARY(end_pt) as end_pt
    from Trips
        left join ShapesGeom using(shape_id, data)
),

GTFSShapes as (
    select data, shape_id, servico,
    ST_GEOGFROM(shape) as shape, ST_GEOGFROM(start_pt) as start_pt, ST_GEOGFROM(end_pt) as end_pt
    from TripShapes
        left join Routes using(route_id, data)
        
),
        
-- lendo a base de gps
        
GPS as (
    select timestamp_gps, data, hora, servico, latitude, longitude, flag_em_movimento,
        tipo_parada, velocidade_instantanea, velocidade_estimada_10_min,
        distancia, id_veiculo
    from `rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo`
    where data between {start_date} and {end_date}
        and flag_em_operacao = TRUE
),
        
-----------------------------
-- identificando shape ids --
-----------------------------

-- transformando multilinestrings das Shapes em um array de linhas mais simples
-- cada elemento da coluna dump é um array de linestrings

AuxDump as (
    select *, 
        ST_DUMP(shape, 1) dump
    from GTFSShapes
),

-- truque para fazer UNNEST da coluna de arrays
-- passam a ter várias linhas, uma para cada segmento de reta
-- que compõe o polígono

AuxLines as (
    with A as (
        select * from AuxDump
    )
    select data, shape_id, servico, lines 
    from A, A.dump as lines
),

-- calculando a distância total acumulada até cada segmento

AuxDistances as (
    select *,
        IFNULL(
        SUM(ST_LENGTH(lines)) over(
            partition by shape_id, servico, data
            rows between unbounded preceding and 1 preceding), 0)
        as dist_traveled_shape
    from AuxLines
),

-- geometria da posição do ônibus

GPSCoords as (
    select *,
        ST_GEOGPOINT(longitude, latitude) posicao_veiculo_geo,
    from GPS
),

-- encontro em qual segmento de reta o ônibus está
-- calculo a distância dos ônibus a cada trecho

GPSShapesDist as (
    select *,
        ST_DISTANCE(posicao_veiculo_geo, lines) as distancia_shape
    from GPSCoords
        left join AuxDistances using(data, servico)
),
        
-- mantendo apenas a line mais próxima de cada ponto
        
GPSShapesClosest as (
    select id_veiculo, timestamp_gps, shape_id, MIN(distancia_shape) as distancia_shape
    from GPSShapesDist
    where distancia_shape < 500
    group by id_veiculo, timestamp_gps, shape_id
),

-- inner join para ficar apenas com os pontos mais proximos
-- filtro para apenas pontos a 500m ou menos de distância,
-- mais que isso seria fora da rota

GPSShapePoints as (
    select * except(distancia_shape)
    from GPSShapesDist
        inner join GPSShapesClosest using(distancia_shape, id_veiculo, shape_id, timestamp_gps)
    qualify
        ROW_NUMBER() over (
        partition by id_veiculo, shape_id, timestamp_gps
        order by distancia_shape) = 1
),

-- projetando o ponto na linha mais próxima para calcular distância

GPSProjectPoint as (
    select * except(posicao_veiculo_geo),
        ST_LINELOCATEPOINT(lines, posicao_veiculo_geo) as perc_traveled
    from GPSShapePoints
),

-- calculando distância real acumulada

GPSProjectDist as (
    select * except(dist_traveled_shape, perc_traveled, lines),
        dist_traveled_shape + perc_traveled * ST_LENGTH(lines) as dist_traveled_shape
    from GPSProjectPoint
),

-----------------------------
-- identificando shape ids --
-----------------------------

-- vejo se o ônibus está andando para frente:
-- dist_traveled_shape tem que aumentar

GPSShapesLag as (
    select *,
        LAG(dist_traveled_shape) over(
            partition by id_veiculo, shape_id, data
            order by id_veiculo, shape_id, data, timestamp_gps)
        as lag_dist_traveled_shape,
        LAG(dist_traveled_shape, 2) over(
            partition by id_veiculo, shape_id, data
            order by id_veiculo, shape_id, data, timestamp_gps)
        as lag_2_dist_traveled_shape,
        LAG(dist_traveled_shape, 3) over(
            partition by id_veiculo, shape_id, data
            order by id_veiculo, shape_id, data, timestamp_gps)
        as lag_3_dist_traveled_shape,
        LAG(dist_traveled_shape, 4) over(
            partition by id_veiculo, shape_id, data
            order by id_veiculo, shape_id, data, timestamp_gps)
        as lag_4_dist_traveled_shape,
        LAG(dist_traveled_shape, 5) over(
            partition by id_veiculo, shape_id, data
            order by id_veiculo, shape_id, data, timestamp_gps)
        as lag_5_dist_traveled_shape
    from GPSProjectDist
),

-- filtrando para caminhos crescentes

GPSShapesIncreasing as (
    select * except(lag_dist_traveled_shape, lag_2_dist_traveled_shape,
    lag_3_dist_traveled_shape, lag_4_dist_traveled_shape),
    from GPSShapesLag
    where dist_traveled_shape >= lag_dist_traveled_shape
        and lag_dist_traveled_shape >= lag_2_dist_traveled_shape
        and lag_2_dist_traveled_shape >= lag_3_dist_traveled_shape
        and lag_3_dist_traveled_shape >= lag_4_dist_traveled_shape
        and lag_4_dist_traveled_shape >= lag_5_dist_traveled_shape
),

GPSLeads as (
    select *,
    LEAD(dist_traveled_shape) over(
            partition by id_veiculo, shape_id
            order by id_veiculo, shape_id, timestamp_gps)
        as lead_dist_traveled_shape,
    LEAD(timestamp_gps) over(
            partition by id_veiculo, shape_id
            order by id_veiculo, shape_id, timestamp_gps)
        as lead_timestamp_gps
    from GPSShapesIncreasing
),

-- se uma observação é ambígua, removo

AuxShapesAmbig as (
    select id_veiculo, timestamp_gps, COUNT(shape_id) as count_obs
    from GPSLeads
    group by id_veiculo, timestamp_gps
),

GPSShapesDesem as (
    select * except(count_obs)
    from GPSLeads
        inner join AuxShapesAmbig using(id_veiculo, timestamp_gps)
    where count_obs = 1
),

------------------------------------
-- identificando pontos de onibus --
------------------------------------

-- encontro os próximos ponto de cada ônibus:
-- cada ponto tem uma distancia viajada no shape,
-- tomo os com distância maior que a viajada

AuxNextStop as (
    select id_veiculo, timestamp_gps, g.data as data,
        g.shape_id as shape_id, dist_traveled_stop, stop_id
    from GPSShapesDesem g
        left join GTFSStops s on
            g.data = s.data and g.servico = s.servico and g.shape_id = s.shape_id
            and g.dist_traveled_shape < s.dist_traveled_stop
),

-- join com a base completa para identificar os pontos seguintes de cada obs.

GPSNextStop as (
    select *,
        dist_traveled_stop - dist_traveled_shape as dist_to_stop,
        -- marca a sequência de pontos seguintes, de 1 a n
        ROW_NUMBER() over(
            partition by id_veiculo, shape_id, timestamp_gps
            order by id_veiculo, shape_id, timestamp_gps, dist_traveled_stop
            ) as stop_order
    from GPSShapesDesem
        left join AuxNextStop using(data, id_veiculo, timestamp_gps, shape_id)
    where dist_traveled_stop > dist_traveled_shape
),

-----------------------
-- Tempos de chegada --
-----------------------

-- o ônibus chegou ao ponto quando a distância que ele viajou supera a
-- distância até o ponto

-- calculando tempo até o ponto seguinte:
-- marco o tempo de chegada como o momento em que há uma mudança de ponto

-- há duas grandes fontes de imprecisão:
-- quando os ônibus ficam parados enquanto o tempo conta,
-- e quando ele desaparece por algum tempo

GPSStopsChange as (
    select * except(dist_traveled_stop, lead_dist_traveled_shape, 
        lag_5_dist_traveled_shape, lead_timestamp_gps),
        case
            -- quando o ônibus está parado há 5 obs.
            when dist_traveled_shape = lag_5_dist_traveled_shape
                then DATETIME(1970, 1, 1, 0, 0, 0)
            -- quando ele some por mais de 5mins em seguida
            when DATETIME_DIFF(
                lead_timestamp_gps,
                timestamp_gps,
                SECOND
            ) > 300
                then DATETIME(1970, 1, 1, 0, 0, 0)
            -- quando ele de fato chega ao ponto    
            when lead_dist_traveled_shape > dist_traveled_stop
                then lead_timestamp_gps
            else null
        end arrival_time
    from GPSNextStop
),

-- estendendo de baixo para cima:
-- se o ônibus chega no ponto X às 13h,
-- às 12h30 o tempo de chegada a X é 13h
        
GPSStopsFill as (
    select * except(arrival_time),
        FIRST_VALUE(arrival_time IGNORE NULLS) over(
            partition by id_veiculo, shape_id, stop_id
            order by id_veiculo, shape_id, stop_id, timestamp_gps
            rows between current row and unbounded following)
        as arrival_time
    from GPSStopsChange
),

-- calculo a diferença entre o momento da chegada e o momento atual

GPSArrivalTime as (
    select * except(arrival_time),
        DATETIME_DIFF(arrival_time, timestamp_gps, MICROSECOND)/(60 * 1000000) as arrival_time
    from GPSStopsFill
    where arrival_time != DATETIME(1970, 1, 1, 0, 0, 0)
)
        
select * from GPSArrivalTime
    where stop_order = 1
    --where --dist_to_stop > 0 --and dist_next_stop < 1000
        --and arrival_time > 0 and arrival_time < 60
        --and (tipo_parada is null)