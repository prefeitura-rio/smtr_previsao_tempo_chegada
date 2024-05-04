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
    select distinct data, shape_id, route_id
    from Trips
        left join ShapesGeom using(shape_id, data)
),

GTFSPreShapes as (
    select distinct data, shape_id, servico
    from TripShapes
        left join Routes using(route_id, data)
        
),

GTFSShapes as (
    select data, shape_id, servico, shape, start_pt, end_pt
    from GTFSPreShapes
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
    select data, shape_id, servico, start_pt, end_pt, lines 
    from A, A.dump as lines
),

-- calculando a distância total acumulada até cada segmento

AuxDistances as (
    select *,
        SUM(ST_LENGTH(lines)) over(
            partition by shape_id, servico, data
            rows between unbounded preceding and current row)
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
    select *,
        ST_LINELOCATEPOINT(lines, posicao_veiculo_geo) as perc_traveled
    from GPSShapePoints
),

-- calculando distância real acumulada

GPSProjectDist as (
    select * except(dist_traveled_shape),
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
    lag_3_dist_traveled_shape, lag_4_dist_traveled_shape, lag_5_dist_traveled_shape)
    from GPSShapesLag
    where dist_traveled_shape >= lag_dist_traveled_shape
        and lag_dist_traveled_shape >= lag_2_dist_traveled_shape
        and lag_2_dist_traveled_shape >= lag_3_dist_traveled_shape
        and lag_3_dist_traveled_shape >= lag_4_dist_traveled_shape
        and lag_4_dist_traveled_shape >= lag_5_dist_traveled_shape
),

-- se uma observação é ambígua, removo

AuxShapesAmbig as (
    select id_veiculo, timestamp_gps, COUNT(shape_id) as count_obs
    from GPSShapesIncreasing
    group by id_veiculo, timestamp_gps
),

GPSShapesDesem as (
    select * except(count_obs)
    from GPSShapesIncreasing
        inner join AuxShapesAmbig using(id_veiculo, timestamp_gps)
    where count_obs = 1
),

------------------------------------
-- identificando pontos de onibus --
------------------------------------

-- encontro o próximo ponto de cada ônibus:
-- cada ponto tem uma distancia viajada no shape,
-- o próximo é o menor com distância acima da atual

AuxNextStop as (
    select id_veiculo, timestamp_gps, g.shape_id as shape_id, dist_traveled_stop
    from GPSShapesDesem g
        left join GTFSStops s on
            g.data = s.data and g.servico = s.servico and g.shape_id = s.shape_id
            and g.dist_traveled_shape < s.dist_traveled_stop
),

AuxMinNextStop as (
    select id_veiculo, timestamp_gps, shape_id, MIN(dist_traveled_stop) as dist_traveled_next_stop
    from AuxNextStop
    group by id_veiculo, timestamp_gps, shape_id
),

-- join com a base completa para identificar o ponto seguinte de cada obs.

GPSNextStop as (
    select *,
    dist_traveled_next_stop - dist_traveled_shape as dist_next_stop
    from GPSShapesDesem
        left join AuxMinNextStop using(id_veiculo, timestamp_gps, shape_id)
),

-----------------------
-- Tempos de chegada --
-----------------------

-- (o ônibus chegou ao ponto quando a distância que ele viajou supera a
-- distância até o ponto: quando o next_stop muda)
-- o metodo acima não funciona bem, tem muitos falsos positivos.
-- no lugar, vejo quando o ônibus fica fisicamente próximo do ponto

GPSStops as (
    select *,
        ST_GEOGPOINT(stop_lon, stop_lat) stop_geo
    from GPSNextStop
        left join GTFSStops using(data, servico, shape_id)
        where ABS(stop_lon - longitude) < 0.005 and ABS(stop_lat - latitude) < 0.005
),
        
-- calculando distancias
        
GPSStopsDist as (
    select * except(stop_lon, stop_lat, stop_geo),
        ST_DISTANCE(posicao_veiculo_geo, stop_geo) as distancia_ponto
    from GPSStops
),
        
-- mantendo o ponto mais próximo
        
GPSStopsClosest as (
    select id_veiculo, shape_id, timestamp_gps, MIN(distancia_ponto) as distancia_ponto
    from GPSStopsDist
    group by id_veiculo, shape_id, timestamp_gps
),

-- chegou no ponto se está a 50m

GPSStopsId as (
    select *,
    case
        when distancia_ponto <= 50
            then stop_sequence
        else null
    end stop
    from GPSStopsDist
        inner join GPSStopsClosest using(distancia_ponto, id_veiculo, shape_id, timestamp_gps)
),
        
-- ponto anterior
        
GPSStopsLag as (
    select *,
        LAG(stop) over(
            partition by id_veiculo, shape_id, data
            order by id_veiculo, shape_id, data, timestamp_gps)
        as lag_stop
    from GPSStopsId
),
        
-- calculando tempo até o ponto seguinte:
-- marco o tempo de chegada como o momento em que há uma mudança de ponto
        
GPSStopsTime as (
    select *,
        case
           when stop != lag_stop and stop is not null
               then timestamp_gps
           else null
        end arrival_time
    from GPSStopsLag
),

-- estendendo de baixo para cima:
-- se o ônibus chega no ponto X às 13h,
-- às 12h30 o tempo de chegada a X é 13h
        
GPSStopsFill as (
    select * except(arrival_time),
        FIRST_VALUE(arrival_time IGNORE NULLS) over(
            partition by id_veiculo, shape_id, data
            order by id_veiculo, shape_id, data, timestamp_gps
            rows between current row and unbounded following)
        as arrival_time,
    from GPSStopsTime
),

-- quando está no ponto, tempo de chegada é o seguinte
-- senão ficam vários zeros
        
GPSStopsLead as (
    select *,
        LEAD(arrival_time) over(
            partition by id_veiculo, shape_id, data
            order by id_veiculo, shape_id, data, timestamp_gps)
        as lead_arrival_time
    from GPSStopsFill
    where arrival_time is not null
),

-- mas se o ônibus fica muito tempo parado, o lead ainda vai ser o mesmo
-- crio uma variável que só não é null quando o ponto muda

GPSStopsNext as (
    select * except(lead_arrival_time),
        case
            when arrival_time != lead_arrival_time
                then lead_arrival_time
            else null
        end lead_arrival_time
    from GPSStopsLead
),

-- estendo essa variável de baixo pra cima:
-- retrinjo ao mesmo dia

GPSArrivalFill as (
    select * except(lead_arrival_time),
        FIRST_VALUE(lead_arrival_time IGNORE NULLS) over(
            partition by id_veiculo, shape_id, data
            order by id_veiculo, shape_id, data, timestamp_gps
            rows between current row and unbounded following)
        as lead_arrival_time
    from GPSStopsNext
),

-- quando o ônibus chega no ponto, conta o tempo do seguinte

GPSArrival as (
    select * except(arrival_time, lead_arrival_time),
        case
           when arrival_time = timestamp_gps
               then lead_arrival_time
           else arrival_time
        end arrival_time
    from GPSArrivalFill
),

-- calculo a diferença entre o momento da chegada e o momento atual

GPSArrivalTime as (
    select * except(arrival_time),
        DATETIME_DIFF(arrival_time, timestamp_gps, MICROSECOND)/(60 * 1000000) as arrival_time
    from GPSArrival
)
        
select * from GPSArrivalTime 
    --where dist_next_stop > 0 and dist_next_stop < 1000
    --    and arrival_time > 0 and arrival_time < 60
    --    and (tipo_parada is null or tipo_parada != "garagem")