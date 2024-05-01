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
    select distinct data, shape_id, route_id, shape_pt_lat, shape_pt_lon, dist_traveled_shape
    from Trips
        left join Shapes using(shape_id, data)
),

GTFSShapes as (
    select distinct data, shape_id, servico, shape_pt_lat, shape_pt_lon, dist_traveled_shape
    from TripShapes
        left join Routes using(route_id, data)
        
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

-- join do gps com shapes para identificar viagens
-- para agilizar o processamento, pego apenas shape_pts
-- em um 'raio' de aprox 500m do ônibus, usando as coordenadas
        
GPSShapes as (
    select *,
        ST_GEOGPOINT(longitude, latitude) posicao_veiculo_geo,
        ST_GEOGPOINT(shape_pt_lon, shape_pt_lat) shape_pt_geo
    from GPS
        left join GTFSShapes using(servico, data)
        where ABS(shape_pt_lon - longitude) < 0.005 and ABS(shape_pt_lat - latitude) < 0.005
),

-- calculo a distância dos ônibus a cada vértice

GPSShapesDist as (
    select *,
        ST_DISTANCE(posicao_veiculo_geo, shape_pt_geo) as distancia_shape_pt
    from GPSShapes
),
        
-- mantendo o ponto mais próximo de cada shape_id
        
GPSShapesClosest as (
    select id_veiculo, shape_id, timestamp_gps, MIN(distancia_shape_pt) as distancia_shape_pt
    from GPSShapesDist
    group by id_veiculo, shape_id, timestamp_gps
),

-- inner join para ficar apenas com os pontos mais proximos
-- filtro para apenas pontos a 500m ou menos de distância,
-- mais que isso seria fora da rota

GPSShapePoints as (
    select * except(distancia_shape_pt)
    from GPSShapesDist
        inner join GPSShapesClosest using(distancia_shape_pt, id_veiculo, shape_id, timestamp_gps)
    where distancia_shape_pt < 500
),

-- agora a ideia é encontrar o último vértice da shape onde o ônibus esteve
-- calculo para a obs. anterior, mas o ônibus pode não ter andado o suficiente

GPSShapesLag as (
    select *,
        LAG(dist_traveled_shape) over(
            partition by id_veiculo, shape_id
            order by id_veiculo, shape_id, timestamp_gps)
        as lag_dist_traveled_shape
    from GPSShapePoints
),

-- crio uma variável que só não é null quando a distância muda

GPSShapesChange as (
    select * except(lag_dist_traveled_shape),
        case
            when dist_traveled_shape != lag_dist_traveled_shape
                then lag_dist_traveled_shape
            else null
        end lag_dist_traveled_shape
    from GPSShapesLag
),

-- estendo essa variável de cima pra baixo:
-- assim pego o último valor distinto

GPSShapesFill as (
    select * except(lag_dist_traveled_shape),
        LAST_VALUE(lag_dist_traveled_shape IGNORE NULLS) over(
            partition by id_veiculo, shape_id
            order by id_veiculo, shape_id, timestamp_gps
            rows between unbounded preceding and current row)
        as lag_dist_traveled_shape
    from GPSShapesChange
),

-- repito para pegar o segundo ponto anterior

GPSShapesLag2 as (
    select *,
        LAG(lag_dist_traveled_shape) over(
            partition by id_veiculo, shape_id
            order by id_veiculo, shape_id, timestamp_gps)
        as lag_2_dist_traveled_shape
    from GPSShapesFill
),

GPSShapesChange2 as (
    select * except(lag_2_dist_traveled_shape),
        case
            when lag_dist_traveled_shape != lag_2_dist_traveled_shape
                then lag_2_dist_traveled_shape
            else null
        end lag_2_dist_traveled_shape
    from GPSShapesLag2
),

GPSShapesFill2 as (
    select * except(lag_2_dist_traveled_shape),
        LAST_VALUE(lag_2_dist_traveled_shape IGNORE NULLS) over(
            partition by id_veiculo, shape_id
            order by id_veiculo, shape_id, timestamp_gps
            rows between unbounded preceding and current row)
        as lag_2_dist_traveled_shape
    from GPSShapesChange2
),

-- repito a terceita vez

GPSShapesLag3 as (
    select *,
        LAG(lag_2_dist_traveled_shape) over(
            partition by id_veiculo, shape_id
            order by id_veiculo, shape_id, timestamp_gps)
        as lag_3_dist_traveled_shape
    from GPSShapesFill2
),

GPSShapesChange3 as (
    select * except(lag_3_dist_traveled_shape),
        case
            when lag_2_dist_traveled_shape != lag_3_dist_traveled_shape
                then lag_3_dist_traveled_shape
            else null
        end lag_3_dist_traveled_shape
    from GPSShapesLag3
),

GPSShapesFill3 as (
    select * except(lag_3_dist_traveled_shape),
        LAST_VALUE(lag_3_dist_traveled_shape IGNORE NULLS) over(
            partition by id_veiculo, shape_id
            order by id_veiculo, shape_id, timestamp_gps
            rows between unbounded preceding and current row)
        as lag_3_dist_traveled_shape
    from GPSShapesChange3
),

-- na shape_id correta, as distâncias só podem ter aumentado

GPSShapesIncreasing as (
    select * except(lag_dist_traveled_shape, lag_2_dist_traveled_shape),
        dist_traveled_shape - lag_dist_traveled_shape as delta
    from GPSShapesFill3
    where dist_traveled_shape > lag_dist_traveled_shape
        and lag_dist_traveled_shape > lag_2_dist_traveled_shape
        and lag_2_dist_traveled_shape > lag_3_dist_traveled_shape
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

-- junto cada obs ao seu conjunto de pontos possíveis, dada a shape_id
-- uso as coordenadas para pegar apenas pontos razoavelmente próximos

GPSStops as (
    select *,
        ST_GEOGPOINT(stop_lon, stop_lat) stop_geo
    from GPSShapesFill
        left join GTFSStops using(data, servico, shape_id)
    where ABS(stop_lon - longitude) < 0.005 and ABS(stop_lat - latitude) < 0.005
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
-- se o ponto está a <= 50m, digo que o ônibus está no ponto
        
GPSStopsId as (
    select *,
        case
           when distancia_ponto <= 50
               then stop_sequence
           else null
        end stop
    from GPSStopsClosest
),
        
-- uso o mesmo método usado no vértice das shapes para encontrar
-- o último ponto diferente do atual

GPSStopsLag as (
    select *,
        LAG(stop) over(
            partition by id_veiculo, shape_id
            order by id_veiculo, shape_id, timestamp_gps)
        as lag_stop
    from GPSStopsId
),

-- crio uma variável que só não é null quando o ponto muda

GPSStopsChange as (
    select *,
        case
            when stop != lag_stop
                then lag_stop
            else null
        end previous_stop
    from GPSStopsLag
),

-- estendo essa variável de cima pra baixo:
-- assim pego o último valor distinto

GPSStopsPrevious as (
    select * except(previous_stop),
        LAST_VALUE(previous_stop IGNORE NULLS) over(
            partition by id_veiculo, shape_id
            order by id_veiculo, shape_id, timestamp_gps
            rows between unbounded preceding and current row)
        as previous_stop
    from GPSStopsChange
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
    from GPSStopsPrevious
),
        
-- estendendo de baixo para cima:
-- se o ônibus chega no ponto X às 13h,
-- às 12h30 o tempo de chegada a X é 13h
        
GPSStopsFill as (
    select * except(arrival_time),
        FIRST_VALUE(arrival_time IGNORE NULLS) over(
            partition by id_veiculo, shape_id
            order by id_veiculo, shape_id, timestamp_gps
            rows between current row and unbounded following)
        as arrival_time
    from GPSStopsTime
),
        
-- quando está no ponto, tempo de chegada é o seguinte
-- senão ficam vários zeros
        
GPSStopsLead as (
    select *,
        LEAD(arrival_time) over(
            partition by id_veiculo, shape_id
            order by id_veiculo, shape_id, timestamp_gps)
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

GPSArrivalFill as (
    select * except(lead_arrival_time),
        FIRST_VALUE(lead_arrival_time IGNORE NULLS) over(
            partition by id_veiculo, shape_id
            order by id_veiculo, shape_id, timestamp_gps
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
    select * except(stop_lon, stop_lat, arrival_time),
        DATETIME_DIFF(arrival_time, timestamp_gps, MICROSECOND)/(60 * 1000000) as arrival_time
    from GPSArrival
),
        
-- calculando distância até o ponto seguinte
-- estendo a variável stop de cima pra baixo

GPSCurrentStop as (
    select *,
        LAST_VALUE(stop IGNORE NULLS) over(
            partition by id_veiculo, shape_id
            order by id_veiculo, shape_id, timestamp_gps
            rows between unbounded preceding and current row)
        as current_stop
    from GPSArrivalTime
),
  
GPSNextStop as (
    select timestamp_gps, g.data as data, hora, g.servico as servico, latitude,
        longitude, flag_em_movimento, current_stop, previous_stop,
        tipo_parada, flag_trajeto_correto, velocidade_instantanea, velocidade_estimada_10_min,
        id_veiculo, arrival_time, dist_traveled_shape,
        s.dist_traveled_stop as dist_traveled_next_stop
    from GPSCurrentStop g
        left join GTFSStops s
            on g.data = s.data and g.servico = s.servico and g.shape_id = s.shape_id and g.current_stop = s.stop_sequence - 1
),
      
GPSStopsDistance as (
    select *,
        dist_traveled_next_stop - dist_traveled_shape as dist_next_stop
    from GPSNextStop
)
        
select * from GPSStopsDistance 
    where dist_next_stop > 0 and dist_next_stop < 1000
        and arrival_time < 60