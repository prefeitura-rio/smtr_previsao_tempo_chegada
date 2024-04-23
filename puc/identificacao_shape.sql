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
        distancia, id_veiculo
    from `rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo`
    where data between {start_date} and {end_date}
        and flag_em_operacao = TRUE
    order by servico, id_veiculo, timestamp_gps
),
        
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
-- primeiro crio uma variável _grp que conta valores não-null
-- cada grupo começa com uma mudança de lag_distancia_shape_pt,
-- seguida de vários nulls

AuxShapesId as (
    select *,
        count(lag_dist_traveled_shape) over(
            partition by id_veiculo, shape_id
            order by id_veiculo, shape_id, timestamp_gps)
        as _grp
    from GPSShapesChange
),

-- tomo o primeiro valor de lag_distancia_shape_pt de cada grupo

GPSShapesFill as (
    select * except(_grp, lag_dist_traveled_shape),
        FIRST_VALUE(lag_dist_traveled_shape) over(
            partition by id_veiculo, shape_id, _grp
            order by id_veiculo, shape_id, timestamp_gps)
        as lag_dist_traveled_shape
    from AuxShapesId
),

-- na shape_id correta, a distância só pode ter aumentado

GPSShapesIncreasing as (
    select * except(lag_dist_traveled_shape),
        dist_traveled_shape - lag_dist_traveled_shape as delta
    from GPSShapesFill
    where dist_traveled_shape > lag_dist_traveled_shape
),

-- em caso de empate (mais de uma shape_id ainda faz sentido),
-- escolho a que teve menor salto de distância

-- base auxiliar com distâncias mínimas

AuxMinDelta as (
    select id_veiculo, timestamp_gps, MIN(delta) as delta
    from GPSShapesIncreasing
    group by id_veiculo, timestamp_gps
),

-- inner join para ficar apenas com linhas que têm distâncias mínimas

GPSShapesId as (
    select * except(delta)
    from GPSShapesIncreasing
        inner join AuxMinDelta using(id_veiculo, timestamp_gps, delta)
),

-- Agora que sabemos as shape_ids, identificamos pontos de ônibus

-- identificando pontos de onibus
        
GPSStops as (
    select *,
        ST_GEOGPOINT(stop_lon, stop_lat) stop_geo
    from GPSShapesId
        left join GTFSStops using(data, servico, shape_id)
        where ABS(stop_lon - longitude) < 0.005 and ABS(stop_lat - latitude) < 0.005
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
           when distancia_ponto <= 50
               then stop_id
           else 'none'
        end stop
    from GPSStops3
),
        
-- ponto anterior
        
GPSStops5 as (
    select *,
        LAG(stop) over(
            partition by id_veiculo
            order by id_veiculo, timestamp_gps)
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
            partition by id_veiculo
            order by id_veiculo, timestamp_gps desc)
        as _grp2
    from GPSStops6
),
        
GPSStops8 as (
    select *,
        FIRST_VALUE(pre_arrival_time) over(
            partition by id_veiculo, _grp2
            order by id_veiculo, timestamp_gps desc)
        as pre_arrival_time2
    from GPSStops7
        ),
        
-- quando está no ponto, tempo de chegada é o seguinte
        
GPSStops9 as (
    select *,
        LEAD(pre_arrival_time2) over(
            partition by id_veiculo
            order by id_veiculo, timestamp_gps)
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
    select * except(stop_lon, stop_lat),
        DATETIME_DIFF(pre_arrival_time3, timestamp_gps, MICROSECOND)/(60 * 1000000) as arrival_time
    from GPSStops10
),
        
-- calculando distância até o ponto seguinte
  
GPSStops15 as (
    select timestamp_gps, g.data as data, hora, g.servico as servico, latitude,
        longitude, flag_em_movimento, g.stop_sequence as stop_sequence,
        tipo_parada, flag_trajeto_correto, velocidade_instantanea, velocidade_estimada_10_min,
        id_veiculo, stop, arrival_time, dist_traveled_shape,
        s.dist_traveled_stop as dist_traveled_next_stop
    from GPSStops11 g
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
        