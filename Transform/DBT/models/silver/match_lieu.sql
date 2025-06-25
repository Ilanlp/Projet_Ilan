with raw as (
    select *
    from {{ source('RAW', 'RAW_OFFRE_CLEAN') }}
    where date(date_extraction) = current_date
),

dim_lieu as (
    select *
    from {{ source('SILVER', 'DIM_LIEU') }}
),

-- Extraction code département à 2 chiffres
normalized_raw as (
    select
        *,
        regexp_substr(
            cast(location as string),
            '\\s*([0-9]{2})\\s*',
            1, 1, 'e', 1
        ) as dept_code
    from raw
),

joined as (
    select
        r.id_offre,
        r.id_local,
        r.location,
        r.latitude,
        r.longitude,
        l.id_lieu,
        l.ville as dim_ville,

        -- Matching textuel
        case when lower(r.location) like '%' || lower(l.ville) || '%' then 1 else 0 end as is_name,
        case when lower(r.location) like '%' || lower(l.departement) || '%' then 1 else 0 end as is_dept,
        case when lower(r.location) like '%' || lower(l.region) || '%' then 1 else 0 end as is_region,
        case when lower(r.location) like '%' || lower(l.pays) || '%' then 1 else 0 end as is_france,

        -- Matching par code département
        case when r.dept_code is not null and substr(l.code_postal, 1, 2) = r.dept_code then 1 else 0 end as is_dept_code,

        -- Distance géographique (Haversine)
        2 * 6371 * asin(sqrt(
            power(sin(radians(r.latitude - l.latitude) / 2), 2) +
            cos(radians(r.latitude)) * cos(radians(l.latitude)) *
            power(sin(radians(r.longitude - l.longitude) / 2), 2)
        )) as distance_km,

        l.population
    from normalized_raw r
    cross join dim_lieu l
),

prioritized as (
    select *,
        row_number() over (
            partition by id_local
            order by
                -- Priorité : coordonnées géo proches, sinon logique textuelle
                case when distance_km < 10 then 1 else 0 end desc,
                distance_km asc,
                is_name desc,
                is_dept_code desc,
                is_dept desc,
                is_region desc,
                is_france desc,
                population desc
        ) as rn
    from joined
)

select
    id_offre,
    id_local,
    id_lieu,
    dim_ville
from prioritized
where rn = 1