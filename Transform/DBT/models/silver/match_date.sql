{{ config(materialized='view') }}

with raw as (
    select *
    from RAW.RAW_OFFRE_CLEAN
    where cast(DATE_EXTRACTION as date) = current_date
),

-- alias pour les dates
dim_date_created as (
    select id_date
    from SILVER.DIM_DATE
),

-- jointures
joined as (
    select
        r.*,
        dc.id_date as id_date_created
    from raw r
    left join dim_date_created dc
        on to_date(r.DATE_CREATED) = dc.id_date
)


-- sélection finale avec fallback sur '1900-01-01'
select
    id_offre,
    ID_LOCAL as id_local,
    coalesce(id_date_created, to_date('1900-01-01')) as id_date_created
    -- ajoute ici les autres colonnes que tu veux projeter
from joined