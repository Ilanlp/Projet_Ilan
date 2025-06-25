{{ config(materialized='view') }}

with raw as (
    select
        id_offre,
        id_local,
        lower(company) as company_name_lower
    from RAW.RAW_OFFRE_CLEAN
    where cast(DATE_EXTRACTION as date) = current_date
),

dim as (
    select
        id_entreprise,nom
    from SILVER.DIM_ENTREPRISE
),

matched as (
    select
        r.id_offre,
        d.id_entreprise,
        r.id_local,
        r.company_name_lower,
        d.nom,
    from raw r
    left join dim d
        on r.company_name_lower like '%' || lower(d.nom) || '%'
)

select *
from matched