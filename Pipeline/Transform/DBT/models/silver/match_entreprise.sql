{{ config(materialized='view') }}

with raw as (
    select
        id_offre,
        id_local,
        lower(company) as company_name_lower
    from {{ ref('raw_offre_clean') }}
    where cast(date_extraction as date) = current_date
),

dim as (
    select
        id_entreprise,
        lower(nom) as nom_lower
    from SILVER.DIM_ENTREPRISE
),

matched as (
    select
        r.id_offre,
        r.id_local,
        r.company_name_lower,
        d.id_entreprise,
        d.nom_lower as nom,

        /* scoring pour prioriser le meilleur match */
        case
            when r.company_name_lower = d.nom_lower                                then 3   -- exact
            when r.company_name_lower like d.nom_lower || '%'                      then 2   -- prefixe
            when r.company_name_lower like '% ' || d.nom_lower || ' %'             then 1   -- mot entier
            when r.company_name_lower like '% ' || d.nom_lower                     then 1
            when r.company_name_lower like d.nom_lower || ' %'                     then 1
            when r.company_name_lower like '%' || d.nom_lower || '%'               then 0   -- substring
            else -1
        end as score,
        len(d.nom_lower) as nom_len
    from raw r
    left join dim d
      on r.company_name_lower like '%' || d.nom_lower || '%'
),

ranked as (
    select
        *,
        row_number() over (
            partition by id_local
            order by score desc, nom_len desc, coalesce(id_entreprise, 9e18)
        ) as rn
    from matched
)

select
    id_offre,
    id_local,
    id_entreprise,
    company_name_lower,
    nom
from ranked
qualify rn = 1
