{{ config(materialized='view') }}

with
offre as (
    select
        id_offre,
        id_local,
        seniorite_type
    from {{ ref('raw_offre_clean') }}
    where date_extraction::date = current_date
),

-- extraction du nombre d'années quand présent (1,2,3,5,6,7…)
offre_norm as (
    select
        id_offre,
        id_local,
        seniorite_type,
        try_to_number(regexp_substr(seniorite_type, '\\d+')) as nb_ans
    from offre
),

-- classification simple: junior / confirmé / senior
seniorite_calc as (
    select
        id_offre,
        id_local,
        seniorite_type,
        case
            when seniorite_type ilike '%débutant accepté%' then 'junior'
            when nb_ans = 1 then 'junior'
            when seniorite_type ilike 'expérience exigée' then 'confirmé'
            when nb_ans in (2, 3) then 'confirmé'
            when nb_ans >= 5 then 'senior'
            else null
        end as seniorite
    from offre_norm
),

dim as (
    select
        id_seniorite,
        type_seniorite
    from {{ source('SILVER','DIM_SENIORITE') }}
),

matching as (
    select
        s.id_offre,
        s.id_local,
        s.seniorite_type,
        s.seniorite,
        d.id_seniorite
    from seniorite_calc s
    left join dim d
      on lower(d.type_seniorite) = lower(s.seniorite)
)

select * from matching
