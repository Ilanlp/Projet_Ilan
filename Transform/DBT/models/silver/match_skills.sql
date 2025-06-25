{{ config(materialized='view') }}

with
offre as (
    SELECT
        id_offre,
        id_local,
        description
    from {{ source('RAW', 'RAW_OFFRE_CLEAN') }}
    where date_extraction::date = current_date
),

competence as (
    select 
        id_competence,
        skill
    from {{source("SILVER","DIM_COMPETENCE")}}
),

matching as (
    select
        o.id_offre,
        o.id_local,
        o.description,
        c.skill,
        c.id_competence
    from offre o
    inner join competence c
        on (
            lower(o.description) LIKE '% ' || lower(c.skill) || ' %'
        )
)

select * from matching