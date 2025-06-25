{{ 
  config(
    materialized = 'incremental',
    unique_key   = 'id_local',
    on_schema_change = 'append_new_columns'
  ) 
}}

with raw as (
    select *
    from {{ ref('raw_offre_clean') }}
    where date_extraction::date = current_date
),
source as (

  select
    raw.id_local,
    raw.source_url,
    contrat.id_contrat,
    lieu.id_lieu,
    date_created.id_date_created    as id_date_creation,
    entreprise.id_entreprise,
    raw.title,
    raw.description
  from raw         as raw
  left join {{ ref('match_lieu') }}       as lieu
    on raw.id_local = lieu.id_local
  left join {{ ref('match_contrat') }}    as contrat
    on raw.id_local = contrat.id_local
  left join {{ ref('match_date') }}       as date_created
    on raw.id_local = date_created.id_local
  left join {{ ref('match_entreprise') }} as entreprise
    on raw.id_local = entreprise.id_local

)

select
    id_local,
    id_contrat,
    id_lieu,
    id_date_creation,
    id_entreprise,
    title,
    description,
    source_url
from source
