with raw as (
    select * 
    from {{ ref('raw_offre_clean') }}
    where date(date_extraction) = current_date
),

mapping as (
    select 'CDI' as Contract_type_raw, 'CDI' as contract_type_standard
    union all select 'CDD', 'CDD'
    union all select 'MIS', 'Intérim'
),

normalized_raw as (
    select 
        r.*,
        m.contract_type_standard
    from raw r
    left join mapping m
        on r.contract_type = m.contract_type_raw
),

dim_contrat as (
    select * 
    from {{source('SILVER', 'DIM_CONTRAT')}}
),

joined as (
    select r.id_offre,
    r.id_local,
    d.id_contrat,
    d.type_contrat,
    r.contract_type,
    r.contract_type_standard,
    r.title,
    r.description,

    case 
        when lower(d.type_contrat) = lower(r.contrat_type_standard) then 1
        else 0
    end as is_exact,

    case
        when 

)
)