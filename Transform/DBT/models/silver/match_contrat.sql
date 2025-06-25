with raw as (
    select *
    from {{ source('RAW', 'RAW_OFFRE_CLEAN') }}
    where date(date_extraction) = current_date
),

-- 1. Mapping contract_type brut → type_contrat standardisé
mapping as (
    select 'CDI' as contract_type_raw, 'CDI' as contract_type_standard
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
    from {{ source('SILVER','DIM_CONTRAT') }}
),

-- 2. Jointure avec tous les contrats pour évaluer les correspondances multiples
joined as (
    select 
        r.id_offre,
        r.id_local,
        d.id_contrat,
        d.type_contrat,
        r.contract_type,
        r.contract_type_standard,
        r.title,
        r.description,

        -- Matching exact via mapping
        case 
            when lower(d.type_contrat) = lower(r.contract_type_standard) then 1
            else 0
        end as is_exact,

        -- Matching partiel (contenu brut)
        case 
            when lower(r.contract_type) like '%' || lower(d.type_contrat) || '%' 
              or lower(r.description) like '%' || lower(d.type_contrat) || '%' then 1
            else 0
        end as is_partial,

        -- Freelance
        case 
            when lower(d.type_contrat) = 'freelance' and (
                lower(r.description) like '%libéral%' or
                lower(r.description) like '%indépendant%' or
                lower(r.description) like '%auto-entrepreneur%' or
                lower(r.description) like '%portage salarial%'
            ) then 1 else 0
        end as is_freelance,

        -- CDI
        case 
            when lower(d.type_contrat) = 'cdi' and (
                lower(r.description) like '%cdi%' or
                lower(r.description) like '%durée indéterminée%' or
                lower(r.description) like '%permanent%'
            ) then 1 else 0
        end as is_cdi_regex,

        -- CDD
        case 
            when lower(d.type_contrat) = 'cdd' and (
                lower(r.description) like '%cdd%' or
                lower(r.description) like '%durée déterminée%' or
                lower(r.description) like '%mission%' or
                lower(r.contract_type) like '%cdd%'
            ) then 1 else 0
        end as is_cdd_regex,

        -- Intérim
        case 
            when lower(d.type_contrat) in ('intérim', 'interim') and (
                lower(r.description) like '%intérim%' or
                lower(r.description) like '%temporaire%' or
                lower(r.description) like '%intérimaire%'
            ) then 1 else 0
        end as is_interim_regex,

        -- Alternance
        case 
            when lower(d.type_contrat) = 'alternance' and (
                lower(r.description) like '%alternance%' or
                lower(r.description) like '%apprentissage%' or
                lower(r.description) like '%professionnalisation%'
            ) then 1 else 0
        end as is_alternance_regex,

        -- Stage
        case 
            when lower(d.type_contrat) = 'stage' and (
                lower(r.description) like '%stage%' or
                lower(r.description) like '%internship%' or
                lower(r.description) like '%stagiaire%'
            ) then 1 else 0
        end as is_stage_regex
    from normalized_raw r
    cross join dim_contrat d
),

-- 3. Priorisation
prioritized as (
    select *,
        row_number() over (
            partition by id_local
            order by
                is_exact desc,
                is_partial desc,
                is_alternance_regex desc,
                is_stage_regex desc,
                is_cdi_regex desc,
                is_cdd_regex desc,
                is_interim_regex desc,
                is_freelance desc
        ) as rn
    from joined
)

-- 4. Résultat final : meilleur match
select 
    id_offre,
    id_local,
    id_contrat,
    type_contrat,
    contract_type
from prioritized
where rn = 1
  and (
      is_exact = 1 or
      is_partial = 1 or
      is_alternance_regex = 1 or
      is_stage_regex = 1 or
      is_cdi_regex = 1 or
      is_cdd_regex = 1 or
      is_interim_regex = 1 or
      is_freelance = 1
  )
