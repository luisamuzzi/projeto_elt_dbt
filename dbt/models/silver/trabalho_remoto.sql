-- import dos dados: extract da fonte
with source as (
    select
        "jobTitle",
        "companyName",
        "jobType",
        "jobGeo",
        "jobLevel",
        "salaryMin",
        "salaryMax",
        "salaryCurrency"
    from {{source("PROJETO_DBT", "remote_jobs")}}
),

-- renamed: inserir todas as transformações
renamed as (
    select
        "jobTitle" as titulo_vaga,
        "companyName" as nome_empresa,
        "jobType" as tipo_trabalho,
        "jobGeo" as localizacao,
        "jobLevel" as senioridade,
        cast("salaryMin" as float) as min_salario_anual,
        cast("salaryMax" as float) as max_salario_anual,
        "salaryCurrency" as moeda
    from source
),
-- final: select final
final as (
    select
        titulo_vaga,
        nome_empresa,
        tipo_trabalho,
        localizacao,
        senioridade,
        round(min_salario_anual/12, 2) as min_salario_mensal,
        min_salario_anual,
        round(max_salario_anual/12, 2) as max_salario_mensal,
        max_salario_anual,
        moeda
    from renamed
    where min_salario_anual != 'NaN'
    and max_salario_anual !='NaN' 
)

select * from final