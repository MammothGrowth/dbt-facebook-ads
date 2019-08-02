{% macro fivetran_fb_ads_adsets() %}

    {{ adapter_macro('facebook_ads.fivetran_fb_ads_adsets') }}

{% endmacro %}

{% macro default__fivetran_fb_ads_adsets() %}

with base as (

    select
    
        id::varchar(256) as adset_id,
        nullif(name,'') as name,
        account_id,
        campaign_id,
        created_time,
        nullif(effective_status,'') as effective_status,
        row_number() over (partition by id, name, account_id, campaign_id, created_time order by _FIVETRAN_SYNCED desc) as row_num

    from {{source('facebook_ads', 'AD_SET_HISTORY')}}
),

final as (

    select 
        * 

    from base 
    where row_num = 1

)

select * from final

{% endmacro %}