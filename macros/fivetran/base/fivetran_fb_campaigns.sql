{% macro fivetran_fb_ads_campaigns() %}

    {{ adapter_macro('facebook_ads.fivetran_fb_ads_campaigns') }}

{% endmacro %}


{% macro default__fivetran_fb_ads_campaigns() %}

WITH ranked AS(

    select

        id::varchar(256) as campaign_id,
        nullif(name,'') as name,
        row_number() OVER(PARTITOIN BY campaign_id ORDER BY _fivetran_synced desc) as latest

    from {{source('facebook_ads', 'CAMPAIGN_HISTORY')}}
    union
    SELECT -1, 'Unknown', 1
)
SELECT 
    *
FROM ranked
WHERE latest = 1

{% endmacro %}