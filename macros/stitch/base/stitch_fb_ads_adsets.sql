{% macro stitch_fb_ads_adsets() %}

    {{ adapter_macro('facebook_ads.stitch_fb_ads_adsets') }}

{% endmacro %}


{% macro default__stitch_fb_ads_adsets() %}

select

    id as adset_id,
    nullif(name,'') as name,
    nullif(account_id,'') as account_id,
    nullif(campaign_id,'') as campaign_id,
    created_time,
    nullif(effective_status,'') as effective_status
    
from {{ source('facebook_ads', 'adsets') }}

{% endmacro %}