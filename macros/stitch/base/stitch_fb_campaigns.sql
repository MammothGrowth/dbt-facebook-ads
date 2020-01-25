{% macro stitch_fb_ads_campaigns() %}

    {{ adapter_macro('facebook_ads.stitch_fb_ads_campaigns') }}

{% endmacro %}


{% macro default__stitch_fb_ads_campaigns() %}

select

    nullif(id,'') as campaign_id,
    nullif(name,'') as name
    
from {{ source('facebook_ads', 'campaigns') }}

{% endmacro %}