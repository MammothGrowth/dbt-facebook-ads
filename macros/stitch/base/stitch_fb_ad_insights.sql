{% macro stitch_fb_ad_insights() %}

    {{ adapter_macro('facebook_ads.stitch_fb_ad_insights') }}

{% endmacro %}


{% macro default__stitch_fb_ad_insights() %}

select

    date_start::date as date_day,
    nullif(account_id,'') as account_id,
    nullif(account_name,'') as account_name,
    nullif(ad_id,'') as ad_id,
    nullif(adset_id,'') as adset_id,
    nullif(campaign_id,'') as campaign_id,
    impressions,
    clicks,
    unique_clicks,
    spend,
    frequency,
    reach,
    nullif(objective,'') as objective,
    canvas_avg_view_percent,
    canvas_avg_view_time,
    inline_link_clicks,
    inline_post_engagement,
    unique_inline_link_clicks

from {{ source('facebook_ads', 'ads_insights') }}
WHERE nullif(campaign_id,'') is not null 


{% endmacro %}