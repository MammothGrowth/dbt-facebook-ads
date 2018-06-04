with ads_base as (

  select * from {{ref('FB_ADS_XF')}}

), creatives as (

  select * from {{ref('FB_AD_CREATIVES')}}

), insights as (

  select * from {{ref('FB_AD_INSIGHTS')}}

), campaigns as (

  select * from {{ref('FB_CAMPAIGNS')}}

), adsets as (

  select * from {{ref('FB_ADSETS_XF')}}

), final as (

    select
        md5(insights.date_day || '|' || ads_base.unique_id) as id,
        insights.*,
        creatives.base_url,
        creatives.url,
        creatives.utm_medium,
        creatives.utm_source,
        creatives.utm_campaign,
        creatives.utm_content,
        creatives.utm_term,
        ads_base.unique_id as ad_unique_id,
        ads_base.name as ad_name,
        campaigns.name as campaign_name,
        adsets.name as adset_name
    from insights
    left outer join ads_base
        on insights.ad_id = ads_base.id
        and insights.date_day >= date_trunc('day', ads_base.effective_from)::date
        and (insights.date_day < date_trunc('day', ads_base.effective_to)::date or ads_base.effective_to is null)
    left outer join creatives on ads_base.creative_id = creatives.id
    left outer join campaigns on campaigns.id = insights.campaign_id
    left outer join adsets on adsets.id = insights.adset_id
--these have to be an outer join because while the stitch integration goes
--back in time for the core reporting tables (insights, etc), it doesn't go back
--in time for the lookup tables. so there are actually lots of ad_ids that don't
--exist when you try to do the join, but they're always prior to the date you
--initially made the connection.

)

select * from final
