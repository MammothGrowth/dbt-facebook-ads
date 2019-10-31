with ads1 as (

  select * from {{ref('fb_ads_xf')}}

), creatives1 as (

  select * from {{ref('fb_ad_creatives')}}

), insights1 as (

  select * from {{ref('fb_ad_insights')}}

), campaigns1 as (

  select * from {{ref('fb_campaigns')}}

), adsets1 as (

  select * from {{ref('fb_adsets_xf')}}

), final1 as (

    select
    
        {{ dbt_utils.surrogate_key('insights.date_day', 'insights.ad_id') }} as id,
        insights.*,
        creatives.base_url,
        creatives.url,
        creatives.url_host,
        creatives.url_path,
        creatives.utm_medium,
        creatives.utm_source,
        creatives.utm_campaign,
        creatives.utm_content,
        creatives.utm_term,
        creatives.ad_id,
        ads.unique_id as ad_unique_id,
        ads.name as ad_name,
        campaigns.name as campaign_name,
        adsets.name as adset_name

    from insights1 as insights
    left outer join ads1 as ads
        on insights.ad_id = ads.ad_id
        and insights.date_day >= date_trunc('day', ads.effective_from)::date
        and (insights.date_day < date_trunc('day', ads.effective_to)::date or ads.effective_to is null)
    left outer join creatives1 as creatives on ads.creative_id = creatives.creative_id
    left outer join campaigns1 as campaigns on campaigns.campaign_id = insights.campaign_id
    left outer join adsets1 as adsets on adsets.adset_id = insights.adset_id
--these have to be an outer join because while the stitch integration goes
--back in time for the core reporting tables (insights, etc), it doesn't go back
--in time for the lookup tables. so there are actually lots of ad_ids that don't
--exist when you try to do the join, but they're always prior to the date you
--initially made the connection.

)

select * from final1
