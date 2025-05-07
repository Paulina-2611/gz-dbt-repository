select
  date,
  sum(impressions) as total_impressions,
  sum(clicks) as total_clicks,
  sum(ads_cost) as total_cost
from {{ ref('int_campaigns') }}
group by date
order by date desc