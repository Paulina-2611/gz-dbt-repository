select
  date_date,
  sum(impression) as total_impression,
  sum(click) as total_clicks,
  sum(ads_cost) as total_cost
from {{ ref('int_campaigns') }}
group by date_date
order by date_date desc