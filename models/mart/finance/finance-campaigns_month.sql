with base as (

    select
      date_trunc(date_date, month) as datemonth,
      sum(ads_cost) as ads_cost,
      sum(impression) as ads_impression,
      sum(click) as ads_click
    from {{ ref('int_campaigns') }}
    group by 1

),

sales as (

    select
      date_trunc(date_date, month) as datemonth,
      sum(quantity) as quantity,
      sum(revenue) as revenue,
      sum(purchase_cost) as purchase_cost,
      sum(margin) as margin,
      sum(shipping_fee) as shipping_fee,
      sum(logcost) as logcost,
      sum(ship_cost) as ship_cost
    from {{ ref('int_orders_operational') }}
    group by 1

),

joined as (

    select
      b.datemonth,

      -- Ad metrics
      b.ads_cost,
      b.ads_impression,
      b.ads_click,

      -- Sales metrics
      s.quantity,
      s.revenue,
      s.purchase_cost,
      s.margin,
      s.shipping_fee,
      s.logcost,
      s.ship_cost,

      -- Finance KPIs
      safe_divide(s.revenue - b.ads_cost, s.revenue) as ads_margin,
      safe_divide(s.revenue, s.quantity) as average_basket,
      safe_divide(s.margin - b.ads_cost, s.revenue) as operational_margin

    from base b
    left join sales s using (datemonth)

)

select * from joined
order by datemonth desc