SELECT *
FROM [pizza].[orders] as p
LEFT OUTER JOIN [dbo].[order_date_to_days] as v On p.order_date = v.order_date
where [v].[order_day] = '2'