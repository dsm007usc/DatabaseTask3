CREATE VIEW [order_date_to_days] AS
SELECT [order_date],
ROW_NUMBER() OVER(ORDER BY(SELECT 1)) [order_day]
FROM [pizza].[orders]
GROUP by [order_date]