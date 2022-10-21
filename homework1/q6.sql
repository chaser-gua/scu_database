select `id`,`orderdate`,`pre`,round(julianday(`orderdate`) - julianday(`pre`),2)  as `differeance`
from(
select `id`,`orderdate`,LAG(`orderdate`,1,`orderdate`) over(order by `orderdate`) as `pre`
from `order`
where `customerid` == 'BLONP'
order by `orderdate`
limit 10)



-- SELECT
--      Id
--      , OrderDate
--      , PrevOrderDate
--      , ROUND(julianday(OrderDate) - julianday(PrevOrderDate), 2)
-- FROM (
--      SELECT Id
--           , OrderDate
--           , LAG(OrderDate, 1, OrderDate) OVER (ORDER BY OrderDate ASC) AS PrevOrderDate
--      FROM 'Order' 
--      WHERE CustomerId = 'BLONP'
--      ORDER BY OrderDate ASC
--      LIMIT 10
-- );