select `categoryName`,
count(*) as `cnt`,
round(avg(`unitprice`),2) as `avgUnitPrice`,
min(`unitprice`) as `minUnitPrice`,
max(`unitprice`) as `maxUnitPrice`,
sum(`unitsonorder`) as `totalUnitsOnOrder`
from `product` p, `category` c
where p.`categoryid` == c.`id`
group by `categoryid`
having `cnt` > 10
order by `categoryid` 