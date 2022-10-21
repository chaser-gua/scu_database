with `expenditures` as(
select ifnull(c.`companyname`,'MISSING_NAME') as `companyname`,`customerid`,round(sum(od.`unitprice`* od.`quantity`),2) as `expenditure`
from `order` o
left join `customer` c on o.`customerid` == c.`id`
join `orderdetail` od on od.`orderid` == o.`id`
group by o.`customerid`),
`quartiles` as(
select *, ntile(4) over(order by `expenditure`) `quartile`
from `expenditures`
)

SELECT `companyname`, `customerid`, `expenditure`
from `quartiles` 
where `quartile` = 1 
order by `expenditure`