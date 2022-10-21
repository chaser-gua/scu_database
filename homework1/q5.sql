select `productname`,`companyname`,`contactname`
from(
select `productname`,`companyname`,`contactname`,min(`orderdate`)
from    
(select `id`,`productname`
from `product`
where `discontinued` == 1) as `d`
join `orderdetail` od on `d`.`id` == od.`productid`
join `order` o on o.`id` == od.`orderid`
join `customer` c on c.`id` == o.`customerid`
group by d.`id`)
order by `productname`