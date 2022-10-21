select `total`.`companyname`, round(`delaycount` * 100.0 / `totalcount`, 2) as `percentage`
from
(select `companyname`,count(*) as `totalcount`
from `shipper` s,`order` o
where s.`id` == o.`shipvia`
group by `companyname`) as `total`
join
(select `companyname`,count(*) as `delaycount`
from `shipper` s,`order` o
where s.`id` == o.`shipvia` and o.`shippeddate` > o.`requireddate`
group by `companyname`) as `delay`
on `delay`.`companyname` == `total`.`companyname`
order by `percentage` desc