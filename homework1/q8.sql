select `regiondescription`,`firstname`,`lastname`,max(`birthdate`) as `young`
from(
(select `id`,`firstname`,`lastname`,`birthdate`
from `employee`) as e
join `employeeterritory` et on e.id == et.`employeeid`
join `territory` t on et.`territoryid` == t.`id`
join `region` r on r.`id` == t.`regionid`)
group by `regionid`
order by r.`id`