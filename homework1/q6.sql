select `id`,`orderdate`,`pre`,round(julianday(`orderdate`) - julianday(`pre`),2)  as `differeance`
from(
select `id`,`orderdate`,LAG(`orderdate`,1,`orderdate`) over(order by `orderdate`) as `pre`
from `order`
where `customerid` == 'BLONP'
order by `orderdate`
limit 10)
