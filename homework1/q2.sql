select `id`, `shipcountry`,
case 
  when `ShipCountry` in ('USA', 'Mexico','Canada')
  then 'NorthAmerica'
  else 'OtherPlace'
end
as `class`
from  `order`
where `Id` >= 15445
order by `id`
limit 20