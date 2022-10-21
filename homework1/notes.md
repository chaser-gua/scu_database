## q1

1. **数据库函数**

从string截取从start到end之间的字符串，包括start和end位置的字符

```
substr(string, start, end)
```

2. where 字段 like ‘’    ——找出字段中像后面的值，类似正则表达式匹配

## q2

1. select中
```
case
  when
  then
  else
end
```
语句 ——方便归类

## q3

1. round（num，x） ——四舍五入num到小数点后x位数字，  注意num的类型
2. select 计算出一个字段后，在之后的字段中不能马上用 —— 此时可以用with临时保存表
3. **保存临时表和from中用子查询原理是一样的，保存临时表结构更清晰一些**

## q5

1. from的子查询，注意查询结构关系

## q6

1. **数据库函数**
```
LAG(express, offset, default)：找到当前元组的上一offset个元组，并根据express返回前面行的值（必须是单一值）
-- express：是根据指定的偏移量对前一行的值求值的表达式。表达式必须返回单个值。
-- offset：两行之间的偏移量
-- default：默认值，当找不到上一行时返回该值
-- PARTITION BY：根据表达式（属性）来进行分组
LAG(express, offset, default) OVER (PARTITION BY expr1, expr2,... ORDER BY exp1 ASC, exp2 DESC)
```
```
julianday(now) - julianday(before)：now和before之间的时间差，单位为天
```
2. select 计算出一个字段后，在之后的字段中不能马上用 —— 此时可以用with临时保存表

## q7

1. 利用with临时保存一张表，保存多张表之间要用，隔开。保存临时表后仍然需要查询语句