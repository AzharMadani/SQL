
create database dataset;
use dataset;
select * from market_fact;
select * from cust_dimen;
select * from orders_dimen;
select * from prod_dimen;
select * from shipping_dimen;
##Composite data of a business organisation, confined to ‘sales and delivery’
##domain is given for the period of last decade. From the given data retrieve 
##solutions for the given scenario.
#1. Join all the tables and create a new table called combined_table.
#(market_fact, cust_dimen, orders_dimen, prod_dimen, shipping_dimen)
create table combined_table as (
select * from cust_dimen join (
market_fact join shipping_dimen using(ship_id) 
join orders_dimen using(ord_id, order_id) 
join prod_dimen using(prod_id)) using(cust_id));

 select * from combined_table;
drop table combined_table;
# 2. Find the top 3 customers who have the maximum number of orders

select * from (
select m.order_quantity,cd.customer_name,
dense_rank() over (order by m.order_quantity desc )max_num
from market_fact m join cust_dimen cd
on m.cust_id=cd.cust_id)top_3
group by customer_name
having max_num<=3;

#3. Create a new column DaysTakenForDelivery that contains the date difference of Order_Date and Ship_Date.

alter table combined_table
add column DaysTakenForDelivery  int;
update combined_table
set DaysTakenForDelivery = abs(datediff(order_date,ship_date));

select * from combined_table;

#4. Find the customer whose order took the maximum time to get delivered.
select c.customer_name,o.order_id, o.order_priority, sd.ship_mode,sd.ship_id,max(abs(datediff(o.order_date,sd.ship_date))) as DaysTakenForDelivery
from orders_dimen o join shipping_dimen sd	
on o.order_id=sd.order_id
inner join market_fact m
on m.ord_id=o.ord_id
inner join cust_dimen c
on m.cust_id=c.cust_id;
#5. Retrieve total sales made by each product from the data (use Windows function)
select p.prod_id, 
sum(sales) over(order by m.sales desc)tot_sales
from market_fact m join prod_dimen p
on m.prod_id=p.prod_id
group by p.prod_id;
#6. Retrieve total profit made from each product from the data (use windows function)
select p.product_sub_category, 
sum(profit) over(order by m.profit desc)tot_profit
from market_fact m join prod_dimen p
on m.prod_id=p.prod_id
group by p.product_sub_category;
#7. Count the total number of unique customers in January and how many of them came back every month over the entire year in 2011
SELECT Year(order_date),Month(order_date),count(distinct cust_id) AS num
FROM combined_table
WHERE year(order_date)=2011 and cust_id in (
select distinct cust_id
from combined_table
where year(order_date)='2011' and	month(order_date)=1)
GROUP BY Year(order_date),Month(order_date);
#8. Retrieve month-by-month customer retention rate since the start of the business.(using views)Tips:
#1: Create a view where each user’s visits are logged by month, allowing for the possibility that these will have occurred over multiple # years since whenever business started operations
# 2: Identify the time lapse between each visit. So, for each person and for each month, we see when the next visit is.
# 3: Calculate the time gaps between visits
# 4: categorise the customer with time gap 1 as retained, >1 as irregular and NULL as churned
# 5: calculate the retention month wise
create or replace view  total_customers as
select distinct month(str_to_date(order_date,"%d-%m-%Y")) m1,count(*) over(partition by month(str_to_date(order_date,"%d-%m-%Y"))) 
as sum from combined_table
where month(str_to_date(order_date,"%d-%m-%Y")) is not null;

select*from total_customers;

select distinct month as m1,count(customer_name) over(partition by month) as retention,status from(
select * ,case when gap=1 then "Retained" when gap >1 then "Irregular" else "Churned" end as "status"from(
select *,abs(month(next_visit)-month(visit)) as gap from
(select customer_name,str_to_date(order_date,"%d-%m-%Y") visit,lead(str_to_date(order_date,"%d-%m-%Y")) over (partition by customer_name order by str_to_date(order_date,"%d-%m-%Y")) next_visit ,
month(str_to_date(order_date,"%d-%m-%Y")) month from combined_table where customer_name is not null order by customer_name)t)t2
where gap<>0 or gap is null)t3 where status="retained";


select*from retention_stats;

#creating a view retention_rate by joining the above two views

create view `customer retention rate` as(
select year,month,number/sum(number) over() * 100 `customer retention rate` from(
select year(str_to_date(order_date,'%d-%m-%y')) year,
monthname(str_to_date(order_date,'%d-%m-%y')) month,
count(distinct order_id) as number
from orders_dimen group by 1,2)t);
select*from retention_rate;

select  * from combined_table ;