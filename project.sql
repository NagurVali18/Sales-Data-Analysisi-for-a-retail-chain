CREATE DATABASE Internpro;

USE Internpro;

Create Table Sales_Data_Transactions(
customer_id VARCHAR(255),
trans_date VARCHAR(255),
tran_amount INT
);

Create Table Sales_Data_Response(
customer_id VARCHAR(255),
response INT
);

show tables;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Retail_Data_Transactions.csv'
INTO TABLE Sales_Data_Transactions
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Retail_Data_Response.csv'
INTO TABLE Sales_Data_Response
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

select * from Sales_Data_Transactions;

select * from Sales_Data_Response;

EXPLAIN select * from Sales_Data_Transactions where customer_id='CS5295';

CREATE index idx_id on Sales_Data_Transactions(customer_id);

EXPLAIN select * from Sales_Data_Transactions where customer_id='CS5295';

-- Handling missing values
UPDATE sales_data_transactions
SET tran_amount = 0
WHERE tran_amount IS NULL;

-- Removing outliers

-- Step 1: Calculate Q1 and Q3
SET @q1 := (
    SELECT tran_amount
    FROM sales_data_transactions
    ORDER BY tran_amount
    LIMIT FLOOR((@total := (SELECT COUNT(*) FROM sales_data_transactions)) * 0.25), 1
);

SET @q3 := (
    SELECT tran_amount
    FROM sales_data_transactions
    ORDER BY tran_amount
    LIMIT FLOOR(@total * 0.75), 1
);

-- Step 2: Delete outliers
DELETE FROM sales_data_transactions
WHERE tran_amount > (@q3 + 1.5 * (@q3 - @q1));


-- Aggregating data
SELECT customer_id, SUM(tran_amount) AS total_sales
FROM sales_data_transactions
GROUP BY customer_id;

-- Data validation
SELECT * FROM sales_data_transactions
WHERE customer_id IS NULL;

/* Data Sampling --> simple random sampling:- If the dataset is large, 
consider taking a random sample for faster testing and analysis. */
SELECT * FROM sales_data_transactions
ORDER BY RAND() LIMIT 100;

-- Data Normalization 
SELECT customer_id, (tran_amount - MIN(tran_amount)) / (MAX(tran_amount) - MIN(tran_amount)) AS normalized_amount
FROM sales_data_transactions;

-- Data Filtering 
DELETE FROM sales_data_transactions
WHERE tran_amount < 10;

--  Number of rows into our datashet
select count(*) from sales_data_response;
select count(*) from sales_data_transactions;

-- Total Revenue 
SELECT SUM(tran_amount) AS total_revenue
FROM sales_data_transactions;

-- Average Transaction Amount
SELECT round(AVG(tran_amount),2) AS average_transaction_amount FROM sales_data_transactions;


-- Number of Transactions per Customer
SELECT customer_id, COUNT(*) AS total_transactions FROM sales_data_transactions
GROUP BY customer_id ORDER BY total_transactions DESC;

-- Total Revenue by Customer
SELECT customer_id, SUM(tran_amount) AS total_revenue
FROM  sales_data_transactions GROUP BY customer_id
ORDER BY total_revenue DESC;


-- Customer Segmentation by Spending
SELECT customer_id, SUM(tran_amount) AS total_spent
FROM sales_data_transactions
GROUP BY customer_id
HAVING SUM(tran_amount) >= 2000
ORDER BY total_spent DESC;

--  Total customers 
select customer_id, count(*) as `Total Customers` from sales_data_transactions 
group by customer_id order by `Total Customers` desc;

--  Show top 10 Customers with Highest Transaction Amount
select * from sales_data_transactions group by customer_id order by tran_amount desc limit 10;

--  datashet for only two customer_id CS1707 AND CS1761
select * from sales_data_transactions where customer_id in ('CS1707','CS1761') order by tran_amount desc;


-- Avg Transaction Amount
select round(avg(tran_amount),3) * 100 as Total_Transaction_Amount from sales_data_transactions;

select customer_id, round(avg(tran_amount), 0) * 100  as Avg_tran_amount from sales_data_transactions
group by customer_id having Avg_tran_amount > 5000 order by Avg_tran_amount desc;

-- Top 3 customers showing highest transaction ratio
select customer_id, avg(tran_amount) * 100 as Avg_growth from sales_data_transactions
group by customer_id order by Avg_growth desc limit 3;


-- bottom 3 customers showing lowest transaction ratio
select customer_id, round(avg(tran_amount),3) * 100 as Avg_Tran from sales_data_transactions
group by customer_id order by Avg_Tran asc limit 3;


-- Both Top and Bottom Transaction:
select * from 
(
select * from (
select customer_id, avg(tran_amount) * 100 as Avg_growth from sales_data_transactions
group by customer_id order by Avg_growth desc limit 3)a
union
select * from (
select customer_id, round(avg(tran_amount),3) * 100 as Avg_Tran from sales_data_transactions
group by customer_id order by Avg_Tran asc limit 3)b
) c;

-- customer_id starting with CS2845, CS5909 --
select concat(concat_ws(" => ", customer_id, tran_amount),'*') as `customer_id, tran_amount` from sales_data_transactions
where Ucase(customer_id) like "CS2845%" or Lcase(customer_id) like "CS5909%";

-- joining both tables 
select * from sales_data_transactions as sal_tran
inner join sales_data_response as sal_res on sal_tran.customer_id = sal_res.customer_id order by tran_amount desc;


-- Total response
select  sal_tran.customer_id, sal_tran.trans_date, sal_tran.tran_amount,
case
when sum(sal_res.response) = 0 then "No Response"
else sum(sal_res.response)
end as Response
from sales_data_transactions as sal_tran
inner join sales_data_response as sal_res on sal_tran.customer_id = sal_res.customer_id
group by customer_id
order by Response asc;


/* Which customers have the most transactions? */
select customer_id, count(*) as count_of_Total_Transactions from sales_data_transactions
group by customer_id order by count_of_Total_Transactions desc;

/* Write a query that returns one customer that has the highest sum of transaction amount totals. 
Return both the customer_id & sum of transactions totals */
SELECT customer_id, sum(tran_amount) AS Total_Transactions
FROM sales_data_transactions
GROUP BY customer_id
ORDER BY Total_Transactions DESC limit 1;


/* Who is the best customer? The customer who has spent the most money will be declared the best customer. 
Write a query that returns the person who has spent the most money.*/

SELECT 
    sal_tran.customer_id, 
    sal_tran.trans_date, 
    sal_tran.tran_amount AS Total_Spending, 
    sal_res.response
FROM 
    sales_data_transactions AS sal_tran
INNER JOIN 
    sales_data_response AS sal_res ON sal_tran.customer_id = sal_res.customer_id
INNER JOIN 
    (
        SELECT 
            customer_id, 
            MAX(tran_amount) AS max_amount
        FROM 
            sales_data_transactions
        GROUP BY 
            customer_id
    ) AS max_tran ON sal_tran.customer_id = max_tran.customer_id AND sal_tran.tran_amount = max_tran.max_amount
ORDER BY 
    Total_Spending DESC
LIMIT 1;

-- Sub- Query (2nd highest transaction amount).
select max(tran_amount) as 'Second highest transaction amount' from sales_data_transactions
where tran_amount not in (select max(tran_amount) from sales_data_transactions);


-- Transaction Status Classification Report / Calculated column
select customer_id, tran_amount,
case
when tran_amount >= 80 then 'High Transaction'
when tran_amount >= 40 and tran_amount <= 80 then 'Medium Transaction'
else 'Low Transaction'
end as `Transaction Status `
from  sales_data_transactions order by tran_amount desc;