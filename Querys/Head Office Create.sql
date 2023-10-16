CREATE SCHEMA pizza;
GO
DROP TABLE IF EXISTS pizza.order_items;
DROP TABLE IF EXISTS pizza.orders;
DROP TABLE IF EXISTS pizza.customers;
DROP TABLE IF EXISTS pizza.summary;

CREATE TABLE pizza.customers(
customer_id int IDENTITY(1,1) NOT NULL PRIMARY KEY,
first_name varchar(255) NOT NULL,
last_name varchar(255) NOT NULL,
phone varchar (25) NULL,
address varchar(255) NULL,
post_code varchar(4) NULL
);

CREATE TABLE pizza.orders(
order_id int NOT NULL PRIMARY KEY,
customer_id int NULL FOREIGN KEY REFERENCES pizza.customers(customer_id),
order_date date NOT NULL,
store_id int NOT NULL,
order_value decimal (10, 2) NULL
);

CREATE TABLE pizza.order_items (
order_item_id int NOT NULL PRIMARY KEY,
order_id int NOT NULL FOREIGN KEY REFERENCES pizza.orders(order_id),
product_name varchar (255) NOT NULL,
quantity int NOT NULL,
list_price [decimal](10, 2) NOT NULL
);

CREATE TABLE pizza.summary (
summary_id int IDENTITY(1,1) NOT NULL PRIMARY KEY,
store_id int NULL,
summary_date date NULL,
total_sales decimal (10, 2) NULL,
total_orders int NULL,
best_product varchar (255) NULL
);
