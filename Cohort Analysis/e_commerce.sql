
-- ----------------------- PART 1 - CREATING DATABASE AND EXECUTING QUERIES ----------------


CREATE DATABASE Ecommerce;      -- creating ecommerce database to store our tables

CREATE TABLE Users(
        UserID int NOT NULL,                 -- User Id can't be null
        SignUPDate DATETIME,
        Location varchar(255),
        PRIMARY KEY (UserID)                 -- Assigning Name to primary keys separately so that they can be modified easily
);
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Users.csv'    -- Loading CSV data into our tables
INTO TABLE Users
FIELDS TERMINATED BY ','         -- Delimeter of CSV File
IGNORE 1 ROWS;                   -- ignoring header


CREATE TABLE Products(           -- Creating Products Table
		ProductID int NOT NULL, 
        ProductName varchar(100), 
        Price float,
        PRIMARY KEY (ProductID)
);

-- Importing CSV data into our table
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Products.csv'                  
INTO TABLE Products
FIELDS TERMINATED BY ','
IGNORE 1 ROWS;


CREATE TABLE Orders(         -- Creating Orders Table
        OrderID int NOT NULL,
        UserID int,
        ProductID int, 
        OrderDate varchar(50), 
        Amount float,
        PRIMARY KEY (OrderID),
        FOREIGN KEY (UserID) REFERENCES Users(UserID),          -- Assigning foreign keys 
        FOREIGN KEY (ProductID) REFERENCES Products(ProductID)
);
-- Loading CSV data into our Orders tables
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Orders.csv'
INTO TABLE Orders
FIELDS TERMINATED BY ','
IGNORE 1 ROWS;


-- ● Queries to Execute:

-- 	 1. List the total number of orders for each product, sorted by the number of Orders.
	
SELECT p.ProductID, p.ProductName,
	COUNT(o.OrderID) as TotalOrders
	FROM Products p
	LEFT JOIN Orders o ON p.ProductID = o.ProductID
	GROUP BY p.ProductID, p.ProductName
	ORDER BY TotalOrders DESC;
    
--   2. Find the total revenue generated by each user.
SELECT UserID, SUM(Amount) as TotalRevenue
From Orders 
GROUP BY UserID;

--  3. Display Users who have not Placed any orders. 
SELECT u.UserID, u.SignUpDate, u.Location
FROM Users u
LEFT JOIN Orders o ON u.UserID = o.UserID
WHERE o.UserID IS NULL;
-- Result is Empty as such there are no Users which haven't placed any orders.




-- -------------------- PART 2 - SQL PIPELINE ---------------------------
Create DATABASE ecommerce2;  -- Database Created for the implementation of Pipeline 



-- ---------------------PART 3 - COHORT ANALYSIS ------------------------

-- creating a temporary cohort selection table
WITH Cohorts AS (
SELECT
    YEAR(SignUpDate) AS CohortYear,
    MONTH(SignUpDate) AS CohortMonth,
    UserID
FROM Users  
GROUP BY CohortYear, CohortMonth, UserID)
SELECT                
    CohortYear, CohortMonth,                        -- Selecting 5 Attributes for final table
    COUNT(DISTINCT Cohorts.UserID) AS TotalUsers,
    -- Using "CASE WHEN THEN" for conditional selection and comparision of cohort months
    COUNT(DISTINCT CASE WHEN MONTH(OrderDate) = CohortMonth + 0 THEN Orders.UserID END) AS Month0Users,
    COUNT(DISTINCT CASE WHEN MONTH(OrderDate) = CohortMonth + 1 THEN Orders.UserID END) AS Month1Users,
    COUNT(DISTINCT CASE WHEN MONTH(OrderDate) = CohortMonth + 2 THEN Orders.UserID END) AS Month2Users
FROM Cohorts
LEFT JOIN Orders
    ON Cohorts.UserID = Orders.UserID
GROUP BY CohortYear, CohortMonth;
