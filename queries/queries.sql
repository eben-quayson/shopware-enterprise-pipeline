-- Create Sales Team Data Mart Table
CREATE TABLE sales_mart (
    product_id INT NOT NULL,
    store_id INT NOT NULL,
    total_sales FLOAT,
    total_units INT,
    stock_availability INT,
    below_restock_threshold BOOLEAN,
    product_turnover_rate FLOAT,
    date DATE NOT NULL,
    PRIMARY KEY (product_id, store_id, date)
)
DISTKEY (product_id)
SORTKEY (date);

-- Create Marketing Team Data Mart Table
CREATE TABLE marketing_mart (
    customer_id INT NOT NULL,
    engagement_score DECIMAL(5,2),
    avg_session_duration DECIMAL(10,2),
    bounce_rate FLOAT,
    loyalty_activity_rate FLOAT,
    loyalty_actions INT,
    date DATE NOT NULL,
    PRIMARY KEY (customer_id, date)
)
DISTKEY (customer_id)
SORTKEY (date);

-- Create Operations Team Data Mart Table
CREATE TABLE operations_mart (
    product_id INT NOT NULL,
    inventory_turnover DECIMAL(10,2),
    restock_count INT,
    stockout_risk VARCHAR(10),
    date DATE NOT NULL,
    PRIMARY KEY (product_id, date)
)
DISTKEY (product_id)
SORTKEY (date);

-- Create Customer Support Team Data Mart Table
CREATE TABLE customer_support_mart (
    customer_id INT NOT NULL,
    avg_rating DECIMAL(3,1),
    complaint_count INT,
    feedback_count INT,
    loyalty_count INT,
    avg_resolution_hours DECIMAL(6,2),
    date DATE NOT NULL,
    PRIMARY KEY (customer_id, date)
)
DISTKEY (customer_id)
SORTKEY (date);

-- Optional: Add comments for clarity
COMMENT ON TABLE sales_mart IS 'Sales Team Data Mart for KPIs: Total Sales, Stock Availability, Product Turnover Rate';
COMMENT ON TABLE marketing_mart IS 'Marketing Team Data Mart for KPIs: Engagement Score, Session Duration, Bounce Rate, Loyalty Activity Rate';
COMMENT ON TABLE operations_mart IS 'Operations Team Data Mart for KPIs: Inventory Turnover, Restock Frequency, Stockout Alerts';
COMMENT ON TABLE customer_support_mart IS 'Customer Support Team Data Mart for KPIs: Feedback Score, Interaction Volume, Time-to-Resolution';