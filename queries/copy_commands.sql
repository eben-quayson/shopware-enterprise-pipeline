COPY dev.public.sales_mart
FROM 's3://group-four-lakehouse-bucket/gold/sales_mart/'
IAM_ROLE 'arn:aws:iam::182399707265:role/service-role/AmazonRedshift-CommandsAccessRole-20250507T140424'
FORMAT AS CSV
DELIMITER ','
QUOTE '"'
IGNOREHEADER 1
REGION 'eu-west-1';

COPY dev.public.sales_mart
FROM 's3://group-four-lakehouse-bucket/gold/marketing_mart/'
IAM_ROLE 'arn:aws:iam::182399707265:role/service-role/AmazonRedshift-CommandsAccessRole-20250507T140424'
FORMAT AS CSV
DELIMITER ','
QUOTE '"'
IGNOREHEADER 1
REGION 'eu-west-1';
