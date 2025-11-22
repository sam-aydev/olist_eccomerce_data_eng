import dlt
from pyspark.sql.functions import *


## Reading the silver tables
@dlt.table(name = "silver_customers")
def silver_customers():
    df = spark.read.table("olist_eccom_catalog.silver.customers")
    return df


@dlt.table(name = "silver_items")
def silver_items():
    df = spark.read.table("olist_eccom_catalog.silver.items")
    return df


@dlt.table(name = "silver_orders")
def silver_orders():
    df = spark.read.table("olist_eccom_catalog.silver.orders")
    return df


@dlt.table(name = "silver_payments")
def silver_payments():
    df = spark.read.table("olist_eccom_catalog.silver.payments")
    return df 


@dlt.table(name = "silver_products")
def silver_products():
    df = spark.read.table("olist_eccom_catalog.silver.products")
    return df


@dlt.table(name = "silver_reviews")
def silver_reviews():
    df = spark.read.table("olist_eccom_catalog.silver.reviews")
    return df 


@dlt.table(name = "silver_sellers")
def silver_sellers():
    df = spark.read.table("olist_eccom_catalog.silver.sellers")
    return df

@dlt.table(name = "silver_invalid_items")
def silver_invalid_items():
    df = spark.read.table("olist_eccom_catalog.silver.invalid_order_items")
    return df





### Create Gold dimensions

@dlt.table
def dim_customers():
    df = dlt.read("silver_customers")
    return df


@dlt.table
def dim_products():
    df = dlt.read("silver_products")
    return df

@dlt.table(
    name="dim_sellers",  
    comment="Seller dimension with SCD2 tracking"
)
def dim_sellers():
    return dlt.apply_changes(
        source="silver_sellers",          
        keys=["seller_id"],               
        sequence_by=col("last_update_date"), 
        stored_as_scd_type=2,             
        target="dim_sellers_internal_scd2" 
    )


@dlt.table
def dim_invalid_order_items():
    df = dlt.read("silver_invalid_items")
    return df





#### gold facts

@dlt.table
def fact_orders():
    orders = dlt.read("silver_orders")
    payments = dlt.read("silver_payments")
    items = dlt.read("silver_items")

    fact = (orders
        .join(payments, "order_id", "left")
        .join(items, "order_id", "left")
        .select(
            orders.order_id,
            orders.customer_id,
            payments.payment_value,
            payments.payment_type,
            items.seller_id,
            items.product_id,
            orders.order_status,
            orders.order_purchase_timestamp,
            orders.order_approved_at,
            orders.order_delivered_carrier_date,
            orders.order_delivered_customer_date
        )
    )

    return fact


# fact_order_items
@dlt.table
def fact_order_items():
    items = dlt.read("silver_items")

    return items.select(
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "price",
        "freight_value"
    )


# fact_payments
@dlt.table
def fact_payments():
    return dlt.read("silver_payments")


# fact_reviews
@dlt.table
def fact_reviews():
    return dlt.read("silver_reviews")


# fact_deliveries
@dlt.table
def fact_deliveries():
    return dlt.read("silver_orders").select(
        "order_id",
        "order_purchase_timestamp",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    )

