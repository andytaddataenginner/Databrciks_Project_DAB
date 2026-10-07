import dlt as dp
from pyspark.sql.functions import (
    col,
    explode,
    from_json,
    trim,
    current_timestamp
)
from pyspark.sql.functions import (
    col,
    explode,
    trim,
    lower,
    current_timestamp
)
from pyspark.sql.types import (
    ArrayType,
    StructType,
    StructField,
    LongType,
    StringType,
    DoubleType,
    IntegerType
)
import dlt as dp

from pyspark.sql.functions import (
    col,
    explode,
    from_json,
    trim,
    current_timestamp
)

from pyspark.sql.types import (
    ArrayType,
    StructType,
    StructField,
    LongType,
    StringType,
    DoubleType,
    IntegerType
)


product_schema = ArrayType(
    StructType([
        StructField("id", LongType(), True),
        StructField("title", StringType(), True),
        StructField("description", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("discountPercentage", DoubleType(), True),
        StructField("rating", DoubleType(), True),
        StructField("stock", IntegerType(), True),
        StructField("brand", StringType(), True),
        StructField("sku", StringType(), True),
        StructField("weight", DoubleType(), True),
        StructField("availabilityStatus", StringType(), True),
        StructField("returnPolicy", StringType(), True),
        StructField("shippingInformation", StringType(), True)
    ])
)


@dp.table(
    name="silver_products",
    comment="Cleansed and standardized product data"
)
@dp.expect_or_drop(
    "valid_product_id",
    "product_id IS NOT NULL"
)
@dp.expect(
    "valid_price",
    "price >= 0"
)
@dp.expect(
    "valid_stock",
    "stock >= 0"
)
@dp.expect(
    "valid_rating",
    "rating >= 0 AND rating <= 5"
)
def silver_products():

    df = dp.read_stream(
        "youtube_dev.bronze.landing_bronze_products"
    )

    return (
        df

        # Convert JSON string → ARRAY<STRUCT>
        .withColumn(
            "products_parsed",
            from_json(
                col("products"),
                product_schema
            )
        )

        # One row per product
        .withColumn(
            "product",
            explode(col("products_parsed"))
        )

        .select(

            col("product.id")
                .cast("long")
                .alias("product_id"),

            trim(col("product.title"))
                .alias("product_name"),

            trim(col("product.description"))
                .alias("description"),

            trim(col("product.category"))
                .alias("category"),

            trim(col("product.brand"))
                .alias("brand"),

            col("product.price")
                .cast("decimal(18,2)")
                .alias("price"),

            col("product.discountPercentage")
                .cast("decimal(10,2)")
                .alias("discount_percentage"),

            col("product.rating")
                .cast("decimal(5,2)")
                .alias("rating"),

            col("product.stock")
                .cast("integer")
                .alias("stock"),

            trim(col("product.sku"))
                .alias("sku"),

            col("product.weight")
                .cast("decimal(10,2)")
                .alias("weight"),

            trim(col("product.availabilityStatus"))
                .alias("availability_status"),

            trim(col("product.returnPolicy"))
                .alias("return_policy"),

            trim(col("product.shippingInformation"))
                .alias("shipping_information"),

            current_timestamp()
                .alias("silver_processed_timestamp"),

            col("_ingestion_timestamp"),

            col("_source_file")
                .alias("source_file")
        )
    )
#customer
customer_schema = ArrayType(
    StructType([

        StructField("id", LongType(), True),

        StructField("firstName", StringType(), True),

        StructField("lastName", StringType(), True),

        StructField("maidenName", StringType(), True),

        StructField("age", IntegerType(), True),

        StructField("gender", StringType(), True),

        StructField("email", StringType(), True),

        StructField("phone", StringType(), True),

        StructField("username", StringType(), True),

        StructField("password", StringType(), True),

        StructField("birthDate", StringType(), True),

        StructField("image", StringType(), True),

        StructField("bloodGroup", StringType(), True),

        StructField("height", DoubleType(), True),

        StructField("weight", DoubleType(), True),

        StructField("eyeColor", StringType(), True),

        StructField("hair", StringType(), True),

        StructField("ip", StringType(), True),

        StructField("address", StringType(), True),

        StructField("macAddress", StringType(), True),

        StructField("university", StringType(), True),

        StructField("ein", StringType(), True),

        StructField("ssn", StringType(), True),

        StructField("userAgent", StringType(), True),

        StructField("role", StringType(), True)
    ])
)


@dp.table(
    name="silver_customers",
    comment="Cleansed and standardized customer data"
)
@dp.expect_or_drop(
    "valid_customer_id",
    "customer_id IS NOT NULL"
)
@dp.expect(
    "valid_email",
    "email IS NULL OR email LIKE '%@%'"
)
def silver_customers():

    df = dp.read_stream(
        "youtube_dev.bronze.landing_bronze_customers"
    )

    return (
        df

        # Convert users JSON string into ARRAY<STRUCT>
        .withColumn(
            "users_parsed",
            from_json(
                col("users"),
                customer_schema
            )
        )

        # One row per customer
        .withColumn(
            "customer",
            explode(col("users_parsed"))
        )

        .select(

            col("customer.id")
                .cast("long")
                .alias("customer_id"),

            trim(col("customer.firstName"))
                .alias("first_name"),

            trim(col("customer.lastName"))
                .alias("last_name"),

            trim(
                col("customer.firstName")
                + " "
                + col("customer.lastName")
            )
                .alias("customer_name"),

            trim(col("customer.maidenName"))
                .alias("maiden_name"),

            col("customer.age")
                .cast("integer")
                .alias("age"),

            trim(col("customer.gender"))
                .alias("gender"),

            lower(
                trim(col("customer.email"))
            )
                .alias("email"),

            trim(col("customer.phone"))
                .alias("phone"),

            trim(col("customer.username"))
                .alias("username"),

            trim(col("customer.birthDate"))
                .alias("birth_date"),

            trim(col("customer.bloodGroup"))
                .alias("blood_group"),

            col("customer.height")
                .cast("decimal(10,2)")
                .alias("height"),

            col("customer.weight")
                .cast("decimal(10,2)")
                .alias("weight"),

            trim(col("customer.eyeColor"))
                .alias("eye_color"),

            trim(col("customer.ip"))
                .alias("ip_address"),

            trim(col("customer.university"))
                .alias("university"),

            trim(col("customer.ein"))
                .alias("ein"),

            trim(col("customer.ssn"))
                .alias("ssn"),

            trim(col("customer.role"))
                .alias("role"),

            current_timestamp()
                .alias("silver_processed_timestamp"),

            col("_ingestion_timestamp"),

            col("_source_file")
                .alias("source_file")
        )
    )

# sales 
# Schema for each product inside a cart
cart_product_schema = StructType([

    StructField("id", LongType(), True),

    StructField("quantity", IntegerType(), True),

    StructField("price", DoubleType(), True),

    StructField("discountPercentage", DoubleType(), True),

    StructField("discountedTotal", DoubleType(), True)
])


# Schema for each cart
cart_schema = ArrayType(
    StructType([

        StructField("id", LongType(), True),

        StructField("userId", LongType(), True),

        StructField(
            "products",
            ArrayType(cart_product_schema),
            True
        ),

        StructField("total", DoubleType(), True),

        StructField("discountedTotal", DoubleType(), True),

        StructField("totalProducts", IntegerType(), True),

        StructField("totalQuantity", IntegerType(), True)
    ])
)

@dp.table(
    name="silver_sales",
    comment="Cleansed sales transactions at order-product grain"
)
@dp.expect_or_drop(
    "valid_order_id",
    "order_id IS NOT NULL"
)
@dp.expect_or_drop(
    "valid_customer_id",
    "customer_id IS NOT NULL"
)
@dp.expect_or_drop(
    "valid_product_id",
    "product_id IS NOT NULL"
)
@dp.expect(
    "valid_quantity",
    "quantity > 0"
)
@dp.expect(
    "valid_unit_price",
    "unit_price >= 0"
)
@dp.expect(
    "valid_discount_percentage",
    "discount_percentage >= 0 AND discount_percentage <= 100"
)
def silver_sales():

    df = dp.read_stream(
        "youtube_dev.bronze.landing_bronze_sales"
    )

    return (
        df

        # Convert carts JSON string → ARRAY<STRUCT>
        .withColumn(
            "carts_parsed",
            from_json(
                col("carts"),
                cart_schema
            )
        )

        # One row per cart
        .withColumn(
            "cart",
            explode(col("carts_parsed"))
        )

        # One row per product within the cart
        .withColumn(
            "product",
            explode(col("cart.products"))
        )

        .select(

            # Order
            col("cart.id")
                .cast("long")
                .alias("order_id"),

            # Customer
            col("cart.userId")
                .cast("long")
                .alias("customer_id"),

            # Product
            col("product.id")
                .cast("long")
                .alias("product_id"),

            # Quantity
            col("product.quantity")
                .cast("integer")
                .alias("quantity"),

            # Unit price
            col("product.price")
                .cast("decimal(18,2)")
                .alias("unit_price"),

            # Discount
            col("product.discountPercentage")
                .cast("decimal(10,2)")
                .alias("discount_percentage"),

            # Product-level discounted amount
            col("product.discountedTotal")
                .cast("decimal(18,2)")
                .alias("discounted_total"),

            # Cart-level metrics
            col("cart.total")
                .cast("decimal(18,2)")
                .alias("order_total"),

            col("cart.discountedTotal")
                .cast("decimal(18,2)")
                .alias("order_discounted_total"),

            col("cart.totalProducts")
                .cast("integer")
                .alias("total_products"),

            col("cart.totalQuantity")
                .cast("integer")
                .alias("order_total_quantity"),

            # Metadata
            current_timestamp()
                .alias("silver_processed_timestamp"),

            col("_ingestion_timestamp"),

            col("_source_file")
                .alias("source_file")
        )
    )

