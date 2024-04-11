from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, quarter, monotonically_increasing_id, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


# • Produtos mais vendidos
# • Faturamento total
# • Faturamento por categoria e por produto
# • Maiores comissões de vendedores
# • Quantidade de Fornecedores por estado
# • Quantidade de clientes por estado
# • Todas as representações devem estar por ano, trimestre e mês.
# • Todas as datas devem estar no formato YYYYMMDD
# • Todos os textos precisam estar em maiúsculo
# • Embora não esteja no sistema OLTP, no DW será preciso criar um campo "region" para guardar a região
# do estado.
# • É preciso calcular e armazenar o subtotal por item de venda.

def getPublicCategories(spark):
    return spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://dpg-co5jfb4f7o1s73a319ag-a.oregon-postgres.render.com:5432/fatorv") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "public.categories") \
        .option("user", "root") \
        .option("password", "vW36eDzFKnl2h2ZFCWo7eqgVth9gMC4x") \
        .load()


def getPublicCustomers(spark):
    return spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://dpg-co5jfb4f7o1s73a319ag-a.oregon-postgres.render.com:5432/fatorv") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "public.customers") \
        .option("user", "root") \
        .option("password", "vW36eDzFKnl2h2ZFCWo7eqgVth9gMC4x") \
        .load()


def getPublicProducts(spark):
    return spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://dpg-co5jfb4f7o1s73a319ag-a.oregon-postgres.render.com:5432/fatorv") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "public.products") \
        .option("user", "root") \
        .option("password", "vW36eDzFKnl2h2ZFCWo7eqgVth9gMC4x") \
        .load()


def getPublicSales(spark):
    return spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://dpg-co5jfb4f7o1s73a319ag-a.oregon-postgres.render.com:5432/fatorv") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "public.sales") \
        .option("user", "root") \
        .option("password", "vW36eDzFKnl2h2ZFCWo7eqgVth9gMC4x") \
        .load()


def getPublicSalesItems(spark):
    return spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://dpg-co5jfb4f7o1s73a319ag-a.oregon-postgres.render.com:5432/fatorv") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "public.sales_items") \
        .option("user", "root") \
        .option("password", "vW36eDzFKnl2h2ZFCWo7eqgVth9gMC4x") \
        .load()


def getPublicSellers(spark):
    return spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://dpg-co5jfb4f7o1s73a319ag-a.oregon-postgres.render.com:5432/fatorv") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "public.sellers") \
        .option("user", "root") \
        .option("password", "vW36eDzFKnl2h2ZFCWo7eqgVth9gMC4x") \
        .load()


def getPublicSuppliers(spark):
    return spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://dpg-co5jfb4f7o1s73a319ag-a.oregon-postgres.render.com:5432/fatorv") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "public.suppliers") \
        .option("user", "root") \
        .option("password", "vW36eDzFKnl2h2ZFCWo7eqgVth9gMC4x") \
        .load()


def printTableInfo(table):
    table.printSchema()


def getDmDates(spark):
    public_sales = getPublicSales(spark)
    dates = public_sales.select("date", "sales_id")

    dm_dates = dates.select(year("date").alias("year"),
                            month("date").alias("month"),
                            dayofmonth("date").alias("day"),
                            quarter("date").alias("quarter"),
                            'sales_id')

    dm_dates = dm_dates.withColumn("sk_date", monotonically_increasing_id())
    return dm_dates


def getDmStates(spark):
    public_customers = getPublicCustomers(spark)
    customer_state = public_customers.select('customer_id', 'state')

    public_sellers = getPublicSellers(spark)
    seller_state = public_sellers.select('seller_id', 'state')

    public_suppliers = getPublicSuppliers(spark)
    suppliers_state = public_suppliers.select('supplier_id', 'state')

    # Adicionando uma coluna identificadora para manter a distinção entre os DataFrames
    customer_state = customer_state.withColumn("source", lit("customer"))
    seller_state = seller_state.withColumn("source", lit("seller"))
    suppliers_state = suppliers_state.withColumn("source", lit("supplier"))

    # Unindo os DataFrames
    dm_states = customer_state.union(seller_state).union(suppliers_state)

    return dm_states

def getDmSellers(spark):
    public_sellers = getPublicSellers(spark)
    data = public_sellers.select('seller_id', 'seller_name', 'tx_commission')
    data = data.withColumn("sk_seller", monotonically_increasing_id())
    return data


def getDmSuppliers(spark):
    public_suppliers = getPublicSuppliers(spark)
    data = public_suppliers.select('supplier_id', 'supplier_name')
    data = data.withColumn("sk_supplier", monotonically_increasing_id())
    return data


def getDmCustomers(spark):
    public_customers = getPublicCustomers(spark)
    data = public_customers.select('customer_id', 'customer_name')
    data = data.withColumn("sk_customer", monotonically_increasing_id())
    return data


def getDmCategories(spark):
    public_categories = getPublicCategories(spark)
    data = public_categories.select('category_id', 'category_name')
    data = data.withColumn("sk_category", monotonically_increasing_id())
    return data


def getDmProducts(spark):
    public_products = getPublicProducts(spark)
    data = public_products.select('product_id', 'product_name', 'price')
    data = data.withColumn("sk_product", monotonically_increasing_id())
    return data


def getDmSales(spark):
    public_sales = getPublicSales(spark)
    data = public_sales.select('sales_id', 'total_price')
    data = data.withColumn("sk_sales", monotonically_increasing_id())
    return data


def getDmSalesItems(spark):
    public_sales_items = getPublicSalesItems(spark)
    data = public_sales_items.select('sales_id', 'quantity', 'price', 'product_id')
    data = data.withColumn("sk_sales_items", monotonically_increasing_id())
    return data

def createFtSales(spark):
    ft_sales_schema = StructType([
        StructField("customer_id", IntegerType()),
        StructField("seller_id", IntegerType()),
        StructField("supplier_id", IntegerType()),
        StructField("product_id", IntegerType())
    ])

    ft_sales = spark.createDataFrame([], schema=ft_sales_schema)

    return ft_sales

def setFtSales(spark):

    ft_sales = createFtSales(spark)

    products = getDmProducts(spark)
    customers = getDmCustomers(spark)
    suppliers = getDmSuppliers(spark)
    sellers = getDmSellers(spark)

    dm_products = products.select("product_id")
    dm_customers = customers.select("customer_id")
    dm_suppliers = suppliers.select("supplier_id")
    dm_sellers = sellers.select("seller_id")

    ft_sales = ft_sales.join(dm_products).join(dm_customers).join(dm_suppliers).join(dm_sellers)
    #ft_sales = ft_sales.join(dm_sellers, on="seller_id", how="inner")
    #ft_sales = ft_sales.join(dm_suppliers, on="supplier_id", how="inner")
    #ft_sales = ft_sales.join(dm_products, on="product_id", how="inner")

    return ft_sales


def main():
    spark = SparkSession.builder.appName("lab_dados") \
        .config('spark.jars.packages', 'org.postgresql:postgresql:42.7.3') \
        .getOrCreate()

    dm_dates = getDmDates(spark)
    # dm_dates.show()

    dm_sellers = getDmSellers(spark)
    # dm_sellers.show()

    dm_suppliers = getDmSuppliers(spark)
    # dm_suppliers.show()

    dm_customers = getDmCustomers(spark)
    # dm_customers.show()

    dm_categories = getDmCategories(spark)
    # dm_categories.show()

    dm_products = getDmProducts(spark)
    # dm_products.show()

    dm_sales = getDmSales(spark)
    # dm_sales.show()

    dm_sales_items = getDmSalesItems(spark)
    # dm_sales_items.show()

    dm_states = getDmStates(spark)
    # dm_states.show()

    ft_sales = setFtSales(spark)
    ft_sales.show()

if __name__ == "__main__":
    main()