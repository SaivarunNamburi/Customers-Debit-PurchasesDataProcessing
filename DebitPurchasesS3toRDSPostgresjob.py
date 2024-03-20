import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

def sparkAggregate(glueContext, parentFrame, groups, aggs, transformation_ctx) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs) if len(groups) > 0 else parentFrame.toDF().agg(*aggsFuncs)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1710879925524 = glueContext.create_dynamic_frame.from_catalog(database="customer-debitcard-purchases", table_name="s3_input_raw_data", transformation_ctx="AWSGlueDataCatalog_node1710879925524")

# Script generated for node Aggregate
Aggregate_node1710880016281 = sparkAggregate(glueContext, parentFrame = AWSGlueDataCatalog_node1710879925524, groups = ["customer_id", "debit_card_number", "bank_name"], aggs = [["amount_spent", "sum"]], transformation_ctx = "Aggregate_node1710880016281")

# Script generated for node Change Schema
ChangeSchema_node1710880578099 = ApplyMapping.apply(frame=Aggregate_node1710880016281, mappings=[("customer_id", "long", "customer_id", "long"), ("debit_card_number", "long", "debit_card_number", "string"), ("bank_name", "string", "bank_name", "string"), ("`sum(amount_spent)`", "double", "total_amount_spend", "float")], transformation_ctx="ChangeSchema_node1710880578099")

# Script generated for node PostgreSQL
PostgreSQL_node1710880914765 = glueContext.write_dynamic_frame.from_catalog(frame=ChangeSchema_node1710880578099, database="customer-debitcard-purchases", table_name="postgres_outputcustomers_public_customer_debitcard_puchases", transformation_ctx="PostgreSQL_node1710880914765")

job.commit()