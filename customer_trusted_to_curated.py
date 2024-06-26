import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1719329176833 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AWSGlueDataCatalog_node1719329176833")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1719328232929 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="AWSGlueDataCatalog_node1719328232929")

# Script generated for node Join
Join_node1719329223918 = Join.apply(frame1=AWSGlueDataCatalog_node1719329176833, frame2=AWSGlueDataCatalog_node1719328232929, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1719329223918")

# Script generated for node SQL Query
SqlQuery670 = '''
select count(*) from myDataSource

'''
SQLQuery_node1719329246315 = sparkSqlQuery(glueContext, query = SqlQuery670, mapping = {"myDataSource":Join_node1719329223918}, transformation_ctx = "SQLQuery_node1719329246315")

# Script generated for node customers_curated
customers_curated_node1719329275850 = glueContext.getSink(path="s3://devnath/customers_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customers_curated_node1719329275850")
customers_curated_node1719329275850.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customers_curated")
customers_curated_node1719329275850.setFormat("json")
customers_curated_node1719329275850.writeFrame(SQLQuery_node1719329246315)
job.commit()