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
AWSGlueDataCatalog_node1719283555337 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_landing", transformation_ctx="AWSGlueDataCatalog_node1719283555337")

# Script generated for node SQL Query
SqlQuery381 = '''
select * from myDataSource where shareWithResearchAsOfDate is not null
'''
SQLQuery_node1719283601958 = sparkSqlQuery(glueContext, query = SqlQuery381, mapping = {"myDataSource":AWSGlueDataCatalog_node1719283555337}, transformation_ctx = "SQLQuery_node1719283601958")

# Script generated for node Customer Trusted
CustomerTrusted_node1719283872694 = glueContext.getSink(path="s3://devnath/customer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1719283872694")
CustomerTrusted_node1719283872694.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
CustomerTrusted_node1719283872694.setFormat("json")
CustomerTrusted_node1719283872694.writeFrame(SQLQuery_node1719283601958)
job.commit()