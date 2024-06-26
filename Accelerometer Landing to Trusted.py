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

# Script generated for node Customer_Landing
Customer_Landing_node1719285098676 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://devnath/customer_landing/"], "recurse": True}, transformation_ctx="Customer_Landing_node1719285098676")

# Script generated for node Accelerometer_Landing
Accelerometer_Landing_node1719284360775 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://devnath/accelerometer_landing/"], "recurse": True}, transformation_ctx="Accelerometer_Landing_node1719284360775")

# Script generated for node Join
Join_node1719285057234 = Join.apply(frame1=Accelerometer_Landing_node1719284360775, frame2=Customer_Landing_node1719285098676, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1719285057234")

# Script generated for node SQL Query
SqlQuery435 = '''
select * from myDataSource where
shareWithResearchAsOfDate is not null
'''
SQLQuery_node1719285203593 = sparkSqlQuery(glueContext, query = SqlQuery435, mapping = {"myDataSource":Join_node1719285057234}, transformation_ctx = "SQLQuery_node1719285203593")

# Script generated for node Accelerometer_Trusted
Accelerometer_Trusted_node1719285292662 = glueContext.getSink(path="s3://devnath/accelerometer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Accelerometer_Trusted_node1719285292662")
Accelerometer_Trusted_node1719285292662.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
Accelerometer_Trusted_node1719285292662.setFormat("json")
Accelerometer_Trusted_node1719285292662.writeFrame(SQLQuery_node1719285203593)
job.commit()