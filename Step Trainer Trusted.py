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

# Script generated for node customer Curated
customerCurated_node1719341712194 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="customerCurated_node1719341712194")

# Script generated for node step Trainer landing
stepTrainerlanding_node1719341702519 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="stepTrainerlanding_node1719341702519")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1719342650888 = ApplyMapping.apply(frame=stepTrainerlanding_node1719341702519, mappings=[("sensorreadingtime", "long", "right_sensorreadingtime", "long"), ("serialnumber", "string", "right_serialnumber", "string"), ("distancefromobject", "int", "right_distancefromobject", "int")], transformation_ctx="RenamedkeysforJoin_node1719342650888")

# Script generated for node Join
Join_node1719341763926 = Join.apply(frame1=customerCurated_node1719341712194, frame2=RenamedkeysforJoin_node1719342650888, keys1=["serialnumber"], keys2=["right_serialnumber"], transformation_ctx="Join_node1719341763926")

# Script generated for node SQL Query
SqlQuery1099 = '''
select * from myDataSource
'''
SQLQuery_node1719342587274 = sparkSqlQuery(glueContext, query = SqlQuery1099, mapping = {"myDataSource":Join_node1719341763926}, transformation_ctx = "SQLQuery_node1719342587274")

# Script generated for node Amazon S3
AmazonS3_node1719343760517 = glueContext.getSink(path="s3://devnath/step_trainer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1719343760517")
AmazonS3_node1719343760517.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
AmazonS3_node1719343760517.setFormat("json")
AmazonS3_node1719343760517.writeFrame(SQLQuery_node1719342587274)
job.commit()