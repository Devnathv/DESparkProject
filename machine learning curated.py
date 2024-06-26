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

# Script generated for node step trainer trusted
steptrainertrusted_node1719346876880 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="steptrainertrusted_node1719346876880")

# Script generated for node accleromter trusted
accleromtertrusted_node1719346904774 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accleromtertrusted_node1719346904774")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1719368541368 = ApplyMapping.apply(frame=steptrainertrusted_node1719346876880, mappings=[("right_distancefromobject", "int", "right_right_distancefromobject", "int"), ("registrationdate", "long", "right_registrationdate", "long"), ("right_sensorreadingtime", "long", "right_right_sensorreadingtime", "long"), ("customername", "string", "right_customername", "string"), ("birthday", "string", "right_birthday", "string"), ("right_serialnumber", "string", "right_right_serialnumber", "string"), ("sharewithfriendsasofdate", "long", "right_sharewithfriendsasofdate", "long"), ("sharewithpublicasofdate", "long", "right_sharewithpublicasofdate", "long"), ("lastupdatedate", "long", "right_lastupdatedate", "long"), ("email", "string", "right_email", "string"), ("serialnumber", "string", "right_serialnumber", "string"), ("sharewithresearchasofdate", "long", "right_sharewithresearchasofdate", "long"), ("phone", "string", "right_phone", "string")], transformation_ctx="RenamedkeysforJoin_node1719368541368")

# Script generated for node Join
Join_node1719346915321 = Join.apply(frame1=accleromtertrusted_node1719346904774, frame2=RenamedkeysforJoin_node1719368541368, keys1=["timestamp"], keys2=["right_right_sensorreadingtime"], transformation_ctx="Join_node1719346915321")

# Script generated for node Drop Fields
DropFields_node1719367163250 = DropFields.apply(frame=Join_node1719346915321, paths=["right_serialnumber", "right_right_distancefromobject", "right_right_serialnumber", "right_lastupdatedate", "right_customername", "right_right_sensorreadingtime", "right_registrationdate", "right_sharewithresearchasofdate", "right_sharewithfriendsasofdate", "right_sharewithpublicasofdate", "right_birthday", "right_email"], transformation_ctx="DropFields_node1719367163250")

# Script generated for node SQL Query
SqlQuery1023 = '''
select * from myDataSource

'''
SQLQuery_node1719369111024 = sparkSqlQuery(glueContext, query = SqlQuery1023, mapping = {"myDataSource":DropFields_node1719367163250}, transformation_ctx = "SQLQuery_node1719369111024")

# Script generated for node Amazon S3
AmazonS3_node1719369217368 = glueContext.getSink(path="s3://devnath/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1719369217368")
AmazonS3_node1719369217368.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
AmazonS3_node1719369217368.setFormat("json")
AmazonS3_node1719369217368.writeFrame(SQLQuery_node1719369111024)
job.commit()