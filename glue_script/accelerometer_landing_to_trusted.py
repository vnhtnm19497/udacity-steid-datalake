import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1711522944591 = glueContext.create_dynamic_frame.from_catalog(database="namnhatvu_stedi_lakehouse", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1711522944591")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1711522903463 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://namnhatvu-stedi-lakehouse/accelerometer/landing/"], "recurse": True}, transformation_ctx="AccelerometerLanding_node1711522903463")

# Script generated for node Apply Join
ApplyJoin_node1711523066436 = Join.apply(frame1=AccelerometerLanding_node1711522903463, frame2=CustomerTrusted_node1711522944591, keys1=["user"], keys2=["email"], transformation_ctx="ApplyJoin_node1711523066436")

# Script generated for node Drop Fields
DropFields_node1711523104687 = DropFields.apply(frame=ApplyJoin_node1711523066436, paths=["email", "phone"], transformation_ctx="DropFields_node1711523104687")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1711523171721 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1711523104687, connection_type="s3", format="json", connection_options={"path": "s3://namnhatvu-stedi-lakehouse/accelerometer/trusted/", "partitionKeys": []}, transformation_ctx="AccelerometerTrusted_node1711523171721")

job.commit()