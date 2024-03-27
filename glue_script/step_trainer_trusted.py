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

# Script generated for node Customer Curated
CustomerCurated_node1711511557922 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://namnhatvu-stedi-lakehouse/customer/curated/"], "recurse": True}, transformation_ctx="CustomerCurated_node1711511557922")

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1711511600226 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://namnhatvu-stedi-lakehouse/step_trainer/landing/"], "recurse": True}, transformation_ctx="StepTrainerLanding_node1711511600226")

# Script generated for node Mapping Join
MappingJoin_node1711511645952 = Join.apply(frame1=StepTrainerLanding_node1711511600226, frame2=CustomerCurated_node1711511557922, keys1=["serialnumber"], keys2=["serialnumber"], transformation_ctx="MappingJoin_node1711511645952")

# Script generated for node Drop Fields
DropFields_node1711511716601 = DropFields.apply(frame=MappingJoin_node1711511645952, paths=["serialnumber", "sensorreadingtime", "`.serialnumber`", "distancefromobject"], transformation_ctx="DropFields_node1711511716601")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1711511800643 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1711511716601, connection_type="s3", format="json", connection_options={"path": "s3://namnhatvu-stedi-lakehouse/step_trainer/trusted/", "partitionKeys": []}, transformation_ctx="StepTrainerTrusted_node1711511800643")

job.commit()