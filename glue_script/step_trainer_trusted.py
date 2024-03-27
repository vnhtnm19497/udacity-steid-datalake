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
CustomerCurated_node1711523741609 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://namnhatvu-stedi-lakehouse/customer/curated/"], "recurse": True}, transformation_ctx="CustomerCurated_node1711523741609")

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1711523769074 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://namnhatvu-stedi-lakehouse/step_trainer/landing/"], "recurse": True}, transformation_ctx="StepTrainerLanding_node1711523769074")

# Script generated for node Apply Join
ApplyJoin_node1711524054135 = Join.apply(frame1=CustomerCurated_node1711523741609, frame2=StepTrainerLanding_node1711523769074, keys1=["serialnumber"], keys2=["serialnumber"], transformation_ctx="ApplyJoin_node1711524054135")

# Script generated for node Drop Fields
DropFields_node1711524183719 = DropFields.apply(frame=ApplyJoin_node1711524054135, paths=["email", "phone"], transformation_ctx="DropFields_node1711524183719")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1711524364151 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1711524183719, connection_type="s3", format="json", connection_options={"path": "s3://namnhatvu-stedi-lakehouse/step_trainer/trusted/", "partitionKeys": []}, transformation_ctx="StepTrainerTrusted_node1711524364151")

job.commit()