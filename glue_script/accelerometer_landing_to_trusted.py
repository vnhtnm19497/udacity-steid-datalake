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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1711947044863 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://namnhatvu-stedi-lakehouse/accelerometer/landing/"], "recurse": True}, transformation_ctx="AccelerometerLanding_node1711947044863")

# Script generated for node Customer Trusted
CustomerTrusted_node1711947027903 = glueContext.create_dynamic_frame.from_catalog(database="namnhatvu_stedi_lakehouse", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1711947027903")

# Script generated for node ApplyJoin
ApplyJoin_node1711947090034 = Join.apply(frame1=CustomerTrusted_node1711947027903, frame2=AccelerometerLanding_node1711947044863, keys1=["email"], keys2=["user"], transformation_ctx="ApplyJoin_node1711947090034")

# Script generated for node Drop Fields
DropFields_node1711947131730 = DropFields.apply(frame=ApplyJoin_node1711947090034, paths=["customername", "email", "phone", "birthday", "serialnumber", "registrationdate", "lastupdatedate", "sharewithresearchasofdate", "sharewithpublicasofdate", "sharewithfriendsasofdate"], transformation_ctx="DropFields_node1711947131730")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1711947173536 = glueContext.getSink(path="s3://namnhatvu-stedi-lakehouse/accelerometer/trusted/", connection_type="s3", updateBehavior="LOG", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1711947173536")
AccelerometerTrusted_node1711947173536.setCatalogInfo(catalogDatabase="namnhatvu_stedi_lakehouse",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1711947173536.setFormat("json")
AccelerometerTrusted_node1711947173536.writeFrame(DropFields_node1711947131730)
job.commit()