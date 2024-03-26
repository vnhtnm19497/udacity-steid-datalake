import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1711468363300 = glueContext.create_dynamic_frame.from_catalog(
    database="namnhatvu_stedi_lakehouse",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1711468363300",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1711468392034 = glueContext.create_dynamic_frame.from_catalog(
    database="namnhatvu_stedi_lakehouse",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1711468392034",
)

# Script generated for node Custom Privacy Filter
CustomPrivacyFilter_node1711468505830 = Join.apply(
    frame1=AccelerometerLanding_node1711468363300,
    frame2=CustomerTrusted_node1711468392034,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="CustomPrivacyFilter_node1711468505830",
)

# Script generated for node Drop Fields
DropFields_node1711468567657 = DropFields.apply(
    frame=CustomPrivacyFilter_node1711468505830,
    paths=["timestamp", "email", "phone"],
    transformation_ctx="DropFields_node1711468567657",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1711468687636 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1711468567657,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://namnhatvu-stedi-lakehouse/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AccelerometerTrusted_node1711468687636",
)

job.commit()
