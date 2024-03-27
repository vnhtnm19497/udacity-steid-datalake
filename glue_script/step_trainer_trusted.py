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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1711519749726 = glueContext.create_dynamic_frame.from_catalog(
    database="namnhatvu_stedi_lakehouse",
    table_name="step_trainer_landing",
    transformation_ctx="StepTrainerLanding_node1711519749726",
)

# Script generated for node Customer Curated
CustomerCurated_node1711519789617 = glueContext.create_dynamic_frame.from_catalog(
    database="namnhatvu_stedi_lakehouse",
    table_name="customer_curated",
    transformation_ctx="CustomerCurated_node1711519789617",
)

# Script generated for node Apply Join
ApplyJoin_node1711519857292 = Join.apply(
    frame1=StepTrainerLanding_node1711519749726,
    frame2=CustomerCurated_node1711519789617,
    keys1=["serialnumber"],
    keys2=["serialnumber"],
    transformation_ctx="ApplyJoin_node1711519857292",
)

# Script generated for node Drop Fields
DropFields_node1711520589325 = DropFields.apply(
    frame=ApplyJoin_node1711519857292,
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "`.serialnumber`",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
        "sharewithfriendsasofdate",
    ],
    transformation_ctx="DropFields_node1711520589325",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1711520740136 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1711520589325,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://namnhatvu-stedi-lakehouse/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerTrusted_node1711520740136",
)

job.commit()
