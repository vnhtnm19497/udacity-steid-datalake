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


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Curated
CustomerCurated_node1711523741609 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://namnhatvu-stedi-lakehouse/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCurated_node1711523741609",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1711523769074 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://namnhatvu-stedi-lakehouse/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1711523769074",
)

# Script generated for node SQL Query 1
SqlQuery73 = """
select distinct * from DataSource1


"""
SQLQuery1_node1711530591914 = sparkSqlQuery(
    glueContext,
    query=SqlQuery73,
    mapping={"DataSource1": CustomerCurated_node1711523741609},
    transformation_ctx="SQLQuery1_node1711530591914",
)

# Script generated for node SQL Query 2
SqlQuery72 = """
select distinct * from DataSource2

"""
SQLQuery2_node1711530798461 = sparkSqlQuery(
    glueContext,
    query=SqlQuery72,
    mapping={"DataSource2": StepTrainerLanding_node1711523769074},
    transformation_ctx="SQLQuery2_node1711530798461",
)

# Script generated for node Apply Join
ApplyJoin_node1711524054135 = Join.apply(
    frame1=SQLQuery1_node1711530591914,
    frame2=SQLQuery2_node1711530798461,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="ApplyJoin_node1711524054135",
)

# Script generated for node Drop Fields
DropFields_node1711524183719 = DropFields.apply(
    frame=ApplyJoin_node1711524054135,
    paths=["email", "phone"],
    transformation_ctx="DropFields_node1711524183719",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1711524364151 = glueContext.getSink(path="s3://namnhatvu-stedi-lakehouse/step_trainer/trusted/", connection_type="s3", updateBehavior="LOG", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1711524364151")
StepTrainerTrusted_node1711524364151.setCatalogInfo(catalogDatabase="namnhatvu_stedi_lakehouse",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1711524364151.setFormat("json")
StepTrainerTrusted_node1711524364151.writeFrame(DropFields_node1711524183719)
job.commit()
