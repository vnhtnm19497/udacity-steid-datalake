import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer Landing To Trusted
CustomerLandingToTrusted_node1711944207636 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://namnhatvu-stedi-lakehouse/customer/landing/"], "recurse": True}, transformation_ctx="CustomerLandingToTrusted_node1711944207636")

# Script generated for node Privacy Filter
PrivacyFilter_node1711945088412 = Filter.apply(frame=CustomerLandingToTrusted_node1711944207636, f=lambda row: (not(row["shareWithResearchAsOfDate"] == 0)), transformation_ctx="PrivacyFilter_node1711945088412")

# Script generated for node Customer Trusted
CustomerTrusted_node1711944321954 = glueContext.getSink(path="s3://namnhatvu-stedi-lakehouse/customer/trusted/", connection_type="s3", updateBehavior="LOG", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1711944321954")
CustomerTrusted_node1711944321954.setCatalogInfo(catalogDatabase="namnhatvu_stedi_lakehouse",catalogTableName="customer_trusted")
CustomerTrusted_node1711944321954.setFormat("json")
CustomerTrusted_node1711944321954.writeFrame(PrivacyFilter_node1711945088412)
job.commit()