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
CustomerLandingToTrusted_node1711466480331 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://namnhatvu-stedi-lakehouse/customer/landing/"], "recurse": True}, transformation_ctx="CustomerLandingToTrusted_node1711466480331")

# Script generated for node Share with Research
SharewithResearch_node1711467029562 = Filter.apply(frame=CustomerLandingToTrusted_node1711466480331, f=lambda row: (not(row["shareWithResearchAsOfDate"] == 0)), transformation_ctx="SharewithResearch_node1711467029562")

# Script generated for node Customer Trusted 
CustomerTrusted_node1711467159735 = glueContext.write_dynamic_frame.from_options(frame=SharewithResearch_node1711467029562, connection_type="s3", format="json", connection_options={"path": "s3://namnhatvu-stedi-lakehouse/customer/trusted/", "partitionKeys": []}, transformation_ctx="CustomerTrusted_node1711467159735")

job.commit()