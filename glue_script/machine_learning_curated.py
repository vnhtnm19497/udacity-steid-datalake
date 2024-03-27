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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1711547520314 = glueContext.create_dynamic_frame.from_catalog(database="namnhatvu_stedi_lakehouse", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1711547520314")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1711547593694 = glueContext.create_dynamic_frame.from_catalog(database="namnhatvu_stedi_lakehouse", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1711547593694")

# Script generated for node Remove STT Fields
RemoveSTTFields_node1711547652173 = DropFields.apply(frame=StepTrainerTrusted_node1711547520314, paths=[], transformation_ctx="RemoveSTTFields_node1711547652173")

# Script generated for node Remove ACT Fields
RemoveACTFields_node1711547638747 = DropFields.apply(frame=AccelerometerTrusted_node1711547593694, paths=[], transformation_ctx="RemoveACTFields_node1711547638747")

# Script generated for node Renamed keys for Apply Join
RenamedkeysforApplyJoin_node1711547853417 = ApplyMapping.apply(frame=RemoveSTTFields_node1711547652173, mappings=[("serialNumber", "string", "right_serialNumber", "string"), ("birthDay", "string", "right_birthDay", "string"), ("shareWithPublicAsOfDate", "long", "right_shareWithPublicAsOfDate", "long"), ("shareWithResearchAsOfDate", "long", "right_shareWithResearchAsOfDate", "long"), ("registrationDate", "long", "right_registrationDate", "long"), ("customerName", "string", "right_customerName", "string"), ("sensorReadingTime", "long", "right_sensorReadingTime", "long"), ("shareWithFriendsAsOfDate", "long", "right_shareWithFriendsAsOfDate", "long"), ("`.serialNumber`", "string", "`right_.serialNumber`", "string"), ("lastUpdateDate", "long", "right_lastUpdateDate", "long"), ("distanceFromObject", "int", "right_distanceFromObject", "int")], transformation_ctx="RenamedkeysforApplyJoin_node1711547853417")

# Script generated for node Apply Join
SqlQuery1131 = '''
select * from act
inner join stt
on stt.right_sensorreadingtime = act.timestamp
'''
ApplyJoin_node1711550082591 = sparkSqlQuery(glueContext, query = SqlQuery1131, mapping = {"act":RemoveACTFields_node1711547638747, "stt":RenamedkeysforApplyJoin_node1711547853417}, transformation_ctx = "ApplyJoin_node1711550082591")

# Script generated for node Drop Fields
DropFields_node1711550443623 = DropFields.apply(frame=ApplyJoin_node1711550082591, paths=["serialNumber", "birthDay", "shareWithResearchAsOfDate", "registrationDate", "customerName", "user", "shareWithFriendsAsOfDate", "lastUpdateDate", "shareWithPublicAsOfDate", "right_birthDay", "right_shareWithPublicAsOfDate", "right_shareWithResearchAsOfDate", "right_registrationDate", "right_customerName", "right_shareWithFriendsAsOfDate", "`right_.serialNumber`", "right_lastUpdateDate"], transformation_ctx="DropFields_node1711550443623")

# Script generated for node Rename ML curated
RenameMLcurated_node1711550707565 = ApplyMapping.apply(frame=DropFields_node1711550443623, mappings=[("z", "double", "z", "double"), ("y", "double", "y", "double"), ("x", "double", "x", "double"), ("timestamp", "long", "timestamp", "long"), ("right_serialNumber", "string", "serialnumber", "string"), ("right_sensorReadingTime", "long", "sensorreadingtime", "long"), ("right_distanceFromObject", "int", "distancefromobject", "int")], transformation_ctx="RenameMLcurated_node1711550707565")

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1711551085994 = glueContext.write_dynamic_frame.from_options(frame=RenameMLcurated_node1711550707565, connection_type="s3", format="json", connection_options={"path": "s3://namnhatvu-stedi-lakehouse/machine_learning/curated/", "partitionKeys": []}, transformation_ctx="MachineLearningCurated_node1711551085994")

job.commit()