import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1684414927006 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://ed-lake-house/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1684414927006",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://ed-lake-house/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerTrusted_node1",
)

# Script generated for node Join
StepTrainerTrusted_node1DF = StepTrainerTrusted_node1.toDF()
AccelerometerTrusted_node1684414927006DF = AccelerometerTrusted_node1684414927006.toDF()
Join_node1684415094880 = DynamicFrame.fromDF(
    StepTrainerTrusted_node1DF.join(
        AccelerometerTrusted_node1684414927006DF,
        (
            StepTrainerTrusted_node1DF["sensorReadingTime"]
            == AccelerometerTrusted_node1684414927006DF["timeStamp"]
        ),
        "left",
    ),
    glueContext,
    "Join_node1684415094880",
)

# Script generated for node Drop Fields
DropFields_node1684415774278 = DropFields.apply(
    frame=Join_node1684415094880,
    paths=["user", "timeStamp"],
    transformation_ctx="DropFields_node1684415774278",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1684415774278,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://ed-lake-house/machine_learning/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="MachineLearningCurated_node3",
)

job.commit()
