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

# Script generated for node Customers Curated
CustomersCurated_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://ed-lake-house/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomersCurated_node1",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1684410433433 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://ed-lake-house/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1684410433433",
)

# Script generated for node Join
Join_node1684410438162 = Join.apply(
    frame1=StepTrainerLanding_node1684410433433,
    frame2=CustomersCurated_node1,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node1684410438162",
)

# Script generated for node Drop Fields
DropFields_node1684410543511 = DropFields.apply(
    frame=Join_node1684410438162,
    paths=[
        "shareWithFriendsAsOfDate",
        "phone",
        "lastUpdateDate",
        "email",
        "customerName",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "birthDay",
        "shareWithPublicAsOfDate",
    ],
    transformation_ctx="DropFields_node1684410543511",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1684410543511,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://ed-lake-house/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerTrusted_node3",
)

job.commit()
