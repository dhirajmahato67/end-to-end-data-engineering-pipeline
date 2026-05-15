import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
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

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Amazon S3
AmazonS3_node1777980784603 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://atliq-motor-db1/raw-file/dataset/dim_date/"], "recurse": True}, transformation_ctx="AmazonS3_node1777980784603")

# Script generated for node Change Schema
ChangeSchema_node1777980885582 = ApplyMapping.apply(frame=AmazonS3_node1777980784603, mappings=[("date", "string", "date", "string"), ("fiscal_year", "string", "fiscal_year", "int"), ("quarter", "string", "quarter", "string")], transformation_ctx="ChangeSchema_node1777980885582")

# Script generated for node SQL Query
SqlQuery2 = '''
SELECT 
  TO_DATE(date, 'dd-MMM-yy') AS date,
  fiscal_year,
  quarter
FROM myDataSource;
'''
SQLQuery_node1777980925804 = sparkSqlQuery(glueContext, query = SqlQuery2, mapping = {"myDataSource":ChangeSchema_node1777980885582}, transformation_ctx = "SQLQuery_node1777980925804")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1777980925804, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1777988058652", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1777988838433 = glueContext.getSink(path="s3://cleaned-atliq-data/date_cleaned_file/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1777988838433")
AmazonS3_node1777988838433.setCatalogInfo(catalogDatabase="default",catalogTableName="date_cleaned")
AmazonS3_node1777988838433.setFormat("glueparquet", compression="snappy")
AmazonS3_node1777988838433.writeFrame(SQLQuery_node1777980925804)
job.commit()