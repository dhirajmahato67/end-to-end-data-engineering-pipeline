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
AmazonS3_node1777990108295 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://atliq-motor-db1/raw-file/dataset/electric_vehicle_sales_by_makers/"], "recurse": True}, transformation_ctx="AmazonS3_node1777990108295")

# Script generated for node Change Schema
ChangeSchema_node1777990275732 = ApplyMapping.apply(frame=AmazonS3_node1777990108295, mappings=[("date", "string", "date", "string"), ("vehicle_category", "string", "vehicle_category", "string"), ("maker", "string", "maker", "string"), ("electric_vehicles_sold", "string", "electric_vehicles_sold", "int")], transformation_ctx="ChangeSchema_node1777990275732")

# Script generated for node SQL Query
SqlQuery3 = '''
SELECT 
  TO_DATE(date, 'dd-MMM-yy') AS date,
  vehicle_category,
  CASE
    WHEN maker = 'OLA ELECTRIC' THEN 'Ola Electric'
    WHEN maker = 'OKAYA EV' THEN 'Okaya EV'
    WHEN maker = 'KIA Motors' THEN 'Kia Motors'
    WHEN maker = 'Mercedes -Benz AG' THEN 'Mercedes-Benz'
    WHEN maker = 'Mahindra & Mahindra' THEN 'Mahindra'
    WHEN maker = 'OKINAWA' THEN 'Okinawa'
    WHEN maker = 'AMPERE' THEN 'Ampere'
    WHEN maker = 'ATHER' THEN 'Ather'
    WHEN maker = 'REVOLT' THEN 'Revolt'
    WHEN maker = 'BAJAJ' THEN 'Bajaj'
    WHEN maker = 'BEING' THEN 'Being'
    WHEN maker = 'JITENDRA' THEN 'Jitendra'
    WHEN maker = 'BATTRE ELECTRIC' THEN 'BattRE Electric'
    WHEN maker = 'KINETIC GREEN' THEN 'Kinetic Green'
    WHEN maker = 'OTHERS' THEN 'Others'
    ELSE maker
  END AS maker,
 electric_vehicles_sold
FROM myDataSource;
'''
SQLQuery_node1777990318647 = sparkSqlQuery(glueContext, query = SqlQuery3, mapping = {"myDataSource":ChangeSchema_node1777990275732}, transformation_ctx = "SQLQuery_node1777990318647")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1777990318647, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1777988058652", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1777993328107 = glueContext.getSink(path="s3://cleaned-atliq-data/electric_vehicle_sales_by_makers_cleaned/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1777993328107")
AmazonS3_node1777993328107.setCatalogInfo(catalogDatabase="default",catalogTableName="cleaned_electric_vehicle_sales_by_makers")
AmazonS3_node1777993328107.setFormat("glueparquet", compression="snappy")
AmazonS3_node1777993328107.writeFrame(SQLQuery_node1777990318647)
job.commit()