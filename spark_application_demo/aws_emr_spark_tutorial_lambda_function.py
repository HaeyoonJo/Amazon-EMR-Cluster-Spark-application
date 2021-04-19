import json
import boto3
from datetime import date, datetime
import datetime

client = boto3.client('emr')

def lambda_handler(event, context):
    d_time = datetime.datetime.now().replace(microsecond=0).isoformat()
    response = client.run_job_flow(
        Name= 'spark_job_cluster_tutorial',
        LogUri= 's3://emr-spark-tutorial/prefix/logs',
        ReleaseLabel = 'emr-5.33.0',
        Instances={
            'MasterInstanceType': 'm5.xlarge',
            'SlaveInstanceType': 'm5.large',
            'InstanceCount': 1,
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False,
            'Ec2SubnetId': 'subnet-xxxxxxx'
        },
        Applications = [ {'Name': 'Spark'} ],
        Configurations = [ 
            { 'Classification': 'spark-hive-site',
              'Properties': { 
                  'hive.metastore.client.factory.class': 'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory'}
            }
        ],
        VisibleToAllUsers=True,
        JobFlowRole = 'EMR_EC2_DefaultRole',
        ServiceRole = 'EMR_DefaultRole',
        Steps=[
            {
                'Name': 'flow-log-analysis',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': [
                            'spark-submit',
                            '--deploy-mode', 'cluster',
                            '--executor-memory', '6G',
                            '--num-executors', '1',
                            '--executor-cores', '2',
                            '--class', 'com.aws.emr.ProfitCalc',
                            's3://emr-spark-tutorial/spark_application/SparkProfitCalc.jar',
                            's3://emr-spark-tutorial/input/fake_sales_data.csv',
                            's3://emr-spark-tutorial/outputs/'+d_time
                        ]
                }
            }
        ]
    )
