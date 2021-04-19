import json
import boto3
import logging
from datetime import date, datetime
import datetime
logger = logging.getLogger()
logger.setLevel(logging.INFO)

client = boto3.client('emr')
"""
    configure constant input: emr_provision_lambda_function_input.json
    triggered by CloudWatch event: emr_demo_trigger_provisioning_lambda_function
"""
def lambda_handler(event, context):
    d_time = datetime.datetime.now().replace(microsecond=0).isoformat()
    try:
        response = client.run_job_flow(
            Name    = 'My_cluster',
            LogUri  = event['LogUri'],
            ReleaseLabel = 'emr-5.33.0',
            Instances = {
                'MasterInstanceType': 'm5.xlarge',
                'SlaveInstanceType': 'm5.xlarge',
                'InstanceCount': 3,
                'KeepJobFlowAliveWhenNoSteps': False,
                'TerminationProtected': False,
                'Ec2SubnetId': event['Ec2SubnetId']
            },
            EbsRootVolumeSize = 10,
            Applications = [
                {
                    'Name': 'Hadoop'
                },
                {
                    'Name': 'Hive'
                },
                {
                    'Name': 'Pig'
                },
                {
                    'Name': 'Hue'
                },
                {
                    'Name': 'Spark'
                }
            ],
            Configurations = [
                {
                    'Classification': 'spark-hive-site',
                    'Properties': {
                        'hive.metastore.client.factory.class': 'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory'
                    }
                }
            ],
            VisibleToAllUsers=True,
            JobFlowRole = 'EMR_EC2_DefaultRole',
            ServiceRole = 'EMR_DefaultRole',
            Steps = [
                {
                    'Name': event['pre_process_health_violation'],
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': [
                            "spark-submit",
                            "--deploy-mode",
                            "cluster",
                            event['pre_process_health_violation_src'],
                            "--data_source",
                            event['pre_process_health_violation_data_source'],
                            "--output_uri",
                            event['pre_process_health_violation_output_uri']+'_'+d_time
                        ]
                    }
                },
                {
                    'Name': 'pre_process_emr_demo',
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': [
                            "spark-submit",
                            "--deploy-mode",
                            "cluster",
                            event['post_process_emr_demo_app'],
                            "--output_uri",
                            event['post_process_emr_demo_app_output_uri']+'_'+d_time
                        ]
                    }
                }
            ]
        )
    except Exception as e:
        logger.error(str(e))
    else:
        logger.info('response:%s', response)
    return {
        'response': response
    }