import airflow
from airflow import DAG
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator, EmrCreateJobFlowOperator, EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor

# this is just an example of how to use SPARK_STEPS, you need to define your own steps
SPARK_STEPS = [
    {
        'Name': 'wcd_data_engineer',
        'ActionOnFailure': "CONTINUE",
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                '/usr/bin/spark-submit',
                '--master', 'yarn',
                '--deploy-mode', 'client',
                's3://midterm-artifacts-wcd0/sales.py',
                #"{{ airflow.macros.ds_format(ds, '%Y-%m-%d', '%Y%m%d')}}",
            ]
        }
    }

]

DEFAULT_ARGS = {
    'owner': 'wcd_data_engineer',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['airflow_data_eng@wcd.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

dag = DAG(
    'midterm_dag',
    default_args = DEFAULT_ARGS,
    dagrun_timeout = timedelta(hours=2),
    schedule_interval = None
)

def retrieve_s3_files(**kwargs):
    calendar = kwargs['dag_run'].conf['calendar']
    inventory = kwargs['dag_run'].conf['inventory']
    product = kwargs['dag_run'].conf['product']
    sales = kwargs['dag_run'].conf['sales']
    store = kwargs['dag_run'].conf['store']
    kwargs['ti'].xcom_push(key = 'calendar', value = calendar)
    kwargs['ti'].xcom_push(key = 'inventory', value = inventory)
    kwargs['ti'].xcom_push(key = 'product', value = product)
    kwargs['ti'].xcom_push(key = 'sales', value = sales)
    kwargs['ti'].xcom_push(key = 'store', value = store)

JOB_FLOW_OVERRIDES = {
    'Name': 'midterm-cluster',
    'ReleaseLabel': 'emr-6.10.0',
    'Applications': [{'Name': 'Spark'}],
    'Instances': {
        'Ec2KeyName': 'ec2_instance',
        'KeepJobFlowAliveWhenNoSteps': False,
        'TerminationProtected': False,
        'InstanceGroups': [
            {
                'Name': 'Master node',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,
            },
            {
                'Name': 'Core node',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'CORE',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,
            },
        ],
    },
    "JobFlowRole": "EMR_EC2_DEFAULTROLE",
    'ServiceRole' : "EMR_DEFAULTROLE",
    'LogUri' : 's3://emr-logs-midterm-wcd0',
    'Steps': SPARK_STEPS,
}

create_job_flow = EmrCreateJobFlowOperator(
    task_id='create_emr_cluster',
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id='aws_default',
    emr_conn_id = 'emr_default',
    region_name = 'us-east-2',
    dag = dag
)


parse_request = PythonOperator(task_id = 'parse_request',
                                provide_context = True, # Airflow will pass a set of keyword arguments that can be used in your function
                                python_callable = retrieve_s3_files,
                                dag = dag
                                ) 

step_adder = EmrAddStepsOperator(
    task_id = 'add_steps',
    job_flow_id = create_job_flow.output,
    aws_conn_id = "aws_default",
    steps = SPARK_STEPS,
    dag = dag
)

step_checker = EmrStepSensor(
    task_id = 'watch_step',
    job_flow_id = create_job_flow.output,
    step_id = "{{task_instance.xcom_pull('add_steps', key='return_value')[0]}}",
    aws_conn_id = "aws_default", 
    dag = dag
)

terminate_cluster = EmrTerminateJobFlowOperator(
    task_id = 'terminate_emr',
    job_flow_id = create_job_flow.output,  
    aws_conn_id ='aws_default', 
    dag = dag
)


create_job_flow >> parse_request >> step_adder >> step_checker >> terminate_cluster
