from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from datetime import datetime, timedelta
import yaml
import os

# 1. 설정 파일 로드
# 환경 변수 AIRFLOW_ENV로 환경 지정 (기본값: prod)
AIRFLOW_ENV = os.getenv('AIRFLOW_ENV', 'prod')
CONFIG_FILE = f'config_airflow_{AIRFLOW_ENV}.yaml' if AIRFLOW_ENV != 'prod' else 'config_airflow.yaml'
CONFIG_PATH = os.path.join(os.path.dirname(__file__), CONFIG_FILE)

if not os.path.exists(CONFIG_PATH):
    raise FileNotFoundError(f"Config file not found: {CONFIG_PATH}")

with open(CONFIG_PATH, 'r') as f:
    config = yaml.safe_load(f)

# 2. 기본 설정
DEFAULT_ARGS = {
    'owner': config['airflow']['owner'],
    'depends_on_past': False,
    'start_date': datetime.strptime(config['airflow']['start_date'], '%Y-%m-%d'),
    'retries': config['airflow']['retries'],
    'retry_delay': timedelta(minutes=config['airflow']['retry_delay_minutes']),
}

# 3. EMR 클러스터 설정 (config에서 로드)
JOB_FLOW_OVERRIDES = {
    "Name": config['emr']['cluster']['name'],
    "ReleaseLabel": config['emr']['cluster']['release_label'],
    "Applications": [{"Name": app} for app in config['emr']['cluster']['applications']],
    "LogUri": config['emr']['cluster']['log_uri'],
    "Instances": {
        "Ec2SubnetId": "subnet-02b504519fdb3d22a",
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": config['emr']['master_node']['market'],
                "InstanceRole": "MASTER",
                "InstanceType": config['emr']['master_node']['instance_type'],
                "InstanceCount": config['emr']['master_node']['instance_count'],
            },
            {
                "Name": "Core nodes",
                "Market": config['emr']['core_nodes']['market'],
                "InstanceRole": "CORE",
                "InstanceType": config['emr']['core_nodes']['instance_type'],
                "InstanceCount": config['emr']['core_nodes']['instance_count'],
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": config['emr']['configuration']['keep_job_flow_alive_when_no_steps'],
        "TerminationProtected": config['emr']['configuration']['termination_protected'],
    },
    "JobFlowRole": config['emr']['configuration']['job_flow_role'],
    "ServiceRole": config['emr']['configuration']['service_role'],
}

# 4. 실행할 Spark Step 정의 (config에서 로드)
SPARK_STEPS = [
    {
        "Name": "Run Anomaly Detection Stage 1",
        "ActionOnFailure": config['airflow_config']['action_on_failure'],
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", config['spark']['deploy_mode'],
                "--master", config['spark']['master'],
                "--py-files", config['spark']['py_files'],
                config['spark']['main_script'],
                "--env", "stage1",
                "--batch-date", "{{ ds }}"
            ],
        },
    }
]

with DAG(
    dag_id=config['airflow']['dag_id'],
    default_args=DEFAULT_ARGS,
    schedule_interval=config['airflow']['schedule_interval'],
    catchup=config['airflow']['catchup']
) as dag:

    # [TASK 1] EMR 클러스터 생성
    create_cluster = EmrCreateJobFlowOperator(
        task_id='create_emr_cluster',
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id=config['airflow_config']['aws_conn_id']
    )

    # [TASK 2] Spark 작업 추가
    add_steps = EmrAddStepsOperator(
        task_id='add_spark_steps',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        steps=SPARK_STEPS,
        aws_conn_id=config['airflow_config']['aws_conn_id']
    )

    # [TASK 3] 작업 완료까지 대기 (감시)
    wait_for_step = EmrStepSensor(
        task_id='wait_for_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_spark_steps', key='return_value')[0] }}",
        aws_conn_id=config['airflow_config']['aws_conn_id']
    )

    # [TASK 4] 클러스터 종료 (에러 대비 명시적 종료)
    terminate_cluster = EmrTerminateJobFlowOperator(
        task_id='terminate_emr_cluster',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id=config['airflow_config']['aws_conn_id'],
        trigger_rule=config['airflow_config']['trigger_rule']
    )

    create_cluster >> add_steps >> wait_for_step >> terminate_cluster