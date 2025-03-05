import json
import boto3
import time


REGION = "us-east-1"
EMR_RELEASE = "emr-7.1.0"
INSTANCE_TYPE = "m5.xlarge"
INSTANCE_COUNT = 1
S3_BUCKET_LOGS = "s3://maicon-donza-web-scraping/04_logs/"
S3_SCRIPT_PATH = "s3://maicon-donza-web-scraping/03_codigos/process_data.py"
ROLE_EMR_EC2 = "EMR_EC2_DefaultRole"
ROLE_EMR_SERVICE = "EMR_DefaultRole"

def get_emr_client():
    """Retorna um cliente para o EMR"""
    return boto3.client("emr", region_name=REGION)

def get_active_cluster():
    """Verifica se há um cluster EMR em execução"""
    emr_client = get_emr_client()
    response = emr_client.list_clusters(ClusterStates=["STARTING", "BOOTSTRAPPING", "RUNNING", "WAITING"])
    
    if response["Clusters"]:
        return response["Clusters"][0]["Id"]  # Retorna o primeiro cluster ativo encontrado
    return None

def start_emr_cluster():
    """Cria um cluster EMR se não houver um ativo"""
    active_cluster = get_active_cluster()
    
    if active_cluster:
        print(f"Já existe um cluster ativo: {active_cluster}. Nenhuma ação necessária.")
        return active_cluster

    emr_client = get_emr_client()
    
    response = emr_client.run_job_flow(
        Name="Cluster_Receita",
        ReleaseLabel=EMR_RELEASE,
        Instances={
            "InstanceGroups": [
                {
                    "Name": "Master",
                    "Market": "ON_DEMAND",
                    "InstanceRole": "MASTER",
                    "InstanceType": INSTANCE_TYPE,
                    "InstanceCount": 1,
                },
                {
                    "Name": "Core",
                    "Market": "ON_DEMAND",
                    "InstanceRole": "CORE",
                    "InstanceType": INSTANCE_TYPE,
                    "InstanceCount": INSTANCE_COUNT,
                }
            ],
            "KeepJobFlowAliveWhenNoSteps": True,
        },
        Applications=[{"Name": "Spark"}],
        LogUri=f"{S3_BUCKET_LOGS}/logs/",
        JobFlowRole=ROLE_EMR_EC2,
        ServiceRole=ROLE_EMR_SERVICE,
    )

    cluster_id = response["JobFlowId"]
    print(f"Cluster criado: {cluster_id}")
    return cluster_id

def add_step_to_cluster(cluster_id):
    """Adiciona um Step Job ao cluster se ele ainda não existir"""
    emr_client = get_emr_client()
    
    # Verifica Steps existentes para evitar duplicação
    steps_response = emr_client.list_steps(ClusterId=cluster_id)
    for step in steps_response["Steps"]:
        if step["Name"] == "Processar Dados Baixados":
            print(f"Step já existe no cluster {cluster_id}. Não será adicionado novamente.")
            return step["Id"]
    
    # Se não existir, adiciona um novo Step
    step = {
        "Name": "Processar Dados Baixados",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "cluster",
                S3_SCRIPT_PATH
            ],
        },
    }
    
    response = emr_client.add_job_flow_steps(JobFlowId=cluster_id, Steps=[step])
    step_id = response["StepIds"][0]
    print(f"Step adicionado: {step_id}")
    return step_id

def wait_for_step_completion(cluster_id, step_id):
    """Aguarda a execução do Step Job"""
    emr_client = get_emr_client()
    
    while True:
        response = emr_client.describe_step(ClusterId=cluster_id, StepId=step_id)
        step_state = response["Step"]["Status"]["State"]
        print(f"Step status: {step_state}")

        if step_state in ["COMPLETED", "FAILED", "CANCELLED"]:
            break
        
        time.sleep(30)

    return step_state

def terminate_cluster(cluster_id):
    """Finaliza o cluster EMR"""
    emr_client = get_emr_client()
    emr_client.terminate_job_flows(JobFlowIds=[cluster_id])
    print(f"Cluster {cluster_id} finalizado.")

def lambda_handler(event, context):
    """Função Lambda acionada pelo encerramento do EC2"""
    print("Evento recebido:", json.dumps(event))
    
    cluster_id = start_emr_cluster()
    step_id = add_step_to_cluster(cluster_id)
    status = wait_for_step_completion(cluster_id, step_id)

    if status == "COMPLETED":
        terminate_cluster(cluster_id)
    else:
        print("Step falhou, investigue o erro.")

    return {"statusCode": 200, "body": json.dumps("Execução concluída.")}