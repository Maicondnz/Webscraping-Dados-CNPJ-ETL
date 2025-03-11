#Importando Bibliotecas
import boto3
import requests
from datetime import datetime
from time import sleep

# Configuração
EC2_INSTANCE_ID = "i-0fa1898fd77da86f9"
AWS_REGION = "us-east-1"
S3_BUCKET = "maicon-donza-web-scraping"
S3_PREFIX = "01_Ingestion/extract_year={}/extract_month={}/"

def lambda_handler(event, context):
    # Obtém a data atual
    year = datetime.now().strftime('%Y')
    month = datetime.now().strftime('%m')
    url = f'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{year}-{month}/'

    # Verifica se os arquivos já estão no S3
    if verificar_arquivos_s3(year, month):
        return {"status": "Arquivos já baixados", "mensagem": f"Os arquivos do mês {month}/{year} já estão no S3. Nenhuma ação necessária."}

    # Se os arquivos não estiverem no S3, verifica se a URL existe
    if verificar_site(url):
        iniciar_ec2(EC2_INSTANCE_ID)
        return {"status": "EC2 iniciada", "mensagem": f"A URL {url} está disponível e a instância EC2 foi iniciada."}
    else:
        return {"status": "Sem dados novos", "mensagem": f"A URL {url} não foi encontrada."}

#Faz requisições à URL com até 10 tentativas em caso de erro
def verificar_site(url, retries=10):
    for i in range(retries):
        try:
            response = requests.get(url)
            if response.status_code < 400:
                return response
            print(f'Tentativa {i+1}: Erro {response.status_code}')
        except requests.RequestException as e:
            print(f'Tentativa {i+1}: Erro {e}')
        sleep(2)
    return False

#Verifica se os arquivos do mês já estão no S3
def verificar_arquivos_s3(year, month):
    s3 = boto3.client("s3")
    prefix = S3_PREFIX.format(year, month)  # Caminho dos arquivos no S3

    try:
        response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
        if "Contents" in response:
            return True  # Se há arquivos no S3, retorna True
    except Exception as e:
        print(f"Erro ao acessar o S3: {e}")
    
    return False  # Se não encontrou arquivos, retorna False

#Inicia uma instância EC2
def iniciar_ec2(instance_id):
    ec2 = boto3.client("ec2", region_name=AWS_REGION)

    try:
        response = ec2.start_instances(InstanceIds=[instance_id])
        print(f"Instância {instance_id} iniciada: {response}")
    except Exception as e:
        print(f"Erro ao iniciar EC2: {e}")
