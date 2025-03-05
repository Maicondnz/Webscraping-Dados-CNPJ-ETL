import requests
import zipfile
import boto3
import logging
import time
from bs4 import BeautifulSoup
from datetime import datetime
from io import BytesIO
from os.path import join
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuração de logs
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Data para o S3
year = datetime.now().strftime('%Y')
month = datetime.now().strftime('%m')
day = datetime.now().strftime('%d')

# URL base da Receita Federal
url = f'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{year}-{month}/'

def get_response(url, retries=5, backoff_factor=1.5):
    """Tenta fazer uma requisição GET até 'retries' vezes com aumento progressivo de tempo entre tentativas."""
    for attempt in range(retries):
        try:
            r = requests.get(url, timeout=15)  # Timeout para evitar requisições travadas
            if r.status_code < 400:
                return r
        except requests.exceptions.RequestException as e:
            logging.warning(f"Tentativa {attempt + 1} falhou: {e}")
        wait_time = backoff_factor ** attempt  # Espera exponencial (1.5s, 2.25s, etc.)
        logging.info(f"Aguardando {round(wait_time, 2)}s antes de tentar novamente...")
        time.sleep(wait_time)
    logging.error("Erro na requisição após várias tentativas.")
    return None

r = get_response(url)

if r:
    soup = BeautifulSoup(r.text, 'html.parser')
else:
    logging.error("Erro ao obter resposta da URL. Encerrando execução.")
    exit()

# Extração de links de arquivos ZIP
objetos = soup.select('table tr a')
arquivos = [join(url, obj.get('href')) for obj in objetos[5:]]
nomes = [obj.get('href') for obj in objetos[5:]]

def classificar_arquivo(nome):
    """Determina a pasta com base no nome do arquivo."""
    nome = nome.lower()
    if "empre" in nome:
        return "empresas"
    elif "estab" in nome:
        return "estabelecimentos"
    elif "socio" in nome:
        return "socios"
    else:
        return os.path.splitext(nome)[0].lower()
    
def download_and_upload_to_s3(file_url, filename, bucket_name, layer, s3_path_prefix):
    """Baixa um arquivo e faz upload para o S3 descompactando o conteúdo."""
    s3 = boto3.client('s3')
    pasta = classificar_arquivo(filename)  # Define a pasta correta para armazenar
    s3_destino = f"{layer}/{s3_path_prefix}/{pasta}"
    
    for attempt in range(5):
        try:
            logging.info(f"Baixando: {filename} em {s3_destino} ({attempt + 1}/5 tentativa)...")
            r = requests.get(file_url, stream=True, timeout=15)

            if r.status_code == 200:
                file_obj = BytesIO(r.content)

                # Descompactar e enviar arquivos ao S3
                with zipfile.ZipFile(file_obj, 'r') as zip_ref:
                    for file_info in zip_ref.infolist():
                        file_name = file_info.filename
                        with zip_ref.open(file_name) as file_data:
                            s3.upload_fileobj(file_data, bucket_name, f"{s3_destino}/{file_name}")

                logging.info(f"✔ {filename} enviado ao S3 e descompactado em '{s3_destino}'.")
                return
            else:
                logging.warning(f"⚠ Falha ao baixar {file_url} - Status {r.status_code}")
        except requests.RequestException as e:
            logging.warning(f"Tentativa {attempt + 1} falhou para {file_url}: {e}")
        time.sleep(5)
    logging.error(f"❌ Falha ao baixar {file_url} após 5 tentativas. Pulando.")


def download_all_files_parallel(files, filenames, bucket_name, layer, s3_path_prefix):
    """Baixa arquivos em paralelo para acelerar o processo."""
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(download_and_upload_to_s3, file_url, filename, bucket_name, layer, s3_path_prefix): filename
            for file_url, filename in zip(files, filenames)
        }
        for future in as_completed(futures):
            try:
                future.result()  # Captura exceções dentro das threads
            except Exception as e:
                logging.error(f"Erro inesperado no processamento: {e}")
    logging.info("\u2705 Todos os downloads foram concluídos.")

def stop_instance(instance_id='i-0fa1898fd77da86f9', region='us-east-1'):
    """Para a instância EC2 após o processamento."""
    try:
        ec2 = boto3.client('ec2', region_name=region)
        ec2.stop_instances(InstanceIds=[instance_id])
        logging.info(f"✅ Instância {instance_id} parada com sucesso.")
    except Exception as e:
        logging.error(f"❌ Erro ao tentar parar a instância: {e}")

# Executando downloads em paralelo
download_all_files_parallel(
    arquivos, nomes, 'maicon-donza-web-scraping', '01_Ingestion',
    f'extract_year={year}/extract_month={month}/extract_day={day}'
)

# Para a instância EC2 após o término do processo
stop_instance()