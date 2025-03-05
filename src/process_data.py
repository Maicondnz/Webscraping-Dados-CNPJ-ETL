from datetime import datetime
from pyspark.sql.functions import col, sum, round, udf, lpad, translate

spark

data = datetime.now().strftime('%Y%m')

def remover_colunas_nulas(df, limite_percentual=0.8):
    """
    Remove colunas com mais de um limite percentual de valores nulos em um DataFrame do Spark.
    
    :param df: DataFrame do Spark
    :param limite_percentual: Percentual limite de valores nulos para remoção (padrão: 70%)
    :return: DataFrame sem as colunas com muitos valores nulos
    """
    total_linhas = df.count()  # Contar total de linhas do DataFrame
    limite_nulos = total_linhas * limite_percentual  # Definir o limite de nulos permitido

    # Calcular a contagem de nulos para cada coluna
    null_counts = df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]).collect()[0].asDict()

    # Filtrar as colunas que excedem o limite de nulos
    colunas_para_remover = [c for c, count in null_counts.items() if count > limite_nulos]

    if colunas_para_remover:
        print(f"📌 Colunas removidas por excesso de nulos (> {limite_percentual * 100}%): {colunas_para_remover}")
    else:
        print("✅ Nenhuma coluna removida. Todas possuem valores suficientes.")

    return df.drop(*colunas_para_remover)


def removeAccents(palavra):
    if palavra != None:
        dic={'ã':'a','â':'a','á':'a','à':'a','ä':'a','Ã':'A','Â':'A','Á':'A','À':'A','Ä':'A'
            ,'ê':'e','é':'e','è':'e','ë':'e','Ê':'E','É':'E','È':'E','Ë':'E'
            ,'í':'i','ì':'i','ï':'i','Í':'I','Ì':'I','Ï':'I'
            ,'õ':'o','ô':'o','ó':'o','ò':'o','ö':'o','Õ':'O','Ô':'O','Ó':'O','Ò':'O','Ö':'O'
            ,'û':'u','ú':'u','ù':'u','ü':'u','Û':'U','Ú':'U','Ù':'U','Ü':'U'
            ,'ç':'c','Ç':'C'
            ,'ñ':'n','Ñ':'N'}
        for i, j in dic.items():
                palavra = palavra.replace(i,j)
    return palavra

def removeSpecialChars(palavra):
    if palavra != None:
        spec = "`´~!@#$%^&*()_-+={}[]|\:;<>.?/''""ªº¨"
        for i in spec:
                palavra = palavra.replace(i,' ')
    return palavra


udf_remove_accents = udf(lambda z: removeAccents(z))
spark.udf.register("rm_accents",udf_remove_accents)

udf_remove_specialchars = udf(lambda z: removeSpecialChars(z))
spark.udf.register("rm_special_chars",udf_remove_specialchars)

empresas = spark.read.csv('s3://maicon-donza-web-scraping/01_Ingestion/extract_year=2025/extract_month=02/extract_day=27/empresas/',sep=';',header=False,encoding='latin1')
socios = spark.read.csv('s3://maicon-donza-web-scraping/01_Ingestion/extract_year=2025/extract_month=02/extract_day=27/socios/',sep=';',header=False,encoding='latin1')
estabelecimentos = spark.read.csv('s3://maicon-donza-web-scraping/01_Ingestion/extract_year=2025/extract_month=02/extract_day=27/estabelecimentos/',sep=';',header=False,encoding='latin1')
cnae = spark.read.csv('s3://maicon-donza-web-scraping/01_Ingestion/extract_year=2025/extract_month=02/extract_day=27/cnaes/',sep=';',header=False,encoding='latin1')
motivo = spark.read.csv('s3://maicon-donza-web-scraping/01_Ingestion/extract_year=2025/extract_month=02/extract_day=27/motivos/',sep=';',header=False,encoding='latin1')
municipio = spark.read.csv('s3://maicon-donza-web-scraping/01_Ingestion/extract_year=2025/extract_month=02/extract_day=27/municipios/',sep=';',header=False,encoding='latin1')
natureza = spark.read.csv('s3://maicon-donza-web-scraping/01_Ingestion/extract_year=2025/extract_month=02/extract_day=27/naturezas/',sep=';',header=False,encoding='latin1')
pais = spark.read.csv('s3://maicon-donza-web-scraping/01_Ingestion/extract_year=2025/extract_month=02/extract_day=27/paises/',sep=';',header=False,encoding='latin1')
qualificacao = spark.read.csv('s3://maicon-donza-web-scraping/01_Ingestion/extract_year=2025/extract_month=02/extract_day=27/qualificacoes/',sep=';',header=False,encoding='latin1')
simples = spark.read.csv('s3://maicon-donza-web-scraping/01_Ingestion/extract_year=2025/extract_month=02/extract_day=27/simples/',sep=';',header=False,encoding='latin1')

columns_empresas =['cnpj_basico','razao_social','natureza_juridica','qualificacao_responsavel','capital_social','porte','ente_fed_responsavel']

columns_socios =['cnpj_basico','tipo_pessoa','nome_razao_social','cpf_cnpj_socio','cod_qualificacao_socio','data_entrada_sociedade',
               'cod_pais_socio_estrang','cpf_repr_legal','nome_repr_legal','qualificacao_repr_legal','faixa_etaria_socio']

columns_estabelecimentos = ['cnpj_basico','cnpj_ordem','cnpj_dv','id_matriz_filial','nome_fantasia','situacao_cadastral','data_situacao_cadastral',
                           'motivo_situacao_cadastral','nome_cidade_exterior','pais','data_inicio_atividade','cnae_principal','cnae_secundario',
                           'tipo_logradouro','logradouro','numero','complemento','bairro','cep','uf','municipio','ddd_1','telefone_1','ddd_2','telefone_2',
                           'ddd_fax','fax','correio_eletronico','situacao_especial','data_situacao_especial']

columns_cnae = ['cod_cnae','descricao_cnae']

columns_motivo = ['cod_motivo','descricao_motivo']

columns_municipio = ['cod_municipio','descricao_municipio']

columns_natureza = ['cod_nat_juridica','descricao_nat_juridica']

columns_pais = ['cod_pais','descricao_pais']

columns_qualificacao = ['cod_quali','descricao_quali']

columns_simples = ['cnpj_basico','opcao_simples','data_opcao_simples','data_exclusao_simples','opcao_mei','data_opcao_mei','data_exclusao_mei']

df_empresas = empresas.toDF(*columns_empresas)
df_socios = socios.toDF(*columns_socios)
df_estabelecimentos = estabelecimentos.toDF(*columns_estabelecimentos)
df_cnae = cnae.toDF(*columns_cnae)
df_motivo = motivo.toDF(*columns_motivo)
df_municipio = municipio.toDF(*columns_municipio)
df_natureza = natureza.toDF(*columns_natureza)
df_pais = pais.toDF(*columns_pais)
df_qualificacao = qualificacao.toDF(*columns_qualificacao)
df_simples = simples.toDF(*columns_simples)

df_empresas.createOrReplaceTempView('empresas')
df_socios.createOrReplaceTempView('socios')
df_estabelecimentos.createOrReplaceTempView('estabelecimentos')
df_cnae.createOrReplaceTempView('cnae')
df_motivo.createOrReplaceTempView('motivo')
df_municipio.createOrReplaceTempView('municipio')
df_natureza.createOrReplaceTempView('natureza')
df_pais.createOrReplaceTempView('pais')
df_qualificacao.createOrReplaceTempView('qualificacao')
df_simples.createOrReplaceTempView('simples')

empresa_1 = spark.sql("""

SELECT
    a.cnpj_basico,
    a.razao_social,
    b.descricao_nat_juridica,
    c.descricao_quali as qualificacao_responsavel,
    replace(a.capital_social,',','.') as capital_social,
    CASE
    WHEN a.porte = '00' THEN 'NAO_INFORMADO'
    WHEN a.porte = '01' THEN 'MICRO_EMPRESA'
    WHEN a.porte = '03' THEN 'EMPRESA_DE_PEQUENO_PORTE'
    WHEN a.porte = '05' THEN 'DEMAIS'
    ELSE 'NAO_INFORMADO'
    END AS porte,
    a.ente_fed_responsavel
FROM
    empresas a
LEFT JOIN
    natureza b
ON
    a.natureza_juridica = b.cod_nat_juridica
LEFT JOIN
    qualificacao c
ON
    a.qualificacao_responsavel = c.cod_quali
""")
empresa_1.createOrReplaceTempView('empresa_1')

empresa_1.cache()

print(empresa_1.count())
print(empresas.count())

socio_final = spark.sql("""

SELECT
    cnpj_basico,
    CASE
    WHEN tipo_pessoa = 1 THEN 'PJ'
    WHEN tipo_pessoa = 2 THEN 'PF'
    WHEN tipo_pessoa = 3 THEN 'ESTRANGEIRO'
    ELSE 'N/A'
    END AS tipo_pessoa,
    nome_razao_social,
    cpf_cnpj_socio,
    cod_qualificacao_socio,
    concat_ws('-',substr(data_entrada_sociedade,1,4),substr(data_entrada_sociedade,5,2),substr(data_entrada_sociedade,7,2)) as data_entrada_sociedade,
    cod_pais_socio_estrang,
    cpf_repr_legal,
    nome_repr_legal,
    qualificacao_repr_legal,
    CASE
    WHEN faixa_etaria_socio = 0 THEN 'N/A'
    WHEN faixa_etaria_socio = 1 THEN 'ATE_12_ANOS'
    WHEN faixa_etaria_socio = 2 THEN 'DE_13_A_20_ANOS'
    WHEN faixa_etaria_socio = 3 THEN 'DE_21_A_30_ANOS'
    WHEN faixa_etaria_socio = 4 THEN 'DE_31_A_40_ANOS'
    WHEN faixa_etaria_socio = 5 THEN 'DE_41_A_50_ANOS'
    WHEN faixa_etaria_socio = 6 THEN 'DE_51_A_60_ANOS'
    WHEN faixa_etaria_socio = 7 THEN 'DE_61_A_70_ANOS'
    WHEN faixa_etaria_socio = 8 THEN 'DE_71_A_80_ANOS'
    WHEN faixa_etaria_socio = 9 THEN 'ACIMA_DE_80_ANOS'
    ELSE 'N/A'
    END AS faixa_etaria_socio
FROM
    socios
""")

estabelecimento_1 = spark.sql("""

SELECT
    cnpj_basico,
    cnpj_ordem,
    cnpj_dv,
    CASE
    WHEN id_matriz_filial = 1 THEN 'MATRIZ'
    WHEN id_matriz_filial = 2 THEN 'FILIAL'
    ELSE 'N/A'
    END as id_matriz_filial,
    rm_accents(rm_special_chars(upper(trim(nome_fantasia)))) as nome_fantasia,
    CASE
    WHEN situacao_cadastral = '01' THEN 'NULA'
    WHEN situacao_cadastral = '02' THEN 'ATIVA'
    WHEN situacao_cadastral = '03' THEN 'SUSPENSA'
    WHEN situacao_cadastral = '04' THEN 'INAPTA'
    WHEN situacao_cadastral = '08' THEN 'BAIXADA'
    ELSE 'N/A'
    END AS situacao_cadastral,
    CASE
    WHEN data_situacao_cadastral = '0' THEN NULL
    ELSE concat_ws('-',substr(data_situacao_cadastral,1,4),substr(data_situacao_cadastral,5,2),substr(data_situacao_cadastral,7,2))
    END as data_situacao_cadastral,
    motivo_situacao_cadastral,
    nome_cidade_exterior,
    pais,
    CASE
    WHEN data_inicio_atividade = '0' THEN NULL
    ELSE concat_ws('-',substr(data_inicio_atividade,1,4),substr(data_inicio_atividade,5,2),substr(data_inicio_atividade,7,2))
    END as data_inicio_atividade,
    cnae_principal,
    cnae_secundario,
    tipo_logradouro,
    logradouro,
    numero,
    complemento,
    bairro,
    cep,
    uf,
    municipio,
    ddd_1,
    telefone_1,
    ddd_2,
    telefone_2,
    ddd_fax,
    fax,
    correio_eletronico,
    situacao_especial,
    CASE
    WHEN data_situacao_especial = '0' THEN NULL
    ELSE concat_ws('-',substr(data_situacao_especial,1,4),substr(data_situacao_especial,5,2),substr(data_situacao_especial,7,2))
    END as data_situacao_especial
FROM
    estabelecimentos
WHERE
    length(concat_ws('-',substr(data_situacao_cadastral,1,4),substr(data_situacao_cadastral,5,2),substr(data_situacao_cadastral,7,2))) != 4
""")
estabelecimento_1.createOrReplaceTempView('estabelecimento_1')

estabelecimento_1.cache()

empresa_etl_1 = spark.sql(f"""

SELECT
    concat(a.cnpj_basico,a.cnpj_ordem,a.cnpj_dv) as cnpj_empresa,
    empresa.razao_social,
    a.nome_fantasia,
    empresa.descricao_nat_juridica,
    empresa.qualificacao_responsavel,
    empresa.capital_social,
    empresa.porte,
    empresa.ente_fed_responsavel,
    a.id_matriz_filial,
    a.situacao_cadastral,
    a.data_situacao_cadastral,
    b.descricao_motivo as motivo_situacao_cadastral,
    a.nome_cidade_exterior,
    c.descricao_pais as pais_empresa,
    CASE
    WHEN a.data_inicio_atividade <= '1582-10-15' THEN NULL
    ELSE a.data_inicio_atividade
    END as data_inicio_atividade,
    d.descricao_cnae,
    a.cnae_secundario,
    a.tipo_logradouro,
    a.logradouro,
    a.numero,
    a.complemento,
    a.bairro,
    a.cep,
    a.uf,
    e.descricao_municipio,
    concat(a.ddd_1,a.telefone_1) as telefone_1,
    concat(a.ddd_2,a.telefone_2) as telefone_2,
    concat(a.ddd_fax,a.fax) as fax,
    a.correio_eletronico,
    a.situacao_especial,
    a.data_situacao_especial,
    {data} as data_processamento
FROM
    estabelecimento_1 a
LEFT JOIN
    empresa_1 empresa
ON
    a.cnpj_basico = empresa.cnpj_basico
LEFT JOIN
    motivo b
ON
    a.motivo_situacao_cadastral = b.cod_motivo
LEFT JOIN
    pais c
ON
    a.pais = c.cod_pais
LEFT JOIN
    cnae d
ON
    a.cnae_principal = d.cod_cnae
LEFT JOIN
    municipio e
ON
    a.municipio = e.cod_municipio


""")
empresa_etl_1.createOrReplaceTempView('empresa_etl_1')

print(empresa_etl_1.count())
print(estabelecimento_1.count())

df_final = spark.sql(

"""
SELECT
    CAST(cnpj_empresa as string) as cnpj_empresa,
    CAST(razao_social as string) as razao_social,
    CAST(nome_fantasia as string) as nome_fantasia,
    CAST(descricao_nat_juridica as string) as descricao_nat_juridica,
    CAST(qualificacao_responsavel as string) as qualificacao_responsavel,
    CAST(capital_social as decimal(38,2)) as capital_social,
    CAST(porte as string) as porte,
    CAST(ente_fed_responsavel as string) as ente_fed_responsavel,
    CAST(id_matriz_filial as string) as id_matriz_filial,
    CAST(situacao_cadastral as string) as situacao_cadastral,
    CAST(data_situacao_cadastral as date) as data_situacao_cadastral,
    CAST(motivo_situacao_cadastral as string) as motivo_situacao_cadastral,
    CAST(nome_cidade_exterior as string) as nome_cidade_exterior,
    CAST(pais_empresa as string) as pais_empresa,
    CAST(data_inicio_atividade as date) as data_inicio_atividade,
    CAST(descricao_cnae as string) as descricao_cnae,
    CAST(cnae_secundario as string) as cnae_secundario,
    CAST(tipo_logradouro as string) as tipo_logradouro,
    CAST(logradouro as string) as logradouro,
    CAST(numero as string) as numero,
    CAST(complemento as string) as complemento,
    CAST(bairro as string) as bairro,
    CAST(cep as string) as cep,
    CAST(uf as string) as uf,
    CAST(descricao_municipio as string) as descricao_municipio,
    CAST(telefone_1 as string) as telefone_1,
    CAST(telefone_2 as string) as telefone_2,
    CAST(fax as string) as fax,
    CAST(correio_eletronico as string) as correio_eletronico,
    CAST(situacao_especial as string) as situacao_especial,
    CAST(data_situacao_especial as date) as data_situacao_especial,
    CAST(data_processamento as string) as data_proc
FROM
    empresa_etl_1
"""
)
df_final.createOrReplaceTempView('df_final')

df_final.cache()

cnae_final = spark.sql(f"""
SELECT
    CAST(cod_cnae as string) as cod_cnae,
    CAST(descricao_cnae as string) as descricao_cnae,
    CAST({data} as string) as data_proc
FROM
    cnae

""")

df_limpo = remover_colunas_nulas(df_final)

df_final.write.partitionBy("data_proc").parquet('s3://maicon-donza-web-scraping/02_Raw/empresas_final',mode='overwrite')

cnae_final.write.partitionBy("data_proc").parquet('s3://maicon-donza-web-scraping/02_Raw/cnaes_final',mode='overwrite')

socio_final.write.partitionBy("data_proc").parquet('s3://maicon-donza-web-scraping/02_Raw/socios_final',mode='overwrite')