# -*- coding: utf-8 -*-
"""
Created on Sat Nov 13 18:54:00 2021

@author: rictom
https://github.com/rictom/cnpj-mysql

#Para mysql, requer biblioteca pymysql ou mysqlclient (msqldb), desempenho semelhantes.
pip install pymysql
pip install mysqlclient (mysqldb). O desempenho com essa biblioteca foi similar a pymysql

Para postgres, instale psycopg2 (recomenda-se psycopg2-binary para instalação mais simples)
pip install psycopg2-binary (testado no Ubuntu)

"""
#%%

import pandas as pd, sqlalchemy, glob, time, dask.dataframe as dd
import os, sys

#%% DEFINA os parâmetros do servidor.
tipo_banco= 'mysql'
dbname = 'cnpj'
username = 'root'
password = ''
host = '127.0.0.1'

# tipo_banco = 'postgres'
# dbname = 'cnpj'
# username = 'postgres'
# password = 'senha'
# host = '127.0.0.1'

pasta_compactados = r"dados-publicos-zip"
pasta_saida = r"dados-publicos" #esta pasta deve estar vazia. 
dataReferencia = 'dd/mm/2023' #input('Data de referência da base dd/mm/aaaa: ')

resp = input(f'Isto irá CRIAR TABELAS ou REESCREVER TABELAS no database {dbname.upper()} no servidor {tipo_banco} {host} e MODIFICAR a pasta {pasta_saida}. Deseja prosseguir? (S/N)?')
if not resp or resp.upper()!='S':
    sys.exit()

if tipo_banco=='mysql':
    engine = sqlalchemy.create_engine(f'mysql+pymysql://{username}:{password}@{host}/{dbname}')
    #engine = sqlalchemy.create_engine(f'mysql+mysqldb://{username}:{password}@{host}/{dbname}?charset=utf8')
elif tipo_banco=='postgres':
    engine = sqlalchemy.create_engine(f'postgresql://{username}:{password}@{host}/{dbname}')
else:
    print('tipo de banco de dados não informado')
    sys.exit()

#%%

#cam = os.path.join(pasta_saida, 'cnpj.db') 
#if os.path.exists(cam):
#    print('o arquivo ' + cam + ' já existe. Apague primeiro e rode este script novamente.')
#    1/0

#%%
arquivos_a_zipar = list(glob.glob(os.path.join(pasta_compactados,r'*.zip')))
import zipfile

for arq in arquivos_a_zipar:
    print('descompactando ' + arq)
    with zipfile.ZipFile(arq, 'r') as zip_ref:
        zip_ref.extractall(pasta_saida)
        
#%%
#tipos = ['.EMPRECSV', '.ESTABELE', '.SOCIOCSV']

#arquivos_emprescsv = list(glob.glob(os.path.join(pasta_saida, '*' + tipos[0])))


sqlTabelas = '''
    DROP TABLE if exists cnae;
    CREATE TABLE cnae (
    codigo VARCHAR(7)
    ,descricao VARCHAR(200)
    );
    DROP TABLE if exists empresas;
    CREATE TABLE empresas (
    cnpj_basico VARCHAR(8)
    ,razao_social VARCHAR(200)
    ,natureza_juridica VARCHAR(4)
    ,qualificacao_responsavel VARCHAR(2)
    ,capital_social_str VARCHAR(20)
    ,porte_empresa VARCHAR(2)
    ,ente_federativo_responsavel VARCHAR(50)
    );
    DROP TABLE if exists estabelecimento;
    CREATE TABLE estabelecimento (
    cnpj_basico VARCHAR(8)
    ,cnpj_ordem VARCHAR(4)
    ,cnpj_dv VARCHAR(2)
    ,matriz_filial VARCHAR(1)
    ,nome_fantasia VARCHAR(200)
    ,situacao_cadastral VARCHAR(2)
    ,data_situacao_cadastral VARCHAR(8)
    ,motivo_situacao_cadastral VARCHAR(2)
    ,nome_cidade_exterior VARCHAR(200)
    ,pais VARCHAR(3)
    ,data_inicio_atividades VARCHAR(8)
    ,cnae_fiscal VARCHAR(7)
    ,cnae_fiscal_secundaria VARCHAR(1000)
    ,tipo_logradouro VARCHAR(20)
    ,logradouro VARCHAR(200)
    ,numero VARCHAR(10)
    ,complemento VARCHAR(200)
    ,bairro VARCHAR(200)
    ,cep VARCHAR(8)
    ,uf VARCHAR(2)
    ,municipio VARCHAR(4)
    ,ddd1 VARCHAR(4)
    ,telefone1 VARCHAR(8)
    ,ddd2 VARCHAR(4)
    ,telefone2 VARCHAR(8)
    ,ddd_fax VARCHAR(4)
    ,fax VARCHAR(8)
    ,correio_eletronico VARCHAR(200)
    ,situacao_especial VARCHAR(200)
    ,data_situacao_especial VARCHAR(8)
    );
    DROP TABLE if exists motivo;
    CREATE TABLE motivo (
    codigo VARCHAR(2)
    ,descricao VARCHAR(200)
    );
    DROP TABLE if exists municipio;
    CREATE TABLE municipio (
    codigo VARCHAR(4)
    ,descricao VARCHAR(200)
    );
    DROP TABLE if exists natureza_juridica;
    CREATE TABLE natureza_juridica (
    codigo VARCHAR(4)
    ,descricao VARCHAR(200)
    );
    DROP TABLE if exists pais;
    CREATE TABLE pais (
    codigo VARCHAR(3)
    ,descricao VARCHAR(200)
    );
    DROP TABLE if exists qualificacao_socio;
    CREATE TABLE qualificacao_socio (
    codigo VARCHAR(2)
    ,descricao VARCHAR(200)
    );
    DROP TABLE if exists simples;
    CREATE TABLE simples (
    cnpj_basico VARCHAR(8)
    ,opcao_simples VARCHAR(1)
    ,data_opcao_simples VARCHAR(8)
    ,data_exclusao_simples VARCHAR(8)
    ,opcao_mei VARCHAR(1)
    ,data_opcao_mei VARCHAR(8)
    ,data_exclusao_mei VARCHAR(8)
    );
    DROP TABLE if exists socios_original;
    CREATE TABLE socios_original (
     cnpj_basico VARCHAR(8)
    ,identificador_de_socio VARCHAR(1)
    ,nome_socio VARCHAR(200)
    ,cnpj_cpf_socio VARCHAR(14)
    ,qualificacao_socio VARCHAR(2)
    ,data_entrada_sociedade VARCHAR(8)
    ,pais VARCHAR(3)
    ,representante_legal VARCHAR(11)
    ,nome_representante VARCHAR(200)
    ,qualificacao_representante_legal VARCHAR(2)
    ,faixa_etaria VARCHAR(1)
    );
    '''

# def sqlCriaTabela(nomeTabela, colunas):
#     sql = 'CREATE TABLE ' + nomeTabela + ' ('
#     for k, coluna in enumerate(colunas):
#         sql += '\n' + coluna + ' TEXT'
#         if k+1<len(colunas):
#             sql+= ',' #'\n'
#     sql += ')\n'
#     return sql

#engine.execute(sqlTabelas)
print('Inicio sqlTabelas:', time.asctime())
for k, sql in enumerate(sqlTabelas.split(';')):
    if not sql.strip():
        continue
    print('-'*20 + f'\nexecutando parte {k}:\n', sql)
    engine.execute(sql)
    print('fim parcial...', time.asctime())
print('fim sqlTabelas...', time.asctime())

#%%

def carregaTabelaCodigo(extensaoArquivo, nomeTabela):
    arquivo = list(glob.glob(os.path.join(pasta_saida, '*' + extensaoArquivo)))[0]
    print('carregando tabela '+arquivo)
    dtab = pd.read_csv(arquivo, dtype=str, sep=';', encoding='latin1', header=None, names=['codigo','descricao'])
    #dqualificacao_socio['codigo'] = dqualificacao_socio['codigo'].apply(lambda x: str(int(x)))
    dtab.to_sql(nomeTabela, engine, if_exists='append', index=None)
    engine.execute(f'CREATE INDEX idx_{nomeTabela} ON {nomeTabela}(codigo);')

carregaTabelaCodigo('.CNAECSV','cnae')
carregaTabelaCodigo('.MOTICSV', 'motivo')
carregaTabelaCodigo('.MUNICCSV', 'municipio')
carregaTabelaCodigo('.NATJUCSV', 'natureza_juridica')
carregaTabelaCodigo('.PAISCSV', 'pais')
carregaTabelaCodigo('.QUALSCSV', 'qualificacao_socio')

#%%

colunas_estabelecimento = ['cnpj_basico','cnpj_ordem', 'cnpj_dv','matriz_filial', 
              'nome_fantasia',
              'situacao_cadastral','data_situacao_cadastral', 
              'motivo_situacao_cadastral',
              'nome_cidade_exterior',
              'pais',
              'data_inicio_atividades',
              'cnae_fiscal',
              'cnae_fiscal_secundaria',
              'tipo_logradouro',
              'logradouro', 
              'numero',
              'complemento','bairro',
              'cep','uf','municipio',
              'ddd1', 'telefone1',
              'ddd2', 'telefone2',
              'ddd_fax', 'fax',
              'correio_eletronico',
              'situacao_especial',
              'data_situacao_especial']    

colunas_empresas = ['cnpj_basico', 'razao_social',
           'natureza_juridica',
           'qualificacao_responsavel',
           'capital_social_str',
           'porte_empresa',
           'ente_federativo_responsavel']

colunas_socios = [
            'cnpj_basico',
            'identificador_de_socio',
            'nome_socio',
            'cnpj_cpf_socio',
            'qualificacao_socio',
            'data_entrada_sociedade',
            'pais',
            'representante_legal',
            'nome_representante',
            'qualificacao_representante_legal',
            'faixa_etaria'
          ]

colunas_simples = [
    'cnpj_basico',
    'opcao_simples',
    'data_opcao_simples',
    'data_exclusao_simples',
    'opcao_mei',
    'data_opcao_mei',
    'data_exclusao_mei']


def carregaTipo(nome_tabela, tipo, colunas):
    #usando dask, bem mais rápido que pandas
    arquivos = list(glob.glob(os.path.join(pasta_saida, '*' + tipo)))
    for arq in arquivos:
        print(f'carregando: {arq=}')
        print('lendo csv ...', time.asctime())
        ddf = dd.read_csv(arq, sep=';', header=None, names=colunas, #nrows=1000,
                         encoding='latin1', dtype=str,
                         na_filter=None)
        #df.columns = colunas.copy()
        #engine.execute('Drop table if exists estabelecimento')
        print('to_sql...', time.asctime())
        ddf.to_sql(nome_tabela, str(engine.url), index=None, if_exists='append', #parallel=True, #method='multi', chunksize=1000, 
                  dtype=sqlalchemy.sql.sqltypes.String) # .TEXT)
        print('fim parcial...', time.asctime())

# def carregaTipoDaskAlternativo(nome_tabela, tipo, colunas):
#     #usando dask, bem mais rápido que pandas
#     print(f'carregando: {tipo=}')
#     print('lendo csv ...', time.asctime())
#     #dask possibilita usar curinga no nome de arquivo
#     ddf = dd.read_csv(pasta_saida+r'\*' + tipo, 
#                       sep=';', header=None, names=colunas, 
#                       encoding='latin1', dtype=str,
#                       na_filter=None)
#     print('to_sql...', time.asctime())
#     ddf.to_sql(nome_tabela, str(engine.url), index=None, if_exists='append', #method='multi', chunksize=1000, 
#               dtype=sqlalchemy.sql.sqltypes.TEXT)
#     print('fim parcial...', time.asctime())

def carregaTipoPandas(nome_tabela, tipo, colunas):
    #usando pandas
    arquivos = list(glob.glob(pasta_saida+r'\*' + tipo))
    for arq in arquivos:
        print(f'carregando: {arq=}')
        print('lendo csv ...', time.asctime())
        df = pd.read_csv(arq, sep=';', header=None, names=colunas, #nrows=1000,
                          encoding='latin1', dtype=str,
                          na_filter=None)
        #df.columns = colunas.copy()
        #engine.execute('Drop table if exists estabelecimento')
        print('to_sql...', time.asctime())
        df.to_sql(nome_tabela, engine, index=None, if_exists='append',method='multi',
                  chunksize=1000, dtype=sqlalchemy.sql.sqltypes.TEXT)
        print('fim parcial...', time.asctime())


carregaTipo('estabelecimento', '.ESTABELE', colunas_estabelecimento)
carregaTipo('socios_original', '.SOCIOCSV', colunas_socios)
carregaTipo('empresas', '.EMPRECSV', colunas_empresas)
carregaTipo('simples', '.SIMPLES.CSV.*', colunas_simples)

#%%
#mysql -> postgres, change column para rename column c1 TO c2; ALTER TABLE t1 RENAME -> ALTER TABLE t1 RENAME TO;  te.matriz_filial="1" -> '1';
sqls = '''

ALTER TABLE empresas ADD COLUMN capital_social DECIMAL(18,2);
UPDATE  empresas
set capital_social = cast(REPLACE(capital_social_str,',', '.') AS DECIMAL(18,2));

ALTER TABLE empresas DROP COLUMN capital_social_str;

ALTER TABLE estabelecimento ADD COLUMN cnpj VARCHAR(14);
Update estabelecimento
set cnpj = CONCAT(cnpj_basico, cnpj_ordem,cnpj_dv);

CREATE  INDEX idx_estabelecimento_cnpj ON estabelecimento (cnpj);
CREATE  INDEX idx_empresas_cnpj_basico ON empresas (cnpj_basico);
CREATE  INDEX idx_empresas_razao_social ON empresas (razao_social);
CREATE  INDEX idx_estabelecimento_cnpj_basico ON estabelecimento (cnpj_basico);

CREATE INDEX idx_socios_original_cnpj_basico
ON socios_original(cnpj_basico);

DROP TABLE IF EXISTS socios;

CREATE TABLE socios AS 
SELECT te.cnpj as cnpj, ts.*
from socios_original ts
left join estabelecimento te on te.cnpj_basico = ts.cnpj_basico
where te.matriz_filial='1';

DROP TABLE IF EXISTS socios_original;

CREATE INDEX idx_socios_cnpj ON socios(cnpj);
CREATE INDEX idx_socios_cnpj_basico ON socios(cnpj_basico);
CREATE INDEX idx_socios_cnpj_cpf_socio ON socios(cnpj_cpf_socio);
CREATE INDEX idx_socios_nome_socio ON socios(nome_socio);

CREATE INDEX idx_simples_cnpj_basico ON simples(cnpj_basico);

DROP TABLE IF EXISTS _referencia;
CREATE TABLE _referencia (
	referencia	VARCHAR(100),
	valor		VARCHAR(100)
);
'''

print('Inicio sqls:', time.asctime())
for k, sql in enumerate(sqls.split(';')):
    if not sql.strip():
        continue
    print('-'*20 + f'\nexecutando parte {k}:\n', sql)
    engine.execute(sql)
    print('fim parcial...', time.asctime())
print('fim sqls...', time.asctime())
                
#%% inserir na tabela referencia_

qtde_cnpjs = engine.execute('select count(*) as contagem from estabelecimento;').fetchone()[0]

engine.execute(f"insert into _referencia (referencia, valor) values ('CNPJ', '{dataReferencia}')")
engine.execute(f"insert into _referencia (referencia, valor) values ('cnpj_qtde', '{qtde_cnpjs}')")

print('-'*20)
print(f'As tabelas foram criadas no servidor {tipo_banco}.')
print('Qtde de empresas (matrizes):', engine.execute('SELECT COUNT(*) FROM empresas').fetchone()[0])
print('Qtde de estabelecimentos (matrizes e fiiais):', engine.execute('SELECT COUNT(*) FROM estabelecimento').fetchone()[0])
print('Qtde de sócios:', engine.execute('SELECT COUNT(*) FROM socios').fetchone()[0])

print('FIM!!!', time.asctime())
