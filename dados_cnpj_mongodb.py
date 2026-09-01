#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Carrega os arquivos publicos de CNPJ em MongoDB.

Fluxo equivalente ao script dados_cnpj_mysql.py, mas com colecoes MongoDB.
"""

import csv
import glob
import os
import sys
import time
import zipfile
from decimal import Decimal, InvalidOperation

from bson.decimal128 import Decimal128
from pymongo import ASCENDING, InsertOne, MongoClient, UpdateOne


# %% DEFINA os parametros do servidor.
dbname = "cnpj"
host = "127.0.0.1"
port = 27017
username = "mongoadmin"
password = "secret"
auth_source = "admin"
mongodb_uri = os.getenv("MONGODB_URI", "").strip()
dbname = os.getenv("MONGODB_DBNAME", dbname).strip() or dbname

pasta_compactados = r"dados-publicos-zip"
pasta_saida = r"dados-publicos"  # esta pasta deve estar vazia.
dataReferencia = "dd/05/2026"  # input('Data de referencia da base dd/mm/aaaa: ')

alvo_mongo = "MONGODB_URI" if mongodb_uri else f"{host}:{port}"
resp = input(
	f"Isto ira RECRIAR COLLECTIONS no database {dbname.upper()} no servidor mongodb "
	f"{alvo_mongo} e MODIFICAR a pasta {pasta_saida}. Deseja prosseguir? (S/N)?"
)
if not resp or resp.upper() != "S":
	sys.exit()


if mongodb_uri:
	client = MongoClient(mongodb_uri)
elif username and password:
	client = MongoClient(
		host=host,
		port=port,
		username=username,
		password=password,
		authSource=auth_source,
	)
else:
	client = MongoClient(host=host, port=port)

db = client[dbname]


def normalize_row(row, col_count):
	"""Pad/truncate para garantir o mesmo numero de colunas."""
	if len(row) < col_count:
		row = row + [""] * (col_count - len(row))
	elif len(row) > col_count:
		row = row[:col_count]
	return row


def bulk_insert_csv(file_path, collection_name, colunas, batch_size=5000):
	"""Le CSV em streaming e insere em lote na collection."""
	collection = db[collection_name]
	total = 0
	ops = []

	with open(file_path, "r", encoding="latin1", newline="") as f:
		reader = csv.reader(f, delimiter=";", quotechar='"')
		for row in reader:
			row = normalize_row(row, len(colunas))
			doc = {colunas[i]: row[i] for i in range(len(colunas))}
			ops.append(InsertOne(doc))

			if len(ops) >= batch_size:
				collection.bulk_write(ops, ordered=False)
				total += len(ops)
				ops = []

	if ops:
		collection.bulk_write(ops, ordered=False)
		total += len(ops)

	return total


def carrega_tabela_codigo(extensao_arquivo, nome_collection):
	arquivos = list(glob.glob(os.path.join(pasta_saida, "*" + extensao_arquivo)))
	if not arquivos:
		raise FileNotFoundError(f"Arquivo nao encontrado para: {extensao_arquivo}")

	arquivo = arquivos[0]
	print("carregando collection", arquivo)
	inserted = bulk_insert_csv(arquivo, nome_collection, ["codigo", "descricao"])
	print(f"inseridos em {nome_collection}: {inserted}")
	db[nome_collection].create_index([("codigo", ASCENDING)], name=f"idx_{nome_collection}")


def carrega_tipo(nome_collection, tipo, colunas):
	arquivos = sorted(glob.glob(os.path.join(pasta_saida, "*" + tipo)))
	if not arquivos:
		print(f"nenhum arquivo encontrado para padrao {tipo}")
		return

	total = 0
	for arq in arquivos:
		print(f"carregando: arq={arq}")
		print("lendo/inserindo csv ...", time.asctime())
		inserted = bulk_insert_csv(arq, nome_collection, colunas)
		total += inserted
		print("fim parcial...", time.asctime(), f"inseridos={inserted}")

	print(f"total inserido em {nome_collection}: {total}")


def parse_decimal128(valor):
	if not valor:
		return None

	try:
		numero = Decimal(valor.replace(",", ".").strip())
		return Decimal128(str(numero))
	except (InvalidOperation, ValueError):
		return None


def atualiza_capital_social_empresas(batch_size=5000):
	"""Converte capital_social_str para capital_social Decimal128 e remove a coluna string."""
	ops = []
	total = 0
	cursor = db.empresas.find({}, {"capital_social_str": 1}, no_cursor_timeout=True)

	try:
		for doc in cursor:
			capital = parse_decimal128(doc.get("capital_social_str", ""))
			update_doc = {"$unset": {"capital_social_str": ""}}
			if capital is not None:
				update_doc["$set"] = {"capital_social": capital}

			ops.append(UpdateOne({"_id": doc["_id"]}, update_doc))
			if len(ops) >= batch_size:
				db.empresas.bulk_write(ops, ordered=False)
				total += len(ops)
				ops = []

		if ops:
			db.empresas.bulk_write(ops, ordered=False)
			total += len(ops)
	finally:
		cursor.close()

	print(f"empresas atualizadas (capital_social): {total}")


# %% descompacta arquivos zip
arquivos_a_zipar = list(glob.glob(os.path.join(pasta_compactados, r"*.zip")))
for arq in arquivos_a_zipar:
	print("descompactando " + arq)
	with zipfile.ZipFile(arq, "r") as zip_ref:
		zip_ref.extractall(pasta_saida)

data_ref_candidates = list(glob.glob(os.path.join(pasta_saida, "*.EMPRECSV")))
if not data_ref_candidates:
	raise FileNotFoundError("Nenhum arquivo .EMPRECSV encontrado apos descompactar.")

dataReferenciaAux = data_ref_candidates[0].split(".")[2]  # formato DYYMMDD
if len(dataReferenciaAux) == len("D30610") and dataReferenciaAux.startswith("D"):
	dataReferencia = (
		dataReferenciaAux[4:6] + "/" + dataReferenciaAux[2:4] + "/202" + dataReferenciaAux[1]
	)


# %% drop/recria collections
collections_recriadas = [
	"cnae",
	"empresas",
	"estabelecimento",
	"motivo",
	"municipio",
	"natureza_juridica",
	"pais",
	"qualificacao_socio",
	"simples",
	"socios_original",
	"socios",
	"_referencia",
]

print("Inicio limpeza de collections:", time.asctime())
for nome in collections_recriadas:
	db[nome].drop()
print("fim limpeza de collections...", time.asctime())


# %% colunas
colunas_estabelecimento = [
	"cnpj_basico",
	"cnpj_ordem",
	"cnpj_dv",
	"matriz_filial",
	"nome_fantasia",
	"situacao_cadastral",
	"data_situacao_cadastral",
	"motivo_situacao_cadastral",
	"nome_cidade_exterior",
	"pais",
	"data_inicio_atividades",
	"cnae_fiscal",
	"cnae_fiscal_secundaria",
	"tipo_logradouro",
	"logradouro",
	"numero",
	"complemento",
	"bairro",
	"cep",
	"uf",
	"municipio",
	"ddd1",
	"telefone1",
	"ddd2",
	"telefone2",
	"ddd_fax",
	"fax",
	"correio_eletronico",
	"situacao_especial",
	"data_situacao_especial",
]

colunas_empresas = [
	"cnpj_basico",
	"razao_social",
	"natureza_juridica",
	"qualificacao_responsavel",
	"capital_social_str",
	"porte_empresa",
	"ente_federativo_responsavel",
]

colunas_socios = [
	"cnpj_basico",
	"identificador_de_socio",
	"nome_socio",
	"cnpj_cpf_socio",
	"qualificacao_socio",
	"data_entrada_sociedade",
	"pais",
	"representante_legal",
	"nome_representante",
	"qualificacao_representante_legal",
	"faixa_etaria",
]

colunas_simples = [
	"cnpj_basico",
	"opcao_simples",
	"data_opcao_simples",
	"data_exclusao_simples",
	"opcao_mei",
	"data_opcao_mei",
	"data_exclusao_mei",
]


# %% tabelas de codigo
carrega_tabela_codigo(".CNAECSV", "cnae")
carrega_tabela_codigo(".MOTICSV", "motivo")
carrega_tabela_codigo(".MUNICCSV", "municipio")
carrega_tabela_codigo(".NATJUCSV", "natureza_juridica")
carrega_tabela_codigo(".PAISCSV", "pais")
carrega_tabela_codigo(".QUALSCSV", "qualificacao_socio")


# %% arquivos principais
carrega_tipo("estabelecimento", ".ESTABELE", colunas_estabelecimento)
carrega_tipo("socios_original", ".SOCIOCSV", colunas_socios)
carrega_tipo("empresas", ".EMPRECSV", colunas_empresas)
carrega_tipo("simples", ".SIMPLES.CSV.*", colunas_simples)


# %% transformacoes equivalentes ao SQL
print("Inicio transformacoes:", time.asctime())

atualiza_capital_social_empresas()

db.estabelecimento.update_many(
	{},
	[
		{
			"$set": {
				"cnpj": {
					"$concat": [
						{"$ifNull": ["$cnpj_basico", ""]},
						{"$ifNull": ["$cnpj_ordem", ""]},
						{"$ifNull": ["$cnpj_dv", ""]},
					]
				}
			}
		}
	],
)

db.estabelecimento.create_index([("cnpj", ASCENDING)], name="idx_estabelecimento_cnpj")
db.estabelecimento.create_index(
	[("cnpj_basico", ASCENDING)], name="idx_estabelecimento_cnpj_basico"
)
db.estabelecimento.create_index(
	[("cnpj_basico", ASCENDING), ("matriz_filial", ASCENDING)],
	name="idx_estabelecimento_cnpj_basico_matriz",
)
db.empresas.create_index([("cnpj_basico", ASCENDING)], name="idx_empresas_cnpj_basico")
db.empresas.create_index([("razao_social", ASCENDING)], name="idx_empresas_razao_social")

db.socios.drop()

pipeline_socios = [
	{
		"$lookup": {
			"from": "estabelecimento",
			"let": {"cb": "$cnpj_basico"},
			"pipeline": [
				{
					"$match": {
						"$expr": {
							"$and": [
								{"$eq": ["$cnpj_basico", "$$cb"]},
								{"$eq": ["$matriz_filial", "1"]},
							]
						}
					}
				},
				{"$project": {"_id": 0, "cnpj": 1}},
			],
			"as": "estab",
		}
	},
	{"$unwind": "$estab"},
	{
		"$project": {
			"_id": 0,
			"cnpj": "$estab.cnpj",
			"cnpj_basico": 1,
			"identificador_de_socio": 1,
			"nome_socio": 1,
			"cnpj_cpf_socio": 1,
			"qualificacao_socio": 1,
			"data_entrada_sociedade": 1,
			"pais": 1,
			"representante_legal": 1,
			"nome_representante": 1,
			"qualificacao_representante_legal": 1,
			"faixa_etaria": 1,
		}
	},
	{
		"$merge": {
			"into": "socios",
			"whenMatched": "replace",
			"whenNotMatched": "insert",
		}
	},
]

for _ in db.socios_original.aggregate(pipeline_socios, allowDiskUse=True):
	pass
db.socios_original.drop()

db.socios.create_index([("cnpj", ASCENDING)], name="idx_socios_cnpj")
db.socios.create_index([("cnpj_basico", ASCENDING)], name="idx_socios_cnpj_basico")
db.socios.create_index([("cnpj_cpf_socio", ASCENDING)], name="idx_socios_cnpj_cpf_socio")
db.socios.create_index([("nome_socio", ASCENDING)], name="idx_socios_nome_socio")
db.simples.create_index([("cnpj_basico", ASCENDING)], name="idx_simples_cnpj_basico")

print("fim transformacoes...", time.asctime())


# %% inserir em _referencia
qtde_cnpjs = db.estabelecimento.count_documents({})

db._referencia.insert_many(
	[
		{"referencia": "CNPJ", "valor": dataReferencia},
		{"referencia": "cnpj_qtde", "valor": str(qtde_cnpjs)},
	]
)

print("-" * 20)
print("As collections foram criadas no servidor mongodb.")
print("Qtde de empresas (matrizes):", db.empresas.count_documents({}))
print("Qtde de estabelecimentos (matrizes e filiais):", db.estabelecimento.count_documents({}))
print("Qtde de socios:", db.socios.count_documents({}))

client.close()
print("FIM!!!", time.asctime())

