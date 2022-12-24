# cnpj-mysql
Script em python para carregar os arquivos de cnpj dos dados públicos da Receita Federal em MYSQL e POSTGRESQL. O código é compatível com o layout das tabelas disponibilizadas pela Receita Federal a partir de 2021.

## Dados públicos de cnpj no site da Receita:
Os arquivos csv zipados com os dados de CNPJs estão disponíveis em https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica-cnpj ou https://dadosabertos.rfb.gov.br/CNPJ/ (http://200.152.38.155/CNPJ/).<br><br>


## Pré-requisitos:
Python 3.8;<br>
Bibliotecas pandas, dask, sqlalchemy. Para mysql instalar a biblioteca pymysql. Para postgres usar psycopg2.<br>

## Utilizando o script:
Baixe todos os arquivos zipados do site da Receita e salve na pasta "dados-publicos-zip".<br>
<s>O download no site da Receita é lento, pode demorar várias horas.</s> Sugiro utilizar um gerenciador de downloads.<br><br>
Obtenha uma relação dos arquivos disponíveis pelo comando python dados_cnpj_lista_url.py<br>
Crie uma pasta com o nome "dados-publicos".<br>

No servidor MYSQL ou POSTGRES, crie um database, por exemplo, cnpj.<br>
Especifique os parâmetros no começo do script:<br>
dbname = 'cnpj'<br>
username = 'root'<br>
password = ''<br>
host = '127.0.0.1'<br>

Para iniciar esse script, em um console digite<br>
python dados_cnpj_mysql.py<br>
ou<br>
python dados_cnpj_postgres.py<br>

A execução durou cerca de 5hs em um notebook i7 de 8a geração com Windows 10 no script para mysql.
No caso do postgres, fiz teste só com uma amostra em Linux (Ubuntu 20.4).

## Outras referências:

Para trabalhar com os dados de cnpj no formato SQLITE, use o meu projeto (https://github.com/rictom/cnpj-sqlite), em que coloco um link com a base de CNPJs em sqlite já tratada.<br>
A criação do arquivo sqlite é muita mais rápida que o carregamento da base em Mysql ou Postgres.<br>
O projeto (https://github.com/rictom/rede-cnpj) utiliza os dados públicos de CNPJ para visualização de relacionamentos entre empresas e sócios.<br>

## Histórico de versões
versão 0.2 (julho/2022)
- alterações menores no sql, para funcionar também em postgres;
- versão para postgres.

versão 0.1 (novembro/2021)
- primeira versão
