# cnpj-mysql
Script em python para carregar os arquivos de cnpj dos dados públicos da Receita Federal em MYSQL e POSTGRESQL. O código é compatível com o layout das tabelas disponibilizadas pela Receita Federal a partir de 2021. O código não foi gerado por IA!!!
<br><br>
**AVISO IMPORTANTE: Ao final de janeiro/2026, a RFB alterou o layout da página, nome e caminho dos arquivos, por isso o script que baixa os arquivos zip parou de funcionar!!! Baixe os arquivos zip manualmente e aguarde uma atualização.**<br>


## Dados públicos de cnpj no site da Receita:
Os arquivos csv zipados com os dados de CNPJs estão disponíveis em https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj<br>


## Pré-requisitos:
Python 3.8;<br>
Bibliotecas pandas, dask, sqlalchemy. Para mysql instalar a biblioteca pymysql. Para postgres usar psycopg2.<br>
Para instalar a biblioteca, use o comando<br>
pip install pymysql<br>
Para postgres, instale psycopg2 (recomenda-se psycopg2-binary para instalação mais simples)<br>
pip install psycopg2-binary (testado no Ubuntu).<br>

## Utilizando o script:

Para obter relação dos arquivos disponíveis no site da Receita Federal ou baixar os arquivos, faça o seguinte comando no Anaconda prompt:<br>
<b>python dados_cnpj_baixa.py</b><br>
Isto irá baixar os arquivos zipados do site da Receita na pasta "dados-publicos-zip".<br><br>

<b>OBSERVAÇÃO</b>: Em janeiro/2026 a RFB alterou o local dos arquivos, por isso o script dados_cnpj_baixa.py parou de funcionar.<br>
Crie uma pasta com o nome <b>"dados-publicos"</b>. Se houver arquivos antigos nesta pasta, apague ou mova-os de lugar.<br>
Para baixar os arquivos csv, vá até a página [https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj](https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj) , procure a seção "Recursos", clique no botão "Acessar Recurso" em "Inscrições no CNPJ". <br>
Isto levará para uma página de Download. Selecione o mês desejado e baixe os (37) arquivos: Cnaes.zip, Motivos.zip, Municipios.zip, Naturezas.zip, Paises.zip, Qualificacoes.zip, Simples.zip, Empresas0.zip, Empresas1.zip, Empresas2.zip, Empresas3.zip, Empresas4.zip, Empresas5.zip, Empresas6.zip, Empresas7.zip, Empresas8.zip, Empresas9.zip, Estabelecimentos0.zip, Estabelecimentos1.zip, Estabelecimentos2.zip, Estabelecimentos3.zip, Estabelecimentos4.zip, Estabelecimentos5.zip, Estabelecimentos6.zip, Estabelecimentos7.zip, Estabelecimentos8.zip, Estabelecimentos9.zip, Socios0.zip, Socios1.zip, Socios2.zip, Socios3.zip, Socios4.zip, Socios5.zip, Socios6.zip, Socios7.zip, Socios8.zip, Socios9.zip.<br>
Copie os arquivos zip para a pasta "dados-publicos-zip".<br>
O site da RFB agora permite baixar uma pasta zipada correspondente ao ANO-MÊS. Se você baixar por esta opção, descompacte o arquivo ANO-MÊS.zip, e copie os 37 arquivos zip para a pasta "dados-publicos-zip".<br>



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
Se a execução deste script demorar muito, uma opção é usar o projeto em https://github.com/rictom/cnpj-sqlite para gerar o arquivo em sqlite e usar uma ferramenta como o pgloader ou o DBeaver para converter depois em postgres.
Este colega usou o pgloader com um bom desempenho: https://github.com/rictom/cnpj-mysql/issues/5

## Outras referências:

Para trabalhar com os dados de cnpj no formato SQLITE, use o meu projeto (https://github.com/rictom/cnpj-sqlite).<br>
A criação do arquivo sqlite é muita mais rápida que o carregamento da base em Mysql ou Postgres.<br>
O projeto (https://github.com/rictom/rede-cnpj) utiliza os dados públicos de CNPJ para visualização de relacionamentos entre empresas e sócios.<br>

## Histórico de versões
versão 0.2 (janeiro/2022)
- aceita sqlalchemy>=2.0;
  
versão 0.2 (julho/2022)
- alterações menores no sql, para funcionar também em postgres;
- versão para postgres.

versão 0.1 (novembro/2021)
- primeira versão
