cnpj-mysql
Script em Python para carregar os arquivos de CNPJ dos dados públicos da Receita Federal em MySQL e PostgreSQL.
📋 Descrição
Este projeto fornece scripts automatizados para download e importação dos dados públicos de CNPJ da Receita Federal do Brasil em bancos de dados MySQL e PostgreSQL. O código é compatível com o layout das tabelas disponibilizadas pela Receita Federal a partir de 2021.
🔗 Fonte dos Dados
Os arquivos CSV zipados com os dados de CNPJs estão disponíveis em:

https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj
https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/ (disponível a partir de 28/10/2024)

🛠️ Pré-requisitos
Software

Python 3.8 ou superior

Bibliotecas Python
bash# Bibliotecas principais
pip install pandas dask sqlalchemy

# Para MySQL
pip install pymysql

# Para PostgreSQL
pip install psycopg2-binary

Nota: O psycopg2-binary é recomendado para instalação mais simples. Testado no Ubuntu.

📥 Download dos Dados
Para baixar os arquivos da Receita Federal, execute:
bashpython dados_cnpj_baixa.py
Este comando irá:

Buscar a relação de arquivos disponíveis no site da Receita Federal
Baixar os arquivos zipados para a pasta dados-publicos-zip


⚠️ Importante: Em 14/08/2024 a página de dados abertos foi modificada. O script dados_cnpj_baixa.py foi atualizado para buscar automaticamente a pasta do mês mais recente.


💡 Dica: Se o download estiver muito lento, considere utilizar um gerenciador de downloads.

⚙️ Configuração
1. Preparar Diretórios
Crie uma pasta vazia chamada dados-publicos:
bashmkdir dados-publicos
2. Configurar Banco de Dados
Crie um database no MySQL ou PostgreSQL:
sqlCREATE DATABASE cnpj;
3. Configurar Parâmetros de Conexão
Edite o início do script com suas credenciais:
pythondbname = 'cnpj'
username = 'root'
password = ''
host = '127.0.0.1'
🚀 Execução
Para MySQL
bashpython dados_cnpj_mysql.py
Para PostgreSQL
bashpython dados_cnpj_postgres.py
⏱️ Tempo de Execução

MySQL: Aproximadamente 5 horas em notebook i7 de 8ª geração com Windows 10
PostgreSQL: Testado com amostra em Linux (Ubuntu 20.04)

Alternativa para Melhor Performance
Se a execução estiver demorando muito, considere:

Usar o projeto cnpj-sqlite para gerar arquivo SQLite
Converter para PostgreSQL usando ferramentas como:

pgloader
DBeaver




📖 Exemplo de uso: Veja este caso de sucesso usando pgloader com bom desempenho.

🔗 Projetos Relacionados

cnpj-sqlite: Trabalhe com os dados de CNPJ no formato SQLite. A criação do arquivo SQLite é muito mais rápida que o carregamento em MySQL ou PostgreSQL.
rede-cnpj: Visualização de relacionamentos entre empresas e sócios utilizando os dados públicos de CNPJ.

📝 Histórico de Versões
Versão 0.3 (janeiro/2022)

✨ Suporte para SQLAlchemy >= 2.0

Versão 0.2 (julho/2022)

🔧 Alterações menores no SQL para compatibilidade com PostgreSQL
➕ Adicionada versão para PostgreSQL

Versão 0.1 (novembro/2021)

🎉 Primeira versão

📄 Licença
Este projeto utiliza dados públicos da Receita Federal do Brasil.
🤝 Contribuições
Contribuições são bem-vindas! Sinta-se à vontade para abrir issues ou pull requests.

Desenvolvido por: rictom
