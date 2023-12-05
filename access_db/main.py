from conectar_db import ConexaoDB
from download_tabela import DownloadTabela

# Configurações de conexão com o banco de dados
db_config = {
    "host": "host",
    "porta": 0000,
    "usuario": "user",
    "senha": "password",
    "db": "your_db"
}

# Conectar ao banco de dados
conexao = ConexaoDB(**db_config)
conexao.conectar()

# Condições para baixar a tabela
condicoes = ["cidade = 'city'", "bairro_tratado = 'neighborhood'", "data_raspagem <= 'YYYY-MM-DD'"]

# Criar uma instância da classe DownloadTabela
download = DownloadTabela(conexao, schema="your_schema", tabela="target_table")

# Configurar a exportação
download.configurar_exportacao(
    diretorio=r"your_directory",
    acao='w',  # 'w' para escrita, 'r' para leitura
    codificacao='utf-8'  # Codificação desejada
)

# Realizar o download da tabela
download.baixar_tabela(condicoes)

# Desconectar do banco de dados
conexao.desconectar()
