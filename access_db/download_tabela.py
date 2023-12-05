import psycopg2
import csv


class DownloadTabela:
    def __init__(self, conexao, schema, tabela):
        self.conexao = conexao
        self.schema = schema
        self.tabela = tabela
        self.diretorio = None
        self.acao = 'r'  # Valor padrão para leitura
        self.codificacao = 'utf-8'  # Codificação padrão

    def configurar_exportacao(self, diretorio, acao='w', codificacao='utf-8'):
        self.diretorio = diretorio
        self.acao = acao
        self.codificacao = codificacao

    def baixar_tabela(self, condicionais):
        if not self.conexao.conexao:
            print("Conexão com o banco de dados não estabelecida.")
            return

        condicoes_sql = " AND ".join(condicionais)
        consulta_sql = f"SELECT * FROM {self.schema}.{self.tabela} WHERE {condicoes_sql}"

        try:
            cursor = self.conexao.conexao.cursor()
            cursor.execute(consulta_sql)
            colunas = [desc[0] for desc in cursor.description]  # Obtenha os nomes das colunas
            dados = cursor.fetchall()
            cursor.close()

            with open(self.diretorio, self.acao, newline='', encoding=self.codificacao) as arquivo_saida:
                writer = csv.writer(arquivo_saida)
                writer.writerow(colunas)  # Escreva os nomes das colunas
                for linha in dados:
                    writer.writerow(linha)

            print(f"Tabela {self.tabela} exportada com sucesso para {self.diretorio}")
        except psycopg2.Error as e:
            print(f"Erro ao baixar a tabela: {e}")