import psycopg2

class ConexaoDB:
    def __init__(self, host, porta, usuario, senha, db):
        self.host = host
        self.porta = porta
        self.usuario = usuario
        self.senha = senha
        self.db = db
        self.conexao = None

    def conectar(self):
        try:
            self.conexao = psycopg2.connect(
                host=self.host,
                port=self.porta,
                user=self.usuario,
                password=self.senha,
                database=self.db
            )
            print("Conexão bem-sucedida!")
        except psycopg2.Error as e:
            print(f"Erro ao conectar ao banco de dados: {e}")

    def desconectar(self):
        if self.conexao:
            self.conexao.close()
            print("Conexão encerrada.")
