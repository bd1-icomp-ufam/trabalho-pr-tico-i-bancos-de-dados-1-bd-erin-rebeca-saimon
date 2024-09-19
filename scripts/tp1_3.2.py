import sys
import subprocess

class Dependencias:
    """Verifica se as dependências estão presentes no ambiente e instala se necessário."""
    def __init__(self):
        self.verificar_ou_instalar('psycopg2', 'psycopg2-binary')

    @staticmethod
    def verificar_ou_instalar(modulo, pacote):
        try:
            __import__(modulo)
        except ImportError:
            try:
                subprocess.check_call([sys.executable, "-m", "pip", "install", pacote])
            except Exception as e:
                sys.exit(f"Erro ao instalar {modulo}: {e}")

dependencias = Dependencias()

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import re

class BancoDeDados:
    """Classe para a manipulação do Banco de Dados"""
    def __init__(self, banco_nome, usuario, senha, host, porta):
        self.config_db = {
            'dbname': banco_nome,
            'user': usuario,
            'password': senha,
            'host': host,
            'port': porta
        }

    def executar_comando(self, comandos, valores=None, muitos=False):
        """Executa comandos SQL de forma eficiente"""
        conn = psycopg2.connect(**self.config_db)
        cursor = conn.cursor()
        try:
            if muitos:
                cursor.executemany(comandos, valores)
            else:
                cursor.execute(comandos, valores)
            conn.commit()
        finally:
            cursor.close()
            conn.close()

    def criar_tabelas(self):
        SQL_comandos = [
            """CREATE TABLE IF NOT EXISTS cliente (idCliente VARCHAR PRIMARY KEY)""",
            """CREATE TABLE IF NOT EXISTS categorias (
                    idCategoria INTEGER PRIMARY KEY,
                    nomeCategoria VARCHAR,
                    idCategoriaPai INTEGER,
                    FOREIGN KEY (idCategoriaPai) REFERENCES categorias (idCategoria)
                )""",
            """CREATE TABLE IF NOT EXISTS grupo (
                    idGrupo SERIAL PRIMARY KEY,
                    nomeGrupo VARCHAR UNIQUE
                )""",
            """CREATE TABLE IF NOT EXISTS produto (
                    idProduto INTEGER PRIMARY KEY,
                    asin VARCHAR UNIQUE,
                    idGrupo INTEGER,
                    rankVendas INTEGER,
                    titulo VARCHAR,
                    "similar" INTEGER,
                    FOREIGN KEY (idGrupo) REFERENCES grupo (idGrupo)
                )""",
            """CREATE TABLE IF NOT EXISTS produto_categoria (
                    idCatProd SERIAL PRIMARY KEY,
                    idProduto INTEGER,
                    idCategoria INTEGER,
                    FOREIGN KEY (idProduto) REFERENCES produto (idProduto),
                    FOREIGN KEY (idCategoria) REFERENCES categorias (idCategoria)
                )""",
            """CREATE TABLE IF NOT EXISTS produto_similar (
                    idSimilar SERIAL PRIMARY KEY,
                    asinProduto VARCHAR,
                    asinProdutoSimilar VARCHAR,
                    FOREIGN KEY (asinProduto) REFERENCES produto (asin),
                    FOREIGN KEY (asinProdutoSimilar) REFERENCES produto (asin)
                )""",
            """CREATE TABLE IF NOT EXISTS review (
                    idReview SERIAL PRIMARY KEY,
                    dataCriacao DATE,
                    votos INTEGER,
                    notaAvaliacao INTEGER,
                    util INTEGER,
                    idProduto INTEGER,
                    idCliente VARCHAR,
                    FOREIGN KEY (idProduto) REFERENCES produto (idProduto),
                    FOREIGN KEY (idCliente) REFERENCES cliente (idCliente)
                )"""
        ]
        for comando in SQL_comandos:
            self.executar_comando(comando)

    def inserir_clientes(self, ids_clientes):
        comandos = "INSERT INTO cliente (idCliente) VALUES (%s) ON CONFLICT (idCliente) DO NOTHING"
        self.executar_comando(comandos, [(cliente,) for cliente in ids_clientes], muitos=True)

    def inserir_grupos(self, nomes_grupos):
        comandos = "INSERT INTO grupo (nomeGrupo) VALUES (%s) ON CONFLICT (nomeGrupo) DO NOTHING"
        self.executar_comando(comandos, [(nome,) for nome in nomes_grupos], muitos=True)

    def inserir_produtos(self, produtos):
        comandos = """
            INSERT INTO produto (idProduto, asin, titulo, idGrupo, rankVendas, "similar")
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (idProduto) DO NOTHING
        """
        dados_produtos = []
        for produto in produtos:
            id_grupo = self.obter_ou_inserir_grupo(produto.get('grupo')) if produto.get('grupo') else None
            dados_produtos.append((
                produto['id_produto'], produto['ASIN'], 
                produto.get('titulo'), id_grupo, 
                produto.get('rank_vendas'), produto.get('valor_similar')
            ))
        self.executar_comando(comandos, dados_produtos, muitos=True)

    def obter_ou_inserir_grupo(self, nome_grupo):
        nome_grupo = nome_grupo.upper()
        conn = psycopg2.connect(**self.config_db)
        cursor = conn.cursor()
        try:
            comando_busca = "SELECT idGrupo FROM grupo WHERE nomeGrupo = %s"
            cursor.execute(comando_busca, (nome_grupo,))
            id_grupo = cursor.fetchone()
            if not id_grupo:
                comando_insercao = "INSERT INTO grupo (nomeGrupo) VALUES (%s) RETURNING idGrupo"
                cursor.execute(comando_insercao, (nome_grupo,))
                id_grupo = cursor.fetchone()[0]
                conn.commit()
            else:
                id_grupo = id_grupo[0]
        finally:
            cursor.close()
            conn.close()
        return id_grupo

    def inserir_categorias(self, produtos):
        categorias = []
        for produto in produtos:
            for categoria in produto.get('categorias', []):
                #print(f"Processando categoria: {categoria}")
                partes_categoria = categoria[1:].split('|')
                parent_id = None

                for subcategoria in partes_categoria:
                    categoria_match = re.match(r"(.*)\[(\d+)\]", subcategoria)
                    if categoria_match:
                        nome_categoria = categoria_match.group(1).strip()
                        id_categoria = int(categoria_match.group(2))

                        categorias.append((id_categoria, nome_categoria, parent_id))
                        #print(f"Categoria válida: ID {id_categoria}, Nome {nome_categoria}, Parent ID {parent_id}")

                        parent_id = id_categoria
                    else:
                        print(f"Formato inválido de categoria: {subcategoria}")

        print(f"Total de categorias processadas: {len(categorias)}")

        comandos = """
            INSERT INTO categorias (idCategoria, nomeCategoria, idCategoriaPai)
            VALUES (%s, %s, %s)
            ON CONFLICT (idCategoria) DO NOTHING
        """
        self.executar_comando(comandos, categorias, muitos=True)

    def inserir_produto_categoria(self, produtos):
        produto_categoria = []
        for produto in produtos:
            for categoria in produto.get('categorias', []):
                try:
                    # Verifica se a categoria está no formato correto e tenta converter o ID
                    categoria_partes = categoria[1:].split('|')
                    for parte in categoria_partes:
                        if len(categoria_partes) >= 2:
                            id_categoria_str = re.findall( r'\[(.*?)\]', parte)
                            if id_categoria_str[0].isdigit():
                                id_categoria = int(id_categoria_str[0])
                                produto_categoria.append((produto['id_produto'], id_categoria))
                            else:
                                print(f"ID da categoria não é um número válido: {id_categoria_str}")
                        else:
                            print(f"Categoria malformada: {categoria}")
                except ValueError as e:
                    print(f"Erro ao processar categoria: {categoria}. Erro: {e}")

        print(f"Total de produto-categorias processadas: {len(produto_categoria)}")

        comandos = """
            INSERT INTO produto_categoria (idProduto, idCategoria)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
        """
        self.executar_comando(comandos, produto_categoria, muitos=True)

    def inserir_produtos_similares(self, produtos):
        produto_similar = []
        produtos_existentes = set()

        # Primeiro, verifique quais produtos existem na tabela
        conn = psycopg2.connect(**self.config_db)
        cursor = conn.cursor()
        
        cursor.execute("SELECT asin FROM produto")
        for row in cursor.fetchall():
            produtos_existentes.add(row[0])
        
        cursor.close()
        conn.close()

        # Agora, insira os pares de produtos similares somente se ambos existirem
        conn = psycopg2.connect(**self.config_db)
        cursor = conn.cursor()

        for produto in produtos:
            if not produto.get('descontinuado', False):
                asin_produto = produto['ASIN']
                if asin_produto not in produtos_existentes:
                    continue  # Pular produtos que não existem na tabela

                for asin_similar in produto.get('produtos_similares', []):
                    if asin_similar in produtos_existentes:
                        produto_similar.append((asin_produto, asin_similar))

        comandos = """
            INSERT INTO produto_similar (asinProduto, asinProdutoSimilar)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
        """
        self.executar_comando(comandos, produto_similar, muitos=True)

        cursor.close()
        conn.close()


    def extrair_e_inserir_avaliacoes(self, nome_arquivo):
        reviews = []
        with open(nome_arquivo, 'r') as arquivo:
            for linha in arquivo:
                if 'review:' in linha:
                    partes = linha.split()
                    data = partes[1]
                    votos = int(partes[2])
                    nota = int(partes[3])
                    util = int(partes[4])
                    id_produto = int(partes[5])
                    id_cliente = partes[6]
                    reviews.append((data, votos, nota, util, id_produto, id_cliente))
        comandos = """
            INSERT INTO review (dataCriacao, votos, notaAvaliacao, util, idProduto, idCliente)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """
        self.executar_comando(comandos, reviews, muitos=True)

# Funções de processamento de arquivo
def processar_arquivo(nome_arquivo):
    ids_clientes = set()
    nomes_grupos = set()

    with open(nome_arquivo, 'r') as arquivo:
        for linha in arquivo:
            if 'cliente:' in linha:
                correspondencia = re.search(r'cliente:\s+(\w+)', linha)
                if correspondencia:
                    ids_clientes.add(correspondencia.group(1))
            elif 'grupo:' in linha and 'titulo:' not in linha:
                nome_grupo = ' '.join(linha.split('grupo:', 1)[1].strip().split()).upper()
                if nome_grupo:
                    nomes_grupos.add(nome_grupo)

    return ids_clientes, nomes_grupos

def extrair_dados_produtos(nome_arquivo):
    produtos = []
    produto_atual = None

    with open(nome_arquivo, 'r') as arquivo:
        for linha in arquivo:
            if linha.startswith("Id:"):
                if produto_atual:
                    produtos.append(produto_atual)
                id_produto = int(linha.split()[1].strip())
                produto_atual = {
                    'id_produto': id_produto,
                    'ASIN': '',
                    'descontinuado': False,
                    'categorias': [],
                    'produtos_similares': [],
                    'valor_similar': 0
                }
            elif linha.startswith("ASIN:"):
                produto_atual['ASIN'] = linha.split()[1].strip()
            elif 'produto descontinuado' in linha:
                produto_atual['descontinuado'] = True
            elif 'titulo:' in linha and produto_atual and not produto_atual.get('descontinuado', False):
                produto_atual['titulo'] = linha.split('titulo:', 1)[1].strip()
            elif 'grupo:' in linha and produto_atual and not produto_atual.get('descontinuado', False):
                produto_atual['grupo'] = linha.split('grupo:', 1)[1].strip()
            elif 'rank_vendas:' in linha and produto_atual and not produto_atual.get('descontinuado', False):
                produto_atual['rank_vendas'] = int(linha.split('rank_vendas:', 1)[1].strip())
            elif 'similar:' in linha and produto_atual and not produto_atual.get('descontinuado', False):
                partes = linha.split()
                quantidade_similares = int(partes[1])
                produto_atual['valor_similar'] = quantidade_similares
                produto_atual['produtos_similares'] = partes[2:2 + quantidade_similares] if quantidade_similares > 0 else []
            elif 'categorias:' in linha and produto_atual and not produto_atual.get('descontinuado', False):
                produto_atual['categorias'] = []
            elif '|' in linha and produto_atual and not produto_atual.get('descontinuado', False):
                produto_atual['categorias'].append(linha.strip())

        if produto_atual:
            produtos.append(produto_atual)

    return produtos

if __name__ == "__main__":
    postgres = BancoDeDados('postgres', 'postgres', 'postgres', 'localhost', 5432)
    print("Criando tabelas...")
    postgres.criar_tabelas()
    print("Tabelas criadas!")
    
    print("Processando arquivo..")
    ids_clientes, nomes_grupos = processar_arquivo('teste.txt')
    print("Inserindo clientes...")
    postgres.inserir_clientes(ids_clientes)
    print("Inserindo grupos...")
    postgres.inserir_grupos(nomes_grupos)
    print("Extraindo dados de produtos...")
    produtos = extrair_dados_produtos('teste.txt')
    print("Inserindo produtos...")
    postgres.inserir_produtos(produtos)
    print("Inserindo vategorias...")
    postgres.inserir_categorias(produtos)
    print("Inserindo produtos às categorias...")
    postgres.inserir_produto_categoria(produtos)
    print("Inserindo produtos similares...")
    postgres.inserir_produtos_similares(produtos)
    print("Inserindo avaliações...")
    postgres.extrair_e_inserir_avaliacoes('teste.txt')
    print("Processo finalizado!")