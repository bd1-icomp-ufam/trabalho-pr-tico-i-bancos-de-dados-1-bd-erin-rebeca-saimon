DATA_PATH = "amazon-meta.txt"

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
        """Executa comandos SQL"""
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
        """Cria as tableas no banco se elas ainda não existirem"""
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
        """Insere os ids dos clientes na tabela cliente"""
        conn = psycopg2.connect(**self.config_db)
        cursor = conn.cursor()
        for id in ids_clientes:
            cursor.execute("INSERT INTO cliente (idCliente) VALUES (%s) ON CONFLICT (idCliente) DO NOTHING;", (id,))
        conn.commit()
        cursor.close()
        conn.close()


    def inserir_grupos(self, nomes_grupos):
        """Insere os nomes dos grupos"""
        comandos = "INSERT INTO grupo (nomeGrupo) VALUES (%s) ON CONFLICT (nomeGrupo) DO NOTHING"
        self.executar_comando(comandos, [(nome,) for nome in nomes_grupos], muitos=True)

    def inserir_produtos(self, produtos):
        """Insere os produtos"""
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

                partes_categoria = categoria[1:].split('|')
                parent_id = None

                for subcategoria in partes_categoria:
                    categoria_match = re.match(r"(.*)\[(\d+)\]", subcategoria)
                    if categoria_match:
                        nome_categoria = categoria_match.group(1).strip()
                        id_categoria = int(categoria_match.group(2))

                        categorias.append((id_categoria, nome_categoria, parent_id))

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
                            Verificador = False
                            for c_str in id_categoria_str:
                                if c_str.isdigit():
                                    Verificador = True
                                    id_categoria = int(c_str)
                                    produto_categoria.append((produto['id_produto'], id_categoria))
                                    break
                            if Verificador == False:
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

        # Verificar quais produtos existem na tabela
        conn = psycopg2.connect(**self.config_db)
        cursor = conn.cursor()
        
        cursor.execute("SELECT asin FROM produto;")
        for row in cursor.fetchall():
            produtos_existentes.add(row[0])
        
        cursor.close()
        conn.close()

        # inserir produtos similares somente se ambos existirem
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

def processar_arquivo(nome_arquivo):
    """Lê os dados do cliente, grupos e titulos"""
    ids_clientes = set()
    nomes_grupos = set()

    with open(nome_arquivo, 'r') as arquivo:
        for linha in arquivo:
            if 'Id:' in linha:
                correspondencia = re.search(r'Id:\s+(\w+)', linha)
                if correspondencia:
                    ids_clientes.add(str(correspondencia.group(1)).strip())
            elif 'group:' in linha and 'title:' not in linha:
                nome_grupo = ' '.join(linha.split('group:', 1)[1].strip().split()).upper()
                if nome_grupo:
                    nomes_grupos.add(nome_grupo)

    return ids_clientes, nomes_grupos

def extrair_dados_produtos(nome_arquivo):
    """Parser para os outros dados, assim não há erros de falta de elementos"""
    products = []
    current_product = None

    with open(DATA_PATH, 'r') as file:
        for line in file:
            if line.startswith("Id:"):
                if current_product:
                    products.append(current_product)
                product_id = int(line.split()[1].strip())
                current_product = {
                    'id_produto': product_id,
                    'ASIN': '',
                    'descontinuado': False,
                    'categorias': [],
                    'produtos_similares': [],
                    'valor_similar': 0
                }
            elif line.startswith("ASIN:"):
                current_product['ASIN'] = line.split()[1].strip()
            elif 'discontinued product' in line:
                current_product['descontinuado'] = True
            elif 'title:' in line and current_product and not current_product.get('descontinuado', False):
                current_product['titulo'] = line.split('title:', 1)[1].strip()
            elif 'group:' in line and current_product and not current_product.get('descontinuado', False):
                current_product['grupo'] = line.split('group:', 1)[1].strip()
            elif 'salesrank:' in line and current_product and not current_product.get('descontinuado', False):
                current_product['salesrank'] = int(line.split('salesrank:', 1)[1].strip())
            elif 'similar:' in line and current_product and not current_product.get('descontinuado', False):
                parts = line.split()
                similar_count = int(parts[1])
                current_product['valor_similar'] = similar_count
                current_product['produtos_similares'] = parts[2:2 + similar_count] if similar_count > 0 else []
            elif 'categories:' in line and current_product and not current_product.get('descontinuado', False):
                current_product['categorias'] = []
            elif '|' in line and current_product and not current_product.get('descontinuado', False):
                current_product['categorias'].append(line.strip())

        if current_product:
            products.append(current_product)

    return products

if __name__ == "__main__":
    postgres = BancoDeDados('postgres', 'postgres', 'postgres', 'localhost', 5432)
    print("Criando tabelas...")
    postgres.criar_tabelas()
    print("Tabelas criadas!")
    
    print("Processando arquivo..")
    ids_clientes, nomes_grupos = processar_arquivo(DATA_PATH)
    print("Inserindo clientes...")
    postgres.inserir_clientes(ids_clientes)
    print("Inserindo grupos...")
    postgres.inserir_grupos(nomes_grupos)
    print("Extraindo dados de produtos...")
    produtos = extrair_dados_produtos(DATA_PATH)
    print("Inserindo produtos...")
    postgres.inserir_produtos(produtos)
    print("Inserindo vategorias...")
    postgres.inserir_categorias(produtos)
    print("Inserindo produtos às categorias...")
    postgres.inserir_produto_categoria(produtos)
    print("Inserindo produtos similares...")
    postgres.inserir_produtos_similares(produtos)
    print("Inserindo avaliações...")
    postgres.extrair_e_inserir_avaliacoes(DATA_PATH)
    print("Processo finalizado!")