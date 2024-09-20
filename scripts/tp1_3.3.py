import psycopg2
import sys

def executar(query, filename):
    try:
        dbConfig = {
            'dbname': 'postgres',
            'user': 'postgres',
            'password': 'postgres',
            'host': 'localhost',
            'port': '5432'
        }
        #print(f"Query:\n{query}")
        
        conn = psycopg2.connect(**dbConfig)
        cursor = conn.cursor()
        cursor.execute(query)
        
        results = cursor.fetchall()
        if not results:
            print("Nenhum resultado foi encontrado.")
        else:
            with open(filename, 'a') as file:
                for row in results:
                    file.write(str(row) + '\n')
            print(f"Resultados salvos em {filename}")
        
        cursor.close()
        conn.close()
    except psycopg2.Error as e:
        print(f"Ocorreu um erro ao executar a query: {e}")

def main():
    filename = "results.txt" 

    while True:
        print("\n - - - - - - - - - D A S H B O A R D - - - - - - - - - \n")
        print("1. Comentários mais úteis e com maior avaliação e comentários mais úteis e com menor avaliação")
        print("2. Produtos similares com maiores vendas")
        print("3. Evolução diária das médias de avaliação")
        print("4. 10 produtos líderes de venda em cada grupo")
        print("5. 10 produtos com a maior média de avaliações úteis positivas")
        print("6. 5 categorias com a maior média de avaliações úteis positivas")
        print("7. 10 clientes que mais fizeram comentários por grupo")
        print("8. Sair")
        op = input("Escolha uma opção: ")

        if op == '1':
            idproduto = input("Digite o ID do produto: ")
            query = f"""
                    SELECT * FROM (
                        SELECT * FROM review r
                        INNER JOIN produto p ON r.idProduto = p.idProduto
                        WHERE p.idProduto = '{idproduto}'
                        ORDER BY notaAvaliacao DESC, util DESC
                        LIMIT 5
                    ) AS mais_uteis_alta_avaliacao
                    UNION ALL
                    SELECT * FROM (
                        SELECT * FROM review r
                        INNER JOIN produto p ON r.idProduto = p.idProduto
                        WHERE p.idProduto = '{idproduto}'
                        ORDER BY notaAvaliacao ASC, util DESC
                        LIMIT 5
                    ) AS mais_uteis_baixa_avaliacao;
                """
            executar(query, filename)

        elif op == '2':
            asin = input("Digite o ASIN do produto: ")
            query = f"""
                SELECT ps.idSimilar, ps.asinProdutoSimilar, p2.rankVendas 
                FROM produto_similar ps
                JOIN produto p1 ON ps.asinProduto = p1.asin
                JOIN produto p2 ON ps.asinProdutoSimilar = p2.asin
                WHERE p2.rankVendas > p1.rankVendas AND p1.asin = '{asin}'
                ORDER BY p2.rankVendas DESC;
            """
            executar(query, filename)

        elif op == '3':
            product_id = input("Digite o ID do produto: ")
            query = f"""
                SELECT rating, created_at FROM product_reviews
                WHERE product_id = {product_id} ORDER BY created_at ASC;
            """
            executar(query, filename)

        elif op == '4':
            query = """
                SELECT *
                FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY idgrupo ORDER BY rankvendas DESC) AS rank
                    FROM produto
                ) AS lideres
                WHERE rank <= 10 AND rankvendas IS NOT NULL;
            """
            executar(query, filename)

        elif op == '5':
            query = """
                WITH uteis AS (
                    SELECT idproduto, AVG(util) AS avgutil, ROW_NUMBER() OVER (ORDER BY AVG(util) DESC) AS rankutil
                    FROM review 
                    WHERE util > 0 GROUP BY idproduto
                )
                SELECT * FROM uteis WHERE rankutil <= 10;
            """
            executar(query, filename)

        elif op == '6':
            query = """
                WITH helpful_categories AS (
                    SELECT pc.category_name, AVG(pr.helpful) AS avg_helpful, ROW_NUMBER() OVER (ORDER BY AVG(pr.helpful) DESC) AS rank_helpful
                    FROM product_reviews pr 
                    JOIN product p ON pr.product_id = p.product_id 
                    JOIN product_category_link pl ON pl.product_id = p.product_id
                    JOIN product_categories pc ON pc.category_id = pl.category_id
                    WHERE pr.helpful > 0 GROUP BY pc.category_name
                )
                SELECT * FROM helpful_categories WHERE rank_helpful <= 5;
            """
            executar(query, filename)

        elif op == '7':
            query = """
                WITH top_reviewers_per_group AS (
                    SELECT p.group_id, pr.customer_id, COUNT(*) AS review_count, ROW_NUMBER() OVER (PARTITION BY p.group_id ORDER BY COUNT(*) DESC) AS rank_reviewer
                    FROM product_reviews pr 
                    JOIN product p ON pr.product_id = p.product_id 
                    GROUP BY p.group_id, pr.customer_id
                )
                SELECT group_id, customer_id, review_count FROM top_reviewers_per_group WHERE rank_reviewer <= 10;
            """
            executar(query, filename)

        elif op == '8':
            print("Até logo! <3")
            sys.exit()
        else:
            print("Opção inválida.")
        input("Pressione qualquer tecla para continuar...")


if __name__ == "__main__":
    main()
