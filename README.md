[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/zixaop7v)

# Trabalho Prático de Banco de Dados
Rebea Madi Oliveira

Erin Dante

Saimnon Tavares

A documentação do projeto está em: `documentacao/tp1_3.1.pdf`

## Configurando o ambiente
Após clonar o repositório para o seu desktop.

### Docker e Docker Compose
Se você não tiver docker e docker-compose instalados na sua máquina siga os links.

Instalando o [docker desktop e docker compose (Windows, Linux e Mac)](https://www.docker.com/products/docker-desktop/)

Instalando na linha de comando

[Docker](https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-on-ubuntu-20-04-pt) e [Docker Compose Ubuntu](https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-compose-on-ubuntu-20-04-pt)

### Postgres

Criar pasta `postgres-data` na raiz do projeto. Essa pasta **não deve ser enviada** para o github.

Depois você deve subir o docker-compose com o postgres. Na primeira vez vai demorar um pouco, fique de olho nos logs.

```bash
docker-compose up -d
```

Para entrar no container execute:
```bash
docker exec -it trabalho-pr-tico-i-bancos-de-dados-1-bd-erin-rebeca-saimon_postgres_1 sh
```
Para entrar no ambiente psql:
```bash
psql -U postgres
```

### Python

Adicione o arquivo de dados da amazon dentro da pasta scripts.

Vá até o diretório de scripts:
```bash
cd scripts
```

No script `tp1_3.2.py` a variavel do caminho do arquivo pra ser processado
É a DATA_PATH em tp1_3.2

Execute:
```bash
python3 tp1_3.2.py
```
Espere até o script de criação ser sexecutado.

Para acessar o dashboard execute na pasta scripts:
```bash
python3 tp1_3.3.py
```
