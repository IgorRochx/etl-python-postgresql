import pandas as pd #importa a biblioteca pandas
import psycopg #importa a biblioteca postgresql
from dotenv import load_dotenv #importa a biblioteca dotenv
import os #importa a biblioteca os

load_dotenv() #carrega as variaveis do arquivo .env

try:
    df = pd.read_csv(r"C:\Users\igoor\Downloads\vendas.csv", encoding="utf-8") #abre o arquivo csv
    print(df.head()) #imprime as 5 primeiras linhas

    df["valor_venda"] = pd.to_numeric(df["valor_venda"], errors="coerce") #converte os valores da coluna valor_venda para float e ERROR para NaN
    df = df.dropna() #remove linhas com valores NaN
    df["id_veiculo"] = df["id_veiculo"].astype(int) #converte a coluna id_veiculo para inteiro
    df["id_cliente"] = df["id_cliente"].astype(int) #converte a coluna id_cliente para inteiro
    df["data_venda"] = pd.to_datetime(df["data_venda"]) #converte a coluna data_venda para datetime
    df["nome_cliente"] = df["nome_cliente"].str.capitalize() #deixa os nomes com a primeira letra maiuscula

    print(df.info())
    print(df.isnull().sum())

    try:
        conn = psycopg.connect( #conecta ao banco
            host=os.getenv("DB_HOST"),
            dbname=os.getenv("DB_NAME"), #psycopg3 usa dbname, nao database
            user=os.getenv("DB_USER"), #informacoes do banco vindas do .env
            password=os.getenv("DB_PASSWORD"),
            port=os.getenv("DB_PORT")
        )
        cursor = conn.cursor() #cria um cursor
        print("Conexao estabelecida com sucesso")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS TB_Cliente (
                id_cliente INTEGER NOT NULL PRIMARY KEY,
                nome_cliente VARCHAR(50) NOT NULL
            );
            CREATE TABLE IF NOT EXISTS TB_Veiculo (
                id_veiculo INTEGER NOT NULL PRIMARY KEY,
                nome_veiculo VARCHAR(50) NOT NULL
            );
            CREATE TABLE IF NOT EXISTS TB_Venda (
                id_venda INTEGER NOT NULL PRIMARY KEY,
                data_venda DATE,
                valor_venda FLOAT,
                ven_cod_cliente INTEGER NOT NULL,
                ven_cod_veiculo INTEGER NOT NULL,
                FOREIGN KEY (ven_cod_cliente) REFERENCES TB_Cliente (id_cliente),
                FOREIGN KEY (ven_cod_veiculo) REFERENCES TB_Veiculo (id_veiculo)
            );
        """)

        conn.commit() #salva as alteracoes no banco
        print("Tabelas criadas com sucesso")

        for _, row in df.iterrows(): #percorre o dataframe linha por linha
            cursor.execute("""
                INSERT INTO TB_Cliente (id_cliente, nome_cliente)
                VALUES (%s, %s)
                ON CONFLICT (id_cliente) DO NOTHING;
            """, (row["id_cliente"], row["nome_cliente"])) #passa os valores da linha atual

        conn.commit() #salva as insercoes no banco
        print("Clientes inseridos com sucesso")

        for _, row in df.iterrows(): #percorre o dataframe linha por linha
            cursor.execute("""
                INSERT INTO TB_Veiculo (id_veiculo, nome_veiculo)
                VALUES (%s, %s)
                ON CONFLICT (id_veiculo) DO NOTHING;
            """, (row["id_veiculo"], row["nome_veiculo"])) #passa os valores da linha atual

        conn.commit() #salva as insercoes no banco
        print("Veiculos inseridos com sucesso")

        for _, row in df.iterrows(): #percorre o dataframe linha por linha
            cursor.execute("""
                INSERT INTO TB_Venda (id_venda, data_venda, valor_venda, ven_cod_cliente, ven_cod_veiculo)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id_venda) DO NOTHING;
            """, (row["id_venda"], row["data_venda"], row["valor_venda"], row["id_cliente"], row["id_veiculo"])) #passa os valores da linha atual

        conn.commit() #salva as insercoes no banco
        print("Vendas inseridas com sucesso")

    except psycopg.Error as e: #tratamento de erro
        print(f"Erro ao conectar ao banco de dados: {e}")

except FileNotFoundError: #tratamento de erro caso arquivo nao encontrado
    print("Arquivo nao encontrado")