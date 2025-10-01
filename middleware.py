import yaml
import psycopg2
from pymongo import MongoClient
from psycopg2 import sql
import os
import time
from datetime import datetime

from apscheduler.schedulers.blocking import BlockingScheduler

# =================================================================
#                         CONFIGURAÇÃO
# =================================================================

# --- Paths e Nomes ---
DRDL_FILE_PATH = "collProcessos.drdl"

# --- Conexão PostgreSQL ---
POSTGRES_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "database": "meu_middleware_db",  # Base de dados que você criou
    "user": "postgres",
    "password": "123456Ab",          # Sua senha do PostgreSQL
    "options": "-c search_path=public"
}

# --- Conexão MongoDB ---
MONGO_CONFIG = {
    "host": "localhost",
    "port": 27017,
    "database": "processo360",
    "collection": "processos"
}

# =================================================================
#                      PARTE 1: CRIAÇÃO DO ESQUEMA
# =================================================================

def load_drdl(file_path):
    """Carrega e faz o parse do arquivo DRDL (YAML), substituindo tabs por espaços."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Arquivo DRDL não encontrado em: {file_path}")

    with open(file_path, 'r') as f:
        content = f.read().replace('\t', '    ')
        return yaml.safe_load(content)

def create_postgres_schema(drdl_data):
    """Conecta ao PostgreSQL e cria as tabelas definidas no DRDL."""
    conn = None
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        conn.autocommit = True
        cursor = conn.cursor()

        print("-> Conexão com PostgreSQL OK.")

        for schema_def in drdl_data.get('schema', []):
            for table_def in schema_def.get('tables', []):
                table_name = table_def.get('table')
                columns = table_def.get('columns', [])

                if not table_name or not columns: continue

                column_defs = []

                primary_key_cols = []
                if table_name == 'collProcessos360':
                    primary_key_cols = ['_id']
                elif table_name == "collProcessos360_lstTitulosEmitidos":
                    primary_key_cols = ['N_Processo']
                elif table_name == 'collProcessos360_procAdministrativo_lstDecisoes':
                    primary_key_cols = ['_id']


                for col in columns:
                    sql_name = col['SqlName']
                    sql_type = col['SqlType']

                    if table_name == 'collProcessos360' and sql_name == 'N_Processo':
                        sql_type = 'VARCHAR'

                    if sql_type.lower() == 'objectid':
                        sql_type = 'TEXT'

                    col_identifier = sql.Identifier(sql_name)
                    col_def = sql.SQL("{} {}").format(col_identifier, sql.SQL(sql_type))

                    if sql_name in primary_key_cols:
                        col_def = sql.SQL("{} NOT NULL").format(col_def)

                    column_defs.append(col_def)

                if primary_key_cols:
                    pk_constraint = sql.SQL("PRIMARY KEY ({})").format(
                        sql.SQL(', ').join(map(sql.Identifier, primary_key_cols))
                    )
                    column_defs.append(pk_constraint)

                if not column_defs: continue

                create_table_command = sql.SQL(
                    "CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
                ).format(
                    table_name=sql.Identifier(table_name),
                    columns=sql.SQL(", \n").join(column_defs)
                )

                try:
                    cursor.execute(create_table_command)
                    print(f" [OK] Tabela \"{table_name}\" criada/verificada.")
                except Exception as e:
                    print(f" [ERRO] Falha ao criar a tabela \"{table_name}\": {e}")

        try:
            alter_table_command = sql.SQL(
                "ALTER TABLE {table_name} ADD CONSTRAINT {constraint_name} UNIQUE ({column_name})"
            ).format(
                table_name=sql.Identifier('collProcessos360'),
                constraint_name=sql.Identifier('collProcessos360_N_Processo_key'),
                column_name=sql.Identifier('N_Processo')
            )
            cursor.execute(alter_table_command)
            print(f" [OK] Adicionado UNIQUE constraint em collProcessos360(N_Processo).")
        except Exception as e:
            if "already exists" not in str(e) and "duplicate key" not in str(e):
                print(f" [AVISO] Falha ao adicionar UNIQUE constraint em collProcessos360(N_Processo): {e}")
            else:
                print(f" [INFO] UNIQUE constraint em collProcessos360(N_Processo) já existe.")

        try:
            alter_table_command = sql.SQL(
                "ALTER TABLE {table_name} ADD CONSTRAINT {fk_name} FOREIGN KEY ({from_column}) REFERENCES {to_table} ({to_column})"
            ).format(
                table_name=sql.Identifier('collProcessos360_lstTitulosEmitidos'),
                fk_name=sql.Identifier('fk_collProcessos360_lstTitulosEmitidos_N_Processo'),
                from_column=sql.Identifier('N_Processo'),
                to_table=sql.Identifier('collProcessos360'),
                to_column=sql.Identifier('N_Processo')
            )
            cursor.execute(alter_table_command)
            print(f" [OK] Adicionada Foreign Key para collProcessos360_lstTitulosEmitidos.")
        except Exception as e:
            if "already exists" not in str(e):
                print(f" [ERRO] Falha ao adicionar Foreign Key para collProcessos360_lstTitulosEmitidos: {e}")
            else:
                print(f" [INFO] Foreign Key para collProcessos360_lstTitulosEmitidos já existe.")

        try:
            alter_table_command = sql.SQL(
                "ALTER TABLE {table_name} ADD CONSTRAINT {fk_name} FOREIGN KEY ({from_column}) REFERENCES {to_table} ({to_column})"
            ).format(
                table_name=sql.Identifier('collProcessos360_procAdministrativo_lstDecisoes'),
                fk_name=sql.Identifier('fk_collProcessos360_procAdministrativo_lstDecisoes__id'),
                from_column=sql.Identifier('_id'),
                to_table=sql.Identifier('collProcessos360'),
                to_column=sql.Identifier('_id')
            )
            cursor.execute(alter_table_command)
            print(f" [OK] Adicionada Foreign Key para collProcessos360_procAdministrativo_lstDecisoes.")
        except Exception as e:
            if "already exists" not in str(e):
                print(f" [ERRO] Falha ao adicionar Foreign Key para collProcessos360_procAdministrativo_lstDecisoes: {e}")
            else:
                print(f" [INFO] Foreign Key para collProcessos360_procAdministrativo_lstDecisoes já existe.")


        print("\n--- Criação de esquema concluída. ---")
        return True

    except psycopg2.Error as e:
        print(f"\n[ERRO FATAL DE CONEXÃO PG]: {e}")
        return False
    finally:
        if conn: conn.close()

# =================================================================
#                      PARTE 2: EXTRAÇÃO E CARREGAMENTO (ETL)
# =================================================================

def run_etl_process(drdl_data):
    """
    Executa o processo de ETL (Extração do Mongo e Carregamento no Postgres).
    """
    start_time = time.time()
    print(f"\n[INÍCIO ETL]: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    pg_conn = None
    mongo_client = None

    try:
        # 1. Conexão MongoDB
        mongo_client = MongoClient(MONGO_CONFIG["host"], MONGO_CONFIG["port"])
        mongo_db = mongo_client[MONGO_CONFIG["database"]]
        print("-> Conexão MongoDB OK.")

        # 2. Conexão PostgreSQL
        pg_conn = psycopg2.connect(**POSTGRES_CONFIG)
        pg_conn.autocommit = False
        pg_cursor = pg_conn.cursor()

        # 3. Extração de dados do MongoDB
        mongo_collection = mongo_db[MONGO_CONFIG["collection"]]
        
        # Carrega todos os documentos da coleção. Para coleções muito grandes,
        # considere processar em batches (lotes).
        print(f"A extrair dados da coleção: {MONGO_CONFIG['collection']}")
        all_mongo_docs = list(mongo_collection.find())
        print(f"-> {len(all_mongo_docs)} documentos encontrados.")


        for schema_def in drdl_data.get('schema', []):
            for table_def in schema_def.get('tables', []):
                table_name = table_def['table']
                columns = table_def['columns']
                sql_names = [col['SqlName'] for col in columns]

                print(f"\nProcessando dados para a tabela: {table_name}")

                data_to_insert = []

                # Lógica para extrair dados para a tabela principal
                if table_name == 'collProcessos360':
                    for doc in all_mongo_docs:
                        row_values = []
                        for col_def in columns:
                            val = doc.get(col_def['SqlName'])
                            # Converte ObjectId para string
                            if col_def['MongoType'] == 'bson.ObjectId' and val:
                                val = str(val)
                            # Garante que N_Processo é string
                            if col_def['SqlName'] == 'N_Processo' and val is not None:
                                val = str(val)
                            row_values.append(val)
                        data_to_insert.append(tuple(row_values))
                
                # Lógica para extrair dados para tabelas aninhadas (sub-documentos)
                else:
                    # Extrai o nome do campo aninhado a partir do nome da tabela
                    # Ex: 'collProcessos360_lstTitulosEmitidos' -> 'lstTitulosEmitidos'
                    nested_field_name = table_name.replace('collProcessos360_', '')

                    for doc in all_mongo_docs:
                        nested_items = doc.get(nested_field_name, [])
                        
                        # Lida com campos que podem não ser uma lista
                        if not isinstance(nested_items, list):
                            continue

                        for item in nested_items:
                            row_values = []
                            for col_def in columns:
                                val = None
                                # Verifica se a coluna é uma chave estrangeira para o documento pai
                                if col_def['SqlName'] in doc:
                                    val = doc.get(col_def['SqlName'])
                                # Senão, busca o valor no item aninhado
                                elif isinstance(item, dict) and col_def['SqlName'] in item:
                                    val = item.get(col_def['SqlName'])

                                # Converte ObjectId para string
                                if val and col_def['MongoType'] == 'bson.ObjectId':
                                    val = str(val)
                                
                                row_values.append(val)
                            data_to_insert.append(tuple(row_values))


                # 4. CARREGAMENTO (PostgreSQL)
                if not data_to_insert:
                    print(" [INFO] Nenhum dado para inserir.")
                    continue

                values_placeholders = sql.SQL(', ').join(sql.SQL('%s') for _ in sql_names)
                pk_columns = {
                    'collProcessos360': ['_id'],
                    'collProcessos360_procAdministrativo_lstDecisoes': ['_id', 'idx']
                }.get(table_name)

                if pk_columns:
                    # Filtra as colunas que não são chave primária
                    update_column_names = [name for name in sql_names if name not in pk_columns]

                    if update_column_names:
                        # Se há colunas para atualizar, cria o UPDATE
                        update_columns_sql = sql.SQL(', ').join(
                            sql.SQL('{}=EXCLUDED.{}').format(sql.Identifier(name), sql.Identifier(name))
                            for name in update_column_names
                        )
                        on_conflict_sql = sql.SQL("DO UPDATE SET {}").format(update_columns_sql)
                    else:
                        # Se não há colunas para atualizar, não faz nada no conflito
                        on_conflict_sql = sql.SQL("DO NOTHING")

                    insert_sql = sql.SQL(
                        "INSERT INTO {table} ({columns}) VALUES ({values_placeholders}) ON CONFLICT ({pk_columns}) {on_conflict}"
                    ).format(
                        table=sql.Identifier(table_name),
                        columns=sql.SQL(', ').join(map(sql.Identifier, sql_names)),
                        values_placeholders=values_placeholders,
                        pk_columns=sql.SQL(', ').join(map(sql.Identifier, pk_columns)),
                        on_conflict=on_conflict_sql
                    )
                else:
                    # Para tabelas sem chave primária definida, truncamos e inserimos
                    pg_cursor.execute(sql.SQL("TRUNCATE TABLE {table} RESTART IDENTITY").format(table=sql.Identifier(table_name)))
                    insert_sql = sql.SQL(
                        "INSERT INTO {table} ({columns}) VALUES ({values_placeholders})"
                    ).format(
                        table=sql.Identifier(table_name),
                        columns=sql.SQL(', ').join(map(sql.Identifier, sql_names)),
                        values_placeholders=values_placeholders
                    )

                pg_cursor.executemany(insert_sql, data_to_insert)
                print(f" [SUCESSO] Inseridas/Atualizadas {len(data_to_insert)} linhas em \"{table_name}\"")


        # 5. COMMIT (Finaliza a transação)
        pg_conn.commit()

    except Exception as e:
        print(f"\n[ERRO DURANTE ETL]: Falha no processamento. Fazendo ROLLBACK. Erro: {e}")
        if pg_conn: pg_conn.rollback()

    finally:
        if pg_conn: pg_conn.close()
        if mongo_client: mongo_client.close()

    end_time = time.time()
    print(f"\n[FIM ETL]: Duração: {end_time - start_time:.2f} segundos.")


# =================================================================
#                      PARTE 3: AGENDAMENTO (MIDDLEWARE)
# =================================================================

def start_scheduler():
    """Inicia o agendador para rodar o processo a cada 1 hora."""
    try:
        drdl_data = load_drdl(DRDL_FILE_PATH)
    except Exception as e:
        print(f"Não foi possível carregar o DRDL. O agendador não pode iniciar. Erro: {e}")
        return

    schema_ready = create_postgres_schema(drdl_data)
    if not schema_ready:
        print("Falha ao configurar o esquema PG. Encerrando o serviço de middleware.")
        return

    scheduler = BlockingScheduler()
    scheduler.add_job(
        run_etl_process,
        'interval',
        minutes=60,
        args=[drdl_data],
        start_date=datetime.now()
    )

    print("\n------------------------------------------------------------")
    print("✅ MIDDLEWARE ATIVO. Pressione Ctrl+C para encerrar.")
    print(f"-> A primeira execução do ETL ocorrerá agora.")
    print("-> Próximas execuções agendadas a cada 1 hora.")
    print("------------------------------------------------------------\n")

    try:
        # Roda o ETL uma vez imediatamente ao iniciar
        run_etl_process(drdl_data)
        # Inicia o agendador para as próximas execuções
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("\nEncerrando o middleware...")
        scheduler.shutdown()

if __name__ == "__main__":
    start_scheduler()
