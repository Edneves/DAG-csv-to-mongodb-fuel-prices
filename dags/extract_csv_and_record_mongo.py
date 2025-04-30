from __future__ import annotations
import pandas as pd
import pendulum, yaml, os
from airflow.sdk import dag, task
from pymongo import MongoClient

path_dag = os.path.dirname(__file__)
# Caminho até o config.yml
path_config = os.path.join(path_dag, '..', 'Config', 'config.yml')
# Caminho absoluto normalizado
path_yaml = os.path.abspath(path_config)

#path_yaml = "/codigo/config.yml"

@dag(
    schedule=None,
    start_date=pendulum.datetime(2025, 4, 28, tz="UTC"),
    catchup=False,
    tags=["ETL"]
)
def extract_csv_and_record_mongodb(path_yaml: str):
    @task
    def read_config_yml(path_yaml: str) -> dict:

        with open(path_yaml, 'r') as file:
            config = yaml.safe_load(file)
        return config
    @task
    def read_data(config: dict) -> pd.DataFrame:

        try:
            path = config.get('PATH')
            df = pd.read_csv(path,sep=';')
            print(f'Arquivo lido com sucesso: {path}')
            return df
        except Exception as e:
            print(f'Não foi possível ler o arquivo devido ao erro: {e}')
            return None
    @task(multiple_outputs=False)
    def transform_data(df: pd.DataFrame) -> dict:

        if not isinstance(df, pd.DataFrame):
            raise TypeError("O argumento df deve ser do tipo DataFrame!")

        try:
            df2 = pd.DataFrame(df, columns=['Estado - Sigla', 'Produto', 'Valor de Venda', 'Unidade de Medida'])
            df2['Valor de Venda'] = df2['Valor de Venda'].str.replace(',', '.')
            df2['Valor de Venda'] = pd.to_numeric(df2['Valor de Venda'], errors='coerce')

            agrupado = df2.groupby(['Estado - Sigla', 'Produto', 'Unidade de Medida']).agg(
                Menor_Valor=('Valor de Venda', 'min'),
                Maior_Valor=('Valor de Venda', 'max')
            ).reset_index()

            df2 = df2.drop_duplicates(subset=['Estado - Sigla', 'Produto','Unidade de Medida'])
            df_resultado = pd.merge(df2, agrupado, on=['Estado - Sigla', 'Produto', 'Unidade de Medida'], how='left')
            df_resultado = df_resultado.drop('Valor de Venda', axis=1)
            df_resultado['Diferanca'] = (df_resultado['Maior_Valor'].fillna(0) - df_resultado['Menor_Valor'].fillna(0)).round(2)
            print('Dados transformados com sucesso!')

            return df_resultado.to_dict(orient='records')
        except Exception as error:
            print(f'Erro apresentado ao transformar os dados! {error}')
            return None
    @task
    def record_data(df_resultado: dict, config: dict) -> None:
        try:
            user = config.get('USER')
            password = config.get('PASSWORD')
            port = config.get('PORT')
            colecao = config.get('COLECAO')
            db = config.get('DB')
            host = config.get('HOST')
            db_authentication = config.get('DB_AUTHENTICATION')

            uri = f'mongodb://{user}:{password}@{host}:{port}/{db}?authSource={db_authentication}'
            client = MongoClient(uri)
            database = client[db]
            print(f'Conexão estabelecida ao banco de dados: {database.name}')
        except Exception as error:
            print(f'Erro ao conectar no banco: {error}')

        if colecao in database.list_collection_names():
            try:
                print(f'A collection existe no banco, não será necessário criar: {colecao}')

                name_collection = database[colecao]
                documentos = name_collection.find()
                resultados_retornados = []

                for doc in documentos:
                    doc.pop('_id')
                    resultados_retornados.append(doc)

                for i, documento_retornado in enumerate(resultados_retornados):
                    if i < len(df_resultado):
                        print(f'\nFazendo a busca de correlações {i + 1}:')

                        if df_resultado[i] == documento_retornado:
                            print(f"O documento já existe no Mongo: {documento_retornado}")
                        else:
                            print(f"O documento possui diferença em relação ao que está gravado no Mongo: (Dataframe ->{df_resultado[i]})")

                            try:
                                for key, value in df_resultado[i].items():
                                    if key in documento_retornado:
                                        if value == documento_retornado[key]:
                                            print("########################################################################")
                                            print(f"({key}): Os valores correspondem ({value})")

                                        else:
                                            print("\nVALOR(RES) ABAIXO NÃO DERAM MATCH")
                                            print(f"({key}): Os valores não correspondem (Dataframe: {value}), (Mongo: {documento_retornado[key]})")
                                            print(f"Será necessário atualizar o documento na base: (Campo -> '{key}':{documento_retornado[key]}), com o novo valor (Campo -> '{key}':{value})")

                                            novo_valor = {
                                                '$set': {
                                                    f'{key}':float(value)
                                                }
                                            }

                                            read_doc = database[colecao].find_one(documento_retornado)
                                            print(f"Este documento na base será atualizado: {dict(read_doc)}")

                                            result = database[colecao].update_one(documento_retornado, novo_valor)
                                            print(f"Documento Atualizado: {result}")

                                    else:
                                        print(f"Campo não encontrado no Mongo: {key}")
                            except Exception as error:
                                print(f'136 -Erro ao cruzar as fontes: {error}')

                    else:
                        print(f"Documento não possui correspondência no Dataframe: {i + 1} -> {documento_retornado[i]}")
                        result = database[colecao].delete_one(documento_retornado[i])
                        print(f"Documento deletado na base: {result}")

                try:
                    if len(df_resultado) > len(resultados_retornados):
                        for i in range(len(resultados_retornados), len(df_resultado)):
                            print(f"\nRegistro {i + 1} do DataFrame não tem correspondência no MongoDB.")
                            result = database[colecao].insert_one(df_resultado[i])
                            print(f"Documento inserido na base: {result}")
                except Exception as error:
                    print(f'136 -Erro ao inserir o documento na collection: {error}')

            except Exception as error:
                print(f'139 - Erro ao inserir os dados na collection: {error}')
        else:
            try:
                database.create_collection(colecao)
                print(f'Collection criada com sucesso! {colecao}')
            except Exception as error:
                print(f'Erro ao criar a collection: {error}')

            try:
                database[colecao].insert_many(df_resultado)
                print(f'Dados inseridos na coleção: {colecao}')
            except Exception as error:
                print(f'Erro ao inserir o documento na collection: {error}')


    config = read_config_yml(path_yaml)
    df = read_data(config)
    result = transform_data(df)
    record_data(result, config)

extract_csv_and_record_mongodb(path_yaml)