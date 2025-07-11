# main.py
import os
import pandas as pd
from pathlib import Path

# Importe os módulos do seu ArbovirusFramework
from ArbovirusFramework import ArbovirusDataFrame, ingestion, transformations, combination, exceptions

# --- Configurações Globais ---
# A PASTA_RAIZ_PROJETO é onde o script está sendo executado.
PASTA_RAIZ_PROJETO = Path(os.getcwd())

# Dicionário para renomear colunas de dados climáticos brutos
COLUNAS_CLIMA_ORIGINAIS_PARA_SIMPLIFICAR = {
    "Data Medicao": "data",
    "PRECIPITACAO TOTAL, DIARIO (AUT)(mm)": "precipitacao",
    "TEMPERATURA DO PONTO DE ORVALHO MEDIA DIARIA (AUT)(Â°C)": "ponto_orvalho",
    "TEMPERATURA MEDIA, DIARIA (AUT)(Â°C)": "temp_media",
    "UMIDADE RELATIVA DO AR, MEDIA DIARIA (AUT)(%)": "umidade",
    "TEMPERATURA MAXIMA, DIARIA (AUT)(Â°C)": "temp_max",
    "TEMPERATURA MINIMA, DIARIA (AUT)(Â°C)": "temp_min"
}

def main(cities_config):
    """
    Função principal que orquestra todo o processo de dados para múltiplas cidades
    usando o ArbovirusFramework.

    Args:
        cities_config (list of dict): Uma lista de dicionários, onde cada dicionário
                                      contém a configuração para uma cidade.
    """
    print(f"Iniciando o processo unificado de dados na pasta base: {PASTA_RAIZ_PROJETO}")

    for city_config in cities_config:
        municipio_name = city_config['municipio_name']
        city_folder = city_config['folder_name']
        raw_climate_filenames = city_config['raw_climate_filenames']

        print(f"\n--- Processando dados para a cidade: {municipio_name} (pasta: {city_folder}) ---")

        # --- Etapa 1: Processamento de Dados Climáticos Brutos ---
        print(f"Iniciando ingestão de arquivos brutos de clima para {city_folder}...")
        try:
            # Esta função agora retorna os caminhos dos arquivos processados, mas não é estritamente necessário aqui
            # para o fluxo subsequente, pois aggregate_and_transform_climate_data irá lê-los diretamente.
            ingestion.process_raw_climate_data(
                city_folder=city_folder,
                raw_climate_file_names=raw_climate_filenames,
                col_mapping_dict=COLUNAS_CLIMA_ORIGINAIS_PARA_SIMPLIFICAR,
                base_path=PASTA_RAIZ_PROJETO
            )
            print(f"Ingestão de arquivos brutos de clima para {city_folder} concluída.")

            # Agregação e transformação de dados climáticos
            climate_features_df = ingestion.aggregate_and_transform_climate_data(
                city_folder=city_folder,
                base_path=PASTA_RAIZ_PROJETO
            )
            if climate_features_df is None:
                print(f"⚠️ Pulando a combinação para {municipio_name} pois os dados climáticos processados não foram gerados ou estão vazios.")
                continue

        except exceptions.ArbovirusFrameworkError as e:
            print(f"⚠️ Erro na etapa de dados climáticos para {municipio_name}: {e}")
            continue # Pula para a próxima cidade se houver erro crítico no clima
        except Exception as e:
            print(f"⚠️ Erro inesperado na etapa de dados climáticos para {municipio_name}: {e}")
            continue

        # --- Etapa 2: Processamento de Dados Epidemiológicos ---
        print(f"Iniciando ingestão de dados epidemiológicos para {municipio_name}...")
        cases_df = None
        try:
            cases_df = ingestion.process_epidemiological_data(
                city_config=city_config,
                base_path=PASTA_RAIZ_PROJETO
            )
            if cases_df is None:
                print(f"⚠️ Pulando a combinação para {municipio_name} pois os dados epidemiológicos processados não foram gerados ou estão vazios.")
                continue

        except exceptions.ArbovirusFrameworkError as e:
            print(f"⚠️ Erro na etapa de dados epidemiológicos para {municipio_name}: {e}")
            continue
        except Exception as e:
            print(f"⚠️ Erro inesperado na etapa de dados epidemiológicos para {municipio_name}: {e}")
            continue

        # --- Etapa 3: Combinação de Dados Climáticos e Epidemiológicos ---
        print(f"Iniciando combinação de dados para {municipio_name}...")
        try:
            combined_df = combination.combine_climate_and_epidemiological_data(
                climate_df=climate_features_df,
                cases_df=cases_df,
                city_config=city_config,
                base_path=PASTA_RAIZ_PROJETO
            )
            if combined_df is None:
                print(f"⚠️ Não foi possível gerar o arquivo combinado para {municipio_name}.")

        except exceptions.ArbovirusFrameworkError as e:
            print(f"⚠️ Erro na etapa de combinação para {municipio_name}: {e}")
        except Exception as e:
            print(f"⚠️ Erro inesperado na etapa de combinação para {municipio_name}: {e}")

    print("\nProcessamento unificado concluído para todas as cidades configuradas.")


if __name__ == "__main__":
    # --- Configuração das Cidades a serem Processadas (Passada pelo Programador) ---
    CITIES_TO_PROCESS = [
        {
            'id_municipio': 310620,
            'municipio_name': 'Belo Horizonte',
            'folder_name': 'belo_horizonte',
            'raw_epi_filename': 'dengue_20_24_MG_reduzido.csv',
            'raw_climate_filenames': ['dados_A521_D_2020-01-01_2024-12-31.csv']
        },
        # Adicione outras cidades aqui, seguindo o mesmo formato:
        # {
        #     'id_municipio': 4314407, # Exemplo de ID para Porto Alegre
        #     'municipio_name': 'Porto Alegre',
        #     'folder_name': 'porto_alegre',
        #     'raw_epi_filename': 'dengue_exemplo_POA_reduzido.csv', # Nome do arquivo bruto de dengue para POA
        #     'raw_climate_filenames': ['dados_estacao_POA_2020-2024.csv'] # Lista de arquivos brutos de clima para POA
        # },
    ]

    # Chama a função principal passando a configuração das cidades
    main(CITIES_TO_PROCESS)