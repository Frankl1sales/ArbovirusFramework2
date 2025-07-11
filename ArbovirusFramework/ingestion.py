# ArbovirusFramework/ingestion.py
import pandas as pd
import os
from pathlib import Path
from typing import List, Dict, Union, Any

from .core import ArbovirusDataFrame
from .exceptions import FileNotFoundError, InvalidCSVError, ColumnNotFoundError, DataProcessingError
from . import transformations

def display_csv_columns(file_path: Union[str, Path]) -> None:
    """
    Carrega um arquivo CSV e imprime o nome de todas as suas colunas.
    Útil para verificar o conteúdo e o cabeçalho dos arquivos.
    """
    path = Path(file_path)
    if not path.is_file():
        print(f"⚠️ Erro: Arquivo não encontrado em '{file_path}' para exibição de colunas.")
        return
    try:
        df = pd.read_csv(path, low_memory=False)
        print(f"\nColunas do arquivo CSV: {path.name}")
        print(df.columns.tolist())
    except Exception as e:
        print(f"⚠️ Erro ao ler o arquivo '{file_path}' para exibir colunas: {e}")

def process_raw_climate_data(city_folder: str, raw_climate_file_names: List[str],
                             col_mapping_dict: Dict[str, str], base_path: Union[str, Path]) -> List[str]:
    """
    Processa os arquivos CSV climáticos brutos, renomeia colunas, arredonda e salva em uma pasta 'processed_climate'.
    Arquivos existentes serão SOBRESCRITOS.
    Retorna uma lista dos caminhos dos arquivos processados.
    """
    base_path = Path(base_path)
    climate_data_path = base_path / city_folder / "dados_climaticos"
    processed_output_path = climate_data_path / "processed_climate" # Nova subpasta para processados
    processed_output_path.mkdir(parents=True, exist_ok=True) # Garante que a pasta exista

    print(f"Iniciando processamento de arquivos brutos de clima em: {climate_data_path}")
    
    processed_files_paths = []

    for file_name in raw_climate_file_names:
        full_raw_file_path = climate_data_path / file_name

        try:
            df = pd.read_csv(
                full_raw_file_path,
                sep=";",
                encoding="latin1",
                skiprows=10,
                decimal=".",
                na_values=["", ".", "NA"]
            )

            df = df.rename(columns=col_mapping_dict)
            
            # Garante que apenas as colunas mapeadas (e existentes) sejam mantidas
            df = df[[col for col in col_mapping_dict.values() if col in df.columns]]

            for col in df.select_dtypes(include=['number']).columns:
                df[col] = df[col].round(3)

            processed_file_path = processed_output_path / f"proc_{file_name}" # Nome do arquivo processado
            df.to_csv(processed_file_path, index=False, sep=";", encoding="utf-8")
            print(f"✅ Arquivo bruto de clima processado salvo: {processed_file_path}")
            processed_files_paths.append(str(processed_file_path))

        except FileNotFoundError:
            print(f"⚠️ Erro: Arquivo bruto de clima não encontrado em {full_raw_file_path}. Pulando...")
        except Exception as e:
            print(f"⚠️ Erro ao processar o arquivo bruto de clima {file_name}: {e}")
    
    return processed_files_paths

def aggregate_and_transform_climate_data(city_folder: str, base_path: Union[str, Path]) -> Union[ArbovirusDataFrame, None]:
    """
    Agrega arquivos climáticos processados, calcula médias/somas móveis e retorna um ArbovirusDataFrame.
    """
    base_path = Path(base_path)
    climate_data_path = base_path / city_folder / "dados_climaticos"
    processed_input_path = climate_data_path / "processed_climate" # Lendo da nova subpasta

    if not processed_input_path.is_dir():
        print(f"⚠️ Erro: Pasta de dados climáticos processados não encontrada para {city_folder}: {processed_input_path}")
        return None

    processed_files = [f for f in os.listdir(processed_input_path) if f.startswith("proc_") and f.endswith(".csv")]
    dfs = []

    for file_name in processed_files:
        full_file_path = processed_input_path / file_name
        try:
            df = pd.read_csv(full_file_path, sep=";", encoding="utf-8", on_bad_lines='skip')
            df.rename(columns={"data": "date"}, inplace=True) # Renomeia aqui para 'date' para padronização

            # Colunas desejadas para garantir que o DataFrame final contenha apenas elas
            desired_cols_base = ["date", "precipitacao", "ponto_orvalho", "temp_media", "temp_max", "temp_min", "umidade"]
            df = df[[col for col in desired_cols_base if col in df.columns]]
            dfs.append(df)
        except Exception as e:
            print(f"⚠️ Erro ao carregar arquivo processado de clima {file_name}: {e}")

    if not dfs:
        print(f"⚠️ Nenhum dado válido encontrado na pasta processados de clima para {city_folder}.")
        return None

    df_consolidated = pd.concat(dfs, ignore_index=True)
    df_consolidated['date'] = pd.to_datetime(df_consolidated['date'], errors='coerce')
    df_consolidated.dropna(subset=['date'], inplace=True) # Remove linhas com datas inválidas
    df_consolidated.sort_values(by="date", inplace=True)
    df_consolidated.drop_duplicates(subset="date", keep="first", inplace=True)

    # Convertendo para numérico antes de calcular médias/somas móveis
    numeric_cols = ["precipitacao", "ponto_orvalho", "temp_media", "temp_max", "temp_min", "umidade"]
    for col in numeric_cols:
        if col in df_consolidated.columns:
            df_consolidated[col] = pd.to_numeric(df_consolidated[col], errors='coerce')

    # Aplicando as transformações de médias/somas móveis
    arbovirus_df = ArbovirusDataFrame.from_dataframe(df_consolidated)

    # Chamando as funções do módulo transformations
    if 'precipitacao' in arbovirus_df.columns:
        for window in [5, 10, 15]:
            arbovirus_df = transformations.calculate_rolling_sum(arbovirus_df, "precipitacao", window)

    for col in ["ponto_orvalho", "temp_media", "temp_max", "temp_min", "umidade"]:
        if col in arbovirus_df.columns:
            for window in [5, 10, 15]:
                arbovirus_df = transformations.calculate_rolling_mean(arbovirus_df, col, window)

    # Arredondar as colunas numéricas
    current_df = arbovirus_df.get_dataframe()
    for col in current_df.select_dtypes(include=['number']).columns:
        current_df[col] = current_df[col].round(3)
    
    return ArbovirusDataFrame.from_dataframe(current_df)


def process_epidemiological_data(city_config: Dict[str, Any], base_path: Union[str, Path]) -> Union[ArbovirusDataFrame, None]:
    """
    Processa os dados epidemiológicos brutos de uma cidade específica.
    1. Filtra por ID do município e adiciona o nome do município.
    2. Conta os casos por dia e por município.
    Retorna um ArbovirusDataFrame com a contagem de casos.
    """
    city_folder = city_config['folder_name']
    municipio_id = city_config['id_municipio']
    municipio_name = city_config['municipio_name']
    raw_epi_filename = city_config['raw_epi_filename']

    print(f"\nIniciando processamento de dados epidemiológicos para {municipio_name}...")

    base_path = Path(base_path)
    raw_epi_path = base_path / city_folder / "dados_epidemiologicos" / raw_epi_filename
    processed_epi_output_path = base_path / city_folder / "dados_epidemiologicos" / "processed_epidemiological"
    processed_epi_output_path.mkdir(parents=True, exist_ok=True)


    try:
        df_raw = pd.read_csv(raw_epi_path, delimiter=';', low_memory=False)
        
        # Manter apenas as colunas necessárias ANTES de filtrar para evitar SettingWithCopyWarning
        df_filtered = df_raw[['dt_notific', 'id_municip']].copy()
        df_filtered = df_filtered[df_filtered['id_municip'] == municipio_id]
        
        # Adicionar coluna 'mun'
        df_filtered['mun'] = municipio_name
        
        # Converter dt_notific para datetime ANTES de agrupar
        df_filtered['dt_notific'] = pd.to_datetime(df_filtered['dt_notific'], errors='coerce')
        df_filtered.dropna(subset=['dt_notific'], inplace=True) # Remover linhas com datas inválidas
        
        # Contar casos por data e município
        df_grouped = df_filtered.groupby(['dt_notific', 'mun']).size().reset_index(name='quantidade_de_casos')
        
        print(f"✅ Processamento epidemiológico concluído para {municipio_name}.")
        return ArbovirusDataFrame.from_dataframe(df_grouped)
    
    except FileNotFoundError:
        print(f"⚠️ Erro: Arquivo epidemiológico bruto não encontrado em '{raw_epi_path}'. Pulando processamento epidemiológico.")
        return None
    except Exception as e:
        raise DataProcessingError(f"Erro durante o processamento epidemiológico para {municipio_name}: {e}")