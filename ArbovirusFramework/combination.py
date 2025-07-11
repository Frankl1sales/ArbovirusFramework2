# ArbovirusFramework/combination.py
import pandas as pd
from pathlib import Path
from typing import Dict, Any, Union

from .core import ArbovirusDataFrame
from .exceptions import FileNotFoundError, DataProcessingError, ColumnNotFoundError, InvalidTransformationError
from . import transformations # Importa o módulo de transformações para usar shift_cases e add_season_column

def combine_climate_and_epidemiological_data(
    climate_df: ArbovirusDataFrame, 
    cases_df: ArbovirusDataFrame, 
    city_config: Dict[str, Any], 
    base_path: Union[str, Path]
) -> Union[ArbovirusDataFrame, None]:
    """
    Combina os dados climáticos (já processados com features) e os dados epidemiológicos (contagem de casos).
    Adiciona colunas de casos adiantados, adiciona a estação do ano, e salva o arquivo combinado.

    Args:
        climate_df (ArbovirusDataFrame): DataFrame climático já processado com features.
        cases_df (ArbovirusDataFrame): DataFrame com a contagem de casos epidemiológicos.
        city_config (Dict[str, Any]): Dicionário de configuração da cidade.
        base_path (Union[str, Path]): Caminho base do projeto.

    Returns:
        ArbovirusDataFrame: O DataFrame combinado.

    Raises:
        DataProcessingError: Se ocorrer um erro durante a combinação.
    """
    city_folder = city_config['folder_name']
    municipio_name = city_config['municipio_name']
    
    print(f"\nIniciando combinação de dados climáticos e epidemiológicos para {municipio_name}...")

    base_path = Path(base_path)
    output_combined_path = base_path / city_folder / "dados_combinados" # Nova pasta para saída combinada
    output_combined_path.mkdir(parents=True, exist_ok=True)
    output_combined_file_path = output_combined_path / f"dados_combinados_{city_folder}.csv"

    try:
        df_climate_pd = climate_df.get_dataframe()
        df_cases_pd = cases_df.get_dataframe()

        # Garante que as colunas de data tenham o mesmo nome e tipo para a junção
        if 'dt_notific' not in df_cases_pd.columns:
            raise ColumnNotFoundError("Coluna 'dt_notific' não encontrada no DataFrame de casos.")
        if 'date' not in df_climate_pd.columns:
            raise ColumnNotFoundError("Coluna 'date' não encontrada no DataFrame climático.")

        # Garante que as colunas de data sejam datetime
        df_cases_pd['dt_notific'] = pd.to_datetime(df_cases_pd['dt_notific'], errors='coerce')
        df_climate_pd['date'] = pd.to_datetime(df_climate_pd['date'], errors='coerce')
        
        # Renomeia a coluna de data do clima para corresponder à coluna de data dos casos para a junção
        df_climate_pd.rename(columns={'date': 'dt_notific'}, inplace=True)

        # Filtra casos para o município específico (já deveria estar filtrado pelo ingestion, mas garante)
        df_cases_municipio = df_cases_pd[df_cases_pd['mun'] == municipio_name].copy()
        
        df_final_combined_pd = pd.merge(
            df_climate_pd,
            df_cases_municipio[['dt_notific', 'quantidade_de_casos']],
            on='dt_notific',
            how='left'
        )

        # Preenche casos ausentes com 0 e converte para int
        df_final_combined_pd['quantidade_de_casos'] = df_final_combined_pd['quantidade_de_casos'].fillna(0).astype(int)
        df_final_combined_pd.sort_values(by='dt_notific', inplace=True)
        
        # Converte de volta para ArbovirusDataFrame para aplicar transformações
        df_final_combined = ArbovirusDataFrame.from_dataframe(df_final_combined_pd)

        print("Adicionando colunas de casos adiantados (7, 14, 21 dias)...")
        df_final_combined = transformations.shift_cases(df_final_combined, 'dt_notific', 'quantidade_de_casos', 7)
        df_final_combined = transformations.shift_cases(df_final_combined, 'dt_notific', 'quantidade_de_casos', 14)
        df_final_combined = transformations.shift_cases(df_final_combined, 'dt_notific', 'quantidade_de_casos', 21)

        print("Adicionando coluna 'estacao'...")
        df_final_combined = transformations.add_season_column(df_final_combined, date_column='dt_notific')

        # Arredondar as colunas numéricas do DataFrame final
        current_df_pd = df_final_combined.get_dataframe()
        for col in current_df_pd.select_dtypes(include=['number']).columns:
            current_df_pd[col] = current_df_pd[col].round(3)
        df_final_combined = ArbovirusDataFrame.from_dataframe(current_df_pd)


        df_final_combined.save_to_csv(output_combined_file_path, index=False)
        print(f"✅ Arquivo combinado para {municipio_name} salvo com sucesso: {output_combined_file_path}")
        return df_final_combined

    except Exception as e:
        raise DataProcessingError(f"Erro ao combinar dados climáticos e de casos para {municipio_name}: {e}")