# ArbovirusFramework/transformations.py
import pandas as pd
from typing import Union, List, Any, Callable, Dict
from datetime import datetime

from .core import ArbovirusDataFrame
from .exceptions import ColumnNotFoundError, InvalidTransformationError

def drop_missing_values(data_frame: ArbovirusDataFrame, columns: Union[str, List[str]] = None, how: str = 'any') -> ArbovirusDataFrame:
    """
    Remove linhas com valores ausentes.

    Args:
        data_frame (ArbovirusDataFrame): A instância do ArbovirusDataFrame.
        columns (Union[str, List[str]], optional): Colunas a serem consideradas para remoção.
                                                   Se None, considera todas as colunas. Padrão para None.
        how (str): 'any' (remove se houver QUALQUER NaN) ou 'all' (remove se TODOS os valores forem NaN). Padrão para 'any'.

    Returns:
        ArbovirusDataFrame: Um novo ArbovirusDataFrame com as linhas contendo valores ausentes removidas.

    Raises:
        ColumnNotFoundError: Se alguma das colunas especificadas não existir.
        ValueError: Se a estratégia 'how' for inválida.
    """
    df = data_frame.get_dataframe()
    if columns:
        if isinstance(columns, str):
            columns = [columns]
        missing_columns = [col for col in columns if col not in df.columns]
        if missing_columns:
            raise ColumnNotFoundError(f"Colunas não encontradas para remoção de valores ausentes: {', '.join(missing_columns)}")
    
    if how not in ['any', 'all']:
        raise ValueError("O argumento 'how' deve ser 'any' ou 'all'.")

    df = df.dropna(subset=columns, how=how)
    return ArbovirusDataFrame(df)

def fill_missing_values(data_frame: ArbovirusDataFrame, strategy: str = 'mean',
                        columns: Union[str, List[str]] = None, value: Any = None,
                        limit: int = None, method: str = None) -> ArbovirusDataFrame:
    """
    Preenche valores ausentes usando uma estratégia ou valor especificado.

    Args:
        data_frame (ArbovirusDataFrame): A instância do ArbovirusDataFrame.
        strategy (str): A estratégia a ser usada ('mean', 'median', 'mode', 'ffill', 'bfill', 'value'). Padrão para 'mean'.
        columns (Union[str, List[str]], optional): Colunas para aplicar o preenchimento. Se None, aplica a todas as colunas adequadas.
        value (Any, optional): O valor a ser usado se a estratégia for 'value'. Padrão para None.
        limit (int, optional): O número máximo de preenchimentos consecutivos para ffill/bfill.
        method (str, optional): O método para ffill/bfill (ex: 'pad', 'bfill').

    Returns:
        ArbovirusDataFrame: Um novo ArbovirusDataFrame com valores ausentes preenchidos.

    Raises:
        ValueError: Se uma estratégia não suportada for fornecida ou 'value' for None quando necessário.
        ColumnNotFoundError: Se alguma das colunas especificadas não existir.
        InvalidTransformationError: Para outros erros durante a transformação.
    """
    df = data_frame.get_dataframe()
    if columns:
        if isinstance(columns, str):
            columns = [columns]
        missing_columns = [col for col in columns if col not in df.columns]
        if missing_columns:
            raise ColumnNotFoundError(f"Colunas não encontradas para preenchimento de valores ausentes: {', '.join(missing_columns)}")
    else:
        if strategy in ['mean', 'median']:
            columns = df.select_dtypes(include=['number']).columns.tolist()
        elif strategy in ['mode', 'ffill', 'bfill', 'value']:
            columns = df.columns.tolist()

    if not columns:
        return ArbovirusDataFrame(df)

    for col in columns:
        try:
            if strategy == 'mean':
                if pd.api.types.is_numeric_dtype(df[col]):
                    df[col] = df[col].fillna(df[col].mean())
            elif strategy == 'median':
                if pd.api.types.is_numeric_dtype(df[col]):
                    df[col] = df[col].fillna(df[col].median())
            elif strategy == 'mode':
                if not df[col].mode().empty:
                    df[col] = df[col].fillna(df[col].mode()[0])
            elif strategy == 'ffill':
                df[col] = df[col].fillna(method='ffill', limit=limit)
            elif strategy == 'bfill':
                df[col] = df[col].fillna(method='bfill', limit=limit)
            elif strategy == 'value':
                if value is None:
                    raise ValueError("Um valor deve ser fornecido quando a estratégia for 'value'.")
                df[col] = df[col].fillna(value)
            else:
                raise ValueError(f"Estratégia de preenchimento não suportada: {strategy}")
        except Exception as e:
            raise InvalidTransformationError(f"Erro ao preencher valores ausentes na coluna '{col}' com estratégia '{strategy}': {e}")
    return ArbovirusDataFrame(df)

def drop_duplicates(data_frame: ArbovirusDataFrame, subset: Union[str, List[str]] = None, keep: str = 'first') -> ArbovirusDataFrame:
    """
    Remove linhas duplicadas do DataFrame.

    Args:
        data_frame (ArbovirusDataFrame): A instância do ArbovirusDataFrame.
        subset (Union[str, List[str]], optional): Colunas a serem consideradas para identificar duplicatas.
                                                  Se None, considera todas as colunas. Padrão para None.
        keep (str): Quais duplicatas manter ('first' - primeira ocorrência, 'last' - última ocorrência, False - remove todas as duplicatas). Padrão para 'first'.

    Returns:
        ArbovirusDataFrame: Um novo ArbovirusDataFrame com as linhas duplicadas removidas.

    Raises:
        ColumnNotFoundError: Se alguma das colunas especificadas no subset não existir.
    """
    df = data_frame.get_dataframe()
    if subset:
        if isinstance(subset, str):
            subset = [subset]
        missing_columns = [col for col in subset if col not in df.columns]
        if missing_columns:
            raise ColumnNotFoundError(f"Colunas não encontradas para remoção de duplicatas: {', '.join(missing_columns)}")
    
    if keep not in ['first', 'last', False]:
        raise ValueError("O argumento 'keep' deve ser 'first', 'last' ou False.")

    df = df.drop_duplicates(subset=subset, keep=keep)
    return ArbovirusDataFrame(df)

def apply_function_to_column(data_frame: ArbovirusDataFrame, column: str, func: Callable[[Any], Any]) -> ArbovirusDataFrame:
    """
    Aplica uma função a uma coluna especificada.

    Args:
        data_frame (ArbovirusDataFrame): A instância do ArbovirusDataFrame.
        column (str): O nome da coluna à qual aplicar a função.
        func (Callable[[Any], Any]): A função a ser aplicada. Deve aceitar um valor da coluna
                                    e retornar o valor transformado.

    Returns:
        ArbovirusDataFrame: Um novo ArbovirusDataFrame com a função aplicada à coluna.

    Raises:
        ColumnNotFoundError: Se a coluna especificada não existir.
        InvalidTransformationError: Se ocorrer um erro ao aplicar a função.
    """
    df = data_frame.get_dataframe()
    if column not in df.columns:
        raise ColumnNotFoundError(f"Coluna '{column}' não encontrada.")
    try:
        df[column] = df[column].apply(func)
    except Exception as e:
        raise InvalidTransformationError(f"Erro ao aplicar função na coluna '{column}': {e}")
    return ArbovirusDataFrame(df)

def create_new_column(data_frame: ArbovirusDataFrame, new_column_name: str, func: Callable[[pd.DataFrame], pd.Series]) -> ArbovirusDataFrame:
    """
    Cria uma nova coluna com base em uma função aplicada ao DataFrame.

    Args:
        data_frame (ArbovirusDataFrame): A instância do ArbovirusDataFrame.
        new_column_name (str): O nome da nova coluna a ser criada.
        func (Callable[[pd.DataFrame], pd.Series]): Uma função que recebe o DataFrame
                                                    e retorna uma Series do pandas para a nova coluna.
                                                    Exemplo: lambda df: df['ColA'] + df['ColB']

    Returns:
        ArbovirusDataFrame: Um novo ArbovirusDataFrame com a nova coluna adicionada.

    Raises:
        ValueError: Se o nome da nova coluna já existir.
        InvalidTransformationError: Se ocorrer um erro ao criar a nova coluna.
    """
    df = data_frame.get_dataframe()
    if new_column_name in df.columns:
        raise ValueError(f"A coluna '{new_column_name}' já existe. Escolha outro nome ou use apply_function_to_column para modificar.")
    try:
        new_series = func(df)
        if not isinstance(new_series, pd.Series):
            raise TypeError("A função para criar a nova coluna deve retornar uma pandas.Series.")
        df[new_column_name] = new_series
    except KeyError as e:
        raise ColumnNotFoundError(f"Erro ao criar nova coluna. Coluna usada na função não encontrada: {e}")
    except Exception as e:
        raise InvalidTransformationError(f"Erro ao criar a nova coluna '{new_column_name}': {e}")
    return ArbovirusDataFrame(df)

def rename_columns(data_frame: ArbovirusDataFrame, column_mapping: Dict[str, str]) -> ArbovirusDataFrame:
    """
    Renomeia colunas do DataFrame.

    Args:
        data_frame (ArbovirusDataFrame): A instância do ArbovirusDataFrame.
        column_mapping (Dict[str, str]): Um dicionário onde as chaves são os nomes das colunas atuais
                                         e os valores são os novos nomes.

    Returns:
        ArbovirusDataFrame: Um novo ArbovirusDataFrame com as colunas renomeadas.

    Raises:
        ColumnNotFoundError: Se alguma coluna a ser renomeada não for encontrada.
        InvalidTransformationError: Para outros erros durante a renomeação.
    """
    df = data_frame.get_dataframe()
    missing_columns = [old_name for old_name in column_mapping.keys() if old_name not in df.columns]
    if missing_columns:
        raise ColumnNotFoundError(f"Colunas a serem renomeadas não encontradas: {', '.join(missing_columns)}")
    try:
        df = df.rename(columns=column_mapping)
    except Exception as e:
        raise InvalidTransformationError(f"Erro ao renomear colunas: {e}")
    return ArbovirusDataFrame(df)

def calculate_rolling_mean(data_frame: ArbovirusDataFrame, column: str, window: int, new_column_suffix: str = '_media') -> ArbovirusDataFrame:
    """Calcula a média móvel para uma coluna específica e adiciona como nova coluna."""
    df = data_frame.get_dataframe()
    if column not in df.columns:
        raise ColumnNotFoundError(f"Coluna '{column}' não encontrada para calcular média móvel.")
    try:
        new_col_name = f"{column}{new_column_suffix}_{window}d"
        df[new_col_name] = df[column].rolling(window=window, min_periods=1).mean()
    except Exception as e:
        raise InvalidTransformationError(f"Erro ao calcular média móvel para coluna '{column}': {e}")
    return ArbovirusDataFrame(df)

def calculate_rolling_sum(data_frame: ArbovirusDataFrame, column: str, window: int, new_column_suffix: str = '_soma') -> ArbovirusDataFrame:
    """Calcula a soma móvel para uma coluna específica e adiciona como nova coluna."""
    df = data_frame.get_dataframe()
    if column not in df.columns:
        raise ColumnNotFoundError(f"Coluna '{column}' não encontrada para calcular soma móvel.")
    try:
        new_col_name = f"{column}{new_column_suffix}_{window}d"
        df[new_col_name] = df[column].rolling(window=window, min_periods=1).sum()
    except Exception as e:
        raise InvalidTransformationError(f"Erro ao calcular soma móvel para coluna '{column}': {e}")
    return ArbovirusDataFrame(df)

def shift_cases(data_frame: ArbovirusDataFrame, date_column: str, cases_column: str, days: int) -> ArbovirusDataFrame:
    """
    Adia (shifts) a quantidade de casos em um número específico de dias.
    A data_column deve ser do tipo datetime.
    """
    df = data_frame.get_dataframe()
    if date_column not in df.columns:
        raise ColumnNotFoundError(f"Coluna de data '{date_column}' não encontrada para adiantar casos.")
    if cases_column not in df.columns:
        raise ColumnNotFoundError(f"Coluna de casos '{cases_column}' não encontrada para adiantar casos.")

    if not pd.api.types.is_datetime64_any_dtype(df[date_column]):
        raise InvalidTransformationError(f"A coluna '{date_column}' deve ser do tipo datetime para adiantar casos.")

    # Garante que o DataFrame esteja ordenado por data para o shift funcionar corretamente
    df_sorted = df.sort_values(by=date_column).copy()
    
    # Criar uma cópia para evitar SettingWithCopyWarning
    df_temp = df_sorted[[date_column, cases_column]].copy()
    df_temp.set_index(date_column, inplace=True)
    
    # Shift com frequência 'D' para deslocar por dias
    shifted_series = df_temp[cases_column].shift(periods=-days, freq='D')
    
    # Adicionar a coluna ao DataFrame original, alinhando pelos índices de data
    new_column_name = f"{cases_column}_{abs(days)}dias" if days != 0 else cases_column
    df_sorted[new_column_name] = shifted_series.reset_index(drop=True) # Resetar index para alinhar por posição

    # Preenche NaNs resultantes do shift no final com os valores originais.
    # Isso é feito após o shift e antes de retornar.
    df_sorted[new_column_name] = df_sorted[new_column_name].fillna(df_sorted[cases_column])

    return ArbovirusDataFrame(df_sorted)

def identify_season(data: datetime) -> Union[str, None]:
    """
    Identifica a estação do ano no hemisfério sul com base na data.
    """
    if pd.isna(data):
        return None
    
    # Datas de início das estações para o hemisfério sul (aproximadas)
    outono_inicio = datetime(data.year, 3, 20)
    inverno_inicio = datetime(data.year, 6, 21)
    primavera_inicio = datetime(data.year, 9, 22)
    verao_inicio = datetime(data.year, 12, 21)
    
    if data >= verao_inicio or data < outono_inicio:
        return "Verão"
    elif data >= outono_inicio and data < inverno_inicio:
        return "Outono"
    elif data >= inverno_inicio and data < primavera_inicio:
        return "Inverno"
    elif data >= primavera_inicio and data < verao_inicio:
        return "Primavera"
    else:
        return None

def add_season_column(data_frame: ArbovirusDataFrame, date_column: str = 'dt_notific') -> ArbovirusDataFrame:
    """
    Adiciona a coluna 'estacao' ao DataFrame com base na coluna de data fornecida.
    Assume que a coluna de data já é do tipo datetime.
    """
    df = data_frame.get_dataframe()
    if date_column not in df.columns:
        raise ColumnNotFoundError(f"Coluna '{date_column}' não encontrada para adicionar estação.")
    
    if not pd.api.types.is_datetime64_any_dtype(df[date_column]):
        raise InvalidTransformationError(f"A coluna '{date_column}' deve ser do tipo datetime para identificar a estação.")

    try:
        df['estacao'] = df[date_column].apply(identify_season)
    except Exception as e:
        raise InvalidTransformationError(f"Erro ao adicionar coluna 'estacao': {e}")
    return ArbovirusDataFrame(df)