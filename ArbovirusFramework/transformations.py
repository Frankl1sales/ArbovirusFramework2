import pandas as pd
from typing import Union, List, Any, Callable, Dict

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
    df = data_frame.get_dataframe() # Obtém uma cópia do DF subjacente
    if columns:
        if isinstance(columns, str):
            columns = [columns]
        missing_columns = [col for col in columns if col not in df.columns]
        if missing_columns:
            raise ColumnNotFoundError(f"Colunas não encontradas para remoção de valores ausentes: {', '.join(missing_columns)}")
    
    if how not in ['any', 'all']:
        raise ValueError("O argumento 'how' deve ser 'any' ou 'all'.")

    df.dropna(subset=columns, how=how, inplace=True)
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
        # Se nenhuma coluna especificada, aplica a todas as colunas numéricas para 'mean'/'median'
        if strategy in ['mean', 'median']:
            columns = df.select_dtypes(include=['number']).columns.tolist()
        elif strategy in ['mode', 'ffill', 'bfill', 'value']:
            columns = df.columns.tolist() # Pode aplicar a qualquer tipo

    if not columns: # Se não houver colunas aplicáveis após a seleção
        return ArbovirusDataFrame(df) # Retorna o DataFrame original se não houver colunas para preencher

    for col in columns:
        try:
            if strategy == 'mean':
                if pd.api.types.is_numeric_dtype(df[col]):
                    df[col].fillna(df[col].mean(), inplace=True)
            elif strategy == 'median':
                if pd.api.types.is_numeric_dtype(df[col]):
                    df[col].fillna(df[col].median(), inplace=True)
            elif strategy == 'mode':
                # Mode pode retornar múltiplos valores, pegue o primeiro se houver
                if not df[col].mode().empty:
                    df[col].fillna(df[col].mode()[0], inplace=True)
            elif strategy == 'ffill':
                df[col].fillna(method='ffill', limit=limit, inplace=True)
            elif strategy == 'bfill':
                df[col].fillna(method='bfill', limit=limit, inplace=True)
            elif strategy == 'value':
                if value is None:
                    raise ValueError("Um valor deve ser fornecido quando a estratégia for 'value'.")
                df[col].fillna(value, inplace=True)
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

    df.drop_duplicates(subset=subset, keep=keep, inplace=True)
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
        df.rename(columns=column_mapping, inplace=True)
    except Exception as e:
        raise InvalidTransformationError(f"Erro ao renomear colunas: {e}")
    return ArbovirusDataFrame(df)