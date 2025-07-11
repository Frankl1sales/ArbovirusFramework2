# ArbovirusFramework/core.py
import pandas as pd
from pathlib import Path
from typing import List, Any, Callable, Union

from .exceptions import FileNotFoundError, InvalidCSVError, ColumnNotFoundError, InvalidFileFormatError, InvalidTransformationError, ArbovirusFrameworkError

class ArbovirusDataFrame:
    """
    Um wrapper em torno de pandas.DataFrame para simplificar a manipulação
    de datasets, com foco em dados de arbovírus (embora genérico para CSVs).
    """
    def __init__(self, data: pd.DataFrame):
        if not isinstance(data, pd.DataFrame):
            raise TypeError("Os dados fornecidos devem ser uma instância de pandas.DataFrame.")
        self._df = data.copy() # Garante que a cópia seja independente

    @classmethod
    def from_csv(cls, file_path: Union[str, Path], **kwargs) -> 'ArbovirusDataFrame':
        """
        Carrega um arquivo CSV em um ArbovirusDataFrame.

        Args:
            file_path (Union[str, Path]): O caminho para o arquivo CSV.
            **kwargs: Argumentos de palavra-chave adicionais para passar para pandas.read_csv.

        Returns:
            ArbovirusDataFrame: Uma instância de ArbovirusDataFrame contendo os dados carregados.

        Raises:
            FileNotFoundError: Se o arquivo não existir.
            InvalidFileFormatError: Se o arquivo não tiver a extensão .csv.
            InvalidCSVError: Se o arquivo CSV estiver malformado ou ilegível.
        """
        path = Path(file_path)

        if not path.is_file():
            raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")

        if path.suffix.lower() != '.csv':
            raise InvalidFileFormatError(f"Formato de arquivo inválido. Esperado '.csv', mas encontrado '{path.suffix}'.")

        try:
            df = pd.read_csv(path, **kwargs)
            return cls(df)
        except pd.errors.EmptyDataError:
            raise InvalidCSVError(f"Arquivo CSV está vazio ou contém apenas cabeçalho: {file_path}")
        except pd.errors.ParserError as e:
            raise InvalidCSVError(f"Erro ao analisar o arquivo CSV. Pode estar malformado: {e} no arquivo: {file_path}")
        except Exception as e:
            raise InvalidCSVError(f"Ocorreu um erro inesperado ao ler o CSV: {e} no arquivo: {file_path}")

    @classmethod
    def from_dataframe(cls, df: pd.DataFrame) -> 'ArbovirusDataFrame':
        """
        Cria uma instância de ArbovirusDataFrame a partir de um pandas.DataFrame existente.

        Args:
            df (pd.DataFrame): O DataFrame do pandas a ser encapsulado.

        Returns:
            ArbovirusDataFrame: Uma nova instância de ArbovirusDataFrame.
        """
        return cls(df.copy()) # Garante que a cópia seja independente


    def head(self, n: int = 5) -> pd.DataFrame:
        """
        Retorna as primeiras n linhas do DataFrame subjacente.

        Args:
            n (int): Número de linhas a serem retornadas.

        Returns:
            pd.DataFrame: As primeiras n linhas.
        """
        return self._df.head(n)

    def tail(self, n: int = 5) -> pd.DataFrame:
        """
        Retorna as últimas n linhas do DataFrame subjacente.

        Args:
            n (int): Número de linhas a serem retornadas.

        Returns:
            pd.DataFrame: As últimas n linhas.
        """
        return self._df.tail(n)

    def info(self) -> None:
        """
        Imprime um resumo do DataFrame, incluindo tipos de dados e valores não nulos.
        """
        self._df.info()

    def describe(self) -> pd.DataFrame:
        """
        Gera estatísticas descritivas do DataFrame.

        Returns:
            pd.DataFrame: Estatísticas descritivas.
        """
        return self._df.describe()

    @property
    def columns(self) -> List[str]:
        """
        Retorna uma lista de nomes de colunas do DataFrame.

        Returns:
            List[str]: Lista de nomes de colunas.
        """
        return self._df.columns.tolist()

    @property
    def shape(self) -> tuple:
        """
        Retorna as dimensões do DataFrame (linhas, colunas).

        Returns:
            tuple: Uma tupla (número de linhas, número de colunas).
        """
        return self._df.shape

    def select_columns(self, columns: List[str]) -> 'ArbovirusDataFrame':
        """
        Seleciona um subconjunto de colunas.

        Args:
            columns (List[str]): Uma lista de nomes de colunas a serem selecionadas.

        Returns:
            ArbovirusDataFrame: Um novo ArbovirusDataFrame com apenas as colunas selecionadas.

        Raises:
            ColumnNotFoundError: Se alguma das colunas especificadas não existir.
        """
        missing_columns = [col for col in columns if col not in self._df.columns]
        if missing_columns:
            raise ColumnNotFoundError(f"Colunas não encontradas: {', '.join(missing_columns)}")
        return ArbovirusDataFrame(self._df[columns].copy())

    def filter_rows(self, condition: Callable[[pd.DataFrame], pd.Series]) -> 'ArbovirusDataFrame':
        """
        Filtra linhas com base em uma condição (função lambda ou callable).

        Args:
            condition (Callable[[pd.DataFrame], pd.Series]): Uma função que recebe o DataFrame
                                                            e retorna uma Series booleana.
                                                            Exemplo: lambda df: df['Age'] > 30

        Returns:
            ArbovirusDataFrame: Um novo ArbovirusDataFrame com as linhas filtradas.
        """
        try:
            filtered_df = self._df[condition(self._df)].copy()
        except KeyError as e:
            raise ColumnNotFoundError(f"Erro ao filtrar. Coluna usada na condição não encontrada: {e}")
        except Exception as e:
            raise InvalidTransformationError(f"Erro ao aplicar filtro: {e}")
        return ArbovirusDataFrame(filtered_df)

    def save_to_csv(self, file_path: Union[str, Path], **kwargs) -> None:
        """
        Salva o DataFrame atual em um arquivo CSV.

        Args:
            file_path (Union[str, Path]): O caminho para salvar o arquivo CSV.
            **kwargs: Argumentos de palavra-chave adicionais para passar para pandas.DataFrame.to_csv.
        """
        path = Path(file_path)
        path.parent.mkdir(parents=True, exist_ok=True) # Garante que o diretório pai exista
        try:
            self._df.to_csv(path, **kwargs)
        except Exception as e:
            raise ArbovirusFrameworkError(f"Erro ao salvar o arquivo CSV em {file_path}: {e}")

    def get_dataframe(self) -> pd.DataFrame:
        """
        Retorna o pandas DataFrame subjacente para manipulação direta, se necessário.

        Returns:
            pd.DataFrame: O DataFrame do Pandas.
        """
        return self._df.copy() # Retorna uma cópia para evitar modificações acidentais no objeto interno