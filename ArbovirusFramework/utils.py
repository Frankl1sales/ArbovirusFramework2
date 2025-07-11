# ArbovirusFramework/utils.py
import pandas as pd
import os
from pathlib import Path
from typing import Union

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