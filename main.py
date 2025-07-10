# main.py
from ArbovirusFramework import ArbovirusDataFrame, transformations
from ArbovirusFramework.exceptions import FileNotFoundError, ColumnNotFoundError, InvalidCSVError
import os
import pandas as pd

# --- Criação de um CSV de exemplo para teste ---
csv_content = """Nome,Idade,Cidade,Salario,Doenca
Alice,30,New York,60000,Dengue
Bob,24,London,45000,Zika
Charlie,,Paris,70000,
David,35,New York,80000,Dengue
Eve,29,London,,Chikungunya
Frank,40,Berlin,95000,
Grace,22,Paris,50000,Dengue
"""
csv_file_path = "dados_arbovirus.csv"
with open(csv_file_path, "w", encoding="utf-8") as f:
    f.write(csv_content)
print(f"Arquivo '{csv_file_path}' criado para demonstração.\n")

# --- Demonstração do uso do ArbovirusFramework ---
try:
    print("--- Carregando o Dataset ---")
    df_arbovirus = ArbovirusDataFrame.from_csv(csv_file_path)
    print(df_arbovirus.head())
    print("\n")

    print("--- Informações do DataFrame ---")
    df_arbovirus.info()
    print("\n")

    print("--- Estatísticas Descritivas ---")
    print(df_arbovirus.describe())
    print("\n")

    # Seleção de colunas
    print("--- Selecionando Colunas 'Nome' e 'Doenca' ---")
    df_selecionado = df_arbovirus.select_columns(['Nome', 'Doenca'])
    print(df_selecionado.head())
    print("\n")

    # Filtragem de linhas (ex: pessoas com Dengue)
    print("--- Filtrando Linhas: Pessoas com 'Dengue' ---")
    df_dengue = df_arbovirus.filter_rows(lambda df: df['Doenca'] == 'Dengue')
    print(df_dengue.head())
    print("\n")

    # Preenchimento de valores ausentes (ex: Idade com a mediana, Salario com a média)
    print("--- Preenchendo Valores Ausentes (Idade com mediana, Salario com média) ---")
    df_preenchido_idade = transformations.fill_missing_values(df_arbovirus, strategy='median', columns='Idade')
    df_preenchido_final = transformations.fill_missing_values(df_preenchido_idade, strategy='mean', columns='Salario')
    print(df_preenchido_final.head())
    print("\n")

    # Criando uma nova coluna (ex: Categoria Salarial)
    print("--- Criando Nova Coluna 'CategoriaSalarial' ---")
    df_com_categoria = transformations.create_new_column(
        df_preenchido_final,
        'CategoriaSalarial',
        lambda df: df['Salario'].apply(lambda s: 'Alta' if s > 70000 else 'Baixa')
    )
    print(df_com_categoria.head())
    print("\n")

    # Aplicando função a uma coluna (ex: Doenca em maiúsculas)
    print("--- Convertendo Coluna 'Doenca' para Maiúsculas ---")
    df_doenca_maiuscula = transformations.apply_function_to_column(
        df_com_categoria,
        'Doenca',
        lambda d: str(d).upper() if pd.notna(d) else d
    )
    print(df_doenca_maiuscula.head())
    print("\n")

    # Removendo duplicatas (ex: com base em Nome e Idade)
    # Vamos adicionar uma duplicata temporária para testar
    temp_df = df_doenca_maiuscula.get_dataframe()
    temp_df.loc[len(temp_df)] = ['Alice', 30, 'New York', 60000, 'DENGUE', 'Baixa'] # Adiciona uma duplicata
    df_with_temp_dup = ArbovirusDataFrame(temp_df)

    print("--- DataFrame com Duplicata Temporária ---")
    print(df_with_temp_dup.tail(3))
    print("\n")

    print("--- Removendo Duplicatas com base em 'Nome' e 'Idade' ---")
    df_sem_duplicatas = transformations.drop_duplicates(df_with_temp_dup, subset=['Nome', 'Idade'])
    print(df_sem_duplicatas.tail()) # Alice duplicada deve ter sido removida
    print(f"Linhas antes: {df_with_temp_dup.shape[0]}, Linhas depois: {df_sem_duplicatas.shape[0]}\n")

    # Renomeando colunas
    print("--- Renomeando Colunas 'Nome' para 'Paciente' e 'Salario' para 'RendaMensal' ---")
    df_renomeado = transformations.rename_columns(
        df_sem_duplicatas,
        {'Nome': 'Paciente', 'Salario': 'RendaMensal'}
    )
    print(df_renomeado.head())
    print("\n")

    # Salvando o DataFrame processado
    output_csv_path = "dados_arbovirus_processados.csv"
    df_renomeado.save_to_csv(output_csv_path, index=False)
    print(f"DataFrame processado salvo em '{output_csv_path}'.\n")

    # --- Testando tratamento de erros ---
    print("--- Testando Tratamento de Erros ---")
    try:
        # Tenta carregar um arquivo que não existe
        ArbovirusDataFrame.from_csv("arquivo_inexistente.csv")
    except FileNotFoundError as e:
        print(f"Erro esperado: {e}")

    try:
        # Tenta selecionar uma coluna que não existe
        df_arbovirus.select_columns(['ColunaQueNaoExiste'])
    except ColumnNotFoundError as e:
        print(f"Erro esperado: {e}")

    # Criar um CSV malformado para teste
    malformed_csv_path = "malformed.csv"
    with open(malformed_csv_path, "w", encoding="utf-8") as f:
        f.write("col1,col2\nvalue1,value2,extra\nvalue3,value4\n")
    try:
        # Tenta carregar um arquivo CSV malformado
        ArbovirusDataFrame.from_csv(malformed_csv_path)
    except InvalidCSVError as e:
        print(f"Erro esperado: {e}")
    finally:
        if os.path.exists(malformed_csv_path):
            os.remove(malformed_csv_path)

except Exception as e:
    print(f"\nOcorreu um erro inesperado durante a execução do framework: {e}")
finally:
    # Limpeza dos arquivos de teste
    if os.path.exists(csv_file_path):
        os.remove(csv_file_path)
    if os.path.exists(output_csv_path):
        os.remove(output_csv_path)
    print("\nArquivos de teste removidos.")