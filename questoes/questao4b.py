# Importando bibliotecas necessárias
import pandas as pd
import numpy as np

# Etapa 1: Preparação dos dados
try:
    # Carregando os dados parquet
    df = pd.read_parquet('data/serie_despesas_diarias_deputados.parquet')

    # Convertendo a coluna 'dataDocumento' para o formato de data
    df['dataDocumento'] = pd.to_datetime(df['dataDocumento'], errors='coerce')

    # Convertendo colunas financeiras para numéricas, tratando erros
    for col in ['valorDocumento', 'valorLiquido']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Tratando valores ausentes (substituindo por 0 para valores financeiros e removendo linhas com datas inválidas)
    df['valorDocumento'].fillna(0, inplace=True)
    df['valorLiquido'].fillna(0, inplace=True)
    df.dropna(subset=['dataDocumento'], inplace=True)

    # Exibindo as primeiras 5 linhas
    print("Primeiras 5 linhas após o pré-processamento:")
    print(df.head())

except FileNotFoundError:
    print("Erro: Arquivo 'data/serie_despesas_diarias_deputados.parquet' não encontrado.")
    exit()
except pd.errors.EmptyDataError:
    print("Erro: Arquivo parquet está vazio.")
    exit()
except Exception as e:
    print(f"Ocorreu um erro durante o pré-processamento: {e}")
    exit()



# Etapa 2: Análise dos maiores gastos
if not df.empty:
    # Agrupando por deputado e tipo de despesa, somando os valores líquidos
    gastos_deputados = df.groupby(['deputado_id', 'tipoDespesa'])['valorLiquido'].sum().reset_index()

    # Encontrando os três deputados com maiores gastos totais
    top_3_deputados = gastos_deputados.groupby('deputado_id')['valorLiquido'].sum().nlargest(3)

    print("\nTop 3 Deputados com maiores gastos líquidos totais:")
    for deputado_id in top_3_deputados.index:
        deputado_gastos = gastos_deputados[gastos_deputados['deputado_id'] == deputado_id]
        print(f"\nDeputado ID: {deputado_id}")
        print(deputado_gastos)

        # Tipo de despesa mais frequente para cada deputado
        tipo_despesa_mais_frequente = deputado_gastos['tipoDespesa'].mode()[0]
        print(f"Tipo de despesa mais frequente: {tipo_despesa_mais_frequente}")


# Etapa 3: Análise das despesas médias
    # Média de despesas líquidas por deputado
    media_despesas_deputado = df.groupby('deputado_id')['valorLiquido'].mean()
    print("\nMédia de despesas líquidas por deputado:")
    print(media_despesas_deputado)


    # Tipo de despesa com maior média de gastos líquidos
    media_despesas_tipo = df.groupby('tipoDespesa')['valorLiquido'].mean()
    tipo_despesa_maior_media = media_despesas_tipo.idxmax()
    print(f"\nTipo de despesa com maior média de gastos líquidos: {tipo_despesa_maior_media}")


# Etapa 4: Proporção de gastos por tipo
    # Proporção do valor líquido total de cada tipo de despesa
    total_gastos = df['valorLiquido'].sum()
    proporcao_tipos_despesa = df.groupby('tipoDespesa')['valorLiquido'].sum() / total_gastos
    print("\nProporção de gastos por tipo de despesa:")
    print(proporcao_tipos_despesa)


    # Três tipos de despesas com maior proporção
    top_3_tipos_despesa = proporcao_tipos_despesa.nlargest(3)
    print("\nTrês tipos de despesas com maior proporção:")
    print(top_3_tipos_despesa)