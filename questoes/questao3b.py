import pandas as pd
import matplotlib.pyplot as plt

# Define o caminho para o arquivo parquet
parquet_file = 'data/deputados.parquet'

try:
    # Lê o arquivo parquet usando pandas
    df = pd.read_parquet(parquet_file)

    # Conta a ocorrência de cada partido
    partido_counts = df['siglaPartido'].value_counts()

    # Cria os rótulos para o gráfico de pizza com quantidade e percentual
    labels = [f'{partido}: {count} ({(count / partido_counts.sum() * 100):.1f}%)' for partido, count in partido_counts.items()]

    # Cria o gráfico de pizza
    plt.figure(figsize=(10, 10))  # Define o tamanho da figura
    plt.pie(partido_counts, labels=labels, autopct='%1.1f%%', startangle=140)
    plt.title('Distribuição de Deputados por Partido')
    plt.axis('equal')  # Garante que a pizza seja um círculo

    # Salva o gráfico em alta resolução
    plt.savefig('docs/distribuicao_deputados.png', dpi=300)

    print("Gráfico salvo com sucesso em docs/distribuicao_deputados.png")

except FileNotFoundError:
    print(f"Erro: Arquivo '{parquet_file}' não encontrado.")
except KeyError:
    print(f"Erro: Coluna 'siglaPartido' não encontrada no arquivo '{parquet_file}'.")
except Exception as e:
    print(f"Ocorreu um erro inesperado: {e}")