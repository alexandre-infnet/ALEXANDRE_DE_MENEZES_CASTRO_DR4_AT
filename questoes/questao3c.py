import pandas as pd


df = pd.read_parquet("data/deputados.parquet")
partidos = df["siglaPartido"].value_counts()

partidos_porcentagem = (partidos / partidos.sum() * 100).round(1)

df_partidos = pd.DataFrame(
    {
        "Partido": partidos.index,
        "Quantidade": partidos.values,
        "Porcentagem": partidos_porcentagem.values,
    }
)

print(df_partidos)

df_partidos.to_csv("data/distribuicao_deputados.csv", index=False)
