# Código gerado para o Dashboard Streamlit
import streamlit as st
import pandas as pd
import json

@st.cache_data
def load_json(filepath):
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    except FileNotFoundError:
        st.error(f"Arquivo JSON não encontrado: {filepath}")
        return None
    except json.JSONDecodeError:
        st.error(f"Erro ao decodificar o arquivo JSON: {filepath}")
        return None


@st.cache_data
def load_parquet(filepath):
    try:
        df = pd.read_parquet(filepath)
        return df
    except FileNotFoundError:
        st.error(f"Arquivo Parquet não encontrado: {filepath}")
        return None
    except pd.errors.EmptyDataError:
        st.error(f"Arquivo Parquet vazio: {filepath}")
        return None
    except Exception as e:
        st.error(f"Erro ao carregar o arquivo Parquet: {filepath}, Erro: {e}")
        return None


def load_data():
    insights_despesas = load_json('data/insights_despesas_deputados.json')
    if insights_despesas is None: return None

    required_cols_insights = ['maior_gasto', 'maior_media_gastos_liquidos', 'despesa_mais_frequente']
    if not all(col in insights_despesas for col in required_cols_insights):
        st.error("O arquivo 'insights_despesas_deputados.json' não contém as colunas necessárias.")
        return None


    serie_despesas = load_parquet('data/serie_despesas_diarias_deputados.parquet')
    if serie_despesas is None: return None

    required_cols_serie = ['deputado_id', 'dataDocumento', 'tipoDespesa', 'valorDocumento', 'valorLiquido']
    if not all(col in serie_despesas.columns for col in required_cols_serie):
        st.error("O arquivo 'serie_despesas_diarias_deputados.parquet' não contém as colunas necessárias.")
        return None


    proposicoes_deputados = load_parquet('data/proposicoes_deputados.parquet')
    if proposicoes_deputados is None: return None

    required_cols_proposicoes = ['deputado_id', 'proposicao_id', 'data_apresentacao', 'ementa']
    if not all(col in proposicoes_deputados.columns for col in required_cols_proposicoes):
        st.error("O arquivo 'proposicoes_deputados.parquet' não contém as colunas necessárias.")
        return None


    sumarizacao_proposicoes = load_json('data/sumarizacao_proposicoes.json')
    if sumarizacao_proposicoes is None: return None

    return insights_despesas, serie_despesas, proposicoes_deputados, sumarizacao_proposicoes

import streamlit as st
import pandas as pd
import json
import plotly.express as px

st.set_page_config(page_title="Despesas", page_icon=":chart_with_upwards_trend:")

with open("data/insights_despesas_deputados.json", "r") as f:
    insights = json.load(f)

df_despesas = pd.read_parquet("data/serie_despesas_diarias_deputados.parquet")

deputados = df_despesas["deputado_id"].unique()
deputado_selecionado = st.selectbox("Selecione o Deputado", deputados, key="deputado_select")

df_deputado = df_despesas[df_despesas["deputado_id"] == deputado_selecionado]


if "dataDocumento" in df_deputado.columns and "valorLiquido" in df_deputado.columns:
    fig = px.bar(df_deputado, x="dataDocumento", y="valorLiquido", title=f"Série Temporal de Despesas - Deputado {deputado_selecionado}")
    st.plotly_chart(fig)
else:
    st.error("Colunas 'dataDocumento' ou 'valorLiquido' não encontradas no DataFrame.")

st.json(insights)

import streamlit as st
import pandas as pd
import json

st.title("Proposições")

try:
    proposicoes = pd.read_parquet("data/proposicoes_deputados.parquet")
    required_cols = ["deputado_id", "proposicao_id", "data_apresentacao", "ementa"]
    if not all(col in proposicoes.columns for col in required_cols):
        st.error("Colunas obrigatórias ausentes no arquivo Parquet.")
    else:
        st.dataframe(proposicoes)

    with open("data/sumarizacao_proposicoes.json", "r") as f:
        sumario = json.load(f)
        st.json(sumario)

except FileNotFoundError:
    st.error("Arquivos de dados não encontrados.")
except Exception as e:
    st.error(f"Ocorreu um erro: {e}")

