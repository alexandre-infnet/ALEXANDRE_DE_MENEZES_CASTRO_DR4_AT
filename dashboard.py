import streamlit as st
import json
import yaml

tab1, tab2, tab3 = st.tabs(["Overview", "Despesas", "Proposições"])

with tab1:
    st.title("Overview")
    st.subheader("Bem vindos ao Dashboard")
    st.image("docs/distribuicao_deputados.png")

    with open('data/insights_distribuicao_deputados.json', 'r', encoding='utf-8') as f:
        data = json.load(f)
        st.write(data['response'])

    with open('data/config.yaml', 'r') as file:
        config = yaml.safe_load(file)
        st.write(config['chave'])

with tab2:
    st.header("Despesas")
    # Adicione o conteúdo da aba "Despesas" aqui

with tab3:
    st.header("Proposições")
    # Adicione o conteúdo da aba "Proposições" aqui
