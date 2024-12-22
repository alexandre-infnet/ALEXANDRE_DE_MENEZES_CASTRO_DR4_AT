import os
import json
import requests
import pandas as pd
import google.generativeai as genai
from google.generativeai.types import GenerationConfig

from dotenv import load_dotenv

load_dotenv()

genai.configure(api_key=os.environ["GENAI_API_KEY"])
model = genai.GenerativeModel("gemini-1.5-flash")


def get_deputados():
    response: requests.Response = requests.get(
        "https://dadosabertos.camara.leg.br/api/v2/deputados"
    )

    deputados = response.json()["dados"]

    return deputados


def get_response_questao_3c():
    deputados_df = pd.read_csv('data/distribuicao_deputados.csv')

    dados_tabela = "\n".join(
        f"{row['Partido']} | {row['Quantidade']} | {row['Porcentagem']}%"
        for _, row in deputados_df.iterrows()
    )

    prompt = f"""
    Você é um analista político especializado em ciência política e sociologia legislativa. Sua tarefa é analisar dados fornecidos sobre a distribuição de
    deputados por partido na Câmara dos Deputados. Use insights qualitativos e quantitativos para responder às questões propostas.

    Os dados estão formatados da seguinte forma:
    Partido | Quantidade | Porcentagem
    {dados_tabela}

    Aqui estão exemplos de como estruturar suas respostas:
    - Identifique tendências de coalizões com base nos partidos majoritários.
    - Compare a proporção de deputados entre partidos ideologicamente alinhados e discuta a formação de blocos.
    - Analise como a presença de partidos menores pode influenciar decisões estratégicas, considerando sua porcentagem.

    Agora, com base nos dados fornecidos, responda:
    1. Quais partidos têm maior probabilidade de liderar coalizões e como isso pode afetar a aprovação de propostas legislativas?
    2. Como a proporção de deputados de partidos menores pode influenciar o equilíbrio de poder?
    3. Quais dinâmicas parlamentares podem surgir entre partidos ideologicamente opostos?

    Sua análise deve ser objetiva, detalhada e incluir projeções sobre o impacto legislativo da configuração atual.
    """

    response = model.generate_content(prompt)

    return response


def questao4(df):
    distinct_ids = df['id'].unique().tolist()
    base_url = "https://dadosabertos.camara.leg.br/api/v2/deputados/{id}/despesas"
    all_data = []

    # Rodando apenas para os 100 primeiros deputados, senão demora muito para rodar
    for id in distinct_ids[:100]:
        try:
            response = requests.get(base_url.format(id=id))
            response.raise_for_status()
            data = response.json()

            for item in data["dados"]:
                item["deputado_id"] = id
            all_data.extend(data["dados"])

        except Exception as e:
            print(f"Erro ao processar ID {id}: {e}")

    df = pd.DataFrame(all_data)

    return df


def questao4a(df):
    df['dataDocumento'] = pd.to_datetime(df['dataDocumento'])

    grouped_df = df.groupby([df['dataDocumento'].dt.date, 'deputado_id', 'tipoDespesa']).agg({
        'valorDocumento': 'sum',
        'valorLiquido': 'sum'
    }).reset_index()

    grouped_df.to_parquet("data/serie_despesas_diarias_deputados.parquet", index=False)


def questao4b():
    prompt = """
    Você é um modelo de linguagem especializado em análise de dados.
    Vou te solicitar uma análise em etapas.
    Os dados sobre despesas de deputados estão em 'data/serie_despesas_diarias_deputados.parquet.
    Aqui estão as colunas disponíveis:

    - dataDocumento: Data do documento da despesa.
    - deputado_id: Identificação do deputado.
    - tipoDespesa: Tipo de despesa realizada.
    - valorDocumento: Valor total do documento.
    - valorLiquido: Valor líquido da despesa.

    Siga as instruções abaixo para realizar a análise:

    ### Etapa 1: Preparação dos dados
    Escreva um código Python que carregue os dados parquet e realize o pré-processamento necessário. Isso inclui:
    - Converter a coluna `dataDocumento` para o formato de data.
    - Garantir que valores financeiros sejam interpretados como numéricos.
    - Tratar valores ausentes, se necessário.

    Depois de processar os dados, exiba as primeiras 5 linhas como exemplo.

    ### Etapa 2: Análise dos maiores gastos
    Com os dados preparados, identifique:
    1. Os três deputados com os maiores valores líquidos totais de despesa, separados por tipo de despesa.
    2. Para cada deputado, exiba o tipo de despesa mais frequente.

    ### Etapa 3: Análise das despesas médias
    Calcule:
    1. A média de despesas líquidas por deputado, considerando o período total.
    2. O tipo de despesa com a maior média de gastos líquidos.

    ### Etapa 4: Proporção de gastos por tipo
    Analise:
    1. A proporção do valor líquido total de cada tipo de despesa em relação ao total geral de despesas líquidas.
    2. Identifique os três tipos de despesas que representam a maior proporção do valor líquido total.

    ### Finalização
    Inclua comentários explicativos em cada etapa do código gerado.
    Retorne apenas o código Python, sem a necessidade de executá-lo.
    """

    response = model.generate_content(prompt)

    with open("questoes/questao4b.py", "w") as file:
        # Removendo o markdown do texto gerado
        texto_limpado = response.text[9:-4].strip()
        file.write(texto_limpado)

def questao4c():
    gastos_df = pd.read_csv("data/maiores_gastos.csv")
    media_deputados_df = pd.read_csv("data/media_deputados.csv")
    tipo_frequente_df = pd.read_csv("data/tipo_mais_frequente.csv")

    prompt = """
    Explique e responda cada pergunta

    1. Qual é a categoria de maior gasto entre os deputados analisados?
    Conhecimento: O maior gasto identificado foi na categoria '{}' com um valor total de R$ {:.2f}, conforme os dados analisados.

    2. Qual é o deputado que apresenta a maior média de gastos líquidos?
    Conhecimento: O deputado com a maior média de gastos líquidos é o de ID {}, com uma média de R$ {:.2f}, conforme os dados analisados.

    3. Qual é o tipo de despesa mais frequente associado aos deputados?
    Conhecimento: O tipo de despesa mais frequente é '{}', que aparece {} vezes nos dados analisados.

    Retorne apenas o resultado em formato JSON
    """.format(
        gastos_df.groupby('tipoDespesa')['valorLiquido'].sum().idxmax(),
        gastos_df.groupby('tipoDespesa')['valorLiquido'].sum().max(),

        media_deputados_df.loc[media_deputados_df['valorLiquido'].idxmax(), 'deputado_id'],
        media_deputados_df['valorLiquido'].max(),

        tipo_frequente_df['tipoDespesa'].value_counts().idxmax(),
        tipo_frequente_df['tipoDespesa'].value_counts().max()
    )

    response = model.generate_content(prompt)

    with open("data/insights_despesas_deputados.json", "w") as file:
        # Removendo o markdown do texto gerado
        texto_limpado = response.text[7:-4].strip()
        file.write(texto_limpado)


def questao5():
    url = "https://dadosabertos.camara.leg.br/api/v2/proposicoes"
    codigos = ["40", "46", "62"]
    proposicoes = []

    for codigo in codigos:
        params = {
            "dataInicio": "2024-08-01",
            "dataFim": "2024-08-30",
            "codTema": codigo,
            "itens": 10
        }

        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            proposicoes.extend(data['dados'])
        else:
            print("Erro ao buscar proposições:", response.text)

    return proposicoes


def questao_5a():
    proposicoes = questao5()

    df = pd.DataFrame(proposicoes)
    df.to_parquet("data/proposicoes_deputados.parquet", index=False)

    return df


def questao_5b():
    data = questao_5a()
    chunk_size = 10
    overlap = 2

    dialogues = data["ementa"].tolist()

    chunks = []
    start = 0
    while start < len(dialogues):
        end = min(start + chunk_size, len(dialogues))
        chunks.append(dialogues[start:end])
        if end == len(dialogues):
            break
        start += chunk_size - overlap

    chunk_prompts = [
        f"Resuma os seguintes textos de proposições:\n\n" +
        "\n".join([f"{i+1}. {line}" for i, line in enumerate(chunk)])
        for chunk in chunks
    ]

    chunk_summaries = {}
    generation_config = GenerationConfig(max_output_tokens=500)
    for idx, chunk_prompt in enumerate(chunk_prompts):
        response = model.generate_content(chunk_prompt, generation_config=generation_config)
        chunk_summaries.update({idx: response.text})

    with open("data/sumarizacao_proposicoes.json", "w") as file:
        json.dump(chunk_summaries, file, ensure_ascii=False, indent=4)


if __name__ == "__main__":
    questao_5b()





    #deputados = get_deputados()
    #df_deputados = pd.DataFrame(deputados)

    # Questão 4
    # df_despesas = questao4(df_deputados)

    # Questão 4a
    #df_agrupado = questao4a(df_despesas)

    # Questão 4b
    #questao4b()

    # Questão 4c
    #questao4c()

    # Questão 5
    #print(questao_5())

    # Questao 5a:
    #questao_5a()

    #df.to_parquet("data/deputados.parquet", index=False)

    """
    prompt =
    Retorne apenas um script em Python utilizando as bibliotecas matplotlib e pandas que gere um gráfico de pizza representando o número total e o percentual
    de deputados de cada partido a partir do conjunto de dados data/deputados.parquet. Considere que os dados serão lidos com o pandas, a coluna siglaPartido
    contem o nome dos partidos. O gráfico de pizza deve conter:
    1.	Os rótulos mostrando o nome do partido.
    2.	A quantidade e o percentual de deputados de cada partido no formato: 'Partido: Qtd (%Perc)'.
    3.	Um título para o gráfico.
    Após criar o código do gráfico, salve-o no arquivo docs/distribuicao_deputados.png com alta qualidade.
    Garanta que o código seja bem estruturado e utilize boas práticas, como comentários explicativos.


    response = model.generate_content(prompt)
    with open("questoes/questao3b.py", "w") as file:
        # Removendo o markdown do texto gerado
        texto_limpado = response.text[9:-4].strip()
        file.write(texto_limpado)

    response = get_response_questao_3c()
    data = {"response": response.text}

    with open("data/insights_distribuicao_deputados.json", 'w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False, indent=4)"""