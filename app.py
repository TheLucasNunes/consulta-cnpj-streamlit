import streamlit as st
import pandas as pd
import requests
import time
import re
import math
from io import BytesIO
from datetime import datetime
import firebase_admin
from firebase_admin import credentials, firestore
import pytz # Para Fuso Horário

# --- Configuração da Página ---
st.set_page_config(
    page_title="Consultor de CNPJs em Lote",
    page_icon="🚀",
    layout="wide"
)

# --- Fuso Horário Local ---
LOCAL_TIMEZONE = pytz.timezone('America/Sao_Paulo') # Ajuste se necessário

# --- Inicialização do Firebase ---
try:
    if not firebase_admin._apps:
        cred = credentials.Certificate('serviceAccountKey.json')
        firebase_admin.initialize_app(cred)
    db = firestore.client()
except Exception as e:
    st.error(f"ERRO: Não foi possível conectar ao Firebase.")
    st.error(f"Certifique-se que o arquivo 'serviceAccountKey.json' está na mesma pasta.")
    st.exception(e)
    st.stop()

# --- Funções Helpers ---

def limpar_cnpj(cnpj):
    """Limpa e formata o CNPJ para 14 dígitos."""
    cnpj_limpo = re.sub(r'\D', '', str(cnpj))
    cnpj_formatado = cnpj_limpo.zfill(14)
    return cnpj_formatado

### CORREÇÃO FINAL DATAS: df_to_excel ###
def df_to_excel(df):
    """Converte um DataFrame para um arquivo Excel em memória."""
    output = BytesIO()
    df_copia = df.copy()
    # Lista abrangente de possíveis nomes de colunas de data
    colunas_data_nomes = [
        'abertura', 'data_situacao', 'ultima_atualizacao', 'data_situacao_especial',
        'simples.ultima_atualizacao', 'simei.ultima_atualizacao', 'data_adicionado', 'data_conclusao'
    ]
    # Adiciona prefixos comuns se existirem
    colunas_data_nomes.extend([f'json_{col}' for col in colunas_data_nomes if f'json_{col}' in df_copia.columns])

    for col in colunas_data_nomes:
        if col in df_copia.columns:
            # Converte a coluna para datetime, tratando erros e UTC
            # errors='coerce' transforma falhas em NaT
            # utc=True tenta interpretar strings/naive como UTC
            dt_series_utc = pd.to_datetime(df_copia[col], errors='coerce', utc=True)

            # Converte para o fuso local APENAS se a conversão para datetime funcionou
            dt_series_local = dt_series_utc.dt.tz_convert(LOCAL_TIMEZONE)

            # Formata para string, preenchendo NaT com 'N/A'
            df_copia[col] = dt_series_local.dt.strftime('%d/%m/%Y %H:%M:%S')
            df_copia[col] = df_copia[col].fillna('N/A') # Preenche onde a conversão falhou (NaT)

    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_copia.to_excel(writer, index=False, sheet_name='Resultados')
    return output.getvalue()

### CORREÇÃO FINAL DATAS: formatar_colunas_data ###
def formatar_colunas_data(df):
    """Formata colunas de data para exibição no Streamlit (convertendo para fuso local)."""
    df_formatado = df.copy()
    colunas_data_nomes = [
        'abertura', 'data_situacao', 'ultima_atualizacao', 'data_situacao_especial',
        'simples.ultima_atualizacao', 'simei.ultima_atualizacao', 'data_adicionado', 'data_conclusao'
    ]
    colunas_data_nomes.extend([f'json_{col}' for col in colunas_data_nomes if f'json_{col}' in df_formatado.columns])

    for col in colunas_data_nomes:
        if col in df_formatado.columns:
             # Converte a coluna para datetime, tratando erros e UTC
            dt_series_utc = pd.to_datetime(df_formatado[col], errors='coerce', utc=True)
            # Converte para o fuso local APENAS se a conversão funcionou
            dt_series_local = dt_series_utc.dt.tz_convert(LOCAL_TIMEZONE)
            # Formata para string
            df_formatado[col] = dt_series_local.dt.strftime('%d/%m/%Y %H:%M')
            # Preenche NaT (que viraram NaN/None) com ''
            df_formatado[col] = df_formatado[col].fillna('')

    return df_formatado


def reordenar_colunas(df):
    """Reordena o DataFrame para uma visualização mais lógica."""
    colunas_principais = [
        'status', 'data_adicionado', 'data_conclusao', 'cnpj_consultado', 'nome',
        'fantasia', 'situacao', 'motivo_situacao', 'atividade_principal',
        'atividade_secundaria', 'quadro_societario',
        'logradouro', 'numero', 'complemento', 'bairro', 'municipio', 'uf', 'cep',
        'telefone', 'email'
    ]
    colunas_existentes = df.columns.tolist()
    colunas_finais = []
    for col in colunas_principais:
        if col in colunas_existentes:
            colunas_finais.append(col)
            # Remove da lista original para evitar duplicar
            if col in colunas_existentes: colunas_existentes.remove(col)
    colunas_finais.extend(colunas_existentes)
    return df[[col for col in colunas_finais if col in df.columns]]


# --- Funções de Interação com o Firebase ---

@st.cache_data(ttl=60)
def carregar_resultados_db():
    """Lê todos os dados do Firebase e transforma em DataFrame."""
    print("Carregando dados do Firebase...")
    tarefas_ref = db.collection('tarefas').order_by(
        'data_adicionado', direction=firestore.Query.DESCENDING
    ).stream()
    lista_de_tarefas = []
    for tarefa in tarefas_ref:
        dados_tarefa = tarefa.to_dict()
        # Converte Timestamps do Firestore para Datetime ciente de UTC
        if 'data_adicionado' in dados_tarefa and hasattr(dados_tarefa['data_adicionado'], 'ToDatetime'):
             dados_tarefa['data_adicionado'] = dados_tarefa['data_adicionado'].ToDatetime(pytz.UTC)
        if 'data_conclusao' in dados_tarefa and hasattr(dados_tarefa['data_conclusao'], 'ToDatetime'):
             dados_tarefa['data_conclusao'] = dados_tarefa['data_conclusao'].ToDatetime(pytz.UTC)

        dados_tarefa['cnpj_consultado'] = tarefa.id
        lista_de_tarefas.append(dados_tarefa)

    if not lista_de_tarefas:
        return pd.DataFrame()

    df_base = pd.DataFrame(lista_de_tarefas)

    if 'resultado_json' in df_base.columns:
        try: # Adiciona try-except para a normalização que pode falhar com dados muito inconsistentes
            df_normalized = pd.json_normalize(df_base['resultado_json'].fillna({}).tolist())
            df_normalized = df_normalized.rename(columns=lambda x: f"json_{x}" if x in df_base.columns else x)
            df_final = pd.merge(
                df_base.drop(columns=['resultado_json'], errors='ignore'), # errors='ignore' se a coluna não existir
                df_normalized,
                left_index=True,
                right_index=True,
                how='left'
            )
        except Exception as e:
            print(f"Erro durante json_normalize: {e}")
            df_final = df_base # Retorna o base se a normalização falhar
    else:
        df_final = df_base

    return df_final


def adicionar_cnpjs_fila(lista_cnpjs_limpos):
    """Adiciona CNPJs ao banco dedados com status 'PENDENTE'."""
    cnpjs_adicionados = 0
    batch = db.batch()
    for cnpj in lista_cnpjs_limpos:
        tarefa_ref = db.collection('tarefas').document(cnpj)
        batch.set(tarefa_ref, {
            'status': 'PENDENTE',
            'data_adicionado': firestore.SERVER_TIMESTAMP,
            'cnpj_consultado': cnpj
        }, merge=True)
        cnpjs_adicionados += 1
    batch.commit()
    return cnpjs_adicionados, 0

# --- Interface do App ---

st.title("🚀 Consultor de CNPJs em Lote")

st.markdown("""
Esta ferramenta consulta CNPJs na API pública e gratuita :green[ReceitaWS].

**Como funciona:**
1.  **Adicione Tarefas:** Cole sua lista de CNPJs e clique em "Adicionar à Fila".
2.  **Aguarde o Processamento:** Um robô processará sua fila em segundo plano (3 CNPJs por minuto).
3.  **Atualize e Visualize:** Clique em "Atualizar Resultados" para ver o progresso.

**Você pode fechar esta aba!** O processamento continuará. Volte mais tarde para ver e baixar seus resultados.
""")

# --- Área de Input ---
st.header("1. Adicionar CNPJs à Fila")
cnpjs_colados = st.text_area(
    "Cole sua lista de CNPJs (um por linha).",
    height=200,
    placeholder="""Exemplos de formatos aceitos:
- 00.000.000/0001-00
- 12345678000199
- 1234567000199 (zeros à esquerda faltantes serão completados)
"""
)

if cnpjs_colados:
    lista_cnpjs_limpos = sorted(list(set(
        [limpar_cnpj(c) for c in cnpjs_colados.split('\n') if c.strip()]
    )))
    total_cnpjs = len(lista_cnpjs_limpos)
    if total_cnpjs > 0:
        st.info(f"**{total_cnpjs} CNPJs únicos** identificados para adicionar/atualizar na fila.")
        total_lotes = math.ceil(total_cnpjs / 3)
        tempo_estimado_min = total_lotes * 1.02
        st.warning(f"Tempo estimado de processamento (se todos forem novos): **~{tempo_estimado_min:.0f} minutos**.")


if st.button("🚀 Adicionar à Fila", type="primary"):
    if cnpjs_colados:
        lista_cnpjs_limpos = sorted(list(set(
            [limpar_cnpj(c) for c in cnpjs_colados.split('\n') if c.strip()]
        )))
        total_cnpjs = len(lista_cnpjs_limpos)
        if total_cnpjs > 0:
            adicionados, ignorados = adicionar_cnpjs_fila(lista_cnpjs_limpos)
            st.success(f"{adicionados} CNPJs foram adicionados ou atualizados para (re)consulta na fila.")
            st.cache_data.clear()
        else:
             st.error("Nenhum CNPJ válido encontrado na lista.")
    else:
        st.error("Por favor, cole ao menos um CNPJ.")

st.divider()

# --- 2. Exibição dos Resultados ---
st.header("2. Resultados da Consulta")

if 'resultados_df' not in st.session_state:
    st.session_state.resultados_df = pd.DataFrame()

if st.button("🔄 Atualizar Resultados"):
    st.cache_data.clear()
    st.session_state.resultados_df = carregar_resultados_db()

if st.session_state.resultados_df.empty:
     st.session_state.resultados_df = carregar_resultados_db()


if not st.session_state.resultados_df.empty:

    df_base = st.session_state.resultados_df.copy()

    df_processado = df_base

    qsa_col = 'json_qsa' if 'json_qsa' in df_processado.columns else ('qsa' if 'qsa' in df_processado.columns else None)
    if qsa_col:
        qsa_dtype_original = df_processado[qsa_col].dtype
        df_processado = df_processado.explode(qsa_col)
        df_processado['quadro_societario'] = df_processado[qsa_col].apply(lambda x: x.get('nome') if isinstance(x, dict) else x if pd.notna(x) else None)
        if qsa_col != 'quadro_societario' and qsa_col in df_processado.columns: df_processado = df_processado.drop(columns=[qsa_col])

    atividades_col = 'json_atividades_secundarias' if 'json_atividades_secundarias' in df_processado.columns else ('atividades_secundarias' if 'atividades_secundarias' in df_processado.columns else None)
    if atividades_col:
        atividades_dtype_original = df_processado[atividades_col].dtype
        df_processado = df_processado.explode(atividades_col)

    atividade_p_col = 'json_atividade_principal' if 'json_atividade_principal' in df_processado.columns else ('atividade_principal' if 'atividade_principal' in df_processado.columns else None)
    if atividade_p_col:
        df_processado['atividade_principal'] = df_processado[atividade_p_col].apply(lambda x: x[0]['text'] if isinstance(x, list) and len(x) > 0 and isinstance(x[0], dict) else x if pd.notna(x) else None)
        if atividade_p_col != 'atividade_principal' and atividade_p_col in df_processado.columns: df_processado = df_processado.drop(columns=[atividade_p_col])

    if atividades_col:
        if atividades_col != 'atividade_secundaria':
             if atividades_col in df_processado.columns:
                 df_processado = df_processado.rename(columns={atividades_col: 'atividade_secundaria'})
             atividades_col = 'atividade_secundaria'
        if atividades_col in df_processado.columns:
            df_processado['atividade_secundaria'] = df_processado[atividades_col].apply(lambda x: x.get('text') if isinstance(x, dict) else x if pd.notna(x) else None)

    df_resultados_brutos = df_processado
    df_resultados = reordenar_colunas(df_resultados_brutos)

    st.subheader("Resumo")
    col1, col2 = st.columns(2)

    with col1:
        st.write("**Resumo Cadastral (Situação)**")
        df_concluidos_resumo = df_resultados[df_resultados['status'] == 'CONCLUIDO']
        if not df_concluidos_resumo.empty and 'situacao' in df_concluidos_resumo.columns:
            df_unicos_resumo = df_concluidos_resumo[['cnpj_consultado', 'situacao']].drop_duplicates()
            contagem_situacao = df_unicos_resumo['situacao'].value_counts().reset_index()
            contagem_situacao.columns = ['Situação', 'Quantidade']
            st.dataframe(contagem_situacao, use_container_width=True, hide_index=True)
        else:
            st.info("Nenhuma consulta concluída ainda.")

    with col2:
        st.write("**Resumo da Fila (Status Geral)**")
        contagem_status_fila = df_resultados['status'].value_counts().reset_index()
        contagem_status_fila.columns = ['Status', 'Quantidade']
        st.dataframe(contagem_status_fila, use_container_width=True, hide_index=True)


    if 'atividade_principal' in df_resultados.columns:
        st.subheader("Atividades Principais (CNAE - Apenas Concluídos)")
        df_cnae = df_resultados[df_resultados['status'] == 'CONCLUIDO'][['cnpj_consultado', 'atividade_principal']].copy()
        if not df_cnae.empty:
            df_cnae_unicos = df_cnae.drop_duplicates()
            cnae_counts = df_cnae_unicos[df_cnae_unicos['atividade_principal'].notna() & (df_cnae_unicos['atividade_principal'] != '')]['atividade_principal'].value_counts().reset_index()
            if not cnae_counts.empty:
                cnae_counts.columns = ['Atividade', 'Contagem']
                st.dataframe(cnae_counts, use_container_width=True, hide_index=True)
            else:
                st.info("Nenhuma atividade principal encontrada nos CNPJs concluídos.")
        else:
             st.info("Nenhuma consulta concluída com atividade principal.")


    st.subheader("Tabela Completa (Todos os Status da Fila)")

    with st.expander("🔍 Aplicar Filtros na Tabela"):
        col_f1, col_f2, col_f3 = st.columns(3)
        with col_f1:
            cnpjs_para_filtrar = st.text_area("Filtrar por CNPJs (um por linha):", height=150)
            nome_para_filtrar = st.text_input("Filtrar por Nome/Fantasia (contém):")
        with col_f2:
            municipio_para_filtrar = st.text_input("Filtrar por Município (contém):")
            atividade_para_filtrar = st.text_input("Filtrar por Atividade Principal (contém):")
        with col_f3:
            # Removido data_inicio
            data_fim = st.date_input("Filtrar por Data de Adição (Até):", value=None, format="DD/MM/YYYY")
            # Usa df_resultados_brutos para pegar todas as situações possíveis ANTES de qualquer filtro
            situacoes_disponiveis = sorted(df_resultados_brutos[df_resultados_brutos['situacao'].notna()]['situacao'].unique().tolist())
            situacoes_para_filtrar = st.multiselect("Filtrar por Situação Cadastral:", options=situacoes_disponiveis)


    # Aplica os filtros ao DataFrame JÁ PROCESSADO (df_resultados)
    df_filtrado = df_resultados.copy()
    if cnpjs_para_filtrar:
        lista_cnpjs_filtro = [limpar_cnpj(c) for c in cnpjs_para_filtrar.split('\n') if c.strip()]
        if lista_cnpjs_filtro:
            df_filtrado = df_filtrado[df_filtrado['cnpj_consultado'].isin(lista_cnpjs_filtro)]
    if nome_para_filtrar:
        nome_presente = 'nome' in df_filtrado.columns
        fantasia_presente = 'fantasia' in df_filtrado.columns
        if nome_presente and fantasia_presente:
             df_filtrado = df_filtrado[
                df_filtrado['nome'].astype(str).str.contains(nome_para_filtrar, case=False, na=False) |
                df_filtrado['fantasia'].astype(str).str.contains(nome_para_filtrar, case=False, na=False)
             ]
        elif nome_presente:
             df_filtrado = df_filtrado[df_filtrado['nome'].astype(str).str.contains(nome_para_filtrar, case=False, na=False)]
        elif fantasia_presente:
             df_filtrado = df_filtrado[df_filtrado['fantasia'].astype(str).str.contains(nome_para_filtrar, case=False, na=False)]

    if municipio_para_filtrar and 'municipio' in df_filtrado.columns:
        df_filtrado = df_filtrado[df_filtrado['municipio'].astype(str).str.contains(municipio_para_filtrar, case=False, na=False)]
    if atividade_para_filtrar and 'atividade_principal' in df_filtrado.columns:
         df_filtrado = df_filtrado[df_filtrado['atividade_principal'].astype(str).str.contains(atividade_para_filtrar, case=False, na=False)]

    # Filtro de Data Fim
    if 'data_adicionado' in df_filtrado.columns:
        # Garante que a coluna é datetime ciente do fuso (UTC padrão do Firestore)
        df_filtrado['data_adicionado_dt'] = pd.to_datetime(df_filtrado['data_adicionado'], errors='coerce', utc=True)

        if data_fim:
            dt_fim_local = pd.Timestamp(data_fim, tz=str(LOCAL_TIMEZONE)) + pd.Timedelta(days=1)
            dt_fim_utc = dt_fim_local.tz_convert('UTC')
            df_filtrado = df_filtrado[df_filtrado['data_adicionado_dt'].notna() & (df_filtrado['data_adicionado_dt'] < dt_fim_utc)]

        # Remove a coluna temporária após o filtro
        if 'data_adicionado_dt' in df_filtrado.columns:
            df_filtrado = df_filtrado.drop(columns=['data_adicionado_dt'])


    if situacoes_para_filtrar:
        df_filtrado = df_filtrado[df_filtrado['situacao'].isin(situacoes_para_filtrar)]


    df_filtrado_formatado = formatar_colunas_data(df_filtrado)

    # Exibe o DataFrame filtrado e formatado
    st.data_editor(
        df_filtrado_formatado,
        key='data_editor_principal',
        height=400,
        use_container_width=True,
        disabled=True,
        hide_index=False,
        column_config={
             "fixed_columns": {"left": 4}
        }
    )

    st.subheader("Download (Apenas Concluídos e Filtrados)")

    agora_str = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    nome_arquivo = f"consulta_cnpjs_resultados_{agora_str}.xlsx"

    df_para_baixar = df_filtrado[df_filtrado['status'] == 'CONCLUIDO']
    excel_data = df_to_excel(df_para_baixar)

    st.download_button(
        label="📥 Baixar Resultados em Excel (.xlsx)",
        data=excel_data,
        file_name=nome_arquivo,
        mime="application/vnd.openxmlformats-officedocument.sheet"
    )

st.divider()
st.caption("Desenvolvido por Lucas Nunes da Silva | [LinkedIn](https://www.linkedin.com/in/lucas-nunes-da-silva-574604216) | lucasnunesss06@gmail.com")
