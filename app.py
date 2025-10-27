import streamlit as st
import pandas as pd
import requests
import time
import re
import math
from io import BytesIO
from datetime import datetime

# --- Configuração da Página ---
st.set_page_config(
    page_title="Consultor de CNPJs em Lote",
    page_icon="🔎",
    layout="wide"
)

# --- Funções Auxiliares ---

def limpar_cnpj(cnpj):
    """
    Remove toda a formatação de um CNPJ, deixando apenas dígitos,
    e preenche com zeros à esquerda até completar 14 dígitos.
    """
    cnpj_limpo = re.sub(r'\D', '', str(cnpj))
    cnpj_formatado = cnpj_limpo.zfill(14)
    return cnpj_formatado

def consultar_cnpj(cnpj_limpo):
    """
    Consulta um único CNPJ na API ReceitaWS.
    Retorna um dicionário com os dados ou um status de erro.
    """
    try:
        if len(cnpj_limpo) != 14:
            return {
                "cnpj_consultado": cnpj_limpo,
                "status": "ERROR",
                "detalhes": "CNPJ deve ter 14 dígitos.",
                "message": "CNPJ deve ter 14 dígitos."
            }

        url = f"https://receitaws.com.br/v1/cnpj/{cnpj_limpo}"
        response = requests.get(url, timeout=5)
        
        if response.status_code != 200:
            dados = response.json() if response.content else {}
            return {
                "cnpj_consultado": cnpj_limpo, 
                "status": dados.get("status", "ERROR"),
                "detalhes": dados.get("message", f"Erro HTTP: {response.status_code}"),
                "message": dados.get("message", f"Erro HTTP: {response.status_code}")
            }
        
        dados = response.json()
        dados['cnpj_consultado'] = cnpj_limpo
        return dados

    except requests.exceptions.Timeout:
        return {
            "cnpj_consultado": cnpj_limpo, 
            "status": "ERROR",
            "detalhes": "Timeout (API demorou muito para responder)",
            "message": "Timeout (API demorou muito para responder)"
        }
    except requests.exceptions.RequestException as e:
        return {
            "cnpj_consultado": cnpj_limpo, 
            "status": "ERROR",
            "detalhes": str(e),
            "message": str(e)
        }

def df_to_excel(df):
    """Converte um DataFrame para um arquivo Excel em memória."""
    output = BytesIO()
    df_copia = df.copy()
    
    colunas_data = [
        'abertura', 'data_situacao', 'ultima_atualizacao', 'data_situacao_especial',
        'simples.ultima_atualizacao', 'simei.ultima_atualizacao'
    ]
    
    for col in colunas_data:
        if col in df_copia.columns:
            df_copia[col] = pd.to_datetime(df_copia[col], errors='coerce').dt.strftime('%d/%m/%Y').fillna('N/A')
            
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_copia.to_excel(writer, index=False, sheet_name='Resultados')
    processed_data = output.getvalue()
    return processed_data

def formatar_colunas_data(df):
    """Formata colunas de data de um DF para exibição no Streamlit."""
    df_formatado = df.copy()
    
    colunas_data = [
        'abertura', 'data_situacao', 'ultima_atualizacao', 'data_situacao_especial',
        'simples.ultima_atualizacao', 'simei.ultima_atualizacao'
    ]
    
    for col in colunas_data:
        if col in df_formatado.columns:
            df_formatado[col] = pd.to_datetime(df_formatado[col], errors='coerce').dt.strftime('%d/%m/%Y')
    return df_formatado

def reordenar_colunas(df):
    """Reordena o DataFrame para uma visualização mais lógica."""
    
    colunas_principais = [
        'cnpj_consultado', 'nome', 'fantasia', 'status', 'situacao', 'motivo_situacao',
        'atividade_principal', 'atividade_secundaria', 'quadro_societario'
    ]
    
    colunas_existentes = df.columns.tolist()
    colunas_finais = []
    
    for col in colunas_principais:
        if col in colunas_existentes:
            colunas_finais.append(col)
            
    for col in colunas_existentes:
        if col not in colunas_finais:
            colunas_finais.append(col)
            
    return df[colunas_finais]

# --- Interface do App ---

st.title("🔎 Consultor de CNPJs em Lote (API Gratuita)")

st.markdown("""
Esta ferramenta consulta CNPJs na API pública `ReceitaWS`.
**Como funciona:**
1.  Cole sua lista de CNPJs no campo abaixo (um por linha).
2.  Aguarde a consulta (limite de 3 CNPJs por minuto).
3.  Visualize os resultados e baixe a planilha.

A API gratuita é limitada e pode falhar. CNPJs com erro serão sinalizados.
""")

# --- Área de Input ---
st.header("1. Cole seus CNPJs")
cnpjs_colados = st.text_area(
    "Cole sua lista de CNPJs (um por linha).", 
    height=200,
    placeholder="""Exemplos de formatos aceitos:
- 00.000.000/0001-00
- 12345678000199
- 1234567000199 (zeros à esquerda faltantes serão completados)
"""
)

if 'resultados_df' not in st.session_state:
    st.session_state.resultados_df = pd.DataFrame()

# --- Lógica de Processamento ---
if cnpjs_colados:
    lista_cnpjs_sujos = cnpjs_colados.split('\n')
    lista_cnpjs_limpos = sorted(list(set(
        [limpar_cnpj(c) for c in lista_cnpjs_sujos if c.strip()]
    )))
    total_cnpjs = len(lista_cnpjs_limpos)
    
    if total_cnpjs > 0:
        st.info(f"**{total_cnpjs} CNPJs únicos** foram identificados para consulta.")
        total_lotes = math.ceil(total_cnpjs / 3)
        tempo_estimado_min = total_lotes * 1.02
        
        st.warning(f"Tempo estimado: **~{tempo_estimado_min:.0f} minutos** ({total_lotes} lotes de 3 CNPJs).")

        if st.button("🚀 Iniciar Consulta", type="primary"):
            st.session_state.resultados_df = pd.DataFrame()
            todos_os_resultados_json = []

            progress_bar = st.progress(0.0)
            status_text = st.empty()
            resultados_parciais_placeholder = st.empty()
            
            try:
                for i in range(0, total_cnpjs, 3):
                    lote = lista_cnpjs_limpos[i:i+3]
                    lote_num = (i // 3) + 1
                    
                    status_text.info(f"Consultando lote {lote_num}/{total_lotes}... ({', '.join(lote)})")
                    
                    for cnpj in lote:
                        dados_json = consultar_cnpj(cnpj)
                        todos_os_resultados_json.append(dados_json)
                    
                    progresso_atual = min((i + 3) / total_cnpjs, 1.0)
                    progress_bar.progress(progresso_atual)
                    
                    df_parcial = pd.json_normalize(todos_os_resultados_json)
                    resultados_parciais_placeholder.dataframe(df_parcial)

                    if i + 3 < total_cnpjs:
                        with st.spinner('Aguardando 61 segundos (limite da API)...'):
                            time.sleep(61) 

                status_text.success("Consulta concluída com sucesso!")
                
                if todos_os_resultados_json:
                    df_base = pd.json_normalize(todos_os_resultados_json)

                    if 'qsa' in df_base.columns:
                        df_com_qsa = df_base.explode('qsa')
                        df_com_qsa['quadro_societario'] = df_com_qsa['qsa'].apply(
                            lambda x: x.get('nome') if isinstance(x, dict) else None
                        )
                        df_com_qsa = df_com_qsa.drop(columns=['qsa'])
                    else:
                        df_com_qsa = df_base
                    
                    if 'atividades_secundarias' in df_com_qsa.columns:
                        df_final = df_com_qsa.explode('atividades_secundarias')
                    else:
                        df_final = df_com_qsa

                    if 'atividade_principal' in df_final.columns:
                        df_final['atividade_principal'] = df_final['atividade_principal'].apply(
                            lambda x: x[0]['text'] if isinstance(x, list) and len(x) > 0 and isinstance(x[0], dict) else None
                        )
                    
                    if 'atividades_secundarias' in df_final.columns:
                        df_final['atividades_secundarias'] = df_final['atividades_secundarias'].apply(
                            lambda x: x.get('text') if isinstance(x, dict) else None
                        )
                        df_final = df_final.rename(columns={'atividades_secundarias': 'atividade_secundaria'})
                    
                    st.session_state.resultados_df = reordenar_colunas(df_final)
                
            except Exception as e:
                status_text.error(f"Ocorreu um erro durante o processamento: {e}")
                if todos_os_resultados_json:
                    df_parcial = pd.json_normalize(todos_os_resultados_json)
                    st.session_state.resultados_df = reordenar_colunas(df_parcial)


# --- 3. Exibição dos Resultados ---
if not st.session_state.resultados_df.empty:
    st.header("2. Resultados da Consulta")
    
    df_resultados = reordenar_colunas(st.session_state.resultados_df.copy())
    
    st.subheader("Resumo")
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**Resumo Cadastral (Situação)**")
        if 'situacao' in df_resultados.columns:
            df_unicos = df_resultados[['cnpj_consultado', 'situacao']].drop_duplicates()
            contagem_situacao = df_unicos['situacao'].value_counts().reset_index()
            contagem_situacao.columns = ['Situação', 'Quantidade']
            st.dataframe(contagem_situacao, use_container_width=True)
        else:
            st.info("Nenhuma informação de situação foi retornada.")

    with col2:
        st.write("**Resumo da Consulta**")
        if 'status' in df_resultados.columns:
            erros_count = df_resultados[df_resultados['status'] == 'ERROR']['cnpj_consultado'].nunique()
            st.metric("CNPJs com Erro na Consulta", erros_count)
        else:
            st.metric("CNPJs com Erro na Consulta", 0)
    
    if 'atividade_principal' in df_resultados.columns:
        st.subheader("Atividades Principais (CNAE)")
        df_cnae = df_resultados[['cnpj_consultado', 'atividade_principal']].copy()
        df_cnae_unicos = df_cnae.drop_duplicates()
        cnae_counts = df_cnae_unicos['atividade_principal'].value_counts().reset_index()
        cnae_counts.columns = ['Atividade', 'Contagem']
        st.dataframe(cnae_counts, use_container_width=True)

    st.subheader("Tabela Completa")
    df_resultados_formatado = formatar_colunas_data(df_resultados)
    
    ### MUDANÇA: Oculta o índice e ajusta o congelamento para 3 colunas ###
    st.data_editor(
        df_resultados_formatado,
        height=400,
        use_container_width=True,
        disabled=True, 
        hide_index=True, # Oculta a coluna de índice (0, 1, 2...)
        column_config={
            "fixed_columns": {"left": 3} # Congela as 3 primeiras colunas de dados
        }
    )
    
    st.subheader("Download")
    
    agora_str = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    nome_arquivo = f"consulta_cnpjs_resultados_{agora_str}.xlsx"
    
    excel_data = df_to_excel(df_resultados) 
    
    st.download_button(
        label="📥 Baixar Resultados em Excel (.xlsx)",
        data=excel_data,
        file_name=nome_arquivo,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

st.divider()
st.caption("Desenvolvido por Lucas Nunes da Silva | [LinkedIn](https://www.linkedin.com/in/lucas-nunes-da-silva-574604216) | lucasnunesss06@gmail.com")