import firebase_admin
from firebase_admin import credentials, firestore
import requests
import time
import re
import json

# --- Funções de Consulta (Copiadas do app antigo) ---

def consultar_cnpj(cnpj_limpo):
    """
    Consulta um único CNPJ na API ReceitaWS.
    Retorna um dicionário com os dados ou um status de erro.
    """
    print(f"  > Consultando CNPJ: {cnpj_limpo}")
    try:
        if len(cnpj_limpo) != 14:
            return {"status": "ERROR", "message": "CNPJ local inválido, não tem 14 dígitos"}

        url = f"https://receitaws.com.br/v1/cnpj/{cnpj_limpo}"
        response = requests.get(url, timeout=10) # Timeout um pouco maior
        
        if response.status_code != 200:
            dados = response.json() if response.content else {}
            return {
                "status": dados.get("status", "ERROR"),
                "message": dados.get("message", f"Erro HTTP: {response.status_code}")
            }
        
        dados = response.json()
        # Adiciona o CNPJ consultado aos dados para referência futura
        dados['cnpj_consultado'] = cnpj_limpo
        return dados

    except requests.exceptions.Timeout:
        return {"status": "ERROR", "message": "Timeout (API demorou muito para responder)"}
    except requests.exceptions.RequestException as e:
        return {"status": "ERROR", "message": str(e)}

# --- Inicialização do Firebase (Obrigatório) ---
try:
    cred = credentials.Certificate('serviceAccountKey.json')
    firebase_admin.initialize_app(cred)
    db = firestore.client()
    print("Worker conectado ao Firebase com SUCESSO.")
except Exception as e:
    print(f"ERRO: Não foi possível conectar ao Firebase.")
    print(f"Certifique-se que o arquivo 'serviceAccountKey.json' está na mesma pasta.")
    print(f"Detalhes do erro: {e}")
    exit() # Encerra o worker se não puder conectar

# --- Loop Principal do Worker ---

def processar_fila():
    """Loop principal do worker."""
    print("🚀 Worker iniciado. Procurando tarefas 'PENDENTE'...")
    
    while True:
        try:
            # 1. Busca 3 tarefas pendentes, ordenadas pela mais antiga
            tarefas_ref = db.collection('tarefas').where(
                filter=firestore.FieldFilter('status', '==', 'PENDENTE')
            ).order_by(
                'data_adicionado', direction=firestore.Query.ASCENDING
            ).limit(3).stream()

            tarefas_encontradas = list(tarefas_ref)

            if not tarefas_encontradas:
                # Se não há tarefas, dorme por 10s e checa de novo
                # print("Nenhuma tarefa pendente. Aguardando 10s...")
                time.sleep(10)
                continue

            print(f"\nEncontrado lote de {len(tarefas_encontradas)} tarefa(s).")

            # 2. Processa as 3 tarefas
            for tarefa in tarefas_encontradas:
                doc_id = tarefa.id # O ID do documento é o próprio CNPJ
                print(f"Iniciando tarefa: {doc_id}")

                # Marca como "processando" para evitar que outro worker pegue
                tarefa_ref = db.collection('tarefas').document(doc_id)
                tarefa_ref.update({'status': 'PROCESSANDO'})

                # A MÁGICA: Chama a API
                resultado_json = consultar_cnpj(doc_id)
                
                # Determina o status final
                status_final = resultado_json.get("status", "ERROR")
                if status_final != "ERROR":
                    status_final = "CONCLUIDO"

                # 3. Salva o resultado final de volta no Firebase
                update_data = {
                    'status': status_final,
                    'resultado_json': resultado_json, # Salva o JSON inteiro
                    'data_conclusao': firestore.SERVER_TIMESTAMP
                }
                
                # Salva dados principais no nível superior para facilitar a leitura do app
                if status_final == "CONCLUIDO":
                    update_data['nome'] = resultado_json.get('nome')
                    update_data['situacao'] = resultado_json.get('situacao')
                
                tarefa_ref.update(update_data)
                print(f"Tarefa {doc_id} finalizada com status: {status_final}")

            # 4. Respeita o limite da API
            # Só dorme se tiver processado um lote
            print(f"Lote concluído. Aguardando 61 segundos (limite da API)...")
            time.sleep(61)

        except Exception as e:
            print(f"Erro inesperado no loop do worker: {e}")
            time.sleep(30) # Dorme 30s se algo der muito errado

# --- Ponto de Entrada ---
if __name__ == "__main__":
    processar_fila()