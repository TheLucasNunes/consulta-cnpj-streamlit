# consulta-cnpj-streamlit

# 🔎 Consultor de CNPJs em Lote

Este é um aplicativo web simples, construído com Python e Streamlit, para automatizar a consulta de múltiplos CNPJs (em lote) usando a API pública e gratuita [ReceitaWS](https://receitaws.com.br/).

O objetivo deste projeto foi evoluir uma solução manual que eu utilizava em Excel com VBA e Power Query para uma ferramenta web pública, mais inteligente e acessível de qualquer lugar.

## 🚀 Acesse o App

Você pode testar a versão online do aplicativo aqui:

**[COLE O LINK DO SEU APP NO STREAMLIT CLOUD AQUI]**

---

## ✨ Funcionalidades Principais

* **Consulta em Lote:** Cole uma lista de CNPJs (um por linha) e deixe o app fazer o trabalho pesado.
* **Limpeza Automática de Dados:** Aceita CNPJs com ou sem formatação (pontos, barras, traços) e até adiciona zeros à esquerda automaticamente.
* **Respeito ao Limite da API:** Processa 3 CNPJs por minuto para se adequar às regras da API gratuita, com uma barra de progresso e status em tempo real.
* **Resumos Visuais:** Mostra um dashboard simples com a contagem de empresas por **Situação Cadastral** (Ativa, Baixada, Suspensa, etc.) e por **Atividade Principal (CNAE)**.
* **Tratamento de Dados:** Expande campos complexos da API (como Quadro Societário e Atividades) em colunas limpas e legíveis.
* **Download em Excel:** Baixe a tabela completa, limpa, formatada e reordenada, com um clique. O nome do arquivo já vem com data e hora para facilitar a organização.

---

## 🛠️ Tecnologias Utilizadas

Este projeto foi construído utilizando:

* **Python** (Linguagem principal)
* **Streamlit** (Para a criação da interface web)
* **Pandas** (Para a manipulação, limpeza e estruturação dos dados)
* **Requests** (Para fazer as chamadas à API ReceitaWS)

---

## 👤 Autor

**Lucas Nunes da Silva**

* **LinkedIn:** [https://www.linkedin.com/in/lucas-nunes-da-silva-574604216](https://www.linkedin.com/in/lucas-nunes-da-silva-574604216)
* **E-mail:** lucasnunesss06@gmail.com
