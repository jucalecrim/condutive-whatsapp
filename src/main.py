from fastapi import FastAPI, Header, HTTPException, Query, Request, Body
from fastapi.responses import HTMLResponse, JSONResponse
from typing import Optional
from pydantic import BaseModel
from enum import Enum

import pacote_back_condutive as pk
from src.core import *

app = FastAPI(title="Condutive WhatsApp API")

# -----------------------------
# Models & Enums
# -----------------------------
class DocumentInput(BaseModel):
    url: str
    
class TipoDocumento(str, Enum):
    CPF = "CPF"
    CNPJ = "CNPJ"

class BancoDados(str, Enum):
    PROD = "dev"
    DEV = "prod"
# -----------------------------
# Frontpage
# -----------------------------
@app.get("/", response_class=HTMLResponse)
def read_root():
    return """
    <html>
    <body style="font-family: monospace; background: #111; color: #eee; text-align: center;">
        <img src="https://inozxjodesulcewzsbln.supabase.co/storage/v1/object/public/documents/af%20welcome%20msg.png" 
             alt="Welcome AF" width="400">
    </body>
    </html>
    """
    
@app.get("/check_telAgente", 
         description = "Conferir se o telefone do Agente existe na base de dados ou não")
def check_cel(tel: int, db: BancoDados = Query(...)):
    return pk.check_agent_tel(tel, db.value)

# -----------------------------
# WhatsApp Flows
# -----------------------------
@app.get("/ver", 
         description = "Ver quais unidade consumidoras estão com o comparador disponível para uso")
def route_ver_ucs(tel: int, db: BancoDados = Query(...)):
    return ver_ucs(tel, db.value)

@app.get("/espera",
         description = "Ver quais unidade consumidoras estão aguardando comparador por alguma pendência")
def route_ucs_problema(tel: int, db: BancoDados = Query(...)):
    return ucs_problema(tel, db.value)

# -----------------------------
# Cadastro de Lead
# -----------------------------
@app.post("/cadastro_lead", 
          summary = "Envio de dados cadastraris de um Prospect/Lead", 
          description = """ O Telefone do agente é utilizado para autenticação e conferir todas as informaçõe da base de dados.
          Caso tenha mais de um dado de entrada ligado ao cadastro dos prospects deste agente a função retornará um alerta""")
def route_new_lead(
    db: BancoDados = Query(...),
    tel_agente: int = Query(...),
    nome: str = Query(...),
    telefone: int = Query(...),
    email: Optional[str] = Query(None)
    
):

    if pk.contains_number(nome):
        raise HTTPException(status_code=400, detail="Nome não pode conter números")

    if len(str(telefone)) not in (10, 11):
        raise HTTPException(
            status_code=400,
            detail="Telefone deve ter 10 ou 11 dígitos (com DDD)"
        )

    if email is not None and not pk.is_valid_email(email):
        raise HTTPException(status_code=400, detail="Email informado é inválido")

    try:
        return cadastro_lead(tel_agente, nome, telefone, email, db.value)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -----------------------------
# Cadastro de Documento
# -----------------------------
@app.post("/cadastro_doct",
          summary = "Validação de unidade consumidora atraves de um documento ou registro do mesmo",
          description= """Realizamos os seguintes passos de conferência neste endpoint
                          - Conferir se o documento existe na base
                              - Se sim: conferir se há alguma unidade consumidora atrelada a este documento
                                  - Se sim: Retornar a lista de UCs atreladas a este documento
                                  - Se não: Autorizar o cliente a inserir uma nova unidade consumidora atrelada ao documento existente
                              - Se não: conferir se o documento é valido
                                  - Se sim: Cadastrar o documento corretamente na base de dados e autorizar o usuário a prosseguir
                                  - Se não: Retornar os dados de erro e solicitar os dados novamente""")
def route_new_doct(
    db: BancoDados = Query(...),
    tipo_doct: TipoDocumento = Query(...),
    nr_documento: str = Query(...),
    id_prospect: int = Query(...),
    
):

    try:
        # Validate CPF
        if tipo_doct == TipoDocumento.CPF and len(nr_documento) != 11:
            raise HTTPException(
                status_code=400,
                detail=f"CPF deve conter 11 dígitos. Recebido: {len(nr_documento)}"
            )

        # Validate CNPJ
        if tipo_doct == TipoDocumento.CNPJ and len(nr_documento) != 14:
            raise HTTPException(
                status_code=400,
                detail=f"CNPJ deve conter 14 dígitos. Recebido: {len(nr_documento)}"
            )

        return cadastro_doct(tipo_doct.value, nr_documento, id_prospect, db.value)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -----------------------------
# Distribuidora por CEP
# -----------------------------
@app.get("/find_disco",
         summary = "Identificação de distribuidora a partir de CEP",
         description = """Antes de pegar todos os novos dados para cadastro de uma UC precisamos que o agente identifique qual é a distribuidora que atende a UC.
                         A partir do CEP validamos quais são as distribuidoras que atendem a região do cliente.
                         Este dado será utilizado no próximo end point cadastro_uc""")
def find_disco(cep: str, db: BancoDados = Query(...)):
    full_data = pk.check_cep(cep)

    if not full_data.get("exists"):
        return full_data

    disco = pk.guess_disco(
        city=full_data["cidade"],
        uf=full_data["uf"], 
        db = db.value
    )

    disco["endereco_par"] = (
        f"{full_data.get('logradouro')}, "
        f"{full_data.get('bairro')}, "
        f"{full_data.get('cidade')} - {full_data.get('uf')} "
        f"- CEP: {cep}"
    )

    return disco

# -----------------------------
# URL Check
# -----------------------------

@app.post("/check_url",
          summary = "Verificação se o documento enviado é legível ou não", 
          description = "Este endpoint é utilizado em segundo plano para conferir se o URL enviado é legpivel e apto para envio de extração de dados ou não. Se a opção request_extraction for True os dados serão solicitados para extração na 4docs e retornara as credenciais na URL")
def solicita_extracao(PDF_URL: str, request_extraction: Optional[bool] = Query(False)):
    return pk.solicita_extract_url(PDF_URL, request_extraction)
# -----------------------------
# Cadastro de UC
# -----------------------------
@app.post("/cadastro_uc",
          summary ="Envio de dados para cadastro da UC",
          description = "Nesta etapa vamos receber os dados principais para serem cadastrados na tabela de UCs e realizar conferências para saber se não há duplicidade ou se alguma das informaçõies inseridas não apresentam erros"
          )
def route_new_uc(
    nr_documento: str = Query(...),
    id_prospect: int = Query(...),
    cod_agente: int = Query(...),
    cep: str = Query(...),
    endereco_par: str = Query(...),
    valor_fatura: int = Query(...),
    url_doct: str = Query(...),
    db: BancoDados = Query(...)
):

    try:
        data = {
            "nr_documento": nr_documento,
            "id_prospect": id_prospect,
            "cod_agente": cod_agente,
            "cep": cep,
            "endereco": endereco_par,
            "valor_fatura": float(valor_fatura),
        }

        return cadastro_uc(data, url_doct, db.value)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
