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

# -----------------------------
# WhatsApp Flows
# -----------------------------
@app.get("/ver")
def route_ver_ucs(tel: int):
    return ver_ucs(tel)

@app.get("/espera")
def route_ucs_problema(tel: int):
    return ucs_problema(tel)

# -----------------------------
# Cadastro de Lead
# -----------------------------
@app.post("/cadastro_lead")
def route_new_lead(
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
        return cadastro_lead(tel_agente, nome, telefone, email)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -----------------------------
# Cadastro de Documento
# -----------------------------
@app.post("/cadastro_doct")
def route_new_doct(
    tipo_doct: TipoDocumento = Query(...),
    nr_documento: str = Query(...),
    id_prospect: int = Query(...)
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

        return cadastro_doct(tipo_doct.value, nr_documento, id_prospect)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -----------------------------
# Distribuidora por CEP
# -----------------------------
@app.get("/find_disco")
def find_disco(cep: str):
    full_data = pk.check_cep(cep)

    if not full_data.get("exists"):
        return full_data

    disco = pk.guess_disco(
        city=full_data["cidade"],
        uf=full_data["uf"]
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
@app.post("/check_url")
def check_document(doc: DocumentInput):
    return pk.url_check(doc)

# -----------------------------
# Cadastro de UC
# -----------------------------
@app.post("/cadastro_uc")
def route_new_uc(
    nr_documento: str = Query(...),
    id_prospect: int = Query(...),
    cod_agente: int = Query(...),
    cep: str = Query(...),
    endereco_par: str = Query(...),
    valor_fatura: int = Query(...),
    url_doct: str = Query(...)
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

        return cadastro_uc(data, url_doct)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
