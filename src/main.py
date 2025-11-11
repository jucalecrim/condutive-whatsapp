
from fastapi import FastAPI, Header, HTTPException, Query, Request, Body
from fastapi.responses import HTMLResponse, JSONResponse
from typing import Optional
import pacote_back_condutive as pk
from src.core import *

app = FastAPI()

@app.get("/", response_class=HTMLResponse)
def read_root():
    return """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Condutive WhatsApp API</title>
</head>
<body style="font-family: monospace; background: #111; color: #eee; text-align: center;">
    <img src="https://inozxjodesulcewzsbln.supabase.co/storage/v1/object/public/documents/af%20welcome%20msg.png" alt="Welcome AF">
</body>
</html>"""

@app.get("/ver")
def route_ver_ucs(tel: int):
    return ver_ucs(tel)


@app.get("/espera")
def route_ucs_problema(tel: int):
    return ucs_problema(tel)


@app.post("/cadastro_lead")
def route_new_lead(
        tel_agente: int = Query(...),
        nome: str = Query(...),
        telefone: int = Query(...),
        email: Optional[str] = Query(None)

):
    if tel_agente is None:
        raise HTTPException(status_code=400, detail="Telefone do agente não informado")
    elif nome is None:
        raise HTTPException(status_code=400, detail="Nome não informado")
    elif pk.contains_number(nome):
        raise HTTPException(status_code=400, detail="Nome não pode receber números apenas texto")
    elif telefone is None:
        raise HTTPException(status_code=400, detail="Telefone do lead não informado")
    elif (len(str(telefone)) < 10) | (len(str(telefone)) > 11):
        raise HTTPException(status_code=400, detail="Telefone informado incorretamente o número deve apenas numerico com os dois digitos de código da região")
    elif email is not None:
        if pk.is_valid_email(email) == False:
            raise HTTPException(status_code=400, detail="Email informado é invalido")
    try:
        return cadastro_lead(tel_agente, nome, telefone, email)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
        
@app.post("/cadastro_doct")
def route_new_doct(       
        tipo_doct: str = Query(...),
        nr_documento: str = Query(...),
        id_prospect: int = Query(...)
):
    try:
        
        if tipo_doct is None:
            raise HTTPException(status_code=400, detail="Tipo de documento: CPF ou CNPJ faltando")
        elif nr_documento is None:
            raise HTTPException(status_code=400, detail="Número do documento não informado")
        elif pk.contains_number(tipo_doct):
            raise HTTPException(status_code=400, detail=f"Apenas indique o tipo de documento não o número do documento. O dado inserido {tipo_doct} está incorreto.")
        elif (tipo_doct == "CPF") & (len(str(nr_documento)) < 11):
            raise HTTPException(status_code=400, detail="O CPF {} que você inseriu, contem apenas {} digitos. Favor inserir todos os 11 caracteres do CPF".format(nr_documento,len(str(nr_documento))))
        elif (tipo_doct == "CNPJ") & (len(str(nr_documento)) < 14):
            raise HTTPException(status_code=400, detail="O CNPJ {} que você inseriu, contem apenas {} digitos. Favor inserir todos os 14 digitos do CNPJ".format(nr_documento,len(str(nr_documento))))
        else:
            return cadastro_doct(tipo_doct, nr_documento, id_prospect)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/find_disco")
def find_disco(cep:str):
    full_data = pk.check_cep(cep)
    if full_data['exists'] == False:
        return full_data
    else:
        return pk.guess_disco(city = full_data['cidade'], uf = ['uf'])
    
@app.post("/cadastro_uc")
def route_new_uc(       
        nr_documento: str = Query(...),
        id_prospect: int = Query(...),
        cod_agente: int = Query(...),
        cep: str = Query(...),
        valor_fatura: int = Query(...),
        url_doct: str = Query(...)
):
    try:
        valor_fatura = float(valor_fatura)
        return cadastro_uc(tipo_doct, nr_documento, id_prospect)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))