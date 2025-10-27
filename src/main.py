
from fastapi import FastAPI, Header, HTTPException, Query, Request, Body
from fastapi.responses import HTMLResponse, JSONResponse
from typing import Optional
import pacote_back_condutive as pk
from src.core import *

app = FastAPI()

@app.get("/", response_class=HTMLResponse)
def read_root():
    return """O repo está funcionando"""

@app.get("/test")
def funct():
    return  hello_world()

@app.get("/captcha")
def get_captcha():
    return pk.charada()

