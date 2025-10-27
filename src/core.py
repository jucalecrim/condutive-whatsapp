import pandas as pd
# import datetime as dt
# import numpy as np
# import re
# import random
# import requests
# from validate_docbr import CPF, CNPJ
import pacote_back_condutive as pk

def stauts_ucs(tel):
    check1 = pk.check_agent_tel(tel)
    if check1['status_code'] == 200:
        query = f"""
                    select
                      tb3.nome as nome_lead,
                      tb3.created_at as criado_em,
                      tb1.id_uc,
                      tb1.apelido_uc,
                      tb2.nr_documento,
                      ad1.status,
                      ad1.created_at as data_status,
                      tb2.tipo_doct,
                      tb3.id_agente,
                      ca.id_lider
                    from
                      dados_uc as tb1
                      left join doct_cliente as tb2 on tb1.nr_documento = tb2.nr_documento
                      left join prospect as tb3 on tb2.id_prospect = tb3.id
                      left join agentes.cadastro_agente as ca on tb3.id_agente = ca.id
                      left join agentes.audit_1 as ad1 on ad1.id_uc = tb1.id_uc
                      WHERE ca.telefone = '{tel}'
                      order by tb1.created_at desc;"""
    
        df = pk.get_db('fornecedores', query, False)
        return df
    else:
        return check1
    
    
def ver_ucs(tel):
    try:
        df_all = stauts_ucs(tel)
        if type(df_all) == dict:
            return df_all
        
        df_filter = df_all[df_all.status == 'Aprovado']
        
        query = "select id, uc_id as id_uc, comparator_type from public.comparators_history where uc_id in {};".format(str([int(x) for x in df_filter.id_uc.unique()]).replace("[", "(").replace("]", ")"))
        comparadores = pk.get_db('public', query, False)
        
        new_dict = {}
        for i in df_filter.id_uc:
            cut_df = comparadores[comparadores.id_uc == i]
            if cut_df.shape[0] > 0:
                if cut_df.comparator_type.iloc[-1] == "GD":
                    link_comparador = 'https://agentes.condutive.com/proposta?comparatorId={}&type=GD'.format(cut_df.id.iloc[-1])
                else:
                    link_comparador = f'https://agentes.condutive.com/proposta_acl?ucId={i}&type=ACL'
            else:
                link_comparador = "Comparador ainda não solicitado"
            
            new_dict[i] = {'Nome do lead':df_filter[df_filter.id_uc == i].nome_lead.iloc[-1],
                           'Data de Criação':str(df_filter[df_filter.id_uc == i].criado_em.iloc[-1].strftime("%Y-%m-%d %H:%M:%S")),
                           'ID da UC':i,
                           'Apelido da UC':df_filter[df_filter.id_uc == i].apelido_uc.iloc[-1],
                           'Link Comparador': link_comparador,
                           'Documento': df_filter[df_filter.id_uc == i].tipo_doct.iloc[-1] + ": " + df_filter[df_filter.id_uc == i].nr_documento.iloc[-1],
                           "Data de Aprovação":str(df_filter[df_filter.id_uc == i].data_status.iloc[-1].strftime("%Y-%m-%d %H:%M:%S"))}
            
            del link_comparador
        dict_return = {"status_code":200, 'response':new_dict, 'message':'Estas são as unidades consumidoras aprovadas para comparação'}
        return new_dict
    except Exception as e:
        dict_return = {"status_code":400, 'response':str(e)}
        return dict_return

def ucs_problema(tel):
    try:
        df_all = stauts_ucs(tel)
        if type(df_all) == dict:
            return df_all
        
        list_aprovado = df_all[df_all.status == 'Aprovado'].id_uc.unique()
        df_filter = df_all[df_all.status != 'Aprovado']
        #TODO erro neste DF sendo originado
        
        new_dict = {}
        for i in df_filter.id_uc:
            if i in list_aprovado:
                pass
            else:
                new_dict[i] = {'Nome do lead':df_filter[df_filter.id_uc == i].nome_lead.iloc[-1],
                               'Data de Criação':str(df_filter[df_filter.id_uc == i].criado_em.iloc[-1].strftime("%Y-%m-%d %H:%M:%S")),
                               'ID da UC':i,
                               'Apelido da UC':df_filter[df_filter.id_uc == i].apelido_uc.iloc[-1],
                               'Documento': df_filter[df_filter.id_uc == i].tipo_doct.iloc[-1] + ": " + df_filter[df_filter.id_uc == i].nr_documento.iloc[-1],
                               'Status': df_filter[df_filter.id_uc == i].status.iloc[-1],
                               "Última atualização":str(df_filter[df_filter.id_uc == i].data_status.iloc[-1].strftime("%Y-%m-%d %H:%M:%S"))}
            if new_dict == {}:
                return {"status_code":400, 'message':"Todas as unidades consumidoras que você cadastrou estão aprovadas e não precisam de edição"}
            else:
                return {"status_code":200, 'response':new_dict, 'message':{'1':"Estas são as unidades consumidoras que apresentam inconsistências. Para aprovação faça o login na sua área logada -> navegue para UCs -> Selecione a unidade desejada e faça as devidas alterações. Caso persistir as dúvidas contacte seu lider.", '2':"Aqui está o link para acessar a sua área logada: https://agentes.condutive.com/auth"}}
    except Exception as e:
        dict_return = {"status_code":400, 'response':str(e)}
        return dict_return

def cadastro_lead(tel_agente, nome, telefone, email):

    #Parte 1: Checkar id do agente
    check1 = pk.check_agent_tel(tel_agente)
    if check1['status_code'] != 200:
        return check1
    
    try:
        nome_agente, id_agente, nome_lider = check1.get("nome_agente"), check1.get("id_agente"), check1.get("nome_lider")
        
        mensagem = f"{nome_agente} recebemos sua solicitação e identificamos que "
        
        #Parte 2: Ver se os dados do lead já estão cadastrados na base
        if email is None:
            query1 = f"SELECT id as id_prospect, nome, telefone, email, id_agente, created_at FROM public.prospect WHERE nome LIKE '%{nome}%' OR telefone = {telefone};"
        else:
            query1 = f"SELECT id as id_prospect, nome, telefone, email, id_agente, created_at FROM public.prospect WHERE nome LIKE '%{nome}%' OR telefone = {telefone} OR email = '{email}';"
        tb_prospect = pk.get_db("public", query1, False)
        
        dados_duplicados = ""
        if tb_prospect.shape[0] > 0:
            
            if tb_prospect['nome'].iloc[0] == nome:
                dados_duplicados = dados_duplicados + f"{nome} já foi cadastrado anteriormente, "
            if tb_prospect['telefone'].iloc[0] == telefone:
                dados_duplicados = dados_duplicados + f"o telefone inserido: {telefone} já cadastrado na nossa base de dados, "
            if tb_prospect['email'].iloc[0] == email:
                dados_duplicados =  dados_duplicados + f"o email informado: {email} já foi inserido previamente, "
                
            if dados_duplicados != "":
                mensagem = mensagem + dados_duplicados[:len(dados_duplicados)-2]
                
            if int(tb_prospect.id_agente.iloc[-1]) != id_agente:
                #Cadastrado por outro agente
                prospect =  "Lead já cadastrado anteriormente por outro agente"
                mensagem = mensagem + " o lead que você informou já foi inserido por outro agente previamente. Por favor entre em contato com seu líder {}".format(nome_lider)
                actions = {"1":"Finalizar solicitação"}
            elif tb_prospect.shape[0] > 1:
                #Cadastrado por mais de um agente
                prospect =  "Lead já cadastrado anteriormente e por mais de um agente"
                mensagem = mensagem + " o lead que você está tentando cadastrar já foi cadastrado por mais de um agente anteriormente. Por favor entre em contato com seu líder {}".format(nome_lider)
                actions = {"1":"Finalizar solicitação"}
            else:
                #Cadastrado por ele mesmo anteriormente
                prospect =  "Lead já cadastrado anteriormente"
                id_prospect = int(tb_prospect.id_prospect.iloc[-1])
                query = f'select cod_cliente, nr_documento, endereco, gru_mod, cons_efp, valor_fatura, url_fatura, created_at  from public.dados_uc where nr_documento in (select nr_documento from public.doct_cliente where id_prospect = {id_prospect});'
                dados_uc = pk.get_db('public', query, False)
                actions = {"1":"Finalizar solicitação", "2":"Cadastrar uma nova unidade consumidora para este mesmo lead"}
                if dados_uc.shape[0] > 0:
                    mensagem = mensagem + " por você no dia "+ str(tb_prospect.created_at.iloc[0])[:19] + ". Veja abaixo a lista das unidades consumidoras atreladas a este lead."
                    return {"status_code":200, "status": prospect, "mensagem":mensagem, "actions":actions, "return_data":{"id_prospect":id_prospect, "dados_uc":dados_uc.to_dict(orient='records')}}
                else:
                    mensagem = mensagem + " por você no dia "+ str(tb_prospect.created_at.iloc[0])[:19] + ", porém ainda não existem unidades consumidoras atreladas a este lead."
                    return {"status_code":200, "status": prospect, "mensagem":mensagem, "actions":actions, "return_data":{"id_prospect":id_prospect}}
        else:
            #Novo cadastro de lead
            prospect = "Novo lead solicitado para cadastro"
            mensagem = mensagem + "você está solicitando um novo lead válido para cadastro. O documento atrelado a conta de luz está em nome de uma pessoa física ou uma empresa? "
            return {"status_code":200, "status": prospect, "mensagem":mensagem, "actions":{"1":"PF", "2":"PJ"}, 'return_data':{'nome':nome, 'telefone':telefone, 'email':email}}
            
        return {"status_code":200, "status": prospect, "mensagem":mensagem, "actions":actions}

    except Exception as e:
        return {'status_code':500, 'detail':str(e)}
    
    
def cadastro_uc(cep, valor_fatura, nr_documento = None, doct_file = None):
    #Parte 4: Conferir se os dados à serem inseridos na UC são novos ou não
    if nr_documento != None:
        query = f"SELECT * FROM public.dados_uc WHERE nr_documento = '{nr_documento}' AND (cep = '{cep}' OR valor_fatura = '{valor_fatura}');"
    else:
        query = f"SELECT * FROM public.dados_uc WHERE valor_fatura = '{valor_fatura}' AND cep = '{cep}';"
    uc_v1 = pk.get_db("public", query, False)
    actions = {"1":"Finalizar solicitação"}
    
    if uc_v1.shape[0] == 0:
        return_cep = pk.check_cep(str(cep))
        if return_cep.get('valid'):
            actions['2'] = "Acessar sua área logada em agentes.condutive.com/auth"
            if return_cep.get('exists'):
                unidade = "Nova UC na base de dados com CEP válido e existente"
                mensagem = "Parabens pelo cadastro desta nova unidade consumidora! O CEP e valor da conta informados estão validos. Por favor acompanhe sua unidade consumidora na sua área logada."
                return {"status_code":200, "status": unidade, "mensagem":mensagem, "actions":actions}
            else:
                unidade = "Nova UC na base de dados com CEP válido mas não encontrado"
                mensagem = "Vimos você quer cadastrar uma nova unidade consumidora. O CEP inserido parece ser válido mas não foi encontrado na base dos correios. Vamos analisar estes dados e seu líder entrará em contato sobre esta unidade em breve."
                return {"status_code":200, "status": unidade, "mensagem":mensagem, "actions":actions}
        else:
            unidade = "Nova UC na base de dados com CEP inválido"
            mensagem = f"O CEP {cep} da unidade consumidora que você está tentando cadastrar não é valido"
            actions['2'] = "Enviar dados novamente"
            return {"status_code":400, "status": unidade, "mensagem":mensagem, "actions":actions}
            
        #TODO: Ler dados da fatura e asumir premissas a partir de dados enviados

    elif uc_v1.shape[0] == 1:
        unidade = "Dado dupliicado na base de dados, conferido via CEP e valor de fatura"
        apelido_uc = uc_v1['apelido_uc'].iloc[-1]
        mensagem = f"O dado inserido está duplicado na base de dados verificamos que a unidade {apelido_uc} registrada no CEP {cep} com o valor de R$ {valor_fatura}"

        return {"status_code":400, "status": unidade, "mensagem":mensagem, "actions":actions, 'return_data':{"insert_data":{"cep":cep, "nr_documento":nr_documento, "valor_fatura":valor_fatura},"dados_uc":uc_v1.to_dict(orient='records')}}
    else:
        unidade = "Dado duplicado para mais de uma UC"
        mensagem = "Foi encontrada mais de uma unidade consumidora neste local com caracteristicas similares. Vamos ter que analisar este caso em particular e seu lider entratá em contrato com você em breve. Obrigado. "
        return {"status_code":400, "status": unidade, "mensagem":mensagem, "actions":actions, 'return_data':{"insert_data":{"cep":cep, "nr_documento":nr_documento, "valor_fatura":valor_fatura},"dados_uc":uc_v1.to_dict(orient='records')}}
    