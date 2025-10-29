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
        nome_agente, id_agente, nome_lider, id_lider = check1.get("nome_agente"), check1.get("id_agente"), check1.get("nome_lider"), check1.get("id_lider")
        
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
            
            mensagem = mensagem + "você está solicitando um novo lead válido para cadastro. O documento atrelado a conta de luz está em nome de uma pessoa física ou uma empresa? "
            #Só escreve novo lead aqui
            if email is None:
                return_data = {'id_agente':id_agente, 'id_lider':id_lider, 'nome':nome, 'telefone':telefone}
            else:
                return_data = {'id_agente':id_agente, 'id_lider':id_lider, 'nome':nome, 'telefone':telefone, 'email':email}
            try:
                print("Tentando escrever novo lead")
                return_insertion = newLead_whats(return_data)
                if return_insertion['status_code'] == 200:
                    return_data['id_prospect'] = return_insertion['id_prospect']
                status_code = 200
                print(return_insertion)
                prospect = "Novo lead solicitado para cadastro e inserido na base"
            except Exception as e:
                print("Não consegui escrever na base de dados este novo lead")
                prospect = f"Novo lead solicitado para cadastro porém com erro ao escrever no banco de dados: {e}"
                status_code = 409
            
            return {"status_code":200, "status": prospect, "mensagem":mensagem, "actions":{"1":"PF", "2":"PJ"}, 'return_data':return_data}
            
        return {"status_code":status_code, "status": prospect, "mensagem":mensagem, "actions":actions}

    except Exception as e:
        return {'status_code':500, 'detail':str(e)}

def cadastro_doct(tipo_doct, nr_documento, id_prospect, db = 'dev'):
    db = True if db == 'prod' else False
    
    return_data = {"tipo_doct":tipo_doct, "nr_documento":nr_documento, "id_prospect":id_prospect, "db":db}
    insert_query = f"INSERT INTO public.doct_cliente (tipo_doct, nr_documento, id_prospect) VALUES ('{tipo_doct}', '{nr_documento}', '{id_prospect}'); "

    query = f"select tb1.nr_documento, tb1.identificacao, tb1.tipo_doct, tb2.cod_cliente, tb2.apelido_uc, tb2.endereco, tb2.gru_mod, tb2.cons_efp, tb2.valor_fatura, tb2.url_fatura, tb1.created_at from public.doct_cliente as tb1 left join public.dados_uc as tb2 on tb1.nr_documento = tb2.nr_documento where tb1.id_prospect = '{id_prospect}';"
    
    tb_doct = pk.get_db("public", query, db)
    tidy_doct_nr = pk.tidy_doct(tipo_doct, nr_documento)
    status_code = 200
    if tb_doct.shape[0] > 0:
        dt_criacao = str(tb_doct.created_at.iloc[-1])[:19]
        if 'None' in str(list(tb_doct.apelido_uc.unique())):
            #Não faz nada, só devolve o CNPJ pro usuário criar uma UC com esse CNPJ
            documento = 'Documento existente mas sem unidade consumidora atrelada'
            mensagem = f"O {tipo_doct} {tidy_doct_nr} já foi cadastrado no sistema em {dt_criacao}, e não existem unidades consumidoras atreladas a este documento."
            actions = {"1":"Finalizar solicitação", "2":f"Cadastrar pela primeira vez uma unidade consumidora atrelada ao documento {tidy_doct_nr}"}
            return {"status_code": status_code, "status": documento, "mensagem":mensagem, "actions":actions, 'return_data':return_data}
        else:
            #Não faz nada, só avisa que é um CNPJ que já tem UCs cadastradas nele e deixa seguir
            documento = 'Documento existente com unidades já existentes'
            mensagem = f"O {tipo_doct} {tidy_doct_nr} já foi cadastrado no sistema em {dt_criacao}. Veja a lista das unidades consumidoras atreladas a este documento."
            actions = {"1":"Finalizar solicitação", "2":f"Cadastrar uma nova unidade consumidora atrelada ao documento {tidy_doct_nr}"}
            return_data['dados_uc'] = tb_doct.to_dict(orient='records')
            return {"status_code": status_code, "status": documento, "mensagem":mensagem, "actions":actions, 'return_data':return_data}
    else:
        # status_doct = valida_doct(nr_documento)
        documento = "Novo documento solicitado para cadastro."
        check_doct = pk.validate_document(nr_documento, tipo_doct)
        if check_doct.get("valid"):
            if tipo_doct == "CNPJ":
                tidy_doct_nr = pk.tidy_doct(tipo_doct, check_doct.get("number"))
                if check_doct.get('exists') == False:
                    #Deixar seguir mas não encontramos na RFB então ou escreve parcialmente ou nem escreve
                    mensagem = "Esta é a primeira vez que o {} está sendo cadastrado no nosso sistema. O documento é valido mas não foi encontrato na base de dados da Receita Federal. Vamos verificar o que ocorreu mas a solicitação de cadastro permanece válida.".format(tidy_doct_nr)
                    actions = {"1":"Finalizar solicitação", "2":"Seguir e cadastrar uma nova unidade consumidora atrelada ao documento {}".format(tidy_doct_nr)}
                else:
                    #Aqui é mar azul tudo lindo pode escrever a porra toda no banco
                    mensagem = "Esta é a primeira vez que o {} está sendo cadastrado no nosso sistema. O documento é valido encontramos todas as informações necessárias na base da Receita Federal.".format(tidy_doct_nr)
                    actions = {"1":"Finalizar solicitação", "2":"Seguir e cadastrar uma nova unidade consumidora atrelada ao documento {}".format(tidy_doct_nr)}
                    company_data = check_doct.get('company_data')
                    del company_data['cep']
                    del company_data['endereco']
                    
                    key_loop = ["tipo_doct", "id_prospect"]
                    values_string = [tipo_doct, id_prospect]
                    for n in company_data.keys():
                        key_loop.append(n)
                        values_string.append(company_data.get(n))
                        
                    keys_string = pk.trata_lista_query(list(company_data.keys()))
                    keys_string_tidy = keys_string.replace("'", '"')
                    values_string_tidy = pk.trata_lista_query(values_string)
                    del insert_query
                    insert_query = f"INSERT INTO public.doct_cliente {keys_string_tidy} VALUES {values_string_tidy};"

                    return_data['companyData'] = company_data
                   
            else:
                #Aqui escreve mas é CPF então n tem nada pra escrever junto, só o doct mesmo e pela primeira vez
                mensagem = "Esta é a primeira vez que o {} está sendo cadastrado no nosso sistema. O documento é valido.".format(tidy_doct_nr)
                actions = {"1":"Finalizar solicitação", "2":"Seguir e cadastrar uma nova unidade consumidora atrelada ao {} {}".format(tipo_doct, tidy_doct_nr)}
        
            write_return = pk.insert_newDoct(insert_query, db)
            if write_return['status_code'] == 201:
                documento = documento + " Escrita do novo documento no DB ok!"
                status_code = 201
            else:
                status_code = 400
                
            return {"status_code": status_code, "status": documento, "mensagem":mensagem, "actions":actions, 'return_data':return_data}

        else:
            #Doct invalido barra tudo
            mensagem = "Você está tentando cadastrar o novo documento {} mas ele não é valido. Por favor confira as informações e insira um {} válido".format(tidy_doct_nr, tipo_doct)
            actions = {"1":"Finalizar solicitação", "2":"Tentar novamente cadastrar o documento atrelado a fatura de energia"}
            return {"status_code": 406, "status": documento, "mensagem":mensagem, "actions":actions}

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


def newLead_whats(return_data):
    db = 'dev'
    canal = 'agentes whatsapp'
    
    if 'email' in list(return_data.keys()):
        return pk.insert_newLead(id_agente = return_data['id_agente'], id_lider = return_data['id_lider'], canal = canal, nome = return_data['nome'], telefone = return_data['telefone'], email = return_data['email'], db = db)        
    else:
        return pk.insert_newLead(id_agente = return_data['id_agente'], id_lider = return_data['id_lider'], canal = canal, nome = return_data['nome'], telefone = return_data['telefone'], email = None, db = db)
        


# def newUC_whats(return_data):
#     return pk.insert_newLead(id_agente = return_data['id_agente'], id_lider = return_data['id_lider'], canal = 'agentes whatsapp', nome = return_data['nome'], telefone = return_data['telefone'], email = return_data['email'], db = 'dev')