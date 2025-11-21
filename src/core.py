import pandas as pd
import pacote_back_condutive as pk
import datetime as dt
from dateutil.relativedelta import relativedelta

def stauts_ucs(tel, db = 'prod'):
    check1 = pk.check_agent_tel(tel, db)
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
    
        df = pk.get_db('fornecedores', query, db)
        return df
    else:
        return check1
    
def ver_ucs(tel, db = 'prod'):
    try:
        df_all = stauts_ucs(tel, db)
        if type(df_all) == dict:
            return df_all
        
        df_filter = df_all[df_all.status == 'Aprovado']
        
        query = "select id, uc_id as id_uc, comparator_type from public.comparators_history where uc_id in {};".format(str([int(x) for x in df_filter.id_uc.unique()]).replace("[", "(").replace("]", ")"))
        comparadores = pk.get_db('public', query, db)
        
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

def ucs_problema(tel, db = 'prod'):
    try:
        df_all = stauts_ucs(tel, db)
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

def cadastro_lead(tel_agente, nome, telefone, email, db = 'dev'):
    #Parte 1: Checkar id do agente
    check1 = pk.check_agent_tel(tel_agente, db)
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
        tb_prospect = pk.get_db("public", query1, db)
        
        dados_duplicados = ""
        if tb_prospect.shape[0] > 0:
            
            if tb_prospect['nome'].iloc[0] == nome:
                dados_duplicados = dados_duplicados + f"{nome} já foi cadastrado anteriormente, "
            if tb_prospect['telefone'].iloc[0] == telefone:
                dados_duplicados = dados_duplicados + f"o telefone inserido: {telefone} já foi cadastrado na nossa base de dados, "
            if tb_prospect['email'].iloc[0] == email:
                if email is None:
                    pass
                else:
                    dados_duplicados =  dados_duplicados + f"o email informado: {email} já foi inserido previamente, "
                
            if dados_duplicados != "":
                mensagem = mensagem + dados_duplicados[:len(dados_duplicados)-2]
                
            if int(tb_prospect.id_agente.iloc[-1]) != id_agente:
                #Cadastrado por outro agente
                prospect =  "Lead já cadastrado anteriormente por outro agente"
                mensagem = mensagem + " o lead que você informou já foi inserido por outro agente previamente. Por favor entre em contato com seu líder {}".format(nome_lider)
                actions = {"1":"Finalizar solicitação"}
                status_code = 403
                subject = f"Erro 403 cadastro lead WPP {nome}"
                pk.notify_error("juca@condutive.com.br", prospect + f" Agente: {nome_agente} ({id_agente})", subject, db)
                
            elif tb_prospect.shape[0] > 1:
                #Cadastrado por mais de um agente
                prospect =  "Lead já cadastrado anteriormente e por mais de um agente"
                mensagem = mensagem + " o lead que você está tentando cadastrar já foi cadastrado por mais de um agente anteriormente. Por favor entre em contato com seu líder {}".format(nome_lider)
                actions = {"1":"Finalizar solicitação"}
                status_code = 403
                subject = f"Erro 403 cadastro lead WPP {nome}"
                pk.notify_error("juca@condutive.com.br", prospect + f" Agente: {nome_agente} ({id_agente})", subject, db)
                
            else:
                #Cadastrado por ele mesmo anteriormente
                prospect =  "Lead já cadastrado anteriormente"
                id_prospect = int(tb_prospect.id_prospect.iloc[-1])
                query = f'select cod_cliente, nr_documento, endereco, gru_mod, cons_efp, valor_fatura, url_fatura, created_at  from public.dados_uc where nr_documento in (select nr_documento from public.doct_cliente where id_prospect = {id_prospect});'
                dados_uc = pk.get_db('public', query, db)
                actions = {"1":"Finalizar solicitação", "2":"Cadastrar uma nova unidade consumidora para este mesmo lead"}
                if dados_uc.shape[0] > 0:
                    mensagem = mensagem + " por você no dia "+ str(tb_prospect.created_at.iloc[0])[:19] + ". Veja abaixo a lista das unidades consumidoras atreladas a este lead."
                    #Status code of data duplicated
                    return {"status_code":208, "status": prospect, "mensagem":mensagem, "actions":actions, "return_data":{"id_prospect":id_prospect, "dados_uc":dados_uc.to_dict(orient='records')}}
                else:
                    mensagem = mensagem + " por você no dia "+ str(tb_prospect.created_at.iloc[0])[:19] + ", porém ainda não existem unidades consumidoras atreladas a este lead."
                    #Status code of data duplicated but no uc
                    return {"status_code":206, "status": prospect, "mensagem":mensagem, "actions":actions, "return_data":{"id_prospect":id_prospect}}
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
                return_insertion = newLead_whats(return_data, db)
                if return_insertion['status_code'] == 201:
                    return_data['id_prospect'] = return_insertion['id_prospect']
                    status_code = 201
                print(return_insertion)
                prospect = "Novo lead solicitado para cadastro e inserido na base"
            except Exception as e:
                print("Não consegui escrever na base de dados este novo lead")
                prospect = f"Novo lead {nome} solicitado para cadastro porém com erro ao escrever no banco de dados: {e}"
                status_code = 417
                subject = f"Erro 417 cadastro lead WPP {nome}"
                pk.notify_error("juca@condutive.com.br", prospect + f" Agente: {nome_agente} ({id_agente})", subject, db)
            
            return {"status_code":status_code, "status": prospect, "mensagem":mensagem, "actions":{"1":"PF", "2":"PJ"}, 'return_data':return_data}
            
        return {"status_code":status_code, "status": prospect, "mensagem":mensagem, "actions":actions}

    except Exception as e:
        subject = f"Erro ao cadastrar novo lead WPP {nome}"
        message = f"nome lead: {nome}, telefone: {telefone}, tel agente: {tel_agente} ERRO: {e}"
        pk.notify_error("juca@condutive.com.br", message, subject, db)
        return {'status_code':500, 'detail':str(e)}

def cadastro_doct(tipo_doct, nr_documento, id_prospect, db = 'dev'):

    return_data = {"tipo_doct":tipo_doct, "nr_documento":nr_documento, "id_prospect":id_prospect, "db":db}

    insert_query = f"INSERT INTO public.doct_cliente (tipo_doct, nr_documento, id_prospect) VALUES ('{tipo_doct}', '{nr_documento}', '{id_prospect}'); "

    query = f"select tb1.nr_documento, tb1.identificacao, tb1.tipo_doct, tb2.cod_cliente, tb2.apelido_uc, tb2.endereco, tb2.gru_mod, tb2.cons_efp, tb2.valor_fatura, tb2.url_fatura, tb1.created_at from public.doct_cliente as tb1 left join public.dados_uc as tb2 on tb1.nr_documento = tb2.nr_documento where tb1.id_prospect = '{id_prospect}' AND tb1.nr_documento = '{nr_documento}';"
    
    tb_doct = pk.get_db("public", query, db)
    tidy_doct_nr = pk.tidy_doct(tipo_doct, nr_documento)
    status_code = 100

    if tb_doct.shape[0] > 0:
        dt_criacao = str(tb_doct.created_at.iloc[-1])[:19]
        if 'None' in str(list(tb_doct.apelido_uc.unique())):
            #Não faz nada, só devolve o CNPJ pro usuário criar uma UC com esse CNPJ
            documento = 'Documento existente mas sem unidade consumidora atrelada'
            mensagem = f"O {tipo_doct} {tidy_doct_nr} já foi cadastrado no sistema em {dt_criacao}, e não existem unidades consumidoras atreladas a este documento."
            actions = {"1":"Finalizar solicitação", "2":f"Cadastrar pela primeira vez uma unidade consumidora atrelada ao documento {tidy_doct_nr}"}
            return {"status_code": 204, "status": documento, "mensagem":mensagem, "actions":actions, 'return_data':return_data}
        else:
            #Não faz nada, só avisa que é um CNPJ que já tem UCs cadastradas nele e deixa seguir
            documento = 'Documento existente com unidades já existentes'
            mensagem = f"O {tipo_doct} {tidy_doct_nr} já foi cadastrado no sistema em {dt_criacao}. Veja a lista das unidades consumidoras atreladas a este documento."
            actions = {"1":"Finalizar solicitação", "2":f"Cadastrar uma nova unidade consumidora atrelada ao documento {tidy_doct_nr}"}
            return_data['dados_uc'] = tb_doct.to_dict(orient='records')
            return {"status_code": 207, "status": documento, "mensagem":mensagem, "actions":actions, 'return_data':return_data}
    else:
        # status_doct = valida_doct(nr_documento)
        documento = "Novo documento solicitado para cadastro."
        check_doct = pk.validate_document(nr_documento, tipo_doct)
        if check_doct.get("valid"):
            if tipo_doct == "CNPJ":
                tidy_doct_nr = pk.tidy_doct(tipo_doct, check_doct.get("number"))
                if check_doct.get('exists') == False:
                    status_code = 206
                    #Deixar seguir mas não encontramos na RFB então ou escreve parcialmente ou nem escreve
                    mensagem = "Esta é a primeira vez que o {} está sendo cadastrado no nosso sistema. O documento é válido, mas não foi encontrado na base de dados da Receita Federal. Vamos verificar o que ocorreu mas a solicitação de cadastro permanece válida.".format(tidy_doct_nr)
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
                        
                    keys_string = pk.trata_lista_query(key_loop)
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
                if status_code == 206:
                    documento = documento + " Escrita do novo documento no DB, mas não encontrado na RFB"
                else:
                    documento = documento + " Escrita do novo documento no DB ok!"
                    status_code = write_return['status_code']
            else:
                documento = documento + " Erro ao escrever novo documento no DB: "  + write_return['message']
                subject = f"Erro 417 cadastro documento WPP {tipo_doct} - {nr_documento}"
                pk.notify_error("juca@condutive.com.br", documento + f" {nr_documento} - Prospect: {id_prospect}", subject, db)
                status_code = 417
                
            return {"status_code": status_code, "status": documento, "mensagem":mensagem, "actions":actions, 'return_data':return_data}

        else:
            #Doct invalido barra tudo
            mensagem = "Você está tentando cadastrar o novo documento {} mas ele não é valido. Por favor confira as informações e insira um {} válido".format(tidy_doct_nr, tipo_doct)
            actions = {"1":"Finalizar solicitação", "2":"Tentar novamente cadastrar o documento atrelado a fatura de energia"}
            subject = f"Erro 406 cadastro documento WPP {tipo_doct} - {nr_documento}"
            pk.notify_error("juca@condutive.com.br", mensagem + f" Prospect: {id_prospect}", subject, db)
            
            return {"status_code": 406, "status": documento, "mensagem":mensagem, "actions":actions}


#Maria Eduarda
#Valeria
#Elisangela
#Vander Hayder

# dicty_initial = {
#     "nr_documento": '04745433743',
#     "id_prospect": 106,
#     "cod_agente": 383,
#     "cep": '28910420',
#     "endereco": 'Rua INDIA 0 00000 852 CSA 2 JDM CAICARA, CABO FRIO - RJ',
#     "valor_fatura": float(427.66),
# }

# def cadastro_uc(dicty_initial, url_doct, request_extraction = True, db = 'dev'): 
#     try:
    
#         query = "SELECT * FROM public.dados_uc WHERE nr_documento = '{nr_documento}' AND cod_agente = '{cod_agente}' AND (cep = '{cep}' OR valor_fatura = '{valor_fatura}');"
#         uc_v1 = pk.get_db("public", query.format(**dicty_initial), db)
        
#         #Dados padrão de insert na base de dados
#         insert_dict = dicty_initial.copy()
#         insert_dict['url_fatura'] = url_doct
        
#         #Dados padrão de retorno da função
#         status = {"write_status_code": 100, "insertion_type":"Cadastro simples", "requested_extraction":request_extraction, 'db':db, 'dicty_initial':insert_dict}
#         messages = dict()
#         block_comparador = True
#         actions = {"1":"Finalizar solicitação"}
        
#         if uc_v1.shape[0] < 2:
#             messages['initial_df'] = "DF inicial com {} linhas".format(uc_v1.shape[0])
#             #Nova UC ou UC com dados existentes
#             url_status = pk.url_check(url_doct, request_extraction)
#             status['doct_readable'] = url_status['readable']
#             if url_status['readable'] == False:
#                 status['insertion_type'] = "Cadastro simples"
#                 messages['leitura_doct'] = "Não foi possível realizar a leitura deste documento"
#                 if uc_v1.shape[0] == 1:
#                     messages['leitura_doct'] = messages['leitura_doct'] + " Por isso não vamos atualizar os dados já cadastrados na base."
#                     dict_return = {'status_code':403, 
#                                    'status':status,
#                                    'messages':messages,
#                                    'actions':actions}
#                 else:
#                     insert_query = pk.key_loops(payload = insert_dict, table = "public.dados_uc")
#                     returno_insert_fatura = pk.get_db('public', insert_query, db)
#                     if returno_insert_fatura['status_code'] == 201:
#                         status['write_status_code'] = 102
#                         id_uc = pk.get_db("public", query.format(**dicty_initial), db).id_uc.iloc[-1]
#                         actions['2'] = f"Acessar área logada e atualizar dados da, na UC {id_uc}"
#                         messages['comparador'] = "Comparador não solicitado dado o cadastro simples"
#                         dict_return = {'status_code':102, 
#                                        'link':'https://agentes.condutive.com/',
#                                        'id_uc':id_uc,
#                                        'status':status,
#                                        'messages':messages,
#                                        'actions':actions}
#                     else:
#                         del returno_insert_fatura['status_code']
#                         messages['dados_ucInsert'] = "Erro ao inserir dados do cadastro simples da UC {}".format(returno_insert_fatura)
#                         dict_return = {'status_code':417, 
#                                        'status':status,
#                                        'messages':messages,
#                                        'actions':actions}
                        
#             else:
#                 from pathlib import Path
#                 status['doc_type'] = Path(url_doct).suffix.lower()
                
#                 if (request_extraction == False) or (status['doc_type'] != ".pdf"):               
#                     for i in url_status['guessed'].keys():
#                         insert_dict[i] = url_status['guessed'].get(i)
#                     msg_leitura = "Fatura legível mas "
#                     extra_txt = "extração não foi solicitada" if request_extraction == False else "documento é " + status['doc_type']
#                     msg_leitura = msg_leitura + extra_txt
#                     messages['leitura_doct'] = msg_leitura
#                     del msg_leitura
#                     status['insertion_type'] = "Cadastro parcial"
                    
#                     if uc_v1.shape[0] == 1:
#                         messages['leitura_doct'] = messages['leitura_doct'] + " Por isso não vamos atualizar os dados já cadastrados na base."
#                         dict_return = {'status_code':403, 
#                                        'status':status,
#                                        'messages':messages,
#                                        'actions':actions}
#                     else:
#                         insert_query = pk.key_loops(payload = insert_dict, table = "public.dados_uc")
#                         returno_insert_fatura = pk.get_db('public', insert_query, db)
#                         if returno_insert_fatura['status_code'] == 201:
#                             status['write_status_code'] = 102
#                             id_uc = pk.get_db("public", query.format(**dicty_initial), db).id_uc.iloc[-1]
#                             actions['2'] = f"Acessar área logada e atualizar dados da, na UC {id_uc}"
#                             messages['comparador'] = "Comparador não solicitado dado o cadastro parcial"
#                             dict_return = {'status_code':102, 
#                                            'link':'https://agentes.condutive.com/',
#                                            'id_uc':id_uc,
#                                            'status':status,
#                                            'messages':messages,
#                                            'actions':actions}
#                         else:
#                             del returno_insert_fatura['status_code']
#                             messages['dados_ucInsert'] = "Erro ao inserir dados do cadastro parcial da UC {}".format(returno_insert_fatura)
#                             dict_return = {'status_code':417, 
#                                            'status':status,
#                                            'messages':messages,
#                                            'actions':actions}
                    
#                 elif (status['doc_type'] == ".pdf") & request_extraction:
#                     #Aqui estamos solicitando a leitura do pdf na 4docs
#                     status['insertion_type'] = "Cadastro completo"
#                     retorno_extract = pk.callBack_fromId_4docs(request_id = url_status['extraction']['request_id'], credenciais = url_status['extraction']['credenciais'], pdf_url = url_status['extraction']['url_fatura'], db=db)
#                     status['extract_status_code'] = retorno_extract['status_code']
                    
#                     if retorno_extract['status_code'] == 200:
#                         messages['extraction_log'] = "Extraction ok"
                        
#                         data_fatura = dt.datetime.strptime(retorno_extract['return']['dados_uc']['data_ref'], '%Y-%m-%d')
#                         diff = relativedelta(dt.datetime.today(), data_fatura)
#                         if diff.months > 3:
#                             messages['defasagem_fatura'] = f"Fatura está com {diff.months} mês de defasagem"
#                             status['write_status_code'] = 103
#                         else:
#                             messages['defasagem_fatura'] = "Fatura atualizada"
                            
#                         if uc_v1.shape[0] == 0:
#                             status['db_write'] = "POST"
#                             messages['update_context'] = "Os dados serão inserido pela primeira vez na base de dados"
#                         else:
#                             status['db_write'] = "PUT"
#                             messages['update_context'] = "Os dados serão atualizados na fatura. versão antiga de: {} atualizada em {} versão atual de {}".format(str(uc_v1.data_ref.iloc[-1])[:10], str(uc_v1.updated_at.iloc[-1])[:17], retorno_extract['return']['dados_uc']['data_ref'])
#                         returno_insert_fatura = pk.insert_dadosFatura(tidy_json = retorno_extract, nr_documento = dicty_initial.get('nr_documento'), id_prospect = dicty_initial.get('id_prospect'), other = None, db = db)
                        
#                         if (returno_insert_fatura['status_code'] == 201) & (status['db_write'] == "PUT"):
#                             status['write_status_code'] = 103
#                         else:
#                             status['write_status_code'] = returno_insert_fatura['status_code']
                    
                        
#                         if status['write_status_code'] != 500:
#                             messages['dados_ucInsert'] = 'Os dados foram inseridos corretamente na tabela public.dados_uc'
#                             block_comparador = False
                            
#                             gru_mod = retorno_extract['return']['dados_uc']['gru_mod']
    
#                             suppliers_data = pk.call_compardor(id_uc = returno_insert_fatura['id_uc'], force_recalculate = True, hist_consumo = False if gru_mod.startswith("A") else True, db = db)
#                             return_comparador = pk.create_comparator(id_uc = returno_insert_fatura['id_uc'], suppliers_data = suppliers_data, db = db)
    
                            
#                             if return_comparador['status_code'] == 200:
#                                 actions['2'] = "Voltar ao início e cadastrar nova unidade consumidora"
#                                 messages['comparador'] = "Comparador gerado com sucesso"
#                                 dict_return = {'status_code':return_comparador['status_code'], 
#                                                'link':return_comparador['link'],
#                                                'id_uc':returno_insert_fatura['id_uc'],
#                                                'status':status,
#                                                'messages':messages,
#                                                'actions':actions}
#                             else:
#                                 actions['2'] = "Finalizar cadastro na área logada"
#                                 messages['comparador'] = return_comparador['detail']
#                                 dict_return = {'status_code':return_comparador['status_code'], 
#                                                'link':'https://agentes.condutive.com/',
#                                                'id_uc':returno_insert_fatura['id_uc'],
#                                                'status':status,
#                                                'messages':messages,
#                                                'actions':actions}
#                         else:
#                             messages['dados_ucInsert'] = 'Os dados não foram inseridos na tabela public.dados_uc. Erro: {}'.format(returno_insert_fatura)
#                             block_comparador = True
#                             actions['2'] = "Fazer cadastro da unidade consumidora na área logada"
#                             dict_return = {'status_code':417, 
#                                            'link':'https://agentes.condutive.com/',
#                                            'status':status,
#                                            'messages':messages,
#                                            'actions':actions}
    
#                     else:
#                         messages['extraction_log'] = "Não foi possível realizar extração da fatura corretamente através da função pk.callBack_fromId Erro: {}".format(retorno_extract['return'])
#                         block_comparador = True
#                         actions['2'] = "Fazer cadastro da unidade consumidora na área logada"
#                         dict_return = {'status_code':417, 
#                                        'link':'https://agentes.condutive.com/',
#                                        'status':status,
#                                        'messages':messages,
#                                        'actions':actions}
#                 else:
#                     messages['doubt'] = f"ELSE extraction: {request_extraction} & doct_type: {status['doc_type']}"
#                     print("Não sei porque caiu aqui. {msg_leitura}")
    
#                     dict_return = {'status_code':100,
#                                        'status':status,
#                                        'messages':messages}
#             return dict_return
#         else:
#             #Erro multiplos dados pra mesma UC, voltar com os dados pro consumidor
#             messages['initial_df'] = "Foi encontrada mais de uma unidade consumidora neste local com caracteristicas similares. Vamos ter que analisar este caso em particular e seu lider entratá em contrato com você em breve. Obrigado. "
#             #envar uma mensagem ao lider falando desta ocorrência
#             return {"status_code":403, "status": status, "messages":messages, "dados_uc":uc_v1.to_dict(orient='records')}
        
#     except Exception as exe:
#         return {"status_code":500, "detail":str(exe)}

def cadastro_uc(dicty_initial, url_doct, request_extraction, db):
    print(request_extraction)
    print(db)
    try:

        def build_return(code, status, messages, actions=None, link=None, id_uc=None):
            return {
                "status_code": code,
                "status": status,
                "messages": messages,
                "actions": actions or {"1": "Finalizar solicitação"},
                **({"id_uc": id_uc} if id_uc else {}),
                **({"link": link} if link else {})
            }

        def insert_uc(insert_dict, status, messages, uc_v1, action_msg, db):
            """Insert UC in DB and build proper response."""
            insert_dict['apelido_uc'] = pk.get_db('public', "SELECT nome FROM public.prospect WHERE id = {}".format(insert_dict['id_prospect']), db).nome.iloc[-1]
            insert_query = pk.key_loops(payload=insert_dict, table="public.dados_uc")
            result = pk.get_db('public', insert_query, db)

            if result["status_code"] != 201:
                messages["dados_ucInsert"] = f"Erro ao inserir {action_msg}: {result}"
                return build_return(417, status, messages)

            # insertion ok
            status['write_status_code'] = 102
            id_uc = int(pk.get_db("public", query.format(**dicty_initial), db).id_uc.iloc[-1])

            messages["comparador"] = action_msg
            actions = {
                "1": "Finalizar solicitação",
                "2": f"Acessar área logada e atualizar dados da UC {id_uc}"
            }

            return build_return(102, status, messages, actions, "https://agentes.condutive.com/", id_uc)

        # ---------------------------
        # Initial Setup
        # ---------------------------
        query = """
            SELECT * FROM public.dados_uc 
            WHERE nr_documento = '{nr_documento}' 
            AND cod_agente = '{cod_agente}' 
            AND (cep = '{cep}' OR valor_fatura = '{valor_fatura}');
        """

        uc_v1 = pk.get_db("public", query.format(**dicty_initial), db)

        insert_dict = {**dicty_initial, "url_fatura": url_doct}
        status = {
            "write_status_code": 100,
            "insertion_type": "Cadastro simples",
            "requested_extraction": request_extraction,
            "db": db,
            "dicty_initial": insert_dict
        }
        messages = {}

        # MULTIPLE UC FOUND
        if uc_v1.shape[0] >= 2:
            messages['initial_df'] = (
                "Foi encontrada mais de uma unidade consumidora neste local. "
                "Seu líder entrará em contato com você."
            )
            return {
                "status_code": 403,
                "status": status,
                "messages": messages,
                "dados_uc": uc_v1.to_dict(orient='records')
            }

        # ---------------------------
        # Single or zero UC
        # ---------------------------
        messages['initial_df'] = f"DF inicial com {uc_v1.shape[0]} linhas"

        url_status = pk.url_check(url_doct, request_extraction)
        status['doct_readable'] = url_status['readable']

        # ------------------------------------------------------------------
        # CASE 1 — DOCUMENT NOT READABLE
        # ------------------------------------------------------------------
        if not url_status["readable"]:
            messages['leitura_doct'] = (
                "Não foi possível realizar a leitura deste documento"
            )

            # existing UC but unreadable doc → do not update
            if uc_v1.shape[0] == 1:
                messages['leitura_doct'] += ". Não atualizaremos os dados já cadastrados."
                return build_return(403, status, messages)

            # new UC → insert basic record
            return insert_uc(
                insert_dict, status, messages,
                uc_v1,
                "Comparador não solicitado dado o cadastro simples", db
            )

        # ------------------------------------------------------------------
        # CASE 2 — DOCUMENT READABLE BUT NO EXTRACTION
        # ------------------------------------------------------------------
        from pathlib import Path
        status['doc_type'] = Path(url_doct).suffix.lower()

        extraction_possible = status['doc_type'] == ".pdf" and request_extraction

        if not extraction_possible:
            # Copy guessed fields
            insert_dict.update(url_status['guessed'])
            status['insertion_type'] = "Cadastro parcial"

            msg = "Fatura legível mas "
            msg += "extração não foi solicitada" if not request_extraction else f"documento é {status['doc_type']}"
            messages["leitura_doct"] = msg

            # do not update existing unreadable UC
            if uc_v1.shape[0] == 1:
                messages['leitura_doct'] += ". Não atualizaremos os dados já cadastrados."
                return build_return(403, status, messages)

            return insert_uc(
                insert_dict, status, messages,
                uc_v1,
                "Comparador não solicitado dado o cadastro parcial", db
            )

        # ------------------------------------------------------------------
        # CASE 3 — FULL EXTRACTION (PDF + extraction requested)
        # ------------------------------------------------------------------
        status["insertion_type"] = "Cadastro completo"

        ret_extract = pk.callBack_fromId_4docs(
            request_id=url_status['extraction']['request_id'],
            credenciais=url_status['extraction']['credenciais'],
            pdf_url=url_status['extraction']['url_fatura'],
            db=db
        )
        status['extract_status_code'] = ret_extract['status_code']

        if ret_extract['status_code'] != 200:
            messages['extraction_log'] = f"Erro na extração: {ret_extract['return']}"
            return build_return(417, status, messages)

        # extraction ok
        messages["extraction_log"] = "Extraction ok"
        dados = ret_extract['return']['dados_uc']

        # check recency
        import datetime as dt
        from dateutil.relativedelta import relativedelta
        data_fatura = dt.datetime.strptime(dados['data_ref'], '%Y-%m-%d')
        diff = relativedelta(dt.datetime.today(), data_fatura)
        if diff.months > 3:
            status['write_status_code'] = 103
            messages["defasagem_fatura"] = f"Fatura está com {diff.months} mês de defasagem"
        else:
            messages["defasagem_fatura"] = "Fatura atualizada"

        # POST or PUT message
        status["db_write"] = "POST" if uc_v1.shape[0] == 0 else "PUT"
        messages["update_context"] = (
            "Inserindo pela primeira vez na base"
            if uc_v1.shape[0] == 0 else
            f"Atualizando dados. Versão antiga {uc_v1.data_ref.iloc[-1]} atualizada para {dados['data_ref']}"
        )

        # insert final UC data
        ret_insert = pk.insert_dadosFatura(
            tidy_json=ret_extract,
            nr_documento=dicty_initial.get('nr_documento'),
            id_prospect=dicty_initial.get('id_prospect'),
            other=None,
            db=db
        )
        status['write_status_code'] = ret_insert['status_code']

        if ret_insert['status_code'] == 500:
            messages['dados_ucInsert'] = f"Erro no insert: {ret_insert}"
            return build_return(417, status, messages)

        # insert ok → comparator
        messages['dados_ucInsert'] = "Dados inseridos com sucesso"
        id_uc = ret_insert["id_uc"]
        gru_mod = dados['gru_mod']

        suppliers_data = pk.call_compardor(
            id_uc=id_uc,
            force_recalculate=True,
            hist_consumo=not gru_mod.startswith("A"),
            db=db
        )

        ret_comp = pk.create_comparator(id_uc=id_uc, suppliers_data=suppliers_data, db=db)

        if ret_comp['status_code'] == 200:
            messages["comparador"] = "Comparador gerado com sucesso"
            return build_return(
                200, status, messages,
                {
                    "1": "Finalizar solicitação",
                    "2": "Voltar ao início e cadastrar nova unidade consumidora"
                },
                ret_comp['link'], id_uc
            )

        messages["comparador"] = ret_comp["detail"]
        return build_return(
            ret_comp['status_code'],
            status,
            messages,
            {"2": "Finalizar cadastro na área logada"},
            "https://agentes.condutive.com/",
            id_uc
        )

    except Exception as exe:
        return {"status_code": 500, "detail": str(exe)}


def newLead_whats(return_data, db):
    canal = 'agentes whatsapp'
    
    if 'email' in list(return_data.keys()):
        return pk.insert_newLead(id_agente = return_data['id_agente'], id_lider = return_data['id_lider'], canal = canal, nome = return_data['nome'], telefone = return_data['telefone'], email = return_data['email'], db = db)        
    else:
        return pk.insert_newLead(id_agente = return_data['id_agente'], id_lider = return_data['id_lider'], canal = canal, nome = return_data['nome'], telefone = return_data['telefone'], email = None, db = db)
        
# def newUC_whats(return_data):
#     return pk.insert_newLead(id_agente = return_data['id_agente'], id_lider = return_data['id_lider'], canal = 'agentes whatsapp', nome = return_data['nome'], telefone = return_data['telefone'], email = return_data['email'], db = 'dev')