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


# def cadastro_uc(cep, valor_fatura, nr_documento = None, doct_file = None):
#Maria Eduarda
#Valeria
#Elisangela
#Vander Hayder
dicty_initial = {
    "nr_documento": '00432976000133',
    "id_prospect": 105,
    "cod_agente": 383,
    "cep": '24230540',
    "endereco": 'RUA JOAQUIM TAVORA 0 310, ICARAI, NITEROI - RJ',
    "valor_fatura": float(1309.92),
}
def cadastro_uc(dicty_initial, url_doct, request_extraction = True, db = 'dev'): #TODO continuar daqui com a documentação da agent fire

    
    #Parte 4: Conferir se os dados à serem inseridos na UC são novos ou não
    query = "SELECT * FROM public.dados_uc WHERE nr_documento = '{nr_documento}' AND cod_agente = '{cod_agente}' AND (cep = '{cep}' OR valor_fatura = '{valor_fatura}');"
    uc_v1 = pk.get_db("public", query.format(**dicty_initial), db)
    
    #Dados padrão de retorno da função
    status = {"status_code": 100, "insertion_type":"Cadastro simples", "requested_extraction":request_extraction, 'db':db}
    messages = dict()
    return_cadastroUC = dict()
    block_comparador = True
    actions = {"1":"Finalizar solicitação"}
    
    #Dados padrão de insert na base de dados
    insert_dict = dicty_initial.copy()
    insert_dict['url_fatura'] = url_doct

    if uc_v1.shape[0] < 2:
        #Nova UC ou UC com dados existentes
        url_status = pk.url_check(url_doct, request_extraction)
        return_cadastroUC['readable'] = url_status['readable']
        if url_status['readable'] == False:
            messages['leitura_doct'] = "Não foi possível realizar a leitura deste documento"
            #MARIA EDUARDA PAROU AQUI
            #VALERIA TAMBEM
        else:
            from pathlib import Path
            status['doc_type'] = Path(url_doct).suffix.lower()
            
            if (request_extraction == False) or (status['doc_type'] != ".pdf"):               
                for i in url_status['guessed'].keys():
                    insert_dict[i] = url_status['guessed'].get(i)
                msg_leitura = "Fatura legível mas "
                extra_txt = "extração não foi solicitada" if request_extraction == False else "documento é " + status['doc_type']
                msg_leitura = msg_leitura + extra_txt
                messages['leitura_doct'] = msg_leitura
                del msg_leitura
                status['insertion_type'] = "Cadastro parcial"
                status['status_code'] = 100
                #ELISANGELA com request_extraction = False ta aqui
                
            elif (status['doc_type'] == ".pdf") & request_extraction:
                #Aqui estamos solicitando a leitura do pdf na 4docs
                retorno_extract = pk.callBack_fromId_4docs(request_id = url_status['extraction']['request_id'], credenciais = url_status['extraction']['credenciais'], pdf_url = url_status['extraction']['url_fatura'], db=db)
                if retorno_extract['status_code'] == 200:
                    messages['extraction_log'] = "Extraction ok"
                    data_fatura = dt.datetime.strptime(retorno_extract['return']['dados_uc']['data_ref'], '%Y-%m-%d')
                    diff = relativedelta(dt.datetime.today(), data_fatura)
                    if diff.months > 3:
                        status['fat_update_status'] = f"Fatura está com {diff.months} mês de defasagem"
                        status['status_code'] = 103
                    else:
                        status['fat_update_status'] = "Fatura atualizada"
                        
                    #TODO trata json que vai ser inserido
                    #WARNING!!!!!!!!!!!!!! Cuidado que essa porra ta escrevendom 2x
                    if uc_v1.shape[0] == 0:
                        status['db_write'] = "POST"
                    else:
                        status['db_write'] = "PUT"
                    returno_insert_fatura = pk.insert_dadosFatura(tidy_json = retorno_extract, nr_documento = dicty_initial.get('nr_documento'), id_prospect = dicty_initial.get('id_prospect'), other = None, db = db)
                    status['write_status'] = returno_insert_fatura['status_code']
                    
                    if status['write_status'] == 201:
                        status['insertion_type'] = "Cadastro completo"
                        block_comparador = False

                        gru_mod = retorno_extract['return']['dados_uc']['gru_mod']
                        suppliers_data = pk.call_compardor(id_uc = returno_insert_fatura['id_uc'], force_recalculate = True, hist_consumo = False if gru_mod.startswith("A") else True, db = db)
                        return_comparador = pk.create_comparator(id_uc = returno_insert_fatura['id_uc'], suppliers_data = suppliers_data, db = db)
                    else:
                        status_code = returno_insert_fatura['status_code']
                        msg_leitura = "Erro escrever os dados da UC no banco de dados {returno_insert_fatura['details']}"
                        status_code = 500

                        
                    
                else:
                    status_code = retorno_extract['status_code']
                    msg_leitura = "Erro ao solicitar extração na função callBack_fromId_4docs: {retorno_extract}"

            else:
                msg_leitura = f"ELSE extraction: {request_extraction} & doct_type: {status['doc_type']}"
                print("Não sei porque caiu aqui. {msg_leitura}")
                status_leitura = "Fatura legível, vamos prosseguir com a extração na 4 docs"
                status_code = 100
            
            #envia fatura para leitura
            #pega a fatura de volta
            #trata e insere os dados

        ### Cadastrar UC com dados completos
        #Cadastrar UC com dados incompletos e solicitar ida pra area logada

        
            unidade = "Dado dupliicado na base de dados, conferido via CEP e valor de fatura"
            mensagem = "O dado inserido está duplicado na base de dados verificamos que a unidade {apelido_uc} registrada no CEP {cep} com o valor de R$ {valor_fatura}".format(**dicty_initial)
        
            # return {"status_code":208, "status": unidade, "mensagem":mensagem, "actions":actions, 'return_data':{"insert_data":{"cep":cep, "nr_documento": nr_documento, "valor_fatura":valor_fatura},"dados_uc":uc_v1.to_dict(orient='records')}}
        
        #se der algum erro notificar o lider
    else:
        #Erro multiplos dados pra mesma UC, voltar com os dados pro consumidor
        unidade = "Dado duplicado para mais de uma UC"
        mensagem = "Foi encontrada mais de uma unidade consumidora neste local com caracteristicas similares. Vamos ter que analisar este caso em particular e seu lider entratá em contrato com você em breve. Obrigado. "
        #envar uma mensagem ao lider falando desta ocorrência
        return {"status_code":status_code, "status": unidade, "mensagem":mensagem, "actions":actions, 'return_data':{"insert_data":{"cep":cep, "nr_documento":nr_documento, "valor_fatura":valor_fatura},"dados_uc":uc_v1.to_dict(orient='records')}}


def newLead_whats(return_data, db):
    canal = 'agentes whatsapp'
    
    if 'email' in list(return_data.keys()):
        return pk.insert_newLead(id_agente = return_data['id_agente'], id_lider = return_data['id_lider'], canal = canal, nome = return_data['nome'], telefone = return_data['telefone'], email = return_data['email'], db = db)        
    else:
        return pk.insert_newLead(id_agente = return_data['id_agente'], id_lider = return_data['id_lider'], canal = canal, nome = return_data['nome'], telefone = return_data['telefone'], email = None, db = db)
        
# def newUC_whats(return_data):
#     return pk.insert_newLead(id_agente = return_data['id_agente'], id_lider = return_data['id_lider'], canal = 'agentes whatsapp', nome = return_data['nome'], telefone = return_data['telefone'], email = return_data['email'], db = 'dev')