import cx_Oracle
cx_Oracle.init_oracle_client(lib_dir=r"C:\instantclient_21_11")


def request_oracle(request, *args, **kwargs):
    myconnection = cx_Oracle.connect('NAU_TEST', 'NAU_TEST', '192.168.201.1/Primus', encoding="UTF-8")
    # myconnection = cx_Oracle.connect('NAU_TEST', 'NAU_TEST', 'ORACLE')
    mycursor = myconnection.cursor()
    mycursor.execute(request, *args, **kwargs)
    result = mycursor.fetchall()
    col_names = [row[0] for row in mycursor.description]
    myconnection.close()
    return [col_names, result]
    # return result


def request_get(request, *args, **kwargs):
    myconnection = cx_Oracle.connect('NAU_TEST', 'NAU_TEST', '192.168.201.1/Primus', encoding="UTF-8")
    mycursor = myconnection.cursor()
    mycursor.execute(request, *args, **kwargs)
    result = mycursor.fetchall()
    myconnection.close()
    return result


def request_k2(request, *args, **kwargs):
    myconnection = cx_Oracle.connect('DICTIONARY', 'KUWAm4PhotcE', '192.168.201.95/Primus', encoding="UTF-8")
    # myconnection = cx_Oracle.connect('NAU_TEST', 'NAU_TEST', 'ORACLE')
    mycursor = myconnection.cursor()
    mycursor.execute(request, *args, **kwargs)
    result = mycursor.fetchall()
    # col_names = [row[0] for row in mycursor.description]
    myconnection.close()
    # return [col_names, result]
    return result


def request_post(request, *args, **kwargs):
    myconnection = cx_Oracle.connect('NAU_TEST', 'NAU_TEST', '192.168.201.1/Primus', encoding="UTF-8")
    # myconnection = cx_Oracle.connect('NAU_TEST', 'NAU_TEST', 'ORACLE')
    mycursor = myconnection.cursor()
    mycursor.execute(request, *args, **kwargs)
    myconnection.commit()
    myconnection.close()
    return 'yes'


def request_schema(request, schema, *args, **kwargs):
    myconnection = cx_Oracle.connect('NAU_TEST', 'NAU_TEST', '192.168.201.1/Primus', encoding="UTF-8")
    # myconnection = cx_Oracle.connect('NAU_TEST', 'NAU_TEST', 'ORACLE')
    myconnection.current_schema = schema
    mycursor = myconnection.cursor()
    mycursor.execute(request, *args, **kwargs)
    result = mycursor.fetchall()
    myconnection.close()
    return result


def request_blob(request, *args, **kwargs):
    myconnection = cx_Oracle.connect('NAU_TEST', 'NAU_TEST', '192.168.201.1/Primus', encoding="UTF-8")
    # myconnection = cx_Oracle.connect('NAU_TEST', 'NAU_TEST', 'ORACLE')
    mycursor = myconnection.cursor()
    mycursor.execute(request, *args, **kwargs)
    result = mycursor.fetchone()
    blob = result[0]
    blob_file = blob.read()
    myconnection.close()
    return blob_file


def request_function(method, json_object):
    myconnection = cx_Oracle.connect('NAU_TEST', 'NAU_TEST', '192.168.201.1/Primus', encoding="UTF-8")
    mycursor = myconnection.cursor()
    result = mycursor.callfunc('nau_test.pkg_exchange_json.' + str(method), str, [json_object])
    myconnection.commit()
    myconnection.close()
    return result


def call_procedure(procedure, params):
    myconnection = cx_Oracle.connect('NAU_TEST', 'NAU_TEST', '192.168.201.1/Primus', encoding="UTF-8")
    mycursor = myconnection.cursor()

    # Prepare a list for the output parameter
    out_seria_id = mycursor.var(int)

    # Update params to include the out parameter
    params.append(out_seria_id)

    # Call the stored procedure
    mycursor.callproc(procedure, params)

    # Fetch the value of the OUT parameter
    result = out_seria_id.getvalue()

    myconnection.commit()
    myconnection.close()
    return result


def request_proc_ora19(userid, message):
    myconnection = cx_Oracle.connect('NAU_TEST', 'NAU_TEST', '192.168.201.178/Primus', encoding="UTF-8")
    mycursor = myconnection.cursor()
    mycursor.callproc('FARMAPTEKA.WEBHOOK_TELEGRAM_MESSAGE', [userid, message])
    myconnection.commit()
    myconnection.close()
    return '{"status": "MASSAGE SEND"}'


def request_execute_many(request, *args, **kwargs):
    myconnection = cx_Oracle.connect('NAU_TEST', 'NAU_TEST', '192.168.201.1/Primus', encoding="UTF-8")
    # myconnection = cx_Oracle.connect('NAU_TEST', 'NAU_TEST', 'ORACLE')
    mycursor = myconnection.cursor()
    mycursor.executemany(request, *args, **kwargs)
    myconnection.commit()
    myconnection.close()
