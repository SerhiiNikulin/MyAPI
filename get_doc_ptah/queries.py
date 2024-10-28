def split_docs(docs, max_size=500):
    # Разделяем список на части по 500 элементов
    return [docs[i:i + max_size] for i in range(0, len(docs), max_size)]


def doctitles_ptah_query(doc_id):
    split_lists = split_docs(doc_id)

    # Создание списка с условиями "ID NOT IN (...) AND ID NOT IN (...)"
    doc_ids_conditions = []
    for part in split_lists:
        if part:
            # Преобразуем список в строку и формируем условие
            doc_ids_conditions.append(f"dt.ID NOT IN ({', '.join(map(str, part))})")

    # Объединяем условия через " AND "
    doc_ids = ' AND '.join(doc_ids_conditions)

    # Возвращаем итоговый SQL-запрос
    return f'''
        select  DT.ID
            , DT.TYPEID
            , DT.NO
            , DT.FIRMID
            , DT.DOCDATE
            , DT.OWNERID
            , DT.FileName
            , DT.SendState
            , DT.NEWSendState
            , DT.AVTSUMMA1
            , DT.SUMMA1
            , DT.FileNamePDF
            , DT.PAYDATE
            , DT.Edrpou
            , DT.RcvEdrpou
        from BZLOGS.DOCTITLES_PTAH dt
        WHERE dt.RECEPTORID = :RECEPTORID
        AND dt.TYpeID = :TypeID
        AND trunc(dt.DocDate) >= trunc(sysdate) - :DAYS
        AND trunc(dt.CREATEDATE) >= trunc(sysdate) - :DAYS
        AND ({doc_ids})  
        AND dt.SendState != '0'
        ORDER BY dt.ID
    '''

def doccontents_ptah_query(doc_id):
    split_lists = split_docs(doc_id)

    # Создание списка с условиями "ID NOT IN (...)"
    doc_ids_conditions = []
    for part in split_lists:
        if part:
            # Преобразуем список в строку и формируем условие
            doc_ids_conditions.append(f"dt.ID NOT IN ({', '.join(map(str, part))})")

    # Объединяем условия через " AND "
    doc_ids = ' AND '.join(doc_ids_conditions)
    return f'''
            SELECT DC.ID  
                , DC.GOODSID
                , DC.DOCID
                , DC.PRICE1
                , DC.QUANTITY
                , DC.SERIALNO
                , DC.EXPIREDDATE
            FROM BZLOGS.DOCTITLES_PTAH DT, BZLOGS.DOCCONTENTS_PTAH DC
            WHERE DT.RECEPTORID = :RECEPTORID  
            AND DT.TYPEID =:TYPEID
            AND TRUNC(DT.DOCDATE) >= TRUNC(SYSDATE) - :DAYS
            AND trunc(dt.CREATEDATE) >= trunc(sysdate) - :DAYS
            AND ({doc_ids}) 
            AND DC.DOCID = DT.ID
            and dt.SendState != '0'
            ORDER BY DC.DOCID'''
