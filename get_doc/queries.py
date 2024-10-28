def split_docs(docs, max_size=500):
    # Разделяем список на части по 500 элементов
    return [docs[i:i + max_size] for i in range(0, len(docs), max_size)]

def doctitles_query(doc_id):
    split_lists = split_docs(doc_id)

    # Создание списка с условиями "ID NOT IN (...) AND ID NOT IN (...)"
    doc_ids_conditions = []
    for part in split_lists:
        if part:
            # Преобразуем список в строку и формируем условие
            doc_ids_conditions.append(f"dt.ID NOT IN ({', '.join(map(str, part))})")

    # Объединяем условия через " AND "
    doc_ids = ' AND '.join(doc_ids_conditions)
    return f'''
        select DT.ID
          , CASE WHEN DT.typeid in (60,1) THEN 1 ELSE DT.typeid END as TYPEID
          , DT.NO
          , DT.NOINTERNAL
          , DT.DOCDATE as BOOKDATE
          , DT.DOCDATE as DOCDATE
          , CASE WHEN DT.typeid in (1,61) THEN DT.FIRMID ELSE DT.OWNERID END as FIRMID  
          , CASE WHEN DT.typeid in (1,61) THEN DT.RECEPTORID ELSE DT.FIRMID END as RECEPTORID
          , CASE WHEN DT.typeid in (1,61) THEN DT.OWNERID ELSE DT.FIRMID END as OWNERID  
          , DT.COMMENTS
          , NVL(EMP.EMPLOYEESID, DT.CREATORID) CREATORID
          --, DT.CREATEDATE
          , SYSDATE AS CREATEDATE
          , DT.CONTRACTID
          , DT.PERSON
          , DT.LANO
          , DT.LADATE
          , DT.AVTMODE
          , DT.DECAVT
          , DT.DECPRICEPR
          , DT.DECPRICEIN
          , DT.DECPRICEOUT
          , DT.DECPRICERET
          , DT.DECSUMPR
          , DT.DECSUMIN
          , DT.DECSUMOUT
          , DT.DECSUMRET
          , DT.DECSUMDOCPR
          , DT.DECSUMDOCIN
          , DT.DECSUMDOCOUT
          , DT.DECSUMDOCRET
          , DT.DECQUANTITY
          , DT.DELIVTYPEID
          , DT.SUMMA
          , DT.PAYDATE
          , DT.PAYTYPESID
          , DT.DISCOUNTPERCENT
          , DT.DISCOUNTSUMMA
          , DT.ISACTIVE
          , DT.ISLOCKED
          , DT.TAXID
          , DT.PRICECONDITIONSID
          , DT.SUMMA0
          , DT.AVTSUMMA0
          , DT.SUMMA1
          , DT.AVTSUMMA1
          , DT.SUMMA2
          , DT.AVTSUMMA2
          , DT.SUMMA3
          , DT.AVTSUMMA3
          , DT.SUMMA4
          , DT.AVTSUMMA4
          , DT.SUMMA5
          , DT.AVTSUMMA5
          , DT.SUMMA6
          , DT.AVTSUMMA6
          , DT.EKATYPE
          , DT.PRICEGROUPID
          , DT.ROUND_TYPE
          , DT.ORIGINALDOCID
          , DT.CARDID
          , DT.TO1C
          , DT.FROM1C
          , DT.SUMBEZNAL
          , DT.CONTROL
          , DT.ANKETAFIOID
          , DT.TO_CASHBOOKS
          , DT.NALNO
          , DT.NALDATE
          , DT.ISREVIEW
          , DT.ISAUTOIMPORTINVOICE
          , DT.ISAUTOIMPORTNOEROR
        from NAU_TEST.DOCTITLES dt
        LEFT JOIN (
            SELECT MAX(U.ID) EMPLOYEESID, W.FIRMSID
            FROM NAU_TEST.WORK_PERIOD W
            LEFT JOIN NAU_TEST.EMPLOYEES E ON E.ID = W.EMPLOYEESID
            LEFT JOIN NAU_TEST.USERS U ON U.ID = E.USERID
            WHERE W.EMPLOYEPOSITIONID = 2
            AND W.DATE_END IS NULL
            GROUP BY W.FIRMSID) EMP ON EMP.FIRMSID = DT.RECEPTORID
        WHERE ((dt.RECEPTORID = :RECEPTORID and dt.TYPEID in (61, 1)) or (dt.FIRMID = :RECEPTORID and dt.TYPEID = 60)) 
        and NVL(dt.ISACTIVE,0) IN (1,3)
        --AND dt.TYpeID =:TypeID
        and (dt.firmid <> dt.ownerid or dt.TYPEID = 61) 
        and dt.TYPEID in (1,60,61)
        AND trunc(dt.DocDate) > trunc(sysdate) - :DAYS
        AND NVL(dt.ISLOCKED, 0) = 0
        AND ({doc_ids}) 
        order by dt.id'''

def doccontents_query(doc_id):
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
                , DC.DOCID
                , CASE WHEN DT.typeID = 61 THEN DC.ID END PRICEREFERENT
                , DC.PARENTID
                , DC.GOODSID
                , DC.SERIALID
                , DC.QUANTITY
                , DC.QTYCLOSED
                , DC.QTYREST
                , CASE WHEN DT.typeID = 61 THEN DC.VENDORID ELSE DC.PRICE0 END AS PRICE0
                , DC.PRICE1
                , CASE WHEN DT.typeID = 61 THEN DC.COLOR ELSE DC.PRICE2 END AS PRICE2
                , DC.PRICE3
                , DC.PRICE4
                , DC.PRICE5
                , DC.PRICE6
                , CASE WHEN DT.typeID = 60 AND F1.PARENTID = F2.PARENTID THEN DC.AVRCOEF ELSE DC.AVRCOEFOUT END AS AVRCOEF
                , DC.CURRENCYID
                , DC.VENDORID
                , DC.BARCODE
                , DC.SIZE_GOODS
                , DC.AVRCOEFOUT
                , DC.CREATEDATE
                , DC.UPDATEDDATE
                , DC.CONTROL
                , DC.QUANTITYFULL
                , DC.MINPRICE
                , DC.COLOR
                , DC.MAXPRICE
                , DC.AVGPRICE
                , DC.QUANTITYREORDER
                , CASE WHEN DT.typeID = 61 THEN SUBSTR(trim(DC.SIZE_GOODS), 1, 20) ELSE REPLACE(UKTZEDID, ' ', '') END UKTZEDID
                , DC.CLORDERNO
                , GS.SERIALNO,
                  GS.EXPIREDDATE,
                  GS.SOQNO,
                  NVL(GS.SOQDATE, to_date('30/12/1899')) SOQDATE,
                  GS.DISABLE,
                  GS.CREATORID,
                  GS.COMMENTS,
                  GS.SIZE_X,
                  GS.STRUCTURE,
                  GS.SERIAIMAGESID 
                  ,G.PRODUCERID
                  ,P.NAME PRODUCERNAME
            FROM NAU_TEST.DOCTITLES DT, NAU_TEST.DOCCONTENTS DC, NAU_TEST.GOODSSERIES GS, NAU_TEST.GOODS G,  NAU_TEST.PRODUCERS P, NAU_TEST.FIRMS F1, NAU_TEST.FIRMS F2
            WHERE ((dt.RECEPTORID = :RECEPTORID and dt.TYPEID in (1,61)) or (dt.FIRMID = :RECEPTORID and dt.TYPEID = 60))
            -- AND DT.TYPEID =:TYPEID
            and NVL(dt.ISACTIVE,0) IN (1,3)
            AND TRUNC(DT.DOCDATE) > TRUNC(SYSDATE) - :DAYS
            AND NVL(dt.ISLOCKED, 0) = 0
            AND ({doc_ids}) 
            AND DC.DOCID = DT.ID 
            AND DC.SERIALID = GS.ID (+)
            AND DC.GOODSID = G.ID (+)
            AND G.PRODUCERID =P.ID (+)
            AND DT.OWNERID = F1.ID (+)
            AND DT.FIRMID = F2.ID (+)
            and dt.TYPEID in (1,60,61)
            and (dt.firmid <> dt.ownerid or dt.TYPEID = 61)
            ORDER BY DC.DOCID'''


