from datetime import datetime
import cx_Oracle
import logging
from dotenv import load_dotenv
import os

load_dotenv()

class SetDelivery:
    def __init__(self, request):
        self.request = request
        self.receptorid = request.data['RECEPTORID']
        self.username = os.getenv("USER")
        self.password = os.getenv("PASSWORD")
        self.host = os.getenv("HOST")
        self.port = os.getenv("PORT")
        self.service_name = os.getenv("SERVICE_NAME")
        self.myconnection = cx_Oracle.connect(username=self.username, password=self.password, host=self.host,
                                              port=self.port, service_name=self.service_name, encoding="UTF-8")
        self.mycursor = self.myconnection.cursor()
        self.columns_titles = self._get_columns('NAU_TEST.TBL_DELIVERY_INFO')
        self.columns_contents = self._get_columns('NAU_TEST.TBL_DELIVERY_DET')
        self.columns_docs = self._get_columns('NAU_TEST.TBL_DELIVERY_DOCS')
        self.query_doctitles = self.create_query('NAU_TEST.TBL_DELIVERY_INFO', self.columns_titles)
        self.query_doccontents = self.create_query('NAU_TEST.TBL_DELIVERY_DET', self.columns_contents)
        self.query_docs = self.create_query('NAU_TEST.TBL_DELIVERY_DOCS', self.columns_docs)

    def __del__(self):
        try:
            if self.myconnection:
                self.myconnection.close()
        except cx_Oracle.InterfaceError:
            pass

    # Получаем поля из таблиц
    def _get_columns(self, table):
        self.mycursor.execute(f'select * from {table} where id = (select max(id) from {table})')
        return [row[0] for row in self.mycursor.description]

    # Создаем insert запросы с заглушками
    @staticmethod
    def create_query(table, columns):
        try:
            columns_str = ', '.join(columns)
            values_str = ', '.join([f':{i}' for i in columns])
            return f'insert into {table} ({columns_str}) values ({values_str})'
        except Exception as e:
            print(f"Error: {str(e)}")
            raise

    # main method
    def set_delivery(self):
        result = []

        # Собираем все оригинальные ID документов
        apt_doc_id = [doc['ID'] for doc in self.request.data['DELIVERIES']]
        try:
            # Сначала делаем один запрос, чтобы проверить, какие документы уже существуют
            check_docs = self.__check_existing_doc(apt_doc_id)

            existing_doc = {doc[2] for doc in check_docs}  # set для быстрого поиска ORIGINALDOCID

            for doc in self.request.data['DELIVERIES']:
                original_doc_id = doc['ID']

                if original_doc_id in existing_doc:
                    # Документ уже существует, добавляем его в результат
                    existing_doc_data = next(or_id for or_id in check_docs if or_id[2] == original_doc_id)
                    result.append({
                        "ORIGINALDOCID": existing_doc_data[0],
                        "DELIVERY_ID": existing_doc_data[1],
                        "ID": existing_doc_data[2],
                        "DELIVERYDATE": datetime.fromisoformat(str(existing_doc_data[3])).strftime('%d/%m/%Y %X') if
                        existing_doc_data[3] else None,
                        "STATUS": existing_doc_data[4],
                        "OWNER_NAME": existing_doc_data[5]
                    })
                else:
                    # Новый документ, подготавливаем данные для вставки
                    titles, contents, docs, unique_id = self.__prepare_doc_data(doc, original_doc_id)
                    try:
                        # Выполняем пакетную вставку
                        self.mycursor.executemany(self.query_doctitles, [titles])
                        self.mycursor.executemany(self.query_doccontents, contents)
                        self.mycursor.executemany(self.query_docs, docs)
                        self.myconnection.commit()

                        result.append({
                            "ORIGINALID": int(unique_id),
                            "ID": original_doc_id,
                            "STATUS": 90,
                            "OWNER_NAME": doc['OWNER_NAME']
                        })
                    except Exception as e:
                        self.myconnection.rollback()
                        logging.error(f"Error processing document ID {original_doc_id}: {str(e)}")
                        continue
            return result
        finally:
            self.mycursor.close()
            self.myconnection.close()


    # Проверка на существование такого документа
    def __check_existing_doc(self, doc_id):
        try:
            # Формируем часть запроса с условиями для всех doc_id
            placeholders = ', '.join([f':ORIGINALDOCID{i}' for i in range(len(doc_id))])

            query = f'''
                    select ID, DELIVERY_ID, ORIGINALDOCID, DELIVERYDATE, STATUS, OWNER_NAME 
                    from NAU_TEST.TBL_DELIVERY_INFO dt
                    where dt.ORIGINALDOCID in ({placeholders}) 
                    and dt.receptorid = :receptorid
                '''

            # Подготавливаем параметры для каждого doc_id
            params = {f'ORIGINALDOCID{i}': doc_id[i] for i in range(len(doc_id))}
            params['receptorid'] = self.receptorid

            # Выполняем запрос
            self.mycursor.execute(query, params)

            # Возвращаем все найденные записи
            return self.mycursor.fetchall()
        except Exception as e:
            raise Exception(f"Error create sql: {str(e)}")

    def __delete_existing_doc(self, bad_id):
        self.mycursor.execute('''
        begin
            DELETE FROM NAU_TEST.TBL_DELIVERY_DOCS WHERE DI_ID = :UNIQUEID;
            DELETE FROM NAU_TEST.TBL_DELIVERY_DET WHERE DI_ID = :UNIQUEID;
            DELETE FROM NAU_TEST.TBL_DELIVERY_INFO WHERE ID = :UNIQUEID;
            commit;   
        end;
            ''', UNIQUEID=bad_id)

    # Перебираем все данные перед вставкой
    def __prepare_doc_data(self, doc, original_doc_id):
        titles = []
        contents = []
        docs = []
        self.mycursor.execute('SELECT NAU_TEST.TBL_DELIVERY_INFO_SEQ.NEXTVAL FROM dual')
        unique_id = self.mycursor.fetchone()[0]
        for col in self.columns_titles:
            if col == 'ID':
                titles.append(unique_id)
            elif col == 'ORIGINALDOCID':
                titles.append(original_doc_id)
            elif col == 'RECEPTORID':
                titles.append(self.receptorid)
            elif col == 'DATECREATE':
                titles.append(datetime.now())
            elif col == 'DATEUPDATE':
                titles.append(datetime.now())
            elif col == 'DELIVERYDATE':
                titles.append(
                    str(doc[col]).replace('.', '/')
                )
            elif col == 'STATUS':
                titles.append(1)
            elif col in doc:
                titles.append(doc[col])
            else:
                titles.append('')

        for cont in doc['TBL_DELIVERY_DET']:
            goods = []
            for col in self.columns_contents:
                if col in cont:
                    if col == 'ID':
                        goods.append('')
                    elif col == 'DI_ID':
                        goods.append(unique_id)
                    else:
                        goods.append(cont[col])
                else:
                    goods.append('')
            contents.append(goods)

        for d in doc['TBL_DELIVERY_DOCS']:
            res = []
            for col in self.columns_docs:
                if col == 'ID':
                    res.append(None)
                elif col == 'DI_ID':
                    res.append(unique_id)
                elif col == 'DOCID':
                    res.append(d)
            docs.append(res)
        return titles, contents, docs, unique_id
