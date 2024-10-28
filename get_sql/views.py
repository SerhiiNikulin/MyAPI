import cx_Oracle
from datetime import datetime
import base64
from dotenv import load_dotenv
import os

load_dotenv()

class GetSql:
    def __init__(self, request):
        self.request = request
        self.username = os.getenv("USER")
        self.password = os.getenv("PASSWORD")
        self.host = os.getenv("HOST")
        self.port = os.getenv("PORT")
        self.service_name = os.getenv("SERVICE_NAME")
        self.myconnection = cx_Oracle.connect(username=self.username, password=self.password, host=self.host,
                                            port=self.port, service_name=self.service_name, encoding="UTF-8")
        self.mycursor = self.myconnection.cursor()
        self.receptorid = self.request.data.get('RECEPTORID', 0)
        self.columns_titles = self._get_columns('NAU_TEST.TBL_GETSQLRESULT')
        self.query_titles = self.create_merge_query('NAU_TEST.TBL_GETSQLRESULT', self.columns_titles,
                                                    ['ID_MSG', 'RECEPTORID'])

    # block finally
    def __del__(self):
        try:
            if hasattr(self, 'myconnection') and self.myconnection:
                self.myconnection.close()
        except cx_Oracle.InterfaceError:
            # Игнорируем ошибку, если курсор или соединение уже закрыты
            pass

    # main method get_sql
    def get_sql(self):
        try:
            rows = self._execute_sql()
            result = self.process_row(rows)
            self.mycursor.close()
            self.myconnection.close()
            return result
        except Exception as e:
            self.myconnection.close()
            return f"Error get_sql: {str(e)}"

    # main method get_sql_results
    def get_sql_results(self):
        response = []
        for doc in self.request.data['SQLRESULTS']:
            col, datesend = self._prepare_doc_data(doc)
            try:
                self.mycursor.execute(self.query_titles, col)
                self.myconnection.commit()
                response.append({
                    "ID_MSG": doc['ID_MSG'],
                    "DATESEND": datesend
                })
                self.mycursor.close()
                self.myconnection.close()
            except Exception as e:
                self.myconnection.rollback()
                self.myconnection.close()
                return f"Error get_sql_results: {str(e)}"
        return response

    # Создание запросов уже без заглушек, перед инсертом
    def _prepare_doc_data(self, doc):
        col = []
        datesend = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
        for row in self.columns_titles:
            if row in doc:
                if row == 'RESULT':
                    # Преобразование base64 в текст
                    decoded_result = base64.b64decode(doc[row]).decode('utf-8')
                    col.append(decoded_result)
                else:
                    col.append(doc[row])
            elif row == 'RECEPTORID':
                col.append(self.receptorid)
            elif row == 'DATESEND':
                col.append(datesend)
            else:
                col.append('')
        return col, datesend

    # Создаем MERGE запрос с заглушками
    @staticmethod
    def create_merge_query(table, columns, match_columns):
        try:
            # Строка для вставки значений
            columns_str = ', '.join(columns)
            select_str = ', '.join([f':{i} as {i}' for i in columns])
            values_str = ', '.join([f':{i}' for i in columns])

            # Создание условий сопоставления
            match_conditions = ' AND '.join([f'trg.{col} = src.{col}' for col in match_columns])

            # Строка для update, исключая те, что находятся в match_columns и исключаем свои условия, если есть
            update_columns = [col for col in columns if col not in match_columns and col != 'ID']
            update_str = ', '.join([f'trg.{col} = src.{col}' for col in update_columns])

            # Формирование MERGE запроса
            merge_query = f'''
                MERGE INTO {table} trg
                USING (
                    SELECT {select_str} FROM dual
                ) src
                ON ({match_conditions})
                WHEN MATCHED THEN
                    UPDATE SET {update_str}
                WHEN NOT MATCHED THEN
                    INSERT ({columns_str})
                    VALUES ({values_str})
            '''
            return merge_query
        except Exception as e:
            raise Exception(f"Error: {str(e)}")

    # Получает названия полей из таблиц
    def _get_columns(self, table):
        self.mycursor.execute(f'select * from {table} where id = (select max(id) from {table})')
        return [row[0] for row in self.mycursor.description]

    # Получаем данный get_sql
    def _execute_sql(self):
        createdate = self.request.data['CREATEDATE']
        try:
            self.mycursor.execute('''
                        SELECT ID, SQL, CREATEDATE, ISSCRIPT
                            FROM NAU_TEST.TLB_GETSQL
                            WHERE CREATEDATE > :CREATEDATE
                                AND ISACTIVE = 1
                                AND (
                                    (RECEPTORID = :RECEPTORID OR :RECEPTORID = 0)
                                    OR (OWNERID = (SELECT parentid
                                                    FROM nau_test.firms
                                                    WHERE id = :RECEPTORID)
                                    OR (RECEPTORID IS NULL AND OWNERID IS NULL))
                                    ) order by CREATEDATE
                                        ''', CREATEDATE=createdate, RECEPTORID=self.receptorid)
            columns = [col[0] for col in self.mycursor.description]
            self.mycursor.rowfactory = lambda *args: dict(zip(columns, args))
            return self.mycursor.fetchall()
        except Exception as e:
            self.myconnection.rollback()
            raise Exception(f"Error _execute_sql: {str(e)}")

    # Перебираем данные перед ответом get_sql
    @staticmethod
    def process_row(rows):
        result = []
        for row in rows:
            if isinstance(row, dict):
                # Проверка типа для поля CREATEDATE
                if isinstance(row['CREATEDATE'], datetime):
                    row['CREATEDATE'] = row['CREATEDATE'].strftime('%d/%m/%Y %X')
                else:
                    row['CREATEDATE'] = datetime.fromisoformat(str(row['CREATEDATE'])).strftime('%d/%m/%Y %X')
                # Конвертация CLOB в строку и форматирование результата
                row['SQL'] = str(base64.b64encode(row['SQL'].read().encode('utf-8')))[2:-1]
                result.append(row)
        return result
