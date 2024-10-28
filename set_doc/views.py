import cx_Oracle
from dotenv import load_dotenv
import os

load_dotenv()

class SetDoc:
    def __init__(self, request):
        self.request = request
        self.logger = request.logger  # Используем логгер из request
        self.username = os.getenv("USER")
        self.password = os.getenv("PASSWORD")
        self.host = os.getenv("HOST")
        self.port = os.getenv("PORT")
        self.service_name = os.getenv("SERVICE_NAME")
        self.myconnection = cx_Oracle.connect(username=self.username, password=self.password, host=self.host,
                                            port=self.port, service_name=self.service_name, encoding="UTF-8")
        self.mycursor = self.myconnection.cursor()
        self.columns_titles = self._get_columns('nau_test.doctitles')
        self.columns_contents = self._get_columns('nau_test.doccontents')
        self.columns_series = ['SERIALNO', 'GOODSID', 'EXPIREDDATE', 'SOQNO', 'SOQDATE', 'USERID']
        self.query_doctitles = self.create_query('nau_test.doctitles', self.columns_titles)
        self.query_doccontents = self.create_query('nau_test.doccontents', self.columns_contents)

    def _get_columns(self, table):
        self.mycursor.execute(f'''select * from {table} where id = (select max(id) from {table})''')
        return [row[0] for row in self.mycursor.description]

    @staticmethod
    def create_query(table, columns):
        try:
            columns_str = ', '.join(columns)
            values_str = ', '.join([f':{i + 1}' for i in range(len(columns))])
            return f'insert into {table} ({columns_str}) values ({values_str})'
        except cx_Oracle.Error as e:
            raise Exception(f"Error: {str(e)}")

    # Вызови set_doc() при создании экземпляра класса
    def set_doc(self):
        result = []
        try:
            for doc in self.request.data['DOCS']:
                check_docks = self.__check_existing_doc(doc)
                if len(check_docks) == 0 or (int(check_docks[0][1]) == 5 and int(doc['ISACTIVE']) == 2):
                    if len(check_docks) > 0:
                        self.__delete_existing_doc(check_docks[0][0])
                    titles, contents, series, unique_id = self.__prepare_doc_data(doc)
                    try:
                        self.insert_data(titles, contents, unique_id, doc)
                        result.append({"DOCID": int(doc['ID'].strip()), "ORIGINALDOCID": unique_id})
                    except Exception as e:
                        self.__delete_existing_doc(unique_id)
                        self.myconnection.rollback()
                        return str(e)
                else:
                    result.append({"DOCID": int(doc['ID'].strip()), "ORIGINALDOCID": check_docks[0][0]})
            return result
        finally:
            self.myconnection.close()


    def __check_existing_doc(self, doc):
        self.mycursor.execute('''select ID, ISACTIVE from nau_test.doctitles dt 
            where dt.ownerid = :OWNERID and trim(dt.no) = trim(:NO) 
            and trunc(dt.bookdate) = trunc(to_date(:bookdate))''',
                              OWNERID=doc['OWNERID'].strip(), NO=doc['NO'].strip(),
                              BOOKDATE=doc['BOOKDATE'])
        return self.mycursor.fetchall()

    # Дропаем все, что на создавали
    def __delete_existing_doc(self, doc_id):
        self.mycursor.execute('''
        BEGIN
            DELETE FROM NAU_TEST.DOCCONTENTS WHERE DOCID = :DOCID;
            DELETE FROM NAU_TEST.DOCTITLES WHERE ID = :DOCID;
        COMMIT;
        END;''', DOCID=doc_id)

    def __prepare_doc_data(self, doc):
        titles = []
        contents = []
        series = []

        self.mycursor.execute('SELECT NAU_TEST.DOCTITLES_SEQ.NEXTVAL FROM dual')
        unique_id = self.mycursor.fetchone()[0]

        for col in self.columns_titles:
            if col in doc:
                if col == 'ID':
                    titles.append(unique_id)
                else:
                    titles.append(doc[col].strip())
            else:
                titles.append('')

        for cont in doc['DOCCONTENTS']:
            goods = []
            ser = []
            for col in self.columns_contents:
                if col in cont:
                    if col == 'ID':
                        goods.append('')
                    elif col == 'DOCID':
                        goods.append(unique_id)
                    else:
                        goods.append(cont[col].strip())
                else:
                    goods.append('')
            contents.append(goods)

            for col in self.columns_series:
                if col in cont['SERIALS']:
                    ser.append(cont['SERIALS'][col].strip())
                else:
                    ser.append('')
            series.append(ser)

        return titles, contents, series, unique_id

    # insert данных в БД
    def insert_data(self, titles, contents, unique_id, doc):
        try:
            self.mycursor.executemany(self.query_doctitles, [titles])
            self.myconnection.commit()
            success = False
            while not success:
                try:
                    self.mycursor.executemany(self.query_doccontents, contents)
                    success = True
                except Exception as e:
                    self.logger.error(f"!!!!Error insert_data {self.request.data['RECEPTORID']}: {str(e)}")
                    self.myconnection.rollback()
                    if 'ORA-02291' in str(e):
                        contents = self.__handle_serials_error(doc, unique_id)
                    else:
                        raise Exception(f'Error insert {str(e)}')

            self.myconnection.commit()
        except Exception as e:
            raise Exception(f'Error insert {str(e)}')

    # except создает новую серию
    def __handle_serials_error(self, doc, unique_id):
        new_contents = []
        for cont in doc['DOCCONTENTS']:
            goods = []
            for col in self.columns_contents:
                if col in cont:
                    if col == 'ID':
                        goods.append('')
                    elif col == 'DOCID':
                        goods.append(unique_id)
                    elif col == 'SERIALID':
                        ser = []
                        for c in self.columns_series:
                            if c == 'USERID':
                                ser.append(740)
                            if c in cont['SERIALS']:
                                if c == 'GOODSID':
                                    ser.append(cont['SERIALS'][c].strip())
                                else:
                                    ser.append(cont['SERIALS'][c].strip())
                            else:
                                ser.append('')
                        ser.pop()

                        out_seria_id = self.mycursor.var(int)
                        ser.append(out_seria_id)
                        self.mycursor.callproc('NAU_TEST.PRICES.GET_SERIAL_ID', ser)

                        r = out_seria_id.getvalue()
                        goods.append(r)
                    else:
                        goods.append(cont[col].strip())
                else:
                    goods.append('')
            new_contents.append(goods)
        return new_contents
