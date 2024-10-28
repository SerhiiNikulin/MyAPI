from . import queries
from datetime import datetime
import cx_Oracle
from dotenv import load_dotenv
import os

load_dotenv()

class GetDoc:
    def __init__(self, request, receptorid):
        self.request = request
        self.receptorid = receptorid
        self.logger = request.logger  # Используем логгер из request
        self.username = os.getenv("USER")
        self.password = os.getenv("PASSWORD")
        self.host = os.getenv("HOST")
        self.port = os.getenv("PORT")
        self.service_name = os.getenv("SERVICE_NAME")
        self.connection = cx_Oracle.connect(username=self.username, password=self.password, host=self.host,
                                            port=self.port ,service_name=self.service_name, encoding="UTF-8")
        self.cursor = self.connection.cursor()
        self.queries = queries
        self.days = self.request.data.get('DAYS')
        self.docs = self._get_docs()
        self.doctitles = []
        self.doccontents = []

    def __del__(self):
        try:
            if self.connection:
                self.connection.close()
        except cx_Oracle.InterfaceError:
            pass

    # получение данных и полей из БД
    def request_oracle(self, request, **kwargs):
        try:
            self.cursor.execute(request, kwargs)
            result = self.cursor.fetchall()
            col_names = [row[0] for row in self.cursor.description]
            return [col_names, result]
        except Exception as e:
            self.logger.error(f"!!!!Error request_oracle {self.request.data['RECEPTORID']}: {str(e)}")
            raise Exception(str(e))

    # main метод
    def get_doc(self):
        try:
            # Выполнение запросов к БД
            self.doctitles = self._fetch_data(self.queries.doctitles_query(self.docs))

            if not self.doctitles:
                self.doccontents = []
            else:
                self.doccontents = self._fetch_data(self.queries.doccontents_query(self.docs))

            result = self.__process_results()

            return result

        except Exception as e:
            self.logger.error(f"!!!!Error all {self.request.data['RECEPTORID']}: {str(e)}")
            return str(e)

        finally:
            self.cursor.close()
            self.connection.close()

    # Метод для получения и обработки списка документов
    def _get_docs(self):
        docs = self.request.data.get('DOCS', [])
        return docs if len(docs) >= 3 else [0]

    # Метод для выполнения запроса к базе данных Oracle
    def _fetch_data(self, query):
        try:
            result = self.request_oracle(query, RECEPTORID=self.receptorid, DAYS=self.days)
            return result
        except Exception as e:
            raise Exception(str(e))
        # start_time = time.perf_counter()  # начало замера времени
        # end_time = time.perf_counter()  # конец замера времени
        # elapsed_time = end_time - start_time
        # print(f"Время выполнения метода _fetch_data: {elapsed_time:.4f} секунд")

    # Метод для обработки результатов запросов
    def __process_results(self):
        result = []
        for dt_row in self.doctitles[1]:
            dt_res = self.process_row(dt_row, self.doctitles[0])
            dt_res['DOCCONTENTS_CNT'], dt_res['DOCCONTENTS'] = 0, []

            for dc_row in self.doccontents[1]:
                if dc_row[1] == dt_row[0]:
                    dc_res = self.process_row(dc_row, self.doccontents[0])
                    dt_res['DOCCONTENTS'].append(dc_res)
                    dt_res['DOCCONTENTS_CNT'] += 1

            result.append(dt_res)

        return result

    # Метод для обработки строки результата запроса
    @staticmethod
    def process_row(row, headers):
        res = {}
        for i, j in zip(row, headers):
            if isinstance(i, datetime):
                res[j] = i.strftime('%d/%m/%Y %X')
            elif isinstance(i, float) and str(i).endswith('.0'):
                res[j] = round(i)
            else:
                res[j] = i
        return res
