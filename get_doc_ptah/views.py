from get_doc.views import GetDoc
from . import queries



class GetDocPtah(GetDoc):
    def __init__(self, request, receptorid):
        super().__init__(request, receptorid)
        self.queries = queries
        self.typeid = self.request.data.get('TYPEID')

    # main метод
    def get_doc_ptah(self):
        try:
            self.doctitles = self._fetch_data_ptah(self.queries.doctitles_ptah_query(self.docs))

            if not self.doctitles:
                self.doccontents = []
            else:
                self.doccontents = self._fetch_data_ptah(self.queries.doccontents_ptah_query(self.docs))

            result = self.__process_results_ptah()

            return result

        except Exception as e:
            self.logger.error(f"!!!!Error all {self.request.data['RECEPTORID']}: {str(e)}")
            return str(e)

        finally:
            self.cursor.close()
            self.connection.close()

    # конект к БД получение данных
    def _fetch_data_ptah(self, query):
        try:
            return self.request_oracle(query, RECEPTORID=self.receptorid, TYPEID=self.typeid, DAYS=self.days)
        except Exception as e:
            self.logger.error(f"!!!!Error request_oracle {self.request.data['RECEPTORID']}: {str(e)}")
            raise Exception(str(e))

    # переборка данных, полученных из БД
    def __process_results_ptah(self):
        result = []
        for dt_row in self.doctitles[1]:
            dt_res = self.process_row(dt_row, self.doctitles[0])
            dt_res['DOCCONTENTS_CNT'], dt_res['DOCCONTENTS'] = 0, []

            for dc_row in self.doccontents[1]:
                if dc_row[2] == dt_row[0]:
                    dc_res = self.process_row(dc_row, self.doccontents[0])
                    dt_res['DOCCONTENTS'].append(dc_res)
                    dt_res['DOCCONTENTS_CNT'] += 1

            result.append(dt_res)

        return result
