import cx_Oracle
from . import queries
from datetime import datetime
import base64
import os
import requests
from django.conf import settings

class GetPDFPtah:
    def __init__(self, request):
        self.request = request
        self.logger = request.logger
        self.myconnection = cx_Oracle.connect('NAU_TEST', 'NAU_TEST', '192.168.201.1/Primus', encoding="UTF-8")
        self.mycursor = self.myconnection.cursor()
        self.filename = self.request.data['FILENAME']
        #Если FACSIMILE отсутствует - по умолчанию 0
        self.is_facsimile = self.request.data.get('FACSIMILE', 0)
        self.queries = queries.get_pdf_query

    def __del__(self):
        try:
            if self.myconnection:
                self.myconnection.close()
        except cx_Oracle.InterfaceError:
            pass

    @staticmethod
    def _get_file_path(base_path, doc_id, edrpou, receptor, date, filename_pdf, prefix=''):
        """Формирование пути до файла на локальном диске."""
        path_to_file = os.path.join(
            str(base_path), str(edrpou), str(receptor),
            str(datetime.date(date).year),
            str(datetime.date(date).month),
            str(datetime.date(date).day)
        )

        return os.path.join(path_to_file, f"{prefix}{str(doc_id)}_{str(filename_pdf)}")


    @staticmethod
    def _download_remote_pdf(url):
        """Загрузка PDF файла с удаленного ресурса и кодирование его в base64."""
        response = requests.get(url, verify=settings.CERT_PATH)
        if response.status_code == 200:
            pdf64 = base64.b64encode(response.content).decode('utf-8')
            return pdf64
        return None

    @staticmethod
    def _read_and_encode_pdf(file_path):
        """Чтение и кодирование локального PDF файла в base64."""
        if os.path.isfile(file_path):
            with open(file_path, 'rb') as f:
                pdf = f.read()
                return base64.b64encode(pdf).decode('utf-8')
        return None

    def get_pdf_ptah(self):
        try:
            """Получение пути к PDF файлу из базы данных и обработка запросов."""
            try:
                # Выполнение SQL-запроса для получения данных о файле
                with self.mycursor as cursor:
                    cursor.execute(self.queries, FILENAME=self.filename)
                    f = cursor.fetchone()
                    if f is None:
                        raise Exception("PDF not found")
                    # Извлекаем данные из запроса
                    path, doc_id, edrpou, date, receptor, filename_pdf = f
            except Exception as e:
                # Логирование ошибки с указанием RECEPTORID, если оно есть
                self.logger.error(f"!!!!Error {self.request.data.get('RECEPTORID', 'Unknown')}: {str(e)}")
                raise

            # Если год документа меньше 2024, используем удаленный путь для загрузки файла
            if int(datetime.date(date).year) < 2024:
                base_url = (
                    f'https://exc101.apteka.org.ua:9001/{str(datetime.date(date).year)}/PTAH_FILES/{str(edrpou)}/{str(receptor)}/'
                    f'{str(datetime.date(date).year)}/{str(datetime.date(date).month)}/'
                    f'{str(datetime.date(date).day)}/'
                )
                if self.is_facsimile == 1:
                    # Проверяем наличие факсимиле файла
                    file_url = os.path.join(base_url, f'f_{str(doc_id)}_{str(filename_pdf)}')
                    pdf64 = self._download_remote_pdf(file_url)
                    if pdf64:
                        return {"body": pdf64}
                else:
                    # Проверяем наличие файлов с префиксами ss_, s_, или без префикса
                    prefixes = ['ss_', 's_', '']
                    for prefix in prefixes:
                        file_url = os.path.join(base_url, f'{prefix}{str(doc_id)}_{str(filename_pdf)}')
                        pdf64 = self._download_remote_pdf(file_url)
                        if pdf64:
                            return {"body": pdf64}
            else:
                # Формирование локального пути к файлу для года 2024 и позже
                if self.is_facsimile == 1:
                    # Проверяем наличие факсимиле файла
                    file_path = self._get_file_path(path, doc_id, edrpou, receptor, date, filename_pdf, prefix='f_')
                    pdf64 = self._read_and_encode_pdf(file_path)
                    if pdf64:
                        return {"body": pdf64}
                else:
                    # Проверяем наличие файлов с префиксами ss_, s_, или без префикса
                    prefixes = ['ss_', 's_', '']
                    for prefix in prefixes:
                        file_path = self._get_file_path(path, doc_id, edrpou, receptor, date, filename_pdf, prefix)
                        pdf64 = self._read_and_encode_pdf(file_path)
                        if pdf64:
                            return {"body": pdf64}

            return {"error": "PDF file not found"}

        except Exception as e:
            self.logger.error(f"!!!!Error {self.request.data.get('RECEPTORID', 'Unknown')}: {str(e)}")
            return str(e)

        finally:
            self.myconnection.close()
