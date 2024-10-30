from django.http import JsonResponse, HttpResponse
from django.views import View
from rest_framework.views import APIView
from rest_framework import status
import json
from rest_framework.permissions import IsAuthenticated

from get_doc.views import GetDoc
from get_doc_ptah.views import GetDocPtah
from get_pdf_ptah.views import GetPDFPtah
from set_doc.views import SetDoc
from set_delivery.views import SetDelivery
from get_sql.views import GetSql
from set_work_period.views import SetWorkPeriod

import logging
from logging.handlers import RotatingFileHandler
from functools import wraps


def create_logs(method):
    # Получаем логгер для метода
    logger = logging.getLogger(method)

    # Проверяем, есть ли у логгера уже обработчики, чтобы не добавлять их повторно
    if not logger.handlers:
        handler = RotatingFileHandler(f"D:\\API LOGS\\_{method}.log", maxBytes=200 * 1024 * 1024, backupCount=3)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)

        logger.setLevel(logging.INFO)
        logger.addHandler(handler)

    return logger


# Декоратор для логирования
def log_request(func):
    @wraps(func)
    def wrapper(self, request, *args, **kwargs):
        # Создаем список для сбора логов в процессе выполнения функции
        log_entries = []
        logger = create_logs(request.data['METHOD'])
        receptorid = request.data.get('RECEPTORID', 'unknown')
        log_entries.append('--------------------------------')
        log_entries.append(f"Request data {receptorid}: {str(request.data)}")
        try:
            # Добавляем логгер в запрос, чтобы его можно было передавать дальше
            request.logger = logger

            result = func(self, request, *args, **kwargs)

            # Исключение методов, где ответ очень большой
            if request.data['METHOD'] in ('GETDOC', 'GETDOCPTAH', 'GETSQL'):
                log_entries.append(f"Response {receptorid}: {str(result)[:900]}")
            else:
                # Проверка результата, если он JsonResponse, получаем данные, иначе преобразуем в JsonResponse
                if isinstance(result, JsonResponse):
                    response_data = result.content.decode('utf-8')
                else:
                    response_data = json.dumps(result, ensure_ascii=False)
                    result = JsonResponse(result, safe=False)

                log_entries.append(f"Response {receptorid}: {response_data[:2000]}")

            return result

        except Exception as e:
            log_entries.append(f"Error during {receptorid} -> {str(e)}")
            logger.error('!!!!!!!')
            logger.error(f"Error during {receptorid} -> {str(e)}")
            raise e

        finally:
            for entry in log_entries:
                logger.info(entry)

    return wrapper


class Doctitles(APIView):
    permission_classes = [IsAuthenticated]

    @log_request
    def post(self, request):
        method = request.data['METHOD']
        is_package = request.data['IS_PACKAGE']
        receptorid = request.data['RECEPTORID']

        if int(is_package) == 1:
            result = json.loads(oracle.request_function(method, str(request.data)))
            return JsonResponse(result, safe=False)

        # Словарь для сопоставления методов
        method_map = {
            'GETDOC': lambda: GetDoc(request, receptorid).get_doc(),
            'GETDOCPTAH': lambda: GetDocPtah(request, receptorid).get_doc_ptah(),
            'GETPDFPTAH': lambda: GetPDFPtah(request).get_pdf_ptah(),
            'SET_DOC': lambda: SetDoc(request).set_doc(),
            'SET_DELIVERY': lambda: SetDelivery(request).set_delivery(),
            'GETSQL': lambda: GetSql(request).get_sql(),
            'SQLRESULT': lambda: GetSql(request).get_sql_results(),
            'SETWORKPERIOD': lambda: SetWorkPeriod(request).set_work_period(),
            'SETWORKSTATE': lambda: SetWorkPeriod(request).set_work_state(),
            'UPDATEWORKCONTENTS': lambda: SetWorkPeriod(request).update_work_contents(),
        }

        # Выполнение соответствующей функции из словаря
        if method in method_map:
            result = method_map[method]()
            return JsonResponse(result, safe=False)
        else:
            return JsonResponse(status.HTTP_204_NO_CONTENT, safe=False)
