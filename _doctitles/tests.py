import base64
from django.test import TestCase
from rest_framework.test import APIClient
from rest_framework import status
from unittest.mock import patch
from django.urls import reverse
from django.contrib.auth.models import User


class DoctitlesTestCase(TestCase):
    def setUp(self):
        # Инициализация клиента для тестирования API
        self.client = APIClient()

        # URL для тестируемого маршрута
        self.url = reverse('doctitles')  # Укажи свой маршрут

        self.user = User.objects.create_user(username='vashchenko', password='1qazxsw2')
        self.username = 'vashchenko'
        self.password = '1qazxsw2'

        # Генерация base64 строки для Basic Auth
        credentials = f'{self.username}:{self.password}'
        base64_credentials = base64.b64encode(credentials.encode()).decode('utf-8')

        # Установка заголовков авторизации для клиента
        self.client.credentials(HTTP_AUTHORIZATION=f'Basic {base64_credentials}')

    @patch('set_doc.views.SetDoc.set_doc')  # Замена метода SetDoc.set_doc с помощью mock
    def test_set_doc(self, mock):
        # Подготавливаем данные для POST-запроса
        request_data = {
            "METHOD": "SET_DOC",
            "RECEPTORID": 225593,
            "IS_PACKAGE": 0,
            "DOCS": [{
                "ID": "438999",
                "TYPEID": "60",
                "NO": "PPM5_796/20",
                "NOINTERNAL": "",
                "DOCDATE": "09.10.2024",
                "BOOKDATE": "09.10.2024",
                "FIRMID": "225594",
                "RECEPTORID": "",
                "OWNERID": "225593",
                "DISCOUNTERID": "",
                "DECPRICEPR": "4",
                "DECPRICEIN": "4",
                "DECPRICEOUT": "2",
                "DECPRICERET": "2",
                "DECSUMPR": "4",
                "DECSUMIN": "4",
                "DECSUMOUT": "2",
                "DECSUMRET": "2",
                "DECSUMDOCPR": "2",
                "DECSUMDOCIN": "2",
                "DECSUMDOCOUT": "2",
                "DECSUMDOCRET": "2",
                "PRICECONDITIONSID": "",
                "DISCOUNTPERCENT": "",
                "DISCOUNTSUMMA": "",
                "ISACTIVE": "2",
                "SUMMA0": "0",
                "AVTSUMMA0": "0",
                "SUMMA1": "525,53",
                "AVTSUMMA1": "105,11",
                "SUMMA6": "723,9",
                "AVTSUMMA6": "120,65",
                "EKATYPE": "",
                "ORIGINALDOCID": "",
                "PAYTYPESID": "",
                "DELIVERYNO": "",
                "DELIVERYDATE": "",
                "DELIVERYNAME": "",
                "DELIVERYADRESS": "",
                "DELIVERYTEL": "",
                "DELIVERYBEG": "",
                "DELIVERYEND": "",
                "DELIVERYSUMMA": "",
                "COMMENTS": "",
                "CREATORID": "4975",
                "CREATEDATE": "09.10.2024 11:29:14",
                "UPDATEDDATE": "09.10.2024 11:30:12",
                "NALNO": "",
                "NALDATE": "",
                "ISREGISTER": "",
                "PAYDATE": "",
                "CLIENTID": "",
                "RESERVEID": "",
                "LANGUAGEID": "",
                "INVENTARYID": "",
                "REFERENTDATE": "",
                "REFERENTNO": "",
                "CASHROUND": "",
                "FRANCHISE": "",
                "FRANCHISEPRSNT": "",
                "ISMOVE": "1",
                "DOCCONTENTS": [{
                    "ID": "1278874",
                    "DOCID": "438999",
                    "GOODSID": "579179",
                    "SERIALID": "27122764",
                    "QUANTITY": "1",
                    "PRICE0": "",
                    "PRICE1": "525,53",
                    "PRICE6": "723,9",
                    "UKTZEDID": "2106909200",
                    "AVRCOEF": "20",
                    "AVRCOEFOUT": "20",
                    "CREATEDATE": "09.10.2024 11:28:31",
                    "UPDATEDDATE": "09.10.2024 11:29:14",
                    "PRICEREFERENT": "",
                    "PRICEFRANCHISE": "",
                    "SERIALS": {
                        "ID": "27122764",
                        "GOODSID": "579179",
                        "SERIALNO": "324136",
                        "EXPIREDDATE": "01.03.2027",
                        "SOQNO": "Б/Н",
                        "SOQDATE": "21.06.2024",
                        "COMMENTS": "",
                        "CREATORID": "",
                        "CREATEDATE": "02.08.2024 18:44:15",
                        "UPDATEDDATE": "02.08.2024 18:44:15"
                    }
                }]
            }]
        }

        # Настраиваем mock для метода set_doc
        mock.return_value = [
            {
                "DOCID": 438999,
                "ORIGINALDOCID": 40787803
            }
        ]

        # Отправляем POST-запрос с данными
        response = self.client.post(self.url, request_data, format='json')

        # Проверяем статус-код ответа
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        # Проверяем, что возвращаемые данные совпадают с ожидаемыми
        self.assertEqual(response.json(), [
            {
                "DOCID": 438999,
                "ORIGINALDOCID": 40787803
            }
        ])

        # Проверяем, что mock метод был вызван один раз
        mock.assert_called_once()

    @patch('get_sql.views.GetSql.get_sql')
    def test_get_sql(self, mock):
        request_data = {
            "METHOD": "GETSQL",
            "RECEPTORID": 500941,
            "IS_PACKAGE": 0,
            "CREATEDATE": "12.09.2024 17:00:22"
        }
        mock.return_value = {"result": "some_value"}
        # Отправляем POST-запрос с данными
        response = self.client.post(self.url, request_data, format='json')

        # Проверяем статус-код ответа
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        # Проверяем, что mock метод был вызван один раз
        mock.assert_called_once()

    @patch('set_doc_state.views.set_docstate')
    def test_set_doc_state(self, mock):
        request_data = {
            "METHOD": "SETDOCSTATE",
            "IS_PACKAGE": 0,
            "RECEPTORID": 6765,
            "DOCS": [{
                "ID": 40787683,
                "ISACTIVE": 1
            }]
        }

        mock.return_value = [
            {
                "ID": 40787683,
                "ISACTIVE": 1
            }
        ]

        # Отправляем POST-запрос с данными
        response = self.client.post(self.url, request_data, format='json')

        # Проверяем статус-код ответа
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        # Проверяем, что mock метод был вызван один раз только для класса
        # mock.assert_called_once()

    @patch('get_doc_state.views.get_doc_state')
    def test_get_doc_state(self, mock):
        request_data = {
            "METHOD": "GETDOCSTATE",
            "IS_PACKAGE": 0,
            "RECEPTORID": 13908,
            "DOCS": [
                40791514
            ]
        }

        mock.return_value = [
            {
                "ID": 40791514,
                "ISACTIVE": 1
            }
        ]

        # Отправляем POST-запрос с данными
        response = self.client.post(self.url, request_data, format='json')

        # Проверяем статус-код ответа
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    @patch('set_delivery.views.SetDelivery.set_delivery')
    def test_set_delivery(self, mock):
        request_data = {
            "METHOD": "SET_DELIVERY",
            "RECEPTORID": 228525,
            "IS_PACKAGE": 0,
            "DELIVERIES": [
                {
                    "ID": 7,
                    "ORIGINALDOCID": 1725,
                    "DELIVERYDATE": "25.09.2024",
                    "DELIVERY_ID": 167340,
                    "OWNER_NAME": "OPTIMA",
                    "DESCRIPTION": "",
                    "SUMM": 38226.9,
                    "STATUS": 110,
                    "TBL_DELIVERY_DET": [
                        {
                            "ID": "192",
                            "DI_ID": "7",
                            "NUMBERBOX": "1",
                            "HEIGTH": "20",
                            "WIDTH": "25",
                            "LENGTH": "40",
                            "WEIGHT": "4,5",
                            "SUMM": "20000",
                            "DESCRIPTION": "",
                            "HASFRAGILE": "0",
                            "BOXUID": "",
                            "HASCOLD": "0",
                            "BARCODEBOX": ""
                        },
                        {
                            "ID": "193",
                            "DI_ID": "7",
                            "NUMBERBOX": "2",
                            "HEIGTH": "20",
                            "WIDTH": "28",
                            "LENGTH": "54",
                            "WEIGHT": "3,5",
                            "SUMM": "18226,9",
                            "DESCRIPTION": "",
                            "HASFRAGILE": "0",
                            "BOXUID": "",
                            "HASCOLD": "0",
                            "BARCODEBOX": ""
                        }
                    ],
                    "TBL_DELIVERY_DOCS": [
                        40252710,
                        40252998
                    ]
                }
            ]
        }

        mock.return_value = [
            {
                "ORIGINALDOCID": 1725,
                "DELIVERY_ID": 167340,
                "ID": 7,
                "DELIVERYDATE": "25/09/2024 00:00:00",
                "STATUS": 199,
                "OWNER_NAME": "OPTIMA"
            }
        ]

        # Отправляем POST-запрос с данными
        response = self.client.post(self.url, request_data, format='json')

        # Проверяем статус-код ответа
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        # Проверяем, что mock метод был вызван один раз
        mock.assert_called_once()

    @patch('get_doc.views.GetDoc.get_doc')
    def test_get_doc(self, mock):
        request_data = {
            "METHOD": "GETDOC",
            "IS_PACKAGE": 0,
            "RECEPTORID": 501411,
            "DAYS": 15,
            "TYPEID": 1,
            "DOCS": [40533453,
                     40566591,
                     40482991,
                     40614634,
                     40376579,
                     40366896,
                     40426683,
                     40577203]
        }
        mock.return_value = {"result": "some_value"}
        # Отправляем POST-запрос с данными
        response = self.client.post(self.url, request_data, format='json')

        # Проверяем статус-код ответа
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        # Проверяем, что mock метод был вызван один раз
        mock.assert_called_once()

    @patch('get_doc_ptah.views.GetDocPtah.get_doc_ptah')
    def test_get_doc_ptah(self, mock):
        request_data = {
            "METHOD": "GETDOCPTAH",
            "RECEPTORID": 500718,
            "IS_PACKAGE": 0,
            "DAYS": 15,
            "TYPEID": 1,
            "DOCS": [6162751,
                     6162778,
                     6163082,
                     6163105,
                     6163106,
                     6163107,
                     6163108]
        }
        mock.return_value = {"result": "some_value"}
        # Отправляем POST-запрос с данными
        response = self.client.post(self.url, request_data, format='json')

        # Проверяем статус-код ответа
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        # Проверяем, что mock метод был вызван один раз
        mock.assert_called_once()

