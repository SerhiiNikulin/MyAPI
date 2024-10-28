from datetime import datetime
import cx_Oracle
import email.utils
import smtplib
from email.message import EmailMessage
from .queries import upd_query
from dotenv import load_dotenv
import os
from set_doc.views import SetDoc

load_dotenv()

class SetWorkPeriod(SetDoc):
    def __init__(self, request):
        super().__init__(request)
        if self.request.data['METHOD'] == 'SETWORKPERIOD':
            self.columns_titles = self._get_columns('TORG.DT_WORKPERIOD')
            self.columns_contents = self._get_columns('TORG.DC_WORKPERIOD')
            self.query_doctitles = self.create_query('TORG.DT_WORKPERIOD', self.columns_titles)
            self.query_doccontents = self.create_query('TORG.DC_WORKPERIOD', self.columns_contents)

        self.months_ua = {
            'January': 'Січень',
            'February': 'Лютий',
            'March': 'Березень',
            'April': 'Квітень',
            'May': 'Травень',
            'June': 'Червень',
            'July': 'Липень',
            'August': 'Серпень',
            'September': 'Вересень',
            'October': 'Жовтень',
            'November': 'Листопад',
            'December': 'Грудень'
        }
        self.status_dict = {
            1: 'Затверджено',
            2: 'Очікує затвердження',
            3: 'Не затверджено',
            4: 'Запит на редагування',
            5: 'Редагування дозволено',
            6: 'Редагування заборонено'
        }

    # Конект к серверу почты
    @staticmethod
    def _connect_mail(sender, recipient_email, subject, content):
        pwd = os.getenv("PASSWORD_MAIL")
        msg = EmailMessage()
        msg['Date'] = email.utils.formatdate(localtime=True)
        msg['Subject'] = subject[:79]
        msg['From'] = sender
        msg['To'] = recipient_email
        # msg.set_content(content)
        msg.set_content(content, subtype='html')
        try:
            with smtplib.SMTP('192.168.201.70', 587) as smtp:
                smtp.login(sender, pwd)
                smtp.send_message(msg)
                print('yes')
        except Exception as e:
            return {"error": f'Error sending email: {str(e)}'}

    # Получает email ком.дира и название аптеки
    def _search_mail(self, doc):
        self.mycursor.execute(
            f'''select u.email, f.name from NAU_TEST.FIRMS f 
                left join NAU_TEST.USERS u on u.id = f.COMMERCIALDIRECTOR
                where f.id = {int(doc['OWNERID'].strip())}''')
        return self.mycursor.fetchall()

    # Получает нужные поля для вставки в HTML
    def _send_mail(self, doc):
        check_doc = self.__check_existing_doc(doc)
        mail = self._search_mail(doc)
        date_str = check_doc[0][3]  # Получаем дату
        date_obj = datetime.strptime(date_str, '%d.%m.%Y')  # Преобразуем в объект datetime
        month_en = date_obj.strftime('%B')  # Получаем месяц на английском
        month_ua = self.months_ua.get(month_en)  # Переводим на украинский
        status_value = check_doc[0][1]  # Получаем статус документа
        status_text = self.status_dict.get(status_value)  # Переводим статус в текст

        return mail, month_ua, status_text

    def set_work_period(self):
        response = []
        for doc in self.request.data['DOCS']:
            check_docks = self.__check_existing_doc(doc)
            if ((len(check_docks) == 0 and 'DOCCONTENTS' in doc) or
                    (int(check_docks[0][1]) == 5 and 'DOCCONTENTS' in doc and doc['DOCCONTENTS'])):
                if len(check_docks) > 0:
                    self.__delete_existing_doc(check_docks[0][0])
                titles, contents, unique_id = self.__prepare_doc_data(doc)
                try:
                    self._insert_data(titles, contents)
                    response.append({"ID": int(doc['ID'].strip()), "STATUS": 2})
                    mail, month_ua, status_text = self._send_mail(doc)
                    self._connect_mail(sender='reports@apteka.org.ua', recipient_email=mail[0][0],
                                       subject=f'''Табель робочого часу за {month_ua} від {mail[0][1]}''',
                                       content=f'''
                                        <html>
                                         <body>
                                           <p>Добрий день!</p>
                                           <p>Отримано новий графік роботи за {month_ua} від {mail[0][1]}</p>
                                           <p>Статус: {status_text}.</p>
                                           <p>Лист створено автоматично, відповідати не потрібно.</p>
                                         </body>
                                        </html>''')
                except Exception as e:
                    self.myconnection.rollback()
                    self.myconnection.close()
                    return f"Error work_period: {str(e)}"

            else:
                if len(check_docks) > 0:
                    response.append({"ID": int(doc['ID'].strip()), "STATUS": check_docks[0][1]})
                else:
                    response.append({"message": f"Not found -> {int(doc['ID'])}"})

        self.myconnection.close()
        return response

    # Проверка на существующие записи
    def __check_existing_doc(self, doc):
        self.mycursor.execute('''select ID, ISACTIVE, APT_ID, TO_CHAR(DATES, 'DD.MM.YYYY') as DATES 
                                from TORG.DT_WORKPERIOD dtw 
                                where dtw.FIRMID = :OWNERID 
                                and dtw.APT_ID = :ID''',
                              OWNERID=int(doc['OWNERID'].strip()), ID=doc['ID'])
        return self.mycursor.fetchall()

    def __delete_existing_doc(self, doc_id):
        self.mycursor.execute('''
        BEGIN
            DELETE FROM TORG.DC_WORKPERIOD WHERE DOCID = :DOCID ;
            DELETE FROM TORG.DT_WORKPERIOD WHERE ID = :DOCID ;
        COMMIT;
        END;''', DOCID=doc_id)

    def _insert_data(self, titles, contents):
        self.mycursor.executemany(self.query_doctitles, [titles])
        # self.myconnection.commit()
        success = False
        while not success:
            try:
                self.mycursor.executemany(self.query_doccontents, contents)
                success = True
            except cx_Oracle.Error as e:
                self.logger.error(f"!!!!Error insert_data {self.request.data['RECEPTORID']}: {str(e)}")
                self.myconnection.rollback()
                raise Exception(str(e))

        self.myconnection.commit()

    # Создание запросов уже без заглушек, перед инсертом
    def __prepare_doc_data(self, doc):
        titles = []
        contents = []

        self.mycursor.execute('SELECT TORG.DT_WORKPERIOD_SEQ.NEXTVAL FROM dual')
        unique_id = self.mycursor.fetchone()[0]

        for col in self.columns_titles:
            if col in doc:
                if col == 'ID':
                    titles.append(unique_id)
                elif col == 'ISACTIVE':
                    titles.append(2)
                else:
                    titles.append(doc[col].strip())
            elif col == 'FIRMID':
                titles.append(int(doc['OWNERID'].strip()))
            elif col == 'APT_ID':
                titles.append(int(doc['ID'].strip()))
            else:
                titles.append('')

        for cont in doc['DOCCONTENTS']:
            items = []
            for col in self.columns_contents:
                if col in cont:
                    if col == 'ID':
                        items.append('')
                    elif col == 'DOCID':
                        items.append(unique_id)
                    elif col == 'EMPLOYERID':
                        items.append(int(cont['EMPLOYERID'].strip()))
                    else:
                        if cont[col].strip():
                            items.append(float(cont[col].strip().replace(',', '.')))
                        else:
                            items.append(None)
                else:
                    items.append('')
            contents.append(items)

        return titles, contents, unique_id

    # апдейт статусов, которые присылаются на SETWORKSTATE
    def set_work_state(self):
        response = []
        try:
            for row in self.request.data['DOCS']:
                docs = [int(row['ISACTIVE']), row['UPDATEDDATE'], row['COMMENTS'], int(row['ID']), int(row['OWNERID'])]
                try:
                    self.mycursor.execute('''update TORG.DT_WORKPERIOD set ISACTIVE = :ISACTIVE, 
                    UPDATEDDATE = :UPDATEDDATE,
                    COMMENTS = :COMMENTS
                    where APT_ID = :ID and FIRMID = :OWNERID
                                          ''', docs)
                    if self.mycursor.rowcount > 0:
                        response.append({"ID": int(row['ID']), "STATUS": int(row['ISACTIVE'])})
                        mail, month_ua, status_text = self._send_mail(row)
                        self._connect_mail(sender='reports@apteka.org.ua', recipient_email=mail[0][0],
                                           subject=f'[Статус оновлено] Табель робочого часу за {month_ua} від {mail[0][1]}',
                                           content=f'''
                                            <html>
                                             <body>
                                               <p>Добрий день!</p>
                                               <p>Оновленно статус на графік роботи за {month_ua} від {mail[0][1]}</p>
                                               <p>Статус: {status_text}.</p>
                                               <p>Лист створено автоматично, відповідати не потрібно.</p>
                                             </body>
                                            </html>''')
                    else:
                        response.append({"message": f"Not found->{int(row['ID'])}"})
                except Exception as e:
                    self.myconnection.rollback()
                    return f"Error work_state {str(e)}"

            self.myconnection.commit()
            return response
        finally:
            self.myconnection.close()


    def update_work_contents(self):
        docs = self.request.data['DOCS']
        try:
            self.mycursor.execute(f"select abbr from nau_test.firms f where f.id = {self.request.data['RECEPTORID']}")
            schema = self.mycursor.fetchone()[0]
            self.mycursor.executemany(upd_query(schema), docs)
            self.myconnection.commit()
            self.myconnection.close()
            return {"status": "updated"}

        except Exception as e:
            self.myconnection.rollback()
            self.myconnection.close()
            return f"Error work_state {str(e)}"
