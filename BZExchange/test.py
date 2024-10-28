from datetime import datetime


d = '2023-04-17T12:58:34'
print(datetime.fromisoformat(d).strftime('%d/%m/%Y %X'))
