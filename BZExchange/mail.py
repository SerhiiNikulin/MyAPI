import email.utils
import smtplib
from email.message import EmailMessage
from django.http import JsonResponse


def send_mail(sender, recipient_email, subject, content):
    pwd = 'Aichove7'
    msg = EmailMessage()
    msg['Date'] = email.utils.formatdate(localtime=True)
    msg['Subject'] = subject[:79]
    msg['From'] = sender  # Указание адреса отправителя
    msg['To'] = recipient_email  # Указание адреса получателя
    # msg.set_content(content)
    msg.set_content(content, subtype='html')
    try:
        with smtplib.SMTP('192.168.201.70', 587) as smtp:
            smtp.login(sender, pwd)
            smtp.send_message(msg)
            print('Email sent successfully')
            return JsonResponse({"message": "Email sent successfully"}, safe=False)
    except Exception as e:
        return JsonResponse({"error": f'Error sending email: {str(e)}'}, status_code=500, safe=False)
