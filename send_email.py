import smtplib
from email.message import EmailMessage
import ssl
import smtplib

email_sender = "medabdallahi.karim@gmail.com"
email_psw = "jbgqcstnnxxsiolc"
email_reciever = "mabdkarim@outlook.fr"
subject = "Test mail database"
body = """
Bonjour,

Ceci est un code de test.

Mohamed KARIM
"""
em = EmailMessage()
em["From"]=email_sender
em["To"]=email_reciever
em["Subject"]=subject
em.set_content(body)

context=ssl.create_default_context()

with smtplib.SMTP_SSL("smtp.gmail.com",465, context=context) as smtp:
    smtp.login(email_sender,email_psw)
    smtp.sendmail(email_sender,email_reciever,em.as_string())

print("Success!")