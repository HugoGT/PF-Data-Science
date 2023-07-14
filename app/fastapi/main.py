# Backend APP

import pickle

from fastapi import FastAPI
from twilio.rest import Client

from keys import ACCOUNT_SID, AUTH_TOKEN


app = FastAPI(title='API ALERTA SISMOS')
user_list = ['+51939135303', '']

with open('predictor.pkl', 'rb') as f:
    predictor = pickle.load(f)


def enviar_mensaje(message):
    auth_token = AUTH_TOKEN
    account_sid = ACCOUNT_SID
    client = Client(account_sid, auth_token)

    from_whatsapp_number = 'whatsapp:+14155238886'
    to_whatsapp_number = ['whatsapp:'+n for n in user_list]

    message_body = message

    message = client.messages.create(
        body=message_body,
        from_=from_whatsapp_number,
        to=to_whatsapp_number
    )

    message.sid


@app.post("/guardar_usuario", tags=['App'])
def guardar_usuario(number: str):
    user_list[1] = number

    return {"message": "Usuario guardado correctamente"}


@app.post("/recibir_sismo", tags=['Alertas'])
def recibir_sismo(mag: str, prof: str):
    nivel = predictor([[float(mag), float(prof)]])[0] - 1

    messages = [
        'Sismo de nivel 1: Precaución, posibles réplicas.',
        'Sismo de nivel 2: Atención, evalúa la seguridad de tu entorno.',
        'Sismo de nivel 3: Peligro, busca refugio inmediatamente.'
    ]
    enviar_mensaje(messages[nivel])

    return {"message": "Mensaje enviado correctamente"}
