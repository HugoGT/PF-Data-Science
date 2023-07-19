# Backend APP

import pickle

import phonenumbers
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from pydantic import BaseModel
from typing import Optional
from twilio.rest import Client

from keys import ACCOUNT_SID, AUTH_TOKEN


class FormData(BaseModel):
    nombre: str
    email: Optional[str]
    celular: str
    ubicacion: str


app = FastAPI(title='API ALERTA SISMOS')
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

users_list = []

with open('predictor.pkl', 'rb') as f:
    predictor = pickle.load(f)


def enviar_mensaje(message):
    client = Client(ACCOUNT_SID, AUTH_TOKEN)

    from_whatsapp_number = 'whatsapp:+14155238886'

    message_body = message

    try:
        message = client.messages.create(
            body=message_body,
            from_=from_whatsapp_number,
            to=users_list
        )

        message.sid

    except Exception as e:
        return {'No se pudo enviar el mensaje.': e}


@app.get("/users")
def i():
    return users_list


@app.post("/guardar_usuario", tags=['App'])
def guardar_usuario(data: FormData):
    try:
        phone = phonenumbers.parse(data.celular, None)

        if phonenumbers.is_valid_number(phone):
            users_list.append('whatsapp:' + phonenumbers.format_number(phone, phonenumbers.PhoneNumberFormat.E164))
            return {"message": "Usuario guardado correctamente"}
        else:
            return {"message": "Número de celular inválido"}

    except phonenumbers.NumberParseException:
        return {"message": "Error en el número de celular"}


@app.post("/recibir_sismo", tags=['Alertas'])
def recibir_sismo(mag: str, prof: str, departamento: str):
    nivel = predictor([[float(mag), float(prof)]])[0] - 1

    messages = [
        f'🫨🫨 ¿Lo sentiste? 🫨🫨 Sismo de nivel 1 en {departamento}, Precaución, posibles réplicas. Asegura objetos que puedan caer.',
        f'🟡🟡 Atento 🟡🟡 Sismo de nivel 2 en {departamento}, evalúa la seguridad de tu entorno. Identifica salidas y zonas seguras cercanas.',
        f'🛑🛑 ¡¡¡Peligro!!! 🛑🛑 Sismo nivel 3 en {departamento}, busca refugio inmediatamente. Aléjate de ventanas y objetos pesados.'
    ]
    enviar_mensaje(messages[nivel])

    return {"message": "Mensaje enviado correctamente"}
