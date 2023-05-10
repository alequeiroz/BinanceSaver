FROM python:3.10.9-slim


COPY . /app
WORKDIR /app

RUN apt-get update \
    && apt-get install gcc -y \
    && apt-get install ffmpeg libsm6 libxext6  -y \
    && apt-get clean

RUN pip install pip --upgrade \ 
    && pip install -r requirements.txt

#RUN docker run -it -e NGROK_AUTHTOKEN=2IzXDoKumt25qA2PGgTOxrx1dd2_6uWidZc9M85EMx7Q47KxK ngrok/ngrok:latest http host.docker.internal:8082

EXPOSE 8081  

CMD [ "python","main.py" ]