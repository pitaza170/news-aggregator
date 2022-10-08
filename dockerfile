FROM alpine:3.6

WORKDIR /app

COPY . /app

RUN apt install python3.8
RUN python3 get-pip.py
RUN pip install -r requirements.txt
RUN apt-get update && apt-get -y install cron
RUN mv crontab /etc/crontabs/root

CMD ["crond", "-f", "-d", "8"]
