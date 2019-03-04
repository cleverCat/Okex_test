FROM ubuntu:17.10

# Устанавливаем локаль
ENV LANG en_US.UTF-8

# Добавляем необходимые репозитoрии и устанавливаем пакеты
RUN apt-get update
RUN apt-get upgrade -y
RUN apt-get install -y wget curl mc nano tar git net-tools build-essential openssh-server
RUN apt-get install -y --reinstall language-pack-en
RUN apt-get install -y language-pack-ru
RUN update-locale LC_ALL=en_US.UTF-8 LANG=en_US.UTF-8
RUN apt-get install -y libffi-dev libssl-dev libxml2-dev libxslt1-dev libjpeg8-dev zlib1g-dev libpq-dev
RUN apt-get install -y python3 python3-dev python3-setuptools python3-pip 
RUN apt-get install -y nginx uwsgi uwsgi-plugin-python3

# Устанавливаем пакеты
RUN pip3 install --upgrade pip
RUN pip install pipenv

COPY hqlib/ /srv
WORKDIR /srv
RUN pipenv install


