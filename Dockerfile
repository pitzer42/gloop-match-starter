FROM python:3.7

WORKDIR /usr/app

COPY match_starter.py .
COPY requirements.txt .

RUN pip install git+https://github.com/pitzer42/gloop-lib.git
RUN pip install -r requirements.txt
RUN rm requirements.txt

CMD ["python", "match_starter.py"]