FROM python:3.8

RUN mkdir /daupler

WORKDIR /daupler

COPY ./requirements.txt /daupler/requirements.txt

RUN pip install --upgrade pip ipython
RUN pip install -r requirements.txt 

CMD ["python", "/daupler/consumer_example.py"]
