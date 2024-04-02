FROM python:3.10-slim-buster
ENV PYTHONUNBUFFERED 1
RUN mkdir -p /home/realtimenotifier/
COPY ./requirements.txt /home/realtimenotifier/
RUN pip install -r /home/realtimenotifier/requirements.txt
COPY ./src/ /home/realtimenotifier/
WORKDIR /home/realtimenotifier
EXPOSE 5000
CMD python3 main.py
