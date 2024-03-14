FROM python:3.11

RUN mkdir -p /locust/scripts
WORKDIR /locust
RUN pip install locust

COPY scripts /locust/scripts

EXPOSE 8089

CMD ["sh", "-c", "locust -f /locust/scripts/produce_to_topic.py"]
