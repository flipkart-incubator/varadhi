FROM python:3.10

RUN mkdir -p /locust/scripts
WORKDIR /locust
RUN pip install locust

COPY scripts /locust/scripts

EXPOSE 8089

# Default configuration
ENV NUM_USERS=100
ENV HATCH_RATE=10
ENV MESSAGE_COUNT=1000
ENV TOPIC_NAME=topic-9jan3


# Command to run when the container starts. User can enter configuration manually.
# CMD ["locust", "-f", "/locust/scripts/produce_to_topic.py", "-u", "$NUM_USERS", "-r", "$HATCH_RATE", "--message-count", "$MESSAGE_COUNT", "--topic-name", "$TOPIC_NAME"]
CMD ["sh", "-c", "locust -f /locust/scripts/produce_to_topic.py -u $NUM_USERS -r $HATCH_RATE --tags \"$MESSAGE_COUNT\" \"$TOPIC_NAME\""]
