import random, string, uuid


def handle_response(self, response, key):
    """
    Method to club different status codes.
    :param self:
    :param response: complete response is passed ( catch_response = True should be passed while making requests )
    :param key: key is the value displayed for failed requests under Error tab in Locust UI
    :return: returns nothing
    """
    with response as response:
        if response.status_code == 0:
            response.failure("0 " + key)
        if response.status_code == 404:
            response.failure("404 " + key)
        elif response.status_code == 400:
            response.failure("400 " + key)
        elif response.status_code == 500:
            response.failure("500 " + key)
        elif response.status_code == 429:
            response.failure("429 " + key)
        elif response.status_code == 401:
            response.failure("401 " + key)
        elif response.status_code == 502:
            response.failure("502 " + key)
        elif response.status_code == 503:
            response.failure("503 " + key)
        elif response.status_code == 408:
            response.failure("408 " + key)
        else:
            if response.status_code == 200:
                response.success()
            elif response.status_code == 202:
                response.success()
            else:
                response.failure("Unknown " + str(response.status_code) + key)

def stop(self):
    """
    Passes the control to the parent TaskSet.
    :param self:
    :return: returns nothing.
    """
    self.interrupt()


def getPublishHeaders(groupId):
    id = getId()
    HEADER = {
        "Content-Type": "application/json",
        "X_RESTBUS_MESSAGE_ID": id,
        "X_PRODUCER_APP_ID": "benchmark",
        "Accept": "application/json",
        "X_RESTBUS_HTTP_METHOD": "POST",
        "X_RESTBUS_GROUP_ID": groupId
    }
    return HEADER

def getId():
    return str(uuid.uuid4())
