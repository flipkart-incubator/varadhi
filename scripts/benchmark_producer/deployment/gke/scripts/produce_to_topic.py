from locust import TaskSet, HttpUser

import random
import string
import os

from util import handle_response, getPublishHeaders, getId
from config import *

class PayloadUsage:
    payload = ""
    used = 0
    max_usage_allowed = 0


class MetaData:
    groupId = ""
    messageCount = 0


def new_payload(i):
    p = PayloadUsage()
    size = random.randint(MIN_MESSAGE_SIZE_KB * 1000, MAX_MESSAGE_SIZE_KB * 1000)
    p.payload = ''.join(random.choices(string.ascii_letters + string.digits, k=size))
    p.used = 0
    p.max_usage_allowed = (i % RANDOM_PAYLOAD_MAX_USAGE) + 1  # just to shuffle some usage.
    return p


def get_topic_distribution():
    buckets = [0] * TOPIC_RANDOMIZATION_BUCKETS
    total_topics = TOPIC_END_IDX - TOPIC_START_IDX + 1


    probabilties = list(TOPIC_PROBABILITIES) # clone
    if len(probabilties) == 0:
        # assuming uniform distribution
        probabilties = list(map(lambda t: 100.0/total_topics, range(0, total_topics)))


    cumulative_probability = 0.0
    bucket_idx = 0
    for i in range(0, total_topics):
        cumulative_probability += probabilties[i]
        bucket_end_idx = (cumulative_probability * TOPIC_RANDOMIZATION_BUCKETS / 100.0) - 1
        while bucket_idx <= bucket_end_idx:
            buckets[bucket_idx] = TOPIC_START_IDX + i
            bucket_idx += 1
    # take care of the last one
    buckets[TOPIC_RANDOMIZATION_BUCKETS - 1] = TOPIC_END_IDX
    return buckets


def new_group_id():
    group_metadata = MetaData()
    group_metadata.groupId = getId()
    group_metadata.messageCount = 1
    return group_metadata


def publish_to_topic(task, topic_name, grouped=True, project_name="project-dec5"):
    groupId = task.user.get_group_id(topic_name, grouped)
    headers = getPublishHeaders(groupId)
    url = PRODUCE_URL.format(project_name, topic_name)
    data = '{{"content":"{}"}}'.format(task.user.get_request_payload())
    response = task.client.post(PRODUCE_ENDPOINT + url, headers=headers, data=data,
                                timeout=(1, 60), verify=False, catch_response=True,
                                name=topic_name)
    # Passing the catched response to handle_response utility method.
    handle_response(task, response, topic_name)


def publish(task):
    topic_id = task.user.get_random_topic_idx()
    topic_name = os.environ.get("TOPIC_NAME")


    if topic_name is None:
        print("TOPIC_NAME environment variable is not set.")
    else:
        print("Topic name:", topic_name)


    project_name = "project-dec5"
    publish_to_topic(task, topic_name, project_name)


class PublishMessageTaskSet(TaskSet):
    min_wait = 0
    max_wait = 0
    tasks = {
        publish: 1
    }


class PublishMessageUser(HttpUser):
    host = ""
    metadataMap = {}
    random_payloads = list(map(lambda i: new_payload(i + 1), range(0, RANDOM_PAYLOAD_POOL_SIZE)))
    topic_distribution = get_topic_distribution()
    tasks = [PublishMessageTaskSet]


    def get_request_payload(self):
        idx = random.randint(0, RANDOM_PAYLOAD_POOL_SIZE - 1)
        p = self.random_payloads[idx]
        if p.used == p.max_usage_allowed:
            self.random_payloads[idx] = new_payload(idx)
        p = self.random_payloads[idx]
        p.used += 1
        return p.payload


    def get_random_topic_idx(self):
        #random number to produce messages to topics in required ratio
        rand_num = random.randint(0, TOPIC_RANDOMIZATION_BUCKETS - 1)
        topic_id = self.topic_distribution[rand_num]
        return topic_id


    def get_group_id(self, topic_name, grouped):
        if grouped :
            if topic_name in self.metadataMap:
                # previous group is still ongoing. use the same group
                if self.metadataMap[topic_name].messageCount < MESSAGE_COUNT_PER_GROUP:
                    self.metadataMap[topic_name].messageCount += 1
                    return self.metadataMap[topic_name].groupId


            # either the topic is not there or the group is complete
            new_group_metadata = new_group_id()
            self.metadataMap[topic_name] = new_group_metadata
            return new_group_metadata.groupId
        else:
            return getId()
