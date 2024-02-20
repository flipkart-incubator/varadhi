PRODUCE_ENDPOINT = "http://127.0.0.1:8080"
PRODUCE_URL = "/v1/projects/{}/topics/{}/produce"

MESSAGE_COUNT_PER_GROUP = 1
MIN_MESSAGE_SIZE_KB = 2
MAX_MESSAGE_SIZE_KB = 8
# pool of random messages that are cached, to reduce randomization costs. also to allow some scope of compression.
RANDOM_PAYLOAD_POOL_SIZE = 1024
# how many times a cached payload can be used to produce message. It is only a max and does not mean that every msg will be used this many times.
RANDOM_PAYLOAD_MAX_USAGE = 10
TOPIC_START_IDX = 1
TOPIC_END_IDX = 2
# to changes the probability of production of topics. list of percentage. If empty uniform distrubtion will be assumed. If populated, then its sum should be 100.
TOPIC_PROBABILITIES = []
# bucket count to find the topic. helpful when probablities are arbitary numbers. Then large # buckets gives accurate distribution.
# for eg: if there is a topic with 0.5% probability, then having multiple of 200 buckets will help represent that probability.
TOPIC_RANDOMIZATION_BUCKETS = 1000
