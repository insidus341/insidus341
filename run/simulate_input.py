import random
from datetime import datetime

from controllers.KafkaController import KafkaProducerLocal

OUTGOING_DATA_STREAM_TOPIC = 'CodingAssignment'

# Simulate a list of Node IDs
example_nodes = [
    12345678900001,
    12345678900002,
    12345678900003,
    12345678900004,
    12345678900005,
    12345678900006,
    12345678900007,
    12345678900008,
    12345678900009,
    12345678900010,
    12345678900011,
    12345678900012,
    12345678900013,
    12345678900014,
    12345678900015,
    12345678900016,
    12345678900017,
    12345678900018,
    12345678900019,
    12345678900020
]

producer = KafkaProducerLocal()

loop = True
while loop:
    # Select a random node from our node list
    random_node = random.randint(0, len(example_nodes) -1)

    # generate a random value
    random_value = random.randint(0, 1000000)

    # convert the timestamp into an int to remove the milliseconds
    timestamp = int(datetime.now().timestamp())

    # collect the data to send
    data = {
        'Node_ID': example_nodes[random_node],
        'Value': random_value,
        'Timestamp': timestamp
    }

    print(data)

    try:
        # send this data to the Kafka Topic
        print('Sending')
        producer.send(OUTGOING_DATA_STREAM_TOPIC, value=data)
    except:
        # If there is an exception thrown here, for now, silently ignore
        # TODO implement error handling here
        print('Send failed')
        continue

