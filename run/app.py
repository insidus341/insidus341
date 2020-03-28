from time import sleep

from controllers.KafkaController import KafkaConsumerLocal, KafkaProducerLocal
# from controllers.DatabaseController import


INCOMING_DATA_STEAM_TOPIC = 'CodingAssignment'

# Every x seconds, send collate the data ready to sent to our database
TIMER_ACTION = 60


class CodingAssignment:

    kafka_producer = None
    kafka_consumer = None

    start_timer = None

    # lists to store our nodes and values
    nodes = None
    node_values = None

    def __init__(self):
        self.kafka_producer = KafkaProducerLocal()
        self.kafka_consumer = KafkaConsumerLocal(INCOMING_DATA_STEAM_TOPIC)

        self.__reset_lists()

        self.read_kafka_stream()

    def read_kafka_stream(self):
        message_count = 0

        # Do something for every message received on the Kafka Topic
        i = 0
        for message in self.kafka_consumer:

            message = message.value
            message_count = message_count + 1
            print(message)

            # Check if the massage matches our expected format
            # if not, skip this message
            try:
                (node_id, value, timestamp) = self.message_is_valid(message)
            except:
                # TODO Add error handling here
                # for now, we can just continue and ignore the message
                print('We hit an error')
                continue

            # Start our timer from the first timestamp received
            if self.start_timer is None:
                self.start_timer = timestamp

            # send the data to be stored in our nodes and node_value lists
            self.add_node_and_value(node_id, value)

            # check if we've had greater then 60 seconds worth of data
            # if we have, send this data to our database to be stored
            if timestamp >= self.start_timer + TIMER_ACTION:
                print('We\' run for ' + str(TIMER_ACTION) + ' seconds')

                nodes_copy = self.nodes
                node_values_copy = self.node_values

                self.__write_to_database(nodes_copy, node_values_copy)

                self.__reset_lists()
                sleep(120)

            # limit run to 1000 entries
            # i = i + 1
            # if i == 1000:
            #     print(self.nodes)
            #     print(self.node_values)
            #     break


    def message_is_valid(self, message):
        # expected format: {'Node_ID': 12345678900001, 'Value': 30, 'Timestamp': 123452242}
        #  if the format is incorrect, throw a value error
        try:
            node_id = message['Node_ID']
            value = message['Value']
            timestamp = message['Timestamp']
        except:
            raise ValueError

        # if the typecast is incorrect, throw a value error
        # return the extracted values if they're correct
        if isinstance(node_id, int) and isinstance(value, int) and isinstance(timestamp, int):
            return node_id, value, timestamp
        else:
            raise ValueError

    def add_node_and_value(self, node_id, value):

        # if we haven't seen this node before, add it to the nodes list
        if node_id not in self.nodes:
            self.nodes.append(node_id)

        # get the key of the node we just added
        # first node will be 0, second 1...
        node_key = self.nodes.index(node_id)

        # make sure we have a matching key between nodes and node_values
        # nodes[0] will match node_value[0]
        # we need a list of node values
        if len(self.node_values) <= node_key:
            self.node_values.insert(node_key, [])

        # add the value in message to the node_values list
        self.node_values[node_key].append(value)

    def __write_to_database(self, nodes_copy, node_values_copy):
        print('Write to database')
        print(len(node_values_copy[0]))
        print(len(node_values_copy[1]))
        print(len(node_values_copy[2]))
        print(len(node_values_copy[3]))

    def __reset_lists(self):
        self.nodes = []
        self.node_values = []
        self.start_timer = None


if __name__ == "__main__":
    # Lets start
    codingAssignment = CodingAssignment()
