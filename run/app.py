from datetime import datetime

from controllers.KafkaController import KafkaConsumerLocal, KafkaProducerLocal
from controllers.DatabaseController import database_insert
import os


INCOMING_DATA_STEAM_TOPIC = 'CodingAssignment'
TIMER_ACTION = 60

CLEAR_SCREEN = lambda: os.system('clear')


class CodingAssignment:
    kafka_producer = None
    kafka_consumer = None

    first_timestamp = None
    five_second_timer = None
    message_count = 0
    database_insert_count = 0

    # lists to store our nodes and values
    nodes = None
    node_values = None

    def __init__(self):
        self.kafka_producer = KafkaProducerLocal()
        self.kafka_consumer = KafkaConsumerLocal(INCOMING_DATA_STEAM_TOPIC)

        self.five_second_timer = int(datetime.now().timestamp())

        self.__reset_lists()

        CLEAR_SCREEN()
        print('Starting...')

        self.read_kafka_stream()

    def read_kafka_stream(self):
        # Do something for every message received on the Kafka Topic
        for message in self.kafka_consumer:

            message = message.value
            self.message_count = self.message_count + 1

            self.messages_per_second()

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
            if self.first_timestamp is None:
                self.first_timestamp = timestamp

            # send the data to be stored in our nodes and node_value lists
            self.add_node_and_value(node_id, value)

            # check if we've had greater then 60 seconds worth of data
            # if we have, send this data to our database to be stored
            if timestamp >= self.first_timestamp + TIMER_ACTION:
                print('We\'ve run for ' + str(TIMER_ACTION) + ' seconds')

                nodes_copy = self.nodes
                node_values_copy = self.node_values

                self.__write_to_database(nodes_copy, node_values_copy)

                # remove the data we've now saved from our lists
                # resets the time field
                self.__reset_lists()

    def messages_per_second(self):
        current_timestamp = int(datetime.now().timestamp())

        if int(current_timestamp > self.five_second_timer + 5):
            CLEAR_SCREEN()

            print('Analysing: ' + str(self.message_count) + ' messages every 5 seconds')

            if self.database_insert_count is not 0:
                print('Inserted: ' + str(self.database_insert_count) + ' rows into the Database')

            # reset message_count and timer
            self.five_second_timer = current_timestamp
            self.message_count = 0

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
        print('Writing to database')

        # get the count of nodes
        nodes_copy_len = len(nodes_copy)

        i = 0
        while i < nodes_copy_len:
            node_id = nodes_copy[i]
            (count, minv, maxv, avgv) = self.get_min_max_avg_values(node_values_copy[i])
            database_insert(node_id, count, minv, maxv, avgv)
            self.database_insert_count = self.database_insert_count + 1

            i = i + 1

        print('Finished Writing to Database')

    def get_min_max_avg_values(self, list):
        min_value = None
        max_value = None
        avg_value = None

        count = 0
        sum_of_values = 0

        for value in list:
            # for every value, increment the count and add to the sum_of_values
            count = count + 1
            sum_of_values = sum_of_values + value

            # for the first value
            if min_value is None:
                min_value = value
                max_value = value
                continue

            # if this value is smaller
            if value < min_value:
                min_value = value

            # if thi value is larger
            if value > max_value:
                max_value = value

        # get the average by dividing sum_of_values by count
        avg_value = sum_of_values / count

        # return as a tuple
        return count, min_value, max_value, avg_value

    def __reset_lists(self):
        self.nodes = []
        self.node_values = []
        self.first_timestamp = None


if __name__ == "__main__":
    # Lets start
    codingAssignment = CodingAssignment()
