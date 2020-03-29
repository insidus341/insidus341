from datetime import datetime
import os

from run.controllers.KafkaController import KafkaConsumerLocal, KafkaProducerLocal
from run.controllers.DatabaseController import database_insert
from tools import message_is_valid, add_node_and_value, get_min_max_avg_values



INCOMING_DATA_STEAM_TOPIC = 'CodingAssignment'
TIMER_ACTION = 60


# used to clear the console screen
def clear_screen():
    clear = lambda: os.system('clear')
    clear()


class CodingAssignment:
    kafka_producer = None
    kafka_consumer = None

    # used to hold the first timestamp reported
    first_timestamp = None

    # lists to store our nodes and their values
    nodes = None
    node_values = None

    # used for statistic reporting
    one_second_timer = None
    message_count = 0
    database_insert_count = 0

    def __init__(self):
        self.kafka_producer = KafkaProducerLocal()
        self.kafka_consumer = KafkaConsumerLocal(INCOMING_DATA_STEAM_TOPIC)

        self.one_second_timer = int(datetime.now().timestamp())

        self.__reset_lists()

        clear_screen()
        print('Starting...')

    def read_kafka_stream(self):
        # Do something for every message received on the Kafka Topic
        for message in self.kafka_consumer:

            message = message.value

            # Check if the massage matches our expected format
            # if not, skip this message
            try:
                (node_id, value, timestamp) = message_is_valid(message)
            except:
                # TODO Add error handling here
                # for now, we can just continue and ignore the message
                print('We hit an error')
                continue

            # used to count how many messages we process every 5 seconds
            self.message_count = self.message_count + 1
            self.messages_per_one_second()

            # Start our timer from the first timestamp received
            if self.first_timestamp is None:
                self.first_timestamp = timestamp

            # send the data to be stored in our nodes and node_value lists
            (self.nodes, self.node_values) = add_node_and_value(node_id, value, self.nodes, self.node_values)

            # check if we've had greater then 60 seconds worth of data
            # if we have, send this data to our database to be stored
            if timestamp >= self.first_timestamp + TIMER_ACTION:
                print('Analyzed ' + str(TIMER_ACTION) + ' seconds worth of data')

                nodes_copy = self.nodes
                node_values_copy = self.node_values

                self.__write_to_database(nodes_copy, node_values_copy)

                # remove the data we've now saved from our lists
                # resets the time field
                self.__reset_lists()

    def messages_per_one_second(self):
        current_timestamp = int(datetime.now().timestamp())

        if int(current_timestamp > self.one_second_timer + 1):
            clear_screen()

            print('Analysing: ' + str(self.message_count) + ' messages every 1 second')

            if self.database_insert_count is not 0:
                print('Inserted: ' + str(self.database_insert_count) + ' rows into the Database')

            # reset message_count and timer
            self.one_second_timer = current_timestamp
            self.message_count = 0

    def __write_to_database(self, nodes_copy, node_values_copy):
        print('Writing to database')

        # get the count of nodes
        nodes_copy_len = len(nodes_copy)

        i = 0
        while i < nodes_copy_len:
            node_id = nodes_copy[i]
            (count, minv, maxv, avgv) = get_min_max_avg_values(node_values_copy[i])
            database_insert(node_id, count, minv, maxv, avgv)

            # update the amount of database inserts we've done
            self.database_insert_count = self.database_insert_count + 1

            i = i + 1

        print('Finished Writing to Database')

    def __reset_lists(self):
        # this run whenever we push data to the database
        # this resets the lists and the first_timestamp field
        # if the lists are not reset, you can imagine an ever expanding list

        self.nodes = []
        self.node_values = []
        self.first_timestamp = None


if __name__ == "__main__":
    # Lets start
    codingAssignment = CodingAssignment()
    codingAssignment.read_kafka_stream()
