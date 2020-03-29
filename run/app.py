from datetime import datetime
import os

from controllers.KafkaController import KafkaConsumerLocal, KafkaProducerLocal
from controllers.DatabaseController import database_insert
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
            node_id = None
            value = None
            timestamp = None

            # Check if the massage matches our expected format
            # if not, skip this message
            # We want to silently fail if a message cannot be read so the service does not fail
            # The failure could be written to a log, or a Kafka topic for further analysis
            try:
                (node_id, value, timestamp) = message_is_valid(message)
            except ValueError as e:
                print('A value was incorrect')
                print(e)
                continue

            except Exception as e:
                print('We hit an error')
                print(e)
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
            count = None
            minv = None
            maxv = None
            avgv = None

            # catch any failures when reading the node and value arrays
            # there should be no failures as we explicitly know the data they contain
            # if a row insert fails, we want to ensure the other rows are still inserted
            try:
                (count, minv, maxv, avgv) = get_min_max_avg_values(node_values_copy[i])
            except Exception as e:
                print('Failed to insert node data into the database')
                print(e)
                continue

            database_insert(node_id, count, minv, maxv, avgv)

            # update the amount of database inserts we've done
            self.database_insert_count = self.database_insert_count + 1

            # increment the loop
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
