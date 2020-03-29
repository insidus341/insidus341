

def message_is_valid(message):
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


def add_node_and_value(node_id, value, node_list, value_list):
    # if we haven't seen this node before, add it to the nodes list
    if node_id not in node_list:
        node_list.append(node_id)

    # get the key of the node we just added
    # first node will be 0, second 1...
    node_key = node_list.index(node_id)

    # make sure we have a matching key between nodes and node_values
    # nodes[0] will match node_value[0]
    # we need a list of node values
    if len(value_list) <= node_key:
        value_list.insert(node_key, [])

    # add the value in message to the node_values list
    value_list[node_key].append(value)

    return node_list, value_list


def get_min_max_avg_values(value_list):
    min_value = None
    max_value = None

    count = 0
    sum_of_values = 0

    for value in value_list:
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
