from run.tools import add_node_and_value

MESSAGE = {'Node_ID': 12345678900001, 'Value': 30, 'Timestamp': 123452242}


def test_add_node_and_value():
    nodes = []
    node_values = []
    node_id = MESSAGE['Node_ID']
    value = MESSAGE['Value']

    (nodes, node_values) = add_node_and_value(node_id, value, nodes, node_values)

    print(nodes)

    assert nodes[0] == node_id
    assert node_values[0][0] == value
