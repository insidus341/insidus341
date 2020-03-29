from run.tools import message_is_valid


def test_message_is_valid():
    message = {'Node_ID': 12345678900001, 'Value': 30, 'Timestamp': 123452242}

    (node_id, value, timestamp) = message_is_valid(message)

    assert node_id == 12345678900001
    assert value == 30
    assert timestamp == 123452242
