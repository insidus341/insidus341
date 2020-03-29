from run.tools import get_min_max_avg_values


def test_get_min_max_avg_values():
    node_values = [
        10,
        20,
        30
    ]

    (count, minv, maxv, avgv) = get_min_max_avg_values(node_values)

    assert count == 3
    assert minv == 10
    assert maxv == 30
    assert avgv == 20
