import mysql.connector as mariadb


def database():
    mariadb_connection = mariadb.connect(user='node_user', password='Pa55word123', database='CodingAssignment')
    cursor = mariadb_connection.cursor()

    return mariadb_connection, cursor


def database_insert(node_id, count, minv, maxv, avgv):
    try:
        (mariadb_connection, cursor) = database()

        cursor.execute("insert into NodeCheckIn (Node_ID, Count, Min_Value, Max_Value, Avg_Value) VALUES (%s, %s, %s, %s, %s)",
                       (node_id, count, minv, maxv, avgv))
        mariadb_connection.commit()
        return True
    except:
        return False