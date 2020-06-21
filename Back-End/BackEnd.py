import pika
import json
import MySQLdb


# TESTED
def add_movie(movie):
    mydb = MySQLdb.connect(host="172.16.3.35", user="root", passwd="pisaflix", db="pisaflix")

    mycursor = mydb.cursor() 
    
    sql = "INSERT INTO movies (name, description, director, year, genre) VALUES (%s, %s, %s, %s, %s)" 
    val = (movie["name"], movie["description"], movie["director"], movie["year"], movie["genre"]) 
    mycursor.execute(sql, val) 
    
    mydb.commit()

    result = mycursor.rowcount

    mycursor.close()
    mydb.close()

    return '{ "rowcount" : ' + result + '}'

# TESTED
def update_movie(movie):
    ''' it creates a movie if it doesn't exists, otherwise it updates it '''
    mydb = MySQLdb.connect(host="172.16.3.35", user="root", passwd="pisaflix", db="pisaflix")

    mycursor = mydb.cursor()

    # Check if a movie with the specified id exists
    sql = "SELECT id FROM movies WHERE id = %s"
    val = (movie["id"], )
    mycursor.execute(sql, val)
    myresult = mycursor.fetchone()

    if myresult is not None:
        #if it exists, I just update it
        sql = "UPDATE movies SET name = %s, description = %s, director = %s, year = %s, genre = %s WHERE id = %s;" 
        val = (movie["name"], movie["description"], movie["director"], movie["year"], movie["genre"], movie["id"])
    else:
        #otherwise I add it in the DB
        sql = "INSERT INTO movies (name, description, director, year, genre) VALUES (%s, %s, %s, %s, %s)" 
        val = (movie["name"], movie["description"], movie["director"], movie["year"], movie["genre"])
    
    mycursor.execute(sql, val)
    mydb.commit()

    result = mycursor.rowcount

    mycursor.close()
    mydb.close()
    
    return '{ "rowcount" : ' + result + '}'

# TESTED
def get_filtered(movie):
    mydb = MySQLdb.connect(host="172.16.3.35", user="root", passwd="pisaflix", db="pisaflix")

    mycursor = mydb.cursor()
    
    sql = "SELECT * FROM movies WHERE 1=1"
    val = ()
    if "name" in movie.keys():
        sql = sql + " AND name = %s"
        val = val + (movie["name"],)
    if "year" in movie.keys():
        sql = sql + " AND year = %s"
        val = val + (movie["year"],)
    if "director" in movie.keys():
        sql = sql + " AND director = %s"
        val = val + (movie["director"],)
    if "genre" in movie.keys():
        sql = sql + " AND genre = %s"
        val = val + (movie["genre"],)
    sql = sql + ";"
    
    mycursor.execute(sql, val) 
    mydb.commit()
    movie_list = mycursor.fetchall()
    mycursor.close()
    mydb.close()

    listJson = json.dumps(movie_list) 

    result = '{ "movieList": ' + listJson + ' }'

    print(result)
    return result
        
# TESTED
def delete_movie(id):
    mydb = MySQLdb.connect(host="172.16.3.35", user="root", passwd="pisaflix", db="pisaflix")
    mycursor = mydb.cursor()

    sql = "DELETE FROM movies WHERE id = %s"
    val = (id, )

    mycursor.execute(sql, val)
    mydb.commit()

    rowcount = mycursor.rowcount
    print(rowcount, "record(s) deleted")

    return '{ "rowcount" : ' + rowcount + '}'
    
# TESTED
def get_by_id(id):
    mydb = MySQLdb.connect(host="172.16.3.35", user="root", passwd="pisaflix", db="pisaflix")
    mycursor = mydb.cursor()

    sql = "SELECT * FROM movies WHERE id = %s"
    val = (id, )

    mycursor.execute(sql, val)
    myresult = mycursor.fetchone()
    mydb.commit()
    mycursor.close()
    mydb.close()

    result = "{}"
    if myresult is not None:
        result = json.dumps(myresult)

    return result

# Define a callback invoked every time a message is received
def callback(ch, method, properties, body):
    print(" [x] %r:%r" % (method.routing_key, body))
    response = ""
    queue_name = properties.headers["queue_name"]

    if method.routing_key == "add_movie":
        response = add_movie(json.loads(body))
    elif method.routing_key == "update_movie":
        response = update_movie(json.loads(body))
    elif method.routing_key == "get_filtered":
        response = get_filtered(json.loads(body))
    elif method.routing_key == "delete_movie":
        response = delete_movie(int(body))
    elif method.routing_key == "get_by_id":
        response = get_by_id(int(body))
    else:
        print("Unknown method")

    connection = pika.BlockingConnection(pika.ConnectionParameters(host='172.16.3.35'))
    channel = connection.channel()

    channel.basic_publish(exchange='backend_to_frontend', routing_key=queue_name, body=response)
    

if __name__ == '__main__':
    # Connect to RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='172.16.3.35'))
    channel = connection.channel()

    # Connect to a queue
    channel.exchange_declare(exchange='frontend_to_backend', exchange_type='direct')
    channel.exchange_declare(exchange='backend_to_frontend', exchange_type='direct')

    # I let the system to create the queue name
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue

    # Bind the queue to one or more keys/exchanges (it can be done at runtime)
    channel.queue_bind(exchange='frontend_to_backend', queue=queue_name, routing_key='add_movie')
    channel.queue_bind(exchange='frontend_to_backend', queue=queue_name, routing_key='update_movie')
    channel.queue_bind(exchange='frontend_to_backend', queue=queue_name, routing_key='get_filtered')
    channel.queue_bind(exchange='frontend_to_backend', queue=queue_name, routing_key='delete_movie')
    channel.queue_bind(exchange='frontend_to_backend', queue=queue_name, routing_key='get_by_id')
        
    channel.basic_consume(
        queue=queue_name, on_message_callback=callback, auto_ack=True)
    channel.start_consuming()

