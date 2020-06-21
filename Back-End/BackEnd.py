import pika
import json
import MySQLdb


#da gestire meglio le connessioni e i cursori


def add_movie(movie):
    mydb = MySQLdb.connect(host="172.16.3.35", user="root", passwd="pisaflix", db="pisaflix")

    mycursor = mydb.cursor() 
    
    sql = "INSERT INTO movies (name, description, director, year, genre) VALUES (%s, %s, %s, %s, %s)" 
    val = (movie["name"], movie["description"], movie["director"], movie["year"], movie["genre"]) 
    mycursor.execute(sql, val) 
    
    mydb.commit()

# DA MODIFICARE PERCHé non è consistente con definizione yaml!!!!
# I CAMPI SONO TUTTI REQUIRED!!!!!
def update_movie(movie):
    mydb = MySQLdb.connect(host="172.16.3.35", user="root", passwd="pisaflix", db="pisaflix")

    mycursor = mydb.cursor()

    # depending on the values passed I add the attributes on the list
    attribute_list = []
    val = ()

    if "name" in movie.keys():
        attribute_list.append("name")
        val = val + (movie["name"],)
    if "description" in movie.keys():
        attribute_list.append("description")
        val = val + (movie["description"],)
    if "director" in movie.keys():
        attribute_list.append("director")
        val = val + (movie["director"],)
    if "year" in movie.keys():
        attribute_list.append("year")
        val = val + (movie["year"],)
    if "genre" in movie.keys():
        attribute_list.append("genre")
        val = val + (movie["genre"],)

    # I prepare the strings to be inserted the sql query
    attribute_string = ",".join(attribute_list)
    parameters_string = "%s,"*len(attribute_list)
    # to remove the last ,
    parameters_string = parameters_string[:-1]

    sql = "UPDATE movies SET ("+ attribute_string +") VALUES ("+ parameters_string +") WHERE id = %s;" 
    val = val + (movie["id"],) 

    mycursor.execute(sql, val)
    
    mydb.commit()

# PROBLEMA: MOVIE è UN DIZIONARIO VUOTO; RISOLVERE NEL FRONT END
def get_filtered(movie):
    mydb = MySQLdb.connect(host="172.16.3.35", user="root", passwd="pisaflix", db="pisaflix")

    mycursor = mydb.cursor()

    # guardo cosa mi è stato passato

    print(movie.keys())

    if "name" in movie.keys() and "year" in movie.keys():
        sql = "SELECT * FROM movies WHERE name = %s and year = %s" 
        val = (movie["name"], movie["year"])
        mycursor.execute(sql, val) 
    elif "name" in movie.keys():
        sql = "SELECT * FROM movies WHERE name = %s" 
        val = (movie["name"], )
        mycursor.execute(sql, val)
    elif "year" in movie.keys():
        sql = "SELECT * FROM movies WHERE year = %s" 
        val = (movie["year"], )
        mycursor.execute(sql, val) 
    

    mydb.commit()

    myresult = mycursor.fetchall()

    for x in myresult:
        print(x)

# DA TESTARE
def delete_movie(id):
    mydb = MySQLdb.connect(host="172.16.3.35", user="root", passwd="pisaflix", db="pisaflix")

    mycursor = mydb.cursor()

    sql = "DELETE FROM movies WHERE id = %s"
    val = (id, )

    mycursor.execute(sql, val)

    mydb.commit()

    print(mycursor.rowcount, "record(s) deleted") 
    
# DA TESTARE
def get_by_id(id):
    mydb = MySQLdb.connect(host="172.16.3.35", user="root", passwd="pisaflix", db="pisaflix")

    mycursor = mydb.cursor()

    sql = "SELECT * FROM movies WHERE id = %s"
    val = (id, )

    mycursor.execute(sql, val)

    myresult = mycursor.fetchall()

    for x in myresult:
        print(x) 


# Define a callback invoked every time a message is received
def callback(ch, method, properties, body):
    print(" [x] %r:%r" % (method.routing_key, body))
    
    if method.routing_key == "add_movie":
        add_movie(json.loads(body))
    elif method.routing_key == "update_movie":
        update_movie(json.loads(body))
    elif method.routing_key == "get_filtered":
        get_filtered(json.loads(body))
    elif method.routing_key == "delete_movie":
        delete_movie(int(body))
    elif method.routing_key == "get_by_id":
        get_by_id(int(body))
    else:
        print("Unknown method")

if __name__ == '__main__':
    # Connect to RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='172.16.3.35'))
    channel = connection.channel()

    # Connect to a queue
    channel.exchange_declare(exchange='frontend_to_backend', exchange_type='direct')

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

