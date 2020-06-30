import pika
import json
import MySQLdb
from kazoo.client import KazooClient

def getConfig():
    config = {}
    with open('config.json') as f:
        config = json.load(f)
    return config

def zookeeperRetrieve(path):
    config = getConfig()
    zk = KazooClient(hosts=config["zookeper-ip"]+':2181')
    zk.start()

    children = zk.get("/" + path)
    zk.stop()

    return children[0].decode("utf-8")

# TESTED
def add_movie(movie):


    db_host = zookeeperRetrieve("/MySql/address")
    db_user = zookeeperRetrieve("/MySql/user")
    db_pass = zookeeperRetrieve("/MySql/pass")
    db_name = zookeeperRetrieve("/MySql/db")
    
    mydb = MySQLdb.connect(host=db_host, user=db_user, passwd=db_pass, db=db_name)

    mycursor = mydb.cursor() 
    
    sql = "INSERT INTO movies (name, description, director, year, genre) VALUES (%s, %s, %s, %s, %s)" 
    val = (movie["name"], movie["description"], movie["director"], movie["year"], movie["genre"]) 
    mycursor.execute(sql, val) 
    sql = "SELECT LAST_INSERT_ID() as id"
    mycursor.execute(sql)
    myresult = mycursor.fetchone()

    mydb.commit()
    #result = mycursor.rowcount
    mycursor.close()
    mydb.close()

    if myresult is not None:
        return '{ "id": '+str(myresult[0])+' }'
    return "{}"

# TESTED
def update_movie(movie):
    ''' it creates a movie if it doesn't exists, otherwise it updates it '''

    db_host = zookeeperRetrieve("/MySql/address")
    db_user = zookeeperRetrieve("/MySql/user")
    db_pass = zookeeperRetrieve("/MySql/pass")
    db_name = zookeeperRetrieve("/MySql/db")
    
    mydb = MySQLdb.connect(host=db_host, user=db_user, passwd=db_pass, db=db_name)

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
    
    return '{ "row-affected" :  '+str(result)+' }'

# TESTED
def get_filtered(movie):
    db_host = zookeeperRetrieve("/MySql/address")
    db_user = zookeeperRetrieve("/MySql/user")
    db_pass = zookeeperRetrieve("/MySql/pass")
    db_name = zookeeperRetrieve("/MySql/db")
    
    mydb = MySQLdb.connect(host=db_host, user=db_user, passwd=db_pass, db=db_name)

    mycursor = mydb.cursor()
    
    sql = "SELECT id,name,year,director,genre,description FROM movies WHERE 1=1"
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
    movie_sql_list = mycursor.fetchall()
    mycursor.close()
    mydb.close()

    movie_list = list()
    result = dict()
    for movie in movie_sql_list:
        tmpMovie = dict()
        tmpMovie["id"] = movie[0]
        tmpMovie["name"] = movie[1]
        tmpMovie["year"] = movie[2]
        tmpMovie["director"] =movie[3]
        tmpMovie["genre"] = movie[4]
        tmpMovie["description"] =movie[5]
        movie_list.append(tmpMovie)
    result["movieList"] = movie_list

    resultJson = json.dumps(result)

    print(resultJson)
    return resultJson
        
# TESTED
def delete_movie(id):

    db_host = zookeeperRetrieve("/MySql/address")
    db_user = zookeeperRetrieve("/MySql/user")
    db_pass = zookeeperRetrieve("/MySql/pass")
    db_name = zookeeperRetrieve("/MySql/db")
    
    mydb = MySQLdb.connect(host=db_host, user=db_user, passwd=db_pass, db=db_name)
    mycursor = mydb.cursor()

    sql = "DELETE FROM movies WHERE id = %s"
    val = (id, )

    mycursor.execute(sql, val)
    mydb.commit()

    result = dict()
    result["rows-affected"] = mycursor.rowcount
    resultJson = json.dumps(result)

    print(resultJson)

    return resultJson
    
# TESTED
def get_by_id(id):
    db_host = zookeeperRetrieve("/MySql/address")
    db_user = zookeeperRetrieve("/MySql/user")
    db_pass = zookeeperRetrieve("/MySql/pass")
    db_name = zookeeperRetrieve("/MySql/db")
    
    mydb = MySQLdb.connect(host=db_host, user=db_user, passwd=db_pass, db=db_name)
    mycursor = mydb.cursor()

    sql = "SELECT id,name,year,director,genre,description FROM movies WHERE id = %s"
    val = (id, )

    mycursor.execute(sql, val)
    myresult = mycursor.fetchone()
    mydb.commit()
    mycursor.close()
    mydb.close()

    tmpMovie = dict()
    if myresult is not None:  
        tmpMovie["id"] = myresult[0]
        tmpMovie["name"] = myresult[1]
        tmpMovie["year"] = myresult[2]
        tmpMovie["director"] =myresult[3]
        tmpMovie["genre"] = myresult[4]
        tmpMovie["description"] =myresult[5]

    return json.dumps(tmpMovie)

# Define a callback invoked every time a message is received
def callback(ch, method, properties, body):
    print(" [x] %r" % (body))
    response = "{}"
    queue_name = properties.headers["queue_name"]

    method_to_do = properties.headers["method"]

    print(method_to_do)

    rabbitMQ_address = zookeeperRetrieve("RabbitMQ/address")
    config = getConfig()
    exchange = config["exchange"]

    if method_to_do == "addMovie":
        response = add_movie(json.loads(body))
    elif method_to_do == "updateMovie":
        response = update_movie(json.loads(body))
    elif method_to_do == "getFilteredMovies":
        response = get_filtered(json.loads(body))
    elif method_to_do == "deleteMovie":
        response = delete_movie(int(body))
    elif method_to_do == "getById":
        response = get_by_id(int(body))
    else:
        print("Unknown method")

    connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitMQ_address))
    channel = connection.channel()

    channel.basic_publish(exchange=exchange, routing_key=queue_name, body=response)
    
# SAREBBE MEGLIO UNA CONNESSIONE GLOBALE ????
if __name__ == '__main__':
    # Connect to RabbitMQ
    rabbitMQ_address = zookeeperRetrieve("RabbitMQ/address")
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitMQ_address))
    channel = connection.channel()

    config = getConfig()
    exchange = config["exchange"]
    # Connect to a queue
    channel.exchange_declare(exchange=exchange, exchange_type='direct')

    # I let the system to create the queue name
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue

    r_k_add = zookeeperRetrieve("Utils/Routing_keys/addMovie")
    r_k_update = zookeeperRetrieve("Utils/Routing_keys/updateMovie")
    r_k_get_f = zookeeperRetrieve("Utils/Routing_keys/getFilteredMovies")
    r_k_delete = zookeeperRetrieve("Utils/Routing_keys/deleteMovie")
    r_k_get_by_id = zookeeperRetrieve("Utils/Routing_keys/getById")

    routing_key = "front_to_back"

    # Bind the queue to one or more keys/exchanges (it can be done at runtime)
    channel.queue_bind(exchange=exchange, queue=queue_name, routing_key=routing_key)
        
    channel.basic_consume(
        queue=queue_name, on_message_callback=callback, auto_ack=True)
    channel.start_consuming()

