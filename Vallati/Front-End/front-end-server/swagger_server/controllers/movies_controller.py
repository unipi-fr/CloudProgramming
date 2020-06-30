import connexion
import six
import pika
import json
from django.utils.crypto import get_random_string
from kazoo.client import KazooClient

from swagger_server.models.movie import Movie  # noqa: E501
from swagger_server.models.movie_data import MovieData  # noqa: E501
from swagger_server import util

responseFromCallBack = {}

routing_key = "front_to_back"

def log(message):
    with open("front-end.log", "a") as myfile:
        myfile.write(message+"\n")

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

def get_response_from_backend_callback(ch, method, properties, body):
    queue_name = method.routing_key
    responseFromCallBack[queue_name] = json.loads(body)
    
    ch.stop_consuming()


def get_response_from_backend(connection, queue_name):
    config = getConfig()
    channelFromBackEnd = connection.channel()

    exchange = config["exchange"]

    channelFromBackEnd.exchange_declare(exchange=exchange, exchange_type='direct')

    channelFromBackEnd.queue_declare(queue=queue_name, exclusive=True)

    channelFromBackEnd.queue_bind(exchange=exchange, queue=queue_name, routing_key=queue_name)
        
    channelFromBackEnd.basic_consume(queue=queue_name, on_message_callback=get_response_from_backend_callback, auto_ack=True)
    channelFromBackEnd.start_consuming()

    channelFromBackEnd.queue_delete(queue_name)

    result = responseFromCallBack[queue_name]
    del responseFromCallBack[queue_name]
    
    return result

def add_movie(body):  # noqa: E501
    """Add a new movie

     # noqa: E501

    :param body: Movie data
    :type body: dict | bytes

    :rtype: Object
    """
    address = zookeeperRetrieve("RabbitMQ/address")
    connection = pika.BlockingConnection(pika.ConnectionParameters(address))
    channelToBackEnd = connection.channel()
    
    config = getConfig()
    exchange = config["exchange"]
    channelToBackEnd.exchange_declare(exchange=exchange, exchange_type='direct')

    if connexion.request.is_json:
        body = connexion.request.get_json() # noqa: E501

    numBytes = int(zookeeperRetrieve("Utils/string_dim"))
    queue_name = get_random_string(numBytes)

    method_name = "addMovie"

    channelToBackEnd.basic_publish(
        exchange=exchange, 
        routing_key=routing_key, 
        body=json.dumps(body), 
        properties=pika.BasicProperties(
            headers={'queue_name': queue_name, 'method': method_name}
        ))

    result = get_response_from_backend(connection, queue_name)

    return result

def delete_movie(movieId):  # noqa: E501
    """Deletes a movie

     # noqa: E501

    :param movieId: Movie id to delete
    :type movieId: int

    :rtype: None
    """
    address = zookeeperRetrieve("RabbitMQ/address")
    connection = pika.BlockingConnection(pika.ConnectionParameters(address))
    channelToBackEnd = connection.channel()

    config = getConfig()
    exchange = config["exchange"]
    channelToBackEnd.exchange_declare(exchange=exchange, exchange_type='direct')

    numBytes = int(zookeeperRetrieve("Utils/string_dim"))
    queue_name = get_random_string(numBytes)

    method_name = "deleteMovie"

    channelToBackEnd.basic_publish(
        exchange=exchange, 
        routing_key=routing_key, 
        body=str(movieId), 
        properties=pika.BasicProperties(
            headers={'queue_name': queue_name, 'method': method_name}
        ))

    result = get_response_from_backend(connection, queue_name)

    return result


def get_movie_by_id(movieId):  # noqa: E501
    """Retrieve movie by ID

    Returns a single movie # noqa: E501

    :param movieId: ID of Movie to return
    :type movieId: int

    :rtype: Movie
    """

    address = zookeeperRetrieve("RabbitMQ/address")
    connection = pika.BlockingConnection(pika.ConnectionParameters(address))
    channelToBackEnd = connection.channel()

    config = getConfig()
    exchange = config["exchange"]
    channelToBackEnd.exchange_declare(exchange=exchange, exchange_type='direct')

    numBytes = int(zookeeperRetrieve("Utils/string_dim"))
    queue_name = get_random_string(numBytes)

    method_name = "getById"

    channelToBackEnd.basic_publish(
        exchange=exchange, 
        routing_key=routing_key, 
        body=str(movieId), 
        properties=pika.BasicProperties(
            headers={'queue_name': queue_name, 'method': method_name}
        ))

    result = get_response_from_backend(connection, queue_name)

    if json.dumps(result) == '{}':
        return json.loads('{ "detail": "The movie was not found on the server. If you entered the URL manually please check your spelling and try again.","status": 404,"title": "Not Found","type": "about:blank"}'), 404
        

    return result


def get_movies(movieName=None, movieYear=None, director=None, genre=None):  # noqa: E501
    """Get films filtered

    Returns a list of movies # noqa: E501

    :param movieName: Movie name filter
    :type movieName: str
    :param movieYear: Movie year filter
    :type movieYear: int
    :param director: Movie director filter
    :type director: str
    :param genre: Movie genre filter
    :type genre: str

    :rtype: object
    """
    config = getConfig()
    address = zookeeperRetrieve("RabbitMQ/address")
    connection = pika.BlockingConnection(pika.ConnectionParameters(address))
    channelToBackEnd = connection.channel()

    config = getConfig()
    exchange = config["exchange"]
    channelToBackEnd.exchange_declare(exchange=exchange, exchange_type='direct')

    filters = {}

    if movieName != None:
        filters["name"] = movieName
    if movieYear != None:
        filters["year"] = movieYear
    if director != None:
        filters["director"] = director
    if genre != None:
        filters["genre"] = genre

    jsonString = json.dumps(filters)

    numBytes = int(zookeeperRetrieve("Utils/string_dim"))
    queue_name = get_random_string(numBytes)

    method_name = "getFilteredMovies"

    channelToBackEnd.basic_publish(
        exchange=exchange, 
        routing_key=routing_key, 
        body = jsonString, 
        properties=pika.BasicProperties(
            headers={'queue_name': queue_name, 'method': method_name}
        ))

    result = get_response_from_backend(connection, queue_name)

    return result


def update_movie(body):  # noqa: E501
    """Create or update a movie

     # noqa: E501

    :param body: Movie object that needs to be added to the store
    :type body: dict | bytes

    :rtype: None
    """
    address = zookeeperRetrieve("RabbitMQ/address")
    connection = pika.BlockingConnection(pika.ConnectionParameters(address))
    channelToBackEnd = connection.channel()

    config = getConfig()
    exchange = config["exchange"]
    channelToBackEnd.exchange_declare(exchange=exchange, exchange_type='direct')

    if connexion.request.is_json:
        body = connexion.request.get_json()  # noqa: E501

    numBytes = int(zookeeperRetrieve("Utils/string_dim"))
    queue_name = get_random_string(numBytes)

    method_name = "updateMovie"
    
    channelToBackEnd.basic_publish(
        exchange=exchange, 
        routing_key=routing_key, 
        body=json.dumps(body), 
        properties=pika.BasicProperties(
            headers={'queue_name': queue_name, 'method': method_name}
        ))

    get_response_from_backend(connection, queue_name)

    return None
