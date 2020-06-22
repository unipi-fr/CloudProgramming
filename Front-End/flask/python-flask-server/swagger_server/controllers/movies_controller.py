import connexion
import six
import pika
import json
from django.utils.crypto import get_random_string
from kazoo.client import KazooClient

from swagger_server.models.movie import Movie  # noqa: E501
from swagger_server import util

def zookeeperRetrieve(path):
    zk = KazooClient(hosts='172.16.3.35:2181')
    zk.start()
 
    children = zk.get("/" + path)
    zk.stop()
 
    return children[0].decode("utf-8")

def get_response_from_backend(connection, queue_name):
    channelFromBackEnd = connection.channel()

    exchange = zookeeperRetrieve("RabbitMQ/Exchange_names/back_to_front1")

    channelFromBackEnd.exchange_declare(exchange=exchange, exchange_type='direct')

    channelFromBackEnd.queue_declare(queue=queue_name, exclusive=True)
    channelFromBackEnd.queue_bind(exchange=exchange, queue=queue_name, routing_key=queue_name)
    
    channelFromBackEnd.basic_qos(prefetch_count=1)
    method_frame, header_frame, body = channelFromBackEnd.basic_get(queue_name)

    while method_frame is None:
        print("Cycling")
        method_frame, header_frame, body = channelFromBackEnd.basic_get(queue_name)

    if method_frame:
        print(method_frame, header_frame, body)
        channelFromBackEnd.basic_ack(method_frame.delivery_tag)

    channelFromBackEnd.queue_delete(queue_name)

    return json.loads(body)

def add_movie(body):  # noqa: E501
    """Add a new movie

     # noqa: E501

    :param body: Movie data
    :type body: dict | bytes

    :rtype: None
    """

    address = zookeeperRetrieve("RabbitMQ/address")
    connection = pika.BlockingConnection(pika.ConnectionParameters(address))
    channelToBackEnd = connection.channel()

    exchange = zookeeperRetrieve("RabbitMQ/Exchange_names/front_to_back1")
    channelToBackEnd.exchange_declare(exchange=exchange, exchange_type='direct')

    if connexion.request.is_json:
        body = connexion.request.get_json()  # noqa: E501

    numBytes = int(zookeeperRetrieve("Utils/string_dim"))
    queue_name = get_random_string(numBytes)

    routing_key = zookeeperRetrieve("Utils/Routing_keys/addMovie")
    channelToBackEnd.basic_publish(
        exchange=exchange, 
        routing_key=routing_key, 
        body=json.dumps(body), 
        properties=pika.BasicProperties(
            headers={'queue_name': queue_name}
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

    exchange = zookeeperRetrieve("RabbitMQ/Exchange_names/front_to_back1")
    channelToBackEnd.exchange_declare(exchange=exchange, exchange_type='direct')

    numBytes = int(zookeeperRetrieve("Utils/string_dim"))
    queue_name = get_random_string(numBytes)

    routing_key = zookeeperRetrieve("Utils/Routing_keys/deleteMovie")
    channelToBackEnd.basic_publish(
        exchange=exchange, 
        routing_key=routing_key, 
        body=str(movieId), 
        properties=pika.BasicProperties(
            headers={'queue_name': queue_name}
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

    exchange = zookeeperRetrieve("RabbitMQ/Exchange_names/front_to_back1")
    channelToBackEnd.exchange_declare(exchange=exchange, exchange_type='direct')

    numBytes = int(zookeeperRetrieve("Utils/string_dim"))
    queue_name = get_random_string(numBytes)

    routing_key = zookeeperRetrieve("Utils/Routing_keys/getById")
    channelToBackEnd.basic_publish(
        exchange=exchange, 
        routing_key=routing_key, 
        body=str(movieId), 
        properties=pika.BasicProperties(
            headers={'queue_name': queue_name}
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

    address = zookeeperRetrieve("RabbitMQ/address")
    connection = pika.BlockingConnection(pika.ConnectionParameters(address))
    channelToBackEnd = connection.channel()

    exchange = zookeeperRetrieve("RabbitMQ/Exchange_names/front_to_back1")
    channelToBackEnd.exchange_declare(exchange=exchange, exchange_type='direct')

    jsonString = "{"

    if movieName != None:
        jsonString += '"name" : "' + movieName + '",'
    if movieYear != None:
        jsonString += '"year" : ' + str(movieYear) + ","
    if director != None:
        jsonString += '"director" : "' + director + '",'
    if genre != None:
        jsonString += '"genre" : "' + genre + '",'

    jsonString = jsonString[:-1]

    jsonString += "}"

    numBytes = int(zookeeperRetrieve("Utils/string_dim"))
    queue_name = get_random_string(numBytes)

    routing_key = zookeeperRetrieve("Utils/Routing_keys/getFilteredMovies")
    channelToBackEnd.basic_publish(
        exchange=exchange, 
        routing_key=routing_key, 
        body = jsonString, 
        properties=pika.BasicProperties(
            headers={'queue_name': queue_name}
        ))

    result = get_response_from_backend(connection, queue_name)

    if json.dumps(result) == '{ "movieList": [] }':
        return json.loads('{ "detail": "The movie was not found on the server. If you entered the URL manually please check your spelling and try again.","status": 404,"title": "Not Found","type": "about:blank"}'), 404

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

    exchange = zookeeperRetrieve("RabbitMQ/Exchange_names/front_to_back1")
    channelToBackEnd.exchange_declare(exchange=exchange, exchange_type='direct')

    if connexion.request.is_json:
        body = connexion.request.get_json()  # noqa: E501

    numBytes = int(zookeeperRetrieve("Utils/string_dim"))
    queue_name = get_random_string(numBytes)

    routing_key = zookeeperRetrieve("Utils/Routing_keys/updateMovie")
    channelToBackEnd.basic_publish(
        exchange=exchange, 
        routing_key=routing_key, 
        body=json.dumps(body), 
        properties=pika.BasicProperties(
            headers={'queue_name': queue_name}
        ))

    result = get_response_from_backend(connection, queue_name)

    return result