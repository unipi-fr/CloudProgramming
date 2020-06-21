import connexion
import six
import pika
import json
from django.utils.crypto import get_random_string

from swagger_server.models.movie import Movie  # noqa: E501
from swagger_server import util

def get_response_from_backend(connection, queue_name):
    channelFromBackEnd = connection.channel()

    channelFromBackEnd.exchange_declare(exchange='backend_to_frontend', exchange_type='direct')

    channelFromBackEnd.queue_declare(queue=queue_name, exclusive=True)
    channelFromBackEnd.queue_bind(exchange='backend_to_frontend', queue=queue_name, routing_key=queue_name)
    
    channelFromBackEnd.basic_qos(prefetch_count=1)
    method_frame, header_frame, body = channelFromBackEnd.basic_get(queue_name)

    while method_frame is None:
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
    connection = pika.BlockingConnection(pika.ConnectionParameters('172.16.3.35'))
    channelToBackEnd = connection.channel()

    channelToBackEnd.exchange_declare(exchange='frontend_to_backend', exchange_type='direct')

    if connexion.request.is_json:
        body = connexion.request.get_json()  # noqa: E501

    queue_name = get_random_string(8)

    channelToBackEnd.basic_publish(
        exchange='frontend_to_backend', 
        routing_key='add_movie', 
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

    connection = pika.BlockingConnection(pika.ConnectionParameters('172.16.3.35'))
    channel = connection.channel()

    channel.exchange_declare(exchange='frontend_to_backend', exchange_type='direct')

    queue_name = get_random_string(8)

    channel.basic_publish(
        exchange='frontend_to_backend', 
        routing_key='delete_movie', 
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

    connection = pika.BlockingConnection(pika.ConnectionParameters('172.16.3.35'))
    channel = connection.channel()

    channel.exchange_declare(exchange='frontend_to_backend', exchange_type='direct')

    queue_name = get_random_string(8)

    channel.basic_publish(
        exchange='frontend_to_backend', 
        routing_key='get_by_id', 
        body=str(movieId), 
        properties=pika.BasicProperties(
            headers={'queue_name': queue_name}
        ))

    result = get_response_from_backend(connection, queue_name)

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
    connection = pika.BlockingConnection(pika.ConnectionParameters('172.16.3.35'))
    channel = connection.channel()

    channel.exchange_declare(exchange='frontend_to_backend', exchange_type='direct')

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

    queue_name = get_random_string(8)

    channel.basic_publish(
        exchange='frontend_to_backend', 
        routing_key='get_filtered', 
        body = jsonString, 
        properties=pika.BasicProperties(
            headers={'queue_name': queue_name}
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
    connection = pika.BlockingConnection(pika.ConnectionParameters('172.16.3.35'))
    channel = connection.channel()

    channel.exchange_declare(exchange='frontend_to_backend', exchange_type='direct')

    if connexion.request.is_json:
        body = connexion.request.get_json()  # noqa: E501

    queue_name = get_random_string(8)

    channel.basic_publish(
        exchange='frontend_to_backend', 
        routing_key='update_movie', 
        body=json.dumps(body), 
        properties=pika.BasicProperties(
            headers={'queue_name': queue_name}
        ))

    result = get_response_from_backend(connection, queue_name)

    return result
