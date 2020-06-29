import connexion
import six

from swagger_server.models.movie import Movie  # noqa: E501
from swagger_server import util


def add_movie(body):  # noqa: E501
    """Add a new movie

     # noqa: E501

    :param body: Movie data
    :type body: dict | bytes

    :rtype: None
    """
    if connexion.request.is_json:
        body = Movie.from_dict(connexion.request.get_json())  # noqa: E501
    return 'do some magic!'


def delete_movie(movieId):  # noqa: E501
    """Deletes a movie

     # noqa: E501

    :param movieId: Movie id to delete
    :type movieId: int

    :rtype: None
    """
    return 'do some magic!'


def get_movie_by_id(movieId):  # noqa: E501
    """Retrieve movie by ID

    Returns a single movie # noqa: E501

    :param movieId: ID of Movie to return
    :type movieId: int

    :rtype: Movie
    """
    return 'do some magic!'


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
    return 'do some magic!'


def update_movie(body):  # noqa: E501
    """Create or update a movie

     # noqa: E501

    :param body: Movie object that needs to be added to the store
    :type body: dict | bytes

    :rtype: None
    """
    if connexion.request.is_json:
        body = Movie.from_dict(connexion.request.get_json())  # noqa: E501
    return 'do some magic!'
