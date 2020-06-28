# coding: utf-8

from __future__ import absolute_import

from flask import json
from six import BytesIO

from swagger_server.models.movie import Movie  # noqa: E501
from swagger_server.test import BaseTestCase


class TestMoviesController(BaseTestCase):
    """MoviesController integration test stubs"""

    def test_add_movie(self):
        """Test case for add_movie

        Add a new movie
        """
        body = Movie()
        response = self.client.open(
            '//movies',
            method='POST',
            data=json.dumps(body),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_delete_movie(self):
        """Test case for delete_movie

        Deletes a movie
        """
        response = self.client.open(
            '//movies/{movieId}'.format(movieId=789),
            method='DELETE')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_get_movie_by_id(self):
        """Test case for get_movie_by_id

        Retrieve movie by ID
        """
        response = self.client.open(
            '//movies/{movieId}'.format(movieId=789),
            method='GET')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_get_movies(self):
        """Test case for get_movies

        Get films filtered
        """
        query_string = [('movieName', 'movieName_example'),
                        ('movieYear', 789),
                        ('director', 'director_example'),
                        ('genre', 'genre_example')]
        response = self.client.open(
            '//movies',
            method='GET',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_update_movie(self):
        """Test case for update_movie

        Create or update a movie
        """
        body = Movie()
        response = self.client.open(
            '//movies',
            method='PUT',
            data=json.dumps(body),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    import unittest
    unittest.main()
