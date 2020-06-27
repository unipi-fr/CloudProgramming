FROM custom-base-flask-server
COPY python-flask-server /usr/src/app

EXPOSE 8080

ENTRYPOINT ["python3"]

CMD ["-m", "swagger_server"]