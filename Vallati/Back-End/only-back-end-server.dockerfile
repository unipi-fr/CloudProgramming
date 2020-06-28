FROM back-end-base-server

COPY back-end-server /usr/src/app
ENTRYPOINT ["python3"]
CMD ["BackEnd.py"]