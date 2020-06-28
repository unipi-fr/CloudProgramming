FROM back-end-server-base

COPY back-end-server /usr/src/app
ENTRYPOINT ["python3"]
CMD ["BackEnd.py"]