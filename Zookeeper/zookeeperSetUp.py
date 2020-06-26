from kazoo.client import KazooClient

# indirizzo rabbitMQ 
# nome cartella -> /RabbitMQ/
# /RabbitMQ/address
# 120to213, 121to242 e viceversa
# nome cartella -> RabbitMQ/Exchange_names/uno di questi sotto
# le varie risorse sono chiamate front_to_back1, front_to_back2, back_to_front1, back_to_front2
# dimensione random string
# Utils/string_dim
# routing keys per ogni funzione
# Utils/Routing_keys/   addMovie | updateMovie | deleteMovie | getFilteredMovies | getById
# indirizzo host mysql + credenziali e nome db
# nome cartella /MySql/
# /MySql/db   /MySql/user   /MySql/pass    /MySql/address

path = input("Write the path of the new resource: ")
name_resource = input("Write the name of the resource: ")
value = input("Write the value to be saved: ")

zk = KazooClient(hosts='172.16.3.35:2181')
zk.start()
zk.ensure_path("/" + path)

zk.create("/" + path + "/" + name_resource, value.encode())

zk.stop()