from kazoo.client import KazooClient

name_resource = input("Write the name of the resource to be retrieved: ")

zk = KazooClient(hosts='172.16.3.35:2181')
zk.start()

children = zk.get("/" + name_resource)
zk.stop()

print(children[0].decode("utf-8"))