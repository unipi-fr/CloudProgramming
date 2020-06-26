from kazoo.client import KazooClient


name_resource = input("Write the name of the resource to be deleted: ")

zk = KazooClient(hosts='172.16.3.35:2181')
zk.start()

zk.delete("/" + name_resource, recursive=True)

zk.stop()