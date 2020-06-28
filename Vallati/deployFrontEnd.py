import paramiko
import os
import zipfile
import tarfile
import json

config = {}

def zipDir(path, filename):
    zipf = zipfile.ZipFile(filename+'.zip', 'w', zipfile.ZIP_DEFLATED)
    for root, dirs, files in os.walk(path):
        for file in files:
            zipf.write(os.path.join(root, file))
    zipf.close()
    
def tarDir(path,tarName):
    tar = tarfile.open(tarName+".tar.gz", "w:gz")
    tar.add(path, arcname=tarName)
    tar.close()

def executeSSHCommand(connection,command):
    ssh_stdin, ssh_stdout, ssh_stderr = connection.exec_command(command)
    print(ssh_stdout.read().decode('ascii').strip("\n"))
    print(ssh_stderr.read().decode('ascii').strip("\n"))

def configureBackEnd(machineIP,sshUser,sshPassword,exchange):
    configForBackEnd = {}
    configForBackEnd["zookeper-ip"] = config["zookeper-ip"]
    configForBackEnd["exchange"] = exchange
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(machineIP,22,username=sshUser,password=sshPassword,timeout=4)

    executeSSHCommand(ssh,"rm -rf Back-End")

    sftp = ssh.open_sftp()
    sftp.put('Back-End.tar.gz', 'Back-End.tar.gz')
    
    
    executeSSHCommand(ssh,"tar -xzvf Back-End.tar.gz")
    executeSSHCommand(ssh,"rm -f FBack-End.tar.gz")
    with open('config-back-end.json', 'w') as json_file:
        json.dump(configForBackEnd, json_file)
    sftp.put('config-back-end.json', 'Back-End/back-end-server/config.json')
    sftp.close()
    #print("[INFO] back end server base image")
    #executeSSHCommand(ssh,"cd Back-End; docker build -t back-end-server-base -f only-back-end-server-base.dockerfile .")
    print("[INFO] building back-end-server image")
    executeSSHCommand(ssh,"cd Back-End; docker build -t back-end-server -f only-back-end-server.dockerfile .")
    print("[INFO] running back end server")
    executeSSHCommand(ssh,"docker run -d --hostname my-back-end --name back-end-server back-end-server")

def configureFrontEnd(machineIP,sshUser,sshPassword,exchange):
    configForFrontEnd = {}
    configForFrontEnd["zookeper-ip"] = config["zookeper-ip"]
    configForFrontEnd["exchange"] = exchange
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(machineIP,22,username=sshUser,password=sshPassword,timeout=4)

    executeSSHCommand(ssh,"rm -rf Front-End")

    sftp = ssh.open_sftp()
    sftp.put('Front-End.tar.gz', 'Front-End.tar.gz')
    
    
    executeSSHCommand(ssh,"tar -xzvf Front-End.tar.gz")
    executeSSHCommand(ssh,"rm -f Front-End.tar.gz")
    with open('config-front-end.json', 'w') as json_file:
        json.dump(configForFrontEnd, json_file)
    sftp.put('config-front-end.json', 'Front-End/front-end-server/config.json')
    sftp.close()
    #print("[INFO] building base flask server image")
    #executeSSHCommand(ssh,"cd Front-End; docker build -t custom-base-flask-server -f only-custom-base-flask-server.dockerfile .")
    print("[INFO] building front-end-server image")
    executeSSHCommand(ssh,"cd Front-End; docker build -t front-end-server -f only-front-end-server.dockerfile .")
    print("[INFO] running front end server")
    executeSSHCommand(ssh,"docker run -d --hostname my-front-end --name front-end-server -p 8080:8080 front-end-server")

if __name__ == '__main__':
    with open('config.json') as f:
        config = json.load(f)
        print("loading configuration:\n"+json.dumps(config))
    tarDir("Front-End","Front-End")
    tarDir("Back-End","Back-End")
    
    for machine in config["front-end-machines"]:
        configureFrontEnd(machine["ip"],machine["ssh-user"],machine["ssh-password"],machine["exchange"])
        
    for machine in config["back-end-machines"]:
       configureBackEnd(machine["ip"],machine["ssh-user"],machine["ssh-password"],machine["exchange"])
    
    