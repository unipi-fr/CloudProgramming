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

def executeSSHCommand(sshSession,command):
    ssh_stdin, ssh_stdout, ssh_stderr = sshSession.exec_command(command)
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
    executeSSHCommand(ssh,"rm -f Back-End.tar.gz")
    with open('config-back-end.json', 'w') as json_file:
        json.dump(configForBackEnd, json_file)
    sftp.put('config-back-end.json', 'Back-End/back-end-server/config.json')
    sftp.close()
    os.remove('config-back-end.json')

    print("[INFO] cleaning configuration: container[%r], images[%r], baseImages[%r]" % (config["clean-back-end-container"], config["clean-back-end-image"], config["clean-base-back-end-image"]))
    if config["clean-back-end-container"] & checkRemoteContainerExists(ssh,"back-end-server"):
        print("[INFO] cleaning container...")
        removeRemoteContainer(ssh,"back-end-server")
    if config["clean-back-end-image"] & checkRemoteImageExists(ssh,"back-end-server"):
        print("[INFO] cleaning images...")
        removeRemoteImage(ssh,"back-end-server")
    if config["clean-base-back-end-image"] & checkRemoteImageExists(ssh,"back-end-base-server"):
        print("[INFO] cleaning base images...")
        removeRemoteImage(ssh,"back-end-base-server")

    if not checkRemoteImageExists(ssh,"back-end-base-server"):
        print("[INFO] building back-end-base-server image")
        buildRemoteImage(sshSession=ssh,remotePath="Back-End",nameImage="back-end-base-server",dockerfile="only-back-end-base-server.dockerfile")
    else:
        print("[INFO] skipping building back-end-base-server image (alredy exits)")
    if not checkRemoteImageExists(ssh,"back-end-server"):
        print("[INFO] building back-end-server image")
        buildRemoteImage(sshSession=ssh,remotePath="Back-End",nameImage="back-end-server",dockerfile="only-back-end-server.dockerfile")
    else:
        print("[INFO] skipping building back-end-server image (alredy exits)")
    if not checkRemoteContainerExists(ssh,"back-end-server"):
        print("[INFO] depploing back-end-server")
        executeSSHCommand(ssh,"docker run -d --hostname my-back-end --name back-end-server -p 8080:8080 back-end-server")
    else:
        print("[INFO] skipping deploy back-end-server container (alredy exits)")

def buildRemoteImage(sshSession,remotePath,nameImage,dockerfile):
    executeSSHCommand(sshSession,"cd "+remotePath+"; docker build -t "+nameImage+" -f "+dockerfile+" .")

def removeRemoteContainer(sshSession,containerName):
    executeSSHCommand(sshSession,"docker container rm -f "+containerName)

def removeRemoteImage(sshSession,imageName):
    executeSSHCommand(sshSession,"docker image rm -f "+imageName)

def checkRemoteContainerExists(sshSession,containerName):
    ssh_stdin, ssh_stdout, ssh_stderr = sshSession.exec_command('docker container ls | grep -w "'+containerName+'"')
    return len(ssh_stdout.readlines()) > 0

def checkRemoteImageExists(sshSession,imageName):
    ssh_stdin, ssh_stdout, ssh_stderr = sshSession.exec_command('docker image ls | grep -w "'+imageName+'"')
    return  len(ssh_stdout.readlines()) > 0

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
    os.remove('config-front-end.json')

    print("[INFO] cleaning configuration: container[%r], images[%r], baseImages[%r]" % (config["clean-front-end-container"], config["clean-front-end-image"], config["clean-base-front-end-image"]))
    if config["clean-front-end-container"] & checkRemoteContainerExists(ssh,"front-end-server"):
        print("[INFO] cleaning container...")
        removeRemoteContainer(ssh,"front-end-server")
    if config["clean-front-end-image"] & checkRemoteImageExists(ssh,"front-end-server"):
        print("[INFO] cleaning images...")
        removeRemoteImage(ssh,"front-end-server")
    if config["clean-base-front-end-image"] & checkRemoteImageExists(ssh,"front-end-base-server"):
        print("[INFO] cleaning base images...")
        removeRemoteImage(ssh,"front-end-base-server")

    if not checkRemoteImageExists(ssh,"front-end-base-server"):
        print("[INFO] building front-end-base-server image")
        buildRemoteImage(sshSession=ssh,remotePath="Front-End",nameImage="front-end-base-server",dockerfile="only-front-end-base-server.dockerfile")
    else:
        print("[INFO] skipping building front-end-base-server image (alredy exits)")
    if not checkRemoteImageExists(ssh,"front-end-server"):
        print("[INFO] building front-end-server image")
        buildRemoteImage(sshSession=ssh,remotePath="Front-End",nameImage="front-end-server",dockerfile="only-front-end-server.dockerfile")
    else:
        print("[INFO] skipping building front-end-server image (alredy exits)")
    if not checkRemoteContainerExists(ssh,"front-end-server"):
        print("[INFO] depploing front-end-server")
        executeSSHCommand(ssh,"docker run -d --hostname my-front-end --name front-end-server -p 8080:8080 front-end-server")
    else:
        print("[INFO] skipping deploy front-end-server container (alredy exits)")

if __name__ == '__main__':
    with open('config.json') as f:
        config = json.load(f)
        print("loading configuration:\n"+json.dumps(config))
    tarDir("Front-End","Front-End")
        
    for machine in config["front-end-machines"]:
        configureFrontEnd(machine["ip"],machine["ssh-user"],machine["ssh-password"],machine["exchange"])
    os.remove('Front-End.tar.gz')
        
    tarDir("Back-End","Back-End")
    for machine in config["back-end-machines"]:
       configureBackEnd(machine["ip"],machine["ssh-user"],machine["ssh-password"],machine["exchange"])
    os.remove('Back-End.tar.gz')
    
    