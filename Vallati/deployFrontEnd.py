import paramiko
import os
import zipfile
import tarfile
import json

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

def configureFrontEnd(machineIP,sshUser,sshPassword,exchange):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(machineIP,22,username=sshUser,password=sshPassword,timeout=4)

    #executeSSHCommand(ssh,"rm -rf Front-End")

    #sftp = ssh.open_sftp()
    #sftp.put('Front-End.tar.gz', 'Front-End.tar.gz')
    #sftp.close()
    
    #executeSSHCommand(ssh,"tar -xzvf Front-End.tar.gz")
    #executeSSHCommand(ssh,"rm -f Front-End.tar.gz")
    #print("[INFO] building base flask server image")
    #executeSSHCommand(ssh,"cd Front-End; docker build -t custom-base-flask-server -f only-custom-base-flask-server.dockerfile .")
    #print("[INFO] building rest-server image")
    #executeSSHCommand(ssh,"cd Front-End; docker build -t rest-server -f only-rest-server.dockerfile .")
    print("[INFO] running rest server")
    executeSSHCommand(ssh,"docker run -d --hostname my-rest --name rest-server -p 8080:8080 rest-server")

if __name__ == '__main__':
    config = {}
    with open('config.json') as f:
        config = json.load(f)
        print("loading configuration:\n"+json.dumps(config))
    tarDir("Front-End","Front-End")

    for machine in config["front-end-machines"]:
        configureFrontEnd(machine["ip"],machine["ssh-user"],machine["ssh-password"],machine["exchange"])
        
    
    
    