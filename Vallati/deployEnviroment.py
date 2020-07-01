import paramiko
import os
import tarfile
import json

config = {}

if __name__ == '__main__':
    with open('config.json','r') as f:
        config = json.load(f)
        print('loading configuration:\n{config}'.format(config=json.dumps(config, indent=4, sort_keys=True)))
    
    tarDir('front-end','front-end')
    for machine in config['front-end-machines']:
        configureMachine('front-end',machine['ip'],machine['ssh-user'],machine['ssh-password'],machine['exchange'])
    os.remove('front-end.tar.gz')
        
    tarDir('back-end','back-end')
    for machine in config['back-end-machines']:
       configureMachine('back-end',machine['ip'],machine['ssh-user'],machine['ssh-password'],machine['exchange'])
    os.remove('back-end.tar.gz')

def configureMachine(machineType,machineIP,sshUser,sshPassword,exchange):
    print('\n[INFO] ---------- Configuring {machineType}[{machineIP}]'.format(machineType=machineType,machineIP=machineIP))
    configForMachine = {}
    configForMachine["zookeper-ip"] = config["zookeper-ip"]
    configForMachine["exchange"] = exchange
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(machineIP,22,username=sshUser,password=sshPassword,timeout=4)

    executeSSHCommand(ssh,'rm -rf {machineType}'.format(machineType=machineType))

    sftp = ssh.open_sftp()
    sftp.put('{machineType}.tar.gz'.format(machineType=machineType), '{machineType}.tar.gz'.format(machineType=machineType))
    
    
    executeSSHCommand(ssh,'tar -xzvf {machineType}.tar.gz'.format(machineType=machineType))
    executeSSHCommand(ssh,'rm -f {machineType}.tar.gz'.format(machineType=machineType))
    with open('config-{machineType}.json'.format(machineType=machineType), 'w') as json_file:
        json.dump(configForMachine, json_file)
    sftp.put('config-{machineType}.json'.format(machineType=machineType), '{machineType}/{machineType}-server/config.json'.format(machineType=machineType))
    sftp.close()
    os.remove('config-{machineType}.json'.format(machineType=machineType))
    
    print('[INFO] cleaning configuration: container[{container}], images[{images}], base-images[{baseImages}]'
    .format(
        container=config['clean-{machineType}-container'.format(machineType=machineType)], 
        images=config['clean-{machineType}-image'.format(machineType=machineType)],
        baseImages=config['clean-base-{machineType}-image'.format(machineType=machineType)]))
        
    if config['clean-{machineType}-container'.format(machineType=machineType)] & checkRemoteContainerExists(ssh,'{machineType}-server'.format(machineType=machineType)):
        print('[INFO] cleaning container...')
        removeRemoteContainer(ssh,'{machineType}-server'.format(machineType=machineType))
    if config['clean-{machineType}-image'.format(machineType=machineType)] & checkRemoteImageExists(ssh,'{machineType}-server'.format(machineType=machineType)):
        print('[INFO] cleaning images...')
        removeRemoteImage(ssh,'{machineType}-server'.format(machineType=machineType))
    if config['clean-base-{machineType}-image'.format(machineType=machineType)] & checkRemoteImageExists(ssh,'{machineType}-base-server'.format(machineType=machineType)):
        print('[INFO] cleaning base images...')
        removeRemoteImage(ssh,'{machineType}-base-server'.format(machineType=machineType))

    if not checkRemoteImageExists(ssh,'{machineType}-base-server'.format(machineType=machineType)):
        print('[INFO] building {machineType}-base-server image'.format(machineType=machineType))
        buildRemoteImage(sshSession=ssh,
                        remotePath='{machineType}'.format(machineType=machineType),
                        nameImage='{machineType}-base-server'.format(machineType=machineType),
                        dockerfile='only-{machineType}-base-server.dockerfile'.format(machineType=machineType))
    else:
        print('[INFO] skipping building {machineType}-base-server image (alredy exits)'.format(machineType=machineType))

    if not checkRemoteImageExists(ssh,'{machineType}-server'.format(machineType=machineType)):
        print('[INFO] building {machineType}-server image'.format(machineType=machineType))
        buildRemoteImage(sshSession=ssh,
                        remotePath='{machineType}'.format(machineType=machineType),
                        nameImage='{machineType}-server'.format(machineType=machineType),
                        dockerfile='only-{machineType}-server.dockerfile'.format(machineType=machineType))
    else:
        print('[INFO] skipping building {machineType}-server image (alredy exits)'.format(machineType=machineType))
        
    if not checkRemoteContainerExists(ssh,'{machineType}-server'.format(machineType=machineType)):
        print('[INFO] depploing {machineType}-server'.format(machineType=machineType))
        executeSSHCommand(ssh,'docker run -d --hostname my-{machineType} --name {machineType}-server -p 8080:8080 {machineType}-server'.format(machineType=machineType))
    else:
        print('[INFO] skipping deploy {machineType}-server container (alredy exits)'.format(machineType=machineType))
    
def tarDir(path,tarName):
    tar = tarfile.open(tarName+".tar.gz", "w:gz")
    tar.add(path, arcname=tarName)
    tar.close()

def executeSSHCommand(sshSession,command):
    ssh_stdin, ssh_stdout, ssh_stderr = sshSession.exec_command(command)
    print(ssh_stdout.read().decode('ascii').strip("\n"))
    print(ssh_stderr.read().decode('ascii').strip("\n"))

def buildRemoteImage(sshSession,remotePath,nameImage,dockerfile):
    executeSSHCommand(sshSession,'cd {remotePath}; docker build -t {nameImage} -f {dockerfile} .'
        .format(remotePath=remotePath,nameImage=nameImage,dockerfile=dockerfile))

def removeRemoteContainer(sshSession,containerName):
    executeSSHCommand(sshSession,'docker container rm -f {containerName}'.format(containerName=containerName))

def removeRemoteImage(sshSession,imageName):
    executeSSHCommand(sshSession,'docker image rm -f {imageName}'.format(imageName=imageName))

def checkRemoteContainerExists(sshSession,containerName):
    ssh_stdin, ssh_stdout, ssh_stderr = sshSession.exec_command('docker container ls -a | grep -w "{containerName}"'.format(containerName=containerName))
    return len(ssh_stdout.readlines()) > 0

def checkRemoteImageExists(sshSession,imageName):
    ssh_stdin, ssh_stdout, ssh_stderr = sshSession.exec_command('docker image ls -a | grep -w "{imageName}"'.format(imageName=imageName))
    return  len(ssh_stdout.readlines()) > 0
    
    