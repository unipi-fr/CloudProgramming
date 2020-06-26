import paramiko
import os
import zipfile

def zipdir(path, ziph):
    # ziph is zipfile handle
    for root, dirs, files in os.walk(path):
        for file in files:
            ziph.write(os.path.join(root, file))

if __name__ == '__main__':
    zipf = zipfile.ZipFile('Front-End.zip', 'w', zipfile.ZIP_DEFLATED)
    zipdir('Front-End', zipf)
    zipf.close()
    
    machine_ip = "172.16.1.121"
    username = "root"
    password = "hal9000"
    s = paramiko.SSHClient()
    s.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    s.connect(machine_ip,22,username=username,password=password,timeout=4)

    sftp = s.open_sftp()
    sftp.put('Front-End.zip', 'cloud-computing/Front-End.zip')