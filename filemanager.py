import os
import json
import sys
from os.path import getsize

from serverconnection import serverconnection

class filemanager:
    def __init__(self, url, working_dir="", config_file="", filelist="files.json"):
        self.print_location = sys.stdout
        #manages files through server connection, keeps track of what files are in the cloud/computer, keeps metadata on local device
        if not os.path.exists(filelist): #the file list needs creation
            header = {
                "ondisk" : 0, #number of files on disk
                "incloud" : 0, #number of files in cloud
                "diskspace" : 0, #space occupied on disk
                "cloudspace" : 0, #space occupied in cloud
                "files" : [] #files (indexed by full path)
            }
            with open(filelist, "w") as f:
                json.dump(header, f, indent=5)
        self.filelist = filelist
        #initialize the connection to server
        self.server = serverconnection(url, working_dir, config_file)

    def changeDir(self, wdir):
        self.server.changeDir(wdir)

    def toLogfile(self, logfile):
        self.print_location = open(logfile, "a")

    def toStdout(self):
        if self.print_location != sys.stdout:
            self.print_location.close()
        self.print_location = sys.stdout
    def readList(self):
        clist = None
        with open(self.filelist, "r") as f:
            clist = json.load(f)
        if clist is None:
            print("Could not read list!", file=self.print_location)
            return None
        return clist

    def writeList(self, nlist):
        open(self.filelist).close() #clear
        with open(self.filelist, "w") as f:
            json.dump(nlist, f, indent=5)

    def exists(self, filename):
        with open(self.filelist, "r") as f:
            clist = self.readList()
            if clist["files"].has_key(os.path.join(self.working_dir, filename)):
                return True
            else:
                return False

    def trackLocal(self, namespace, bucket_name, filename, multipart=False):
        if not os.path.isfile(os.path.join(self.server.working_dir, filename)):
            print("Track failed: file does not exist on local machine!", file=self.print_location)
            return False
        clist = self.readList()
        if clist is None:
            return False
        if self.exists(filename):
            print("Track failed: file already tracked!", file=self.print_location)
            return False
        cfile = {
            "name" : filename,
            "dir" : self.server.working_dir,
            "size" : getsize(os.path.join(self.working_dir, filename)),
            "namespace" : namespace,
            "bucket" : bucket_name,
            "multipart" : multipart
        }
        clist["files"][os.path.join(self.working_dir, filename)] = cfile
        self.writeList(clist)
        print("Track successful!", file=self.print_location)
        return True
    def trackCloud(self, namespace, bucket_name, filename, multipart=False):
        clist = self.readList()
        if clist is None:
            return False
        if not self.server.exists(namespace, bucket_name, filename):
            print("Track failed: file does not exist on server!", file=self.print_location)
            return False
        if self.exists(filename):
            print("Track failed: file already tracked!", file=self.print_location)
            return False
        cfile = {
            "name": filename,
            "dir": self.server.working_dir,
            "size": getsize(os.path.join(self.working_dir, filename)),
            "namespace": namespace,
            "bucket": bucket_name,
            "multipart": multipart
        }
        clist["files"][os.path.join(self.working_dir, filename)] = cfile
        self.writeList(clist)
        print("Track successful!", file=self.print_location)
        return True

    def download(self, filename, overwrite=False, chunk_size=8192, attempts=10):
        if not self.exists(filename):
            print("File not tracked!", file=self.print_location)
            return None
        if os.path.isfile(os.path.join(self.server.working_dir, filename)) and not overwrite:
            print("File already exists on local machine and overwrite is turned off.", file=self.print_location)
            return None
        clist = self.readList()
        ns = clist["files"][os.path.join(self.working_dir, filename)]["namespace"]
        bk = clist["files"][os.path.join(self.working_dir, filename)]["bucket"]
        if not self.server.exists(ns, bk, filename):
            print("File does not exist on server!", file=self.print_location)
            return None
        return self.server.getFile(ns, bk, filename, filename, chunk_size, attempts)
