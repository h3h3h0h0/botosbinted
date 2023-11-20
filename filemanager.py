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
                "files" : [] #files
            }
            with open(filelist, "w") as f:
                json.dump(header, f)
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

    def trackLocal(self, filename, multipart=False):
        os.path.isfile(os.path.join(self.server.working_dir, filename))
        clist = self.readList()
        if clist is None:
            return False
        for entry in clist["files"]:
            if entry["name"] == filename and entry["dir"] == self.server.working_dir:
                print("Track failed: file already exists!", file=self.print_location)
                return False
        cfile = {
            "name" : filename,
            "dir" : self.server.working_dir,
            "size" : getsize(os.path.join(self.working_dir, filename))
            "multipart" : multipart
        }
        clist["files"].append(cfile)
        print("Track successful!", file=self.print_location)
        return True
    def trackCloud(self, filename):
        #do this
