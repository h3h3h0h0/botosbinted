
class filemanager:
    def __init__(self, url, working_dir="", config_file="", filelist="files.json"):
        #manages files through server connection, keeps track of what files are in the cloud/computer, keeps metadata on local device
        if os.path.exists(filelist): #the file list already exists

