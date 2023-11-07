from oci.config import from_file
from oci.signer import Signer
from oci.object_storage import ObjectStorageClient
from tqdm import tqdm

class filemanager:
    def __init__(self, url, working_dir="", config_file=""):
        self.working_dir = working_dir
        self.url = url
        if len(config_file) > 0:
            self.config = from_file(config_file) #custom config file location
        else:
            self.config = from_file() #default location (~/.oci/config)
        self.storage_client = ObjectStorageClient(self.config)

    def getFile(self, namespace, bucket_name, object_name, filename="", chunk_size=8192) -> str:
        local_filename = object_name
        if len(filename) != 0:
            local_filename = filename
        #retrieve metadata first
        metadata = self.storage_client.head_object(
            namespace_name=namespace,
            bucket_name=bucket_name,
            object_name=object_name
        )
        if metadata.status != 200:
            print("ERROR! Status:", metadata.status)
            return "" #could not get the requested object
        osize = int(metadata.headers["content-length"])
        #after getting stuff, open a file and start writing
        with open(local_filename, 'wb') as f:
            for i in tqdm(range(0, osize, chunk_size)):
                endat = min(i+chunk_size-1, osize-1)
                rangestring = str(i) + "-" + str(endat)
                chunk = self.storage_client.get_object(
                    namespace_name=namespace,
                    bucket_name=bucket_name,
                    object_name=object_name,
                    range=rangestring
                )
                if chunk.status != 200:
                    print("ERROR! Status:", chunk.status)
                    return ""  # could not get the requested object
                f.write(chunk.content)
        return local_filename #if success, return filename (otherwise
