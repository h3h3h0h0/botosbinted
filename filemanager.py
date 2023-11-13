import sys
from os.path import getsize

import oci
from halo import Halo
from oci.config import from_file
from oci.signer import Signer
from oci.object_storage import ObjectStorageClient
from tqdm import tqdm
import os

print_location = sys.stdout

class filemanager:
    def __init__(self, url, working_dir="", config_file=""):
        self.working_dir = working_dir
        self.url = url
        if len(config_file) > 0:
            self.config = from_file(config_file) #custom config file location
        else:
            self.config = from_file() #default location (~/.oci/config)
        self.storage_client = ObjectStorageClient(self.config)

    def getFile(self, namespace, bucket_name, object_name, filename="", chunk_size=8192, attempts=10) -> str:
        local_filename = object_name
        if len(filename) != 0:
            local_filename = filename
        print("Attempting to download file named", object_name, "to current folder as", filename, file=print_location)
        #retrieve metadata first
        metadata = self.storage_client.head_object(
            namespace_name=namespace,
            bucket_name=bucket_name,
            object_name=object_name
        )
        if metadata.status != 200:
            print("ERROR! Status:", metadata.status, file=print_location)
            return "" #file isn't created yet so it's fine
        osize = int(metadata.headers["content-length"])
        #after getting stuff, open a file and start writing
        valid_file = True
        with open(os.path.join(self.working_dir, local_filename), 'wb') as f:
            for i in tqdm(range(0, osize, chunk_size), desc="DOWNLOADING!"):
                endat = min(i+chunk_size-1, osize-1)
                rangestring = str(i) + "-" + str(endat)
                for j in range(attempts):
                    chunk = self.storage_client.get_object(
                        namespace_name=namespace,
                        bucket_name=bucket_name,
                        object_name=object_name,
                        range=rangestring
                    )
                    if chunk.status == 200:
                        break
                if chunk.status != 200: #tried already, something is broken so exit
                    valid_file = False
                    print("ERROR! Status:", chunk.status, file=print_location)
                    break
                f.write(chunk.content)
        #if parts could not be reached and file is incomplete, delete the file and return empty string
        if not valid_file:
            print("File at", local_filename, "could not be fully downloaded, cleaning up.", file=print_location)
            if os.path.exists(local_filename):
                os.remove(local_filename)
            return ""
        print("SUCCESS!", file=print_location)
        return local_filename #if success, return filename (otherwise would be empty to signal caller something went wrong)

    def putFile(self, namespace, bucket_name, filename, object_name="", attempts=10, tier="", replace_existing=True) -> bool: #standard PUT operation, up to 50 GiB (but use for small files only)
        if len(object_name) == 0:
            object_name = filename
        print("Attempting to upload file named", filename, "to server as", object_name, file=print_location)
        #check if object exists
        metadata = self.storage_client.head_object(
            namespace_name=namespace,
            bucket_name=bucket_name,
            object_name=object_name
        )
        if metadata.status == 200: #object exists (we may need to overwrite)
            print("Object exists with size", metadata.headers["content-length"], file=print_location)
            if not replace_existing:
                return False
        spinner = Halo(text='Uploading', spinner='dots')
        spinner.start()
        with open(os.path.join(self.working_dir, filename), 'rb') as f:
            fcontent = f.read()
            for i in range(attempts):
                response = None
                if len(tier) != 0:
                    response = self.storage_client.put_object(
                        namespace_name=namespace,
                        bucket_name=bucket_name,
                        object_name=object_name,
                        storage_tier=tier,
                        put_object_body=fcontent
                    )
                else:
                    response = self.storage_client.put_object(
                        namespace_name=namespace,
                        bucket_name=bucket_name,
                        object_name=object_name,
                        put_object_body=fcontent
                    )
                if response.status == 200:
                    spinner.stop()
                    print("SUCCESS!", file=print_location)
                    return True
        spinner.stop()
        print("FAILURE!", file=print_location)
        return False

    def multiPutFile(self, namespace, bucket_name, filename, object_name="", chunk_size=8192, attempts=10, tier="", replace_existing=True):
        if len(object_name) == 0:
            object_name = filename
        # check if object exists
        metadata = self.storage_client.head_object(
            namespace_name=namespace,
            bucket_name=bucket_name,
            object_name=object_name
        )
        if metadata.status == 200:  # object exists (we may need to overwrite)
            print("Object exists with size", metadata.headers["content-length"], file=print_location)
            if not replace_existing:
                return False
        #create upload
        mpu_details = oci.object_storage.models.CreateMultipartUploadDetails(
            object=object_name)
        if len(tier) != 0:
            mpu_details.storage_tier = tier
        print("Initializing multipart upload.", file=print_location)
        create_response = self.storage_client.create_multipart_upload(
            namespace_name=namespace,
            bucket_name=bucket_name,
            create_multipart_upload_details=mpu_details
        )
        if create_response.status != 200:
            print("Could not initialize multipart upload. Status:", create_response.status, file=print_location)
            return False
        up_id = create_response.data["uploadId"]
        #make parts
        fsize = getsize(os.path.join(self.working_dir, filename))
        wchunks = fsize//chunk_size
        rchunk = fsize%chunk_size
        fqueue = []
        for i in range(wchunks):
            fqueue.append((i*chunk_size, i+1))
        if rchunk:
            fqueue.append((wchunks*chunk_size, wchunks+1))
        fin_size = len(fqueue)
        print("Upload will consist of", fin_size, "parts.", file=print_location)
        tattempts = attempts*fin_size #attempts is per-chunk
        commits = []
        #attempt to upload all chunks
        with open(os.path.join(self.working_dir, filename), 'rb') as f:
            with tqdm(total=fin_size, desc="UPLOADING!") as tq:
                while len(fqueue) and tattempts:
                    cur = fqueue.pop(0)
                    f.seek(cur[0])
                    cchunk = f.read(chunk_size)
                    up_num = cur[1]
                    upload_part_response = self.storage_client.upload_part(
                        namespace_name=namespace,
                        bucket_name=bucket_name,
                        object_name=object_name,
                        upload_id=up_id,
                        upload_part_num=up_num,
                        upload_part_body=cchunk
                    )
                    if upload_part_response.status != 200:
                        fqueue.append(cur)
                    else:
                        commits.append(oci.object_storage.models.CommitMultipartUploadPartDetails(
                            part_num=up_num,
                            etag=upload_part_response.data["ETag"])
                        )
                        tq.update(1)
                    tattempts -= 1
        if len(fqueue):
            print("FAILURE!", len(fqueue), "parts could not be uploaded,", attempts*fin_size, "attempts used.", file=print_location)
            return False
        #attempt to commit
        for i in range(attempts):
            commit_response = self.storage_client.commit_multipart_upload(
                namespace_name=namespace,
                bucket_name=bucket_name,
                object_name=object_name,
                upload_id=up_id,
                commit_multipart_upload_details=oci.object_storage.models.CommitMultipartUploadDetails(
                    parts_to_commit=commits
                )
            )
            if commit_response.status == 200:
                print("SUCCESS!", file=print_location)
                return True
        print("Could not commit upload!", file=print_location)
        return False
