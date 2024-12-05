from io import BytesIO
import zipfile
import json


def compress_message(msg):
    # convert the dictionary to a json string
    json_string = json.dumps(msg)
    # create a binary stream for the json string
    json_bytes = BytesIO(json_string.encode())
    # create a binary stream for the zip file
    zip_bytes = BytesIO()
    # create the zip file and add the json string to it
    

    with zipfile.ZipFile(zip_bytes, mode="w", compression=zipfile.ZIP_DEFLATED,strict_timestamps=False) as zf:
        zf.writestr("data.json", json_bytes.getvalue())
    # return the binary content of the zip file
    data = zip_bytes.getvalue()
    return data

def decompress_message(data):
    clean_json=[]
    if len(data)>0:
        zip_data=data
        binary_stream=BytesIO(zip_data)
        z=zipfile.ZipFile(binary_stream)
        info_list=z.infolist()
        if len(info_list) >0:
            unzip_content = z.read(z.infolist()[0])
            json_str=unzip_content.decode("utf8")
            clean_json = json.loads(json_str)
        z.close()
            #print("unzip data:",clean_json)
    return clean_json
