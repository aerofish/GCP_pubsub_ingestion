import os
import sys
import json
import time
import threading
import zipfile
import traceback
from datetime import datetime, timedelta
from io import BytesIO
from unittest import result

from google.auth import jwt
from google.cloud import pubsub_v1

from pyspark.sql.types import StructType,StructField, StringType, MapType, BinaryType

import compress


class PubSubSubscriber:
    def __init__(self, project_name, subscription_name, topic_name, service_account_info=None, folderpath="", memory_quota=1024 * 1024, flow_control=1000, patrol_interval=60 * 8, decompress=False, output_format="json", tenant_id_list = None, job_name = None):
        """
        The function creates a subscriber object that can be used to pull messages from a Pub/Sub
        subscription
        
        :param project_name: The name of the project that the subscription belongs to
        :param subscription_name: the name of the subscription you created in the Google Cloud Console
        :param topic_name: the name of the topic you want to subscribe to
        :param service_account_info: a dictionary containing the service account info
        :param folderpath: the folder path to store the output files
        :param memory_quota: the maximum memory size in bytes that the subscriber can use to cache the
        messages
        :param flow_control: the number of messages to pull from the subscription at a time, defaults to
        1000 (optional)
        :param patrol_interval: the interval in seconds between each time the cache is cleared
        :param decompress: if the message is compressed, set this to True, defaults to False (optional)
        :param output_format: json or csv, defaults to json (optional)
        """

        if service_account_info:
            self.audience = "https://pubsub.googleapis.com/google.pubsub.v1.Subscriber"
            credentials = jwt.Credentials.from_service_account_info(
            service_account_info, audience=self.audience)
            self.subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
        else:
            self.subscriber = pubsub_v1.SubscriberClient()
        self.subscription_path = self.subscriber.subscription_path(project_name, subscription_name)
        self.topic_path = self.subscriber.topic_path(project_name, topic_name)
        self.topic_name = topic_name
        self.subscription_name = subscription_name
        self.accumulated_event_list=[]
        self.cache_size=0
        self.watch_dog=None
        self.memory_quota=memory_quota
        self.flow_control=flow_control
        self.patrol_interval=patrol_interval
        self.lock=threading.Lock()
        self.latest_cache_clear_time=datetime.now()
        self.folderpath=os.path.join(folderpath, self.topic_name, self.subscription_name)
        dbutils.fs.mkdirs(self.folderpath)
        self.message_obj_list=[]
        self.list_to_output=[]
        self.pull_future=None
        self.decompress=decompress
        self.output_format=output_format
        self.tenant_id_list = tenant_id_list
        self.saved_filepaths = []
        self.databricks_context = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()) 
        self.job_name = job_name
        if not self.job_name:
            if not self.databricks_context["currentRunId"]:
                self.job_name = self.databricks_context["extraContext"]["notebook_path"].split("/")[-1]
            else:
                self.job_name = self.databricks_context["tags"]["jobId"]

    def cacheOnDisk(self, data, folderpath = "", prefix = None):
        """
        It takes a dataframe, converts it to a json file, and saves it to the specified folder
        
        :param data: the data to be cached
        :param folderpath: The path to the folder where you want to save the file
        :param prefix: The prefix of the file name
        :return: The filepath of the file that was created.
        """

        timestr = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        if prefix is None:
            prefix = ""
        else:
            prefix = prefix + "_"
        filename = prefix + self.job_name + "_" + timestr + ".json"
        db_folderpath="/dbfs"+folderpath
        filepath = os.path.join(db_folderpath, filename)
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
            #f.writelines(str(data))
        print("output file: ", filepath)
        self.saved_filepaths.append(filepath)
        return filepath


    def _asyncCallback(message):
        """
        The function is called when a message is received. The message is appended to a list, and then
        acknowledged
        
        :param message: The message object
        """

        result.append(message) 
        message.ack()  # Asynchronously acknowledge the message.

    def getAsync(self):
        """
        It creates a subscription to the topic, and then returns the result of the subscription
        :return: A list of messages.
        """

        result = []
        future = self.subscriber.subscribe(self.subscription_path, self._asyncCallback)
        return result
    
    def calculateSize(self, obj, id_set=None):
        """
        It recursively calculates the size of an object by adding the size of the object itself to the size
        of all the objects it references
        
        :param obj: the object whose size you want to know
        :param id_set: This is a set of ids of objects that have already been processed. This is used to
        avoid infinite recursion
        :return: The size of the object in bytes.
        """

        size = sys.getsizeof(obj)
        if id_set is None:
            id_set = set()
        obj_id = id(obj)
        if obj_id in id_set:
            return 0
        id_set.add(obj_id)
        if isinstance(obj, dict):
            size += sum([self.calculateSize(v, id_set) for v in obj.values()])
            size += sum([self.calculateSize(k, id_set) for k in obj.keys()])
        elif hasattr(obj, "__dict__"):
            size += self.calculateSize(obj.__dict__, id_set)
        elif hasattr(obj, "__iter__") and not isinstance(obj, (str, bytes, bytearray)):
            size += sum([self.calculateSize(i, id_set) for i in obj])
        return size
    
    def writeDelta(self, content_list, folderpath):
        """
        It takes a list of tuples, each tuple containing a message, message_id, meta_data, and publish_time,
        and writes them to a Delta table
        
        :param content_list: a list of tuples, each tuple is a row in the dataframe
        :param folderpath: The path to the folder where the delta table is stored
        """

        if self.decompress:
            schema = StructType([
                StructField("data", BinaryType(), True),
                StructField("message_id", StringType(), True),
                StructField("meta_data", MapType(StringType(), StringType(), True), True),
                StructField("publish_time", StringType(), True)
            ])
            df = spark.createDataFrame(content_list, schema=schema)
            df.withColumn("data", compress.decompress_message_udf(df["data"])).write.mode("append").format("delta").save(folderpath)

        else:
            schema = StructType([
                StructField("data", MapType(StringType(), StringType(), True), True),
                StructField("message_id", StringType(), True),
                StructField("meta_data", MapType(StringType(), StringType(), True), True),
                StructField("publish_time", StringType(), True)
            ])
            df = spark.createDataFrame(content_list, schema=schema)

            df.write.mode("append").format("delta").save(folderpath)
            self.saved_filepaths.append(folderpath)

    def outputMessages(self, obj_list, content_list):
        thread_id=threading.currentThread().ident
        message_count=len(obj_list)
        if message_count > 0:

            start_time = datetime.now()
            if self.output_format == "delta" :
                self.writeDelta(content_list, folderpath=self.folderpath)
            else:
                self.cacheOnDisk(data=content_list, folderpath=self.folderpath)
                
            end_time = datetime.now()
            time_taken=end_time-start_time
            print(f"Thread {thread_id} spent {time_taken} to output {message_count} messages")
            for msg in obj_list:
                msg.ack()
            obj_list.clear()
            content_list.clear()
            self.latest_cache_clear_time = datetime.now()
            current_time=datetime.now()
    
    def asyncOutputMessages(self, obj_list, content_list):
        """
        It takes a list of objects and a list of strings, and then it outputs the strings to the objects
        
        :param obj_list: a list of objects to send the message to
        :param content_list: a list of strings that will be outputted to the console
        """

        t1 = threading.Thread(target=self.outputMessages, args=(obj_list, content_list))
        t1.start()
        
    def processMessage(self, message: pubsub_v1.subscriber.message.Message, data = None):
        """
        It takes a message from the Pub/Sub subscription, extracts the data, message_id, publish_time, and
        metadata, and returns a dictionary containing all of these
        
        :param message: The message object that was received from the Pub/Sub subscription
        :type message: pubsub_v1.subscriber.message.Message
        :return: A dictionary with the data, message_id, meta_data, and publish_time.
        """
        if data:
            pass
        else:
            data = str(message.data)
        meta_data=dict(message.attributes)
        result = {"data": str(data), 
                    "message_id": message.message_id,
                    "meta_data": meta_data, 
                    "topic_id": self.topic_name,
                    "subscription_id": self.subscription_name,
                    "publish_time": str(message.publish_time),
                    "publish_date": str(message.publish_time)[:10]
                    }
        return result
       
    def buildBronzeData(self, message: pubsub_v1.subscriber.message.Message):
        """
        It takes a message from the PubSub subscription, and returns a dictionary with the message data,
        message id, meta data, and publish time
        
        :param message: The message object that was received from the Pub/Sub subscription
        :type message: pubsub_v1.subscriber.message.Message
        """

        data={}
        meta_data={}
        
        if self.tenant_id_list:
            try:
                tenant_id = message.attributes.get("tenantId")
                if tenant_id in self.tenant_id_list:
                    if self.output_format == "delta" :
                        return self.processMessage(message)
                    else:
                        if self.decompress: 
                            data=self.decompressMessage(message.data)
                            return self.processMessage(message, data = data)
                        else:
                            return self.processMessage(message)
                elif tenant_id == None:
                    # tenant_id = "deadletter"   
                    return "deadletter"
                else:
                    return None
            except:
                # tenant_id = "deadletter"      
                return "deadletter"
        else:
            if self.output_format == "delta" :
                return self.processMessage(message)
            else:
                if self.decompress: 
                    data=self.decompressMessage(message.data)
                    return self.processMessage(message, data = data)
                else:
                    return self.processMessage(message)
    
    def decompressMessage(self, data):
        """
        It takes a byte array, converts it to a binary stream, then uses the zipfile library to unzip the
        stream, and finally converts the unzipped data to a JSON object.
        
        :param data: The data to be decompressed
        :return: a list of dictionaries.
        """

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
        return clean_json
        
    def processMessageThreadsafe(self, message: pubsub_v1.subscriber.message.Message):
        """
        If the cache size is greater than the memory quota or the number of messages in the message object
        list is greater than the flow control, then acquire the lock, if the cache size is greater than the
        memory quota or the number of messages in the message object list is greater than the flow control,
        then set the object list to the message object list, set the message object list to an empty list,
        set the content list to the accumulated event list, set the accumulated event list to an empty list,
        output the messages, and set the cache size to 0. 
        
        Release the lock, append the extracted message to the accumulated event list, append the message to
        the message object list, and set the cache size to the cache size plus the size of the extracted
        message. 
        
        Otherwise, append the extracted message to the accumulated event list, append the message to the
        message object list, and set the cache size to the cache size plus the size of the extracted
        message.
        
        :param message: the raw message object from the pubsub subscription
        :type message: pubsub_v1.subscriber.message.Message
        """

        extracted_message = self.buildBronzeData(message)
        
        if extracted_message != "deadletter":
            current_time=datetime.now()
            if self.cache_size >= int(self.memory_quota) or len(self.message_obj_list) >= self.flow_control:
                self.lock.acquire()
                if self.cache_size >= int(self.memory_quota) or len(self.message_obj_list) >= self.flow_control:
                    print(f"cache size: {self.cache_size}")
                    obj_list=self.message_obj_list
                    self.message_obj_list=[]
                    content_list=self.accumulated_event_list
                    self.accumulated_event_list=[]
                    self.outputMessages(obj_list, content_list)
                    self.cache_size=0
                self.lock.release()
                if extracted_message != None:
                    self.accumulated_event_list.append(extracted_message)
                    self.cache_size = self.cache_size + self.calculateSize(extracted_message)
                self.message_obj_list.append(message)

            else:
                if extracted_message != None:
                    self.accumulated_event_list.append(extracted_message)
                    self.cache_size = self.cache_size + self.calculateSize(extracted_message)
                self.message_obj_list.append(message)


    def _callback(self, message: pubsub_v1.subscriber.message.Message) -> None:
        """
        > The function `_callback` is called by the Google Pub/Sub client library when a message is received
        
        :param message: The message that was received
        :type message: pubsub_v1.subscriber.message.Message
        """

        self.processMessageThreadsafe(message)
    
    def _gracefulShutdown(self, output_all=True) -> None:
        """
        > The function will output all the messages in the buffer to the bucket, and then acknowledge all
        the messages in the buffer
        
        :param output_all: If True, all messages will be output to the bucket. If False, only the latest
        message will be output to the bucket, defaults to True (optional)
        """

        current_time=datetime.now()
        print(f"Perform gracefully shutdown at {current_time} ...")
        try:
            if output_all and len(self.accumulated_event_list) > 0:
                message_count=len(self.accumulated_event_list)
                print(f"Shutdown thread Output {message_count} messages to Bucket.")
                ack_ids=[]
                for msg in self.message_obj_list:
                    ack_ids.append(msg.ack_id)
                print("output all messages...")
                if self.output_format == "delta" :
                    self.writeDelta(self.accumulated_event_list, folderpath=self.folderpath)
                else:
                    self.cacheOnDisk(data=self.accumulated_event_list, folderpath=self.folderpath)
                    
                print(f"number of messages to acknowledge: {len(ack_ids)}")
                step=2000
                start_index=0
                end_index=start_index+step
                while True:
                    if len(ack_ids[start_index:end_index]) > 0:
                        self.subscriber.acknowledge(
                        request={
                            "subscription": self.subscription_path,
                            "ack_ids": ack_ids[start_index:end_index],
                            }
                        )
                    num=len(ack_ids[start_index:end_index])
                    print(f"acknowledge number of messages: {num}")
                    if num < step:
                        break
                    start_index=end_index
                    end_index=end_index+step
                #self.subscriber.acknowledge(subscription=self.subscription_path, ack_ids=ack_ids, timeout=5)
        except Exception as e:
            print(
            f"Graceful shutdown with error: {e}.")
            traceback.print_exc()
        finally:
            print("close subscriber...")
            self.subscriber.close()
            print("cancel watch dog...")
            self.watch_dog.cancel()
            print("Gracefully shutdown done...")
        
    def startStreamingPull(self):
        """
        It starts the streaming pull process, and returns a future object that can be used to check if the
        streaming pull process is done
        :return: The streaming_pull_future is being returned.
        """

        flow_control_obj = pubsub_v1.types.FlowControl(max_messages=self.flow_control)
        before_sub=time.time()
        current_time=datetime.now()
        print(f"Streaming pull messages starts at {current_time} ...")
        streaming_pull_future = self.subscriber.subscribe(self.subscription_path, callback=self._callback, flow_control=flow_control_obj)
        streaming_pull_future.add_done_callback(fn=self._gracefulShutdown)
        after_sub=time.time()
        print(f"finish sub time: {after_sub}" )
        print(f"time to do subscribe: {after_sub-before_sub}")
        return streaming_pull_future
    
    def watchDogPatrol(self):
        """
        The function is called every 60 seconds. If the current time is greater than the latest cache clear
        time plus the patrol interval, then the function will force output the messages to the bucket
        """
        
        print("Start watch dog patrol...")
        current_time=datetime.now()
        if current_time > self.latest_cache_clear_time + timedelta(seconds=self.patrol_interval):
            if len(self.message_obj_list) > 0:
                print("watch dog force output messages...")
                self.lock.acquire()
                message_count=len(self.message_obj_list)
                print(f"Watch dog thread {id} Output {message_count} messages to Bucket.")
                ack_ids=[]
                content_list=[]
                self.accumulated_event_list=[]
                obj_list=self.message_obj_list
                self.message_obj_list=[]
                self.cache_size=0
                for message in obj_list:
                    extracted_message =  self.buildBronzeData(message)
                    content_list.append(extracted_message)
                    ack_ids.append(message.ack_id)
                if self.output_format == "delta" :
                    self.writeDelta(content_list, folderpath=self.folderpath)
                else:
                    self.cacheOnDisk(data=content_list, folderpath=self.folderpath)
                    
                self.subscriber.acknowledge(subscription=self.subscription_path, ack_ids=ack_ids, timeout=5)
                self.lock.release()
        self.watch_dog = threading.Timer(60, function=self.watchDogPatrol)
        self.watch_dog.start()

    def streamMessages(self, timeout = 10):
        self.watchDogPatrol()
        future = self.startStreamingPull()
        try:
            future.result(timeout=timeout)

        except Exception as e:
            if e.__class__.__name__ == "TimeoutError":
                pass
            else:
                print(
                    f"Listening for messages threw an exception: {e}."
                )
                raise Exception(e)
        finally:
            future.cancel() 
