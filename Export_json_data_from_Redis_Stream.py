#!/usr/bin/env python
# coding: utf-8

# In[12]:


from os import environ
from redis import Redis
import json


# In[2]:


stream_key = environ.get("STREAM", "crypto")


# In[3]:


hostname = environ.get("REDIS_HOSTNAME", "redis-15514.c246.us-east-1-4.ec2.cloud.redislabs.com")
port = environ.get("REDIS_PORT", 15514)
username = "default"
pwd = "DcxdU4uZGRtn4CgGoJ6lLmd1S6TkdqsV"


# In[6]:


redis_connection = Redis(hostname, port, retry_on_timeout=True, password=pwd, username=username)
last_id = 0
sleep_ms = 0


# In[7]:


resp = redis_connection.xread(
                {stream_key: last_id}, count=100000, block=sleep_ms
            )


# In[8]:


key, messages = resp[0]


# In[13]:


json_file = open("/Users/Sunil/crypto_data.json", "w")


# In[14]:


for k,v in enumerate(messages):
    last_id, data = v
    data_dict = {k.decode("utf-8"): data[k].decode("utf-8") for k in data}
    json.dump(data_dict, json_file)


# In[15]:


json_file.close()


# In[ ]:




