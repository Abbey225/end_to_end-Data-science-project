#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import pyodbc
from sqlalchemy import create_engine
import schedule
import time
import psycopg2
import getpass


# In[2]:


engine_source = create_engine('mssql+pyodbc://DESKTOP-TS0EFTA/knowledge?driver=SQL+Server+Native+Client+11.0',fast_executemany=True)


# In[3]:


engine_destination = create_engine('mssql+pyodbc://DESKTOP-TS0EFTA/machine_learning?driver=SQL+Server+Native+Client+11.0',fast_executemany=True)


# In[7]:


sql = """
select * from[dbo].[zamota_restaurant_data]
"""


# In[16]:


df=pd.read_sql_query(sql,engine_source ,chunksize=1000000)


# In[17]:


df


# In[18]:


df1=pd.DataFrame()
for a in df:
    df=df1.append(a)


# In[19]:


df.head(2)


# In[20]:


df.shape


# In[21]:


df.isnull().sum(
)


# In[22]:


df.info()


# In[23]:


df.head(1)


# In[24]:


df=df.rename(columns={'approx_cost(for two people)':'cost','menu_item':'items','type':'listed_in(type)'})


# In[26]:


df.head(1)


# In[27]:


df.rate.unique(
)


# In[38]:


df=df[df['rate']!='NEW']


# In[39]:


df=df[df['rate']!='-']


# In[40]:


df.rate.unique()


# In[41]:


df.isnull().sum()


# In[42]:


del df['dish_liked']


# In[43]:


df.shape


# In[44]:


df.cost.unique()


# In[46]:


df.cost=df.cost.astype(str).apply(lambda x:x.replace(',',''))


# In[56]:


df.cost.unique()


# In[55]:


df=df[df['cost']!='None']


# In[57]:


df.isnull().sum()


# In[58]:


df.dropna(inplace=True)


# In[59]:


df.cost=df.cost.astype(int)


# In[60]:


df.head(2)


# In[61]:


df.rate.unique()


# In[63]:


df.rate=df.rate.astype(str).apply(lambda x:x.replace('/5',''))


# In[64]:


df.rate=df.rate.astype(float)


# In[65]:


df.head(1)


# In[66]:


df.info()


# In[67]:


df.head(1)


# In[69]:


df.online_order.value_counts().plot(kind='pie',title='online order counts',xlabel='yes or no',y='counts',figsize=(6,6))


# In[74]:


import seaborn as sms
import matplotlib.pyplot as plt


# In[71]:


df.head(1)


# In[79]:


plt.figure(figsize=(10,6))
sms.barplot(x='online_order',y='rate',hue='book_table',data=df)

plt.xlabel('online')


# In[80]:


df.head(1)


# In[82]:


def etl_process():
    df.to_csv("C:/Users/stone/Documents/ddd/zmsample")
    df.to_sql('mysundaydata',con=engine_destination,if_exists='append',index=False)
    


# In[ ]:


schedule.every().sunday.at("08:25").do(etl_process)

while True:
    schedule.run_pending()
    time.sleep(1)


# In[ ]:




