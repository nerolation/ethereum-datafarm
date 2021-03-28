#!/usr/bin/env python
# coding: utf-8

# In[1]:


from farm.Farm import *


# In[2]:


contracts = load_contracts(location="contracts2")



secKey = ".apikey/key2.txt"

farm = Farm(contracts=contracts, keyPath=secKey).status()
farm.start_farming()


# In[ ]:




