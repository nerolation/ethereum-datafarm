#!/usr/bin/env python
# coding: utf-8

# In[1]:


from farm.Farm import *


# In[2]:


contracts = []   
    
contracts = load_contracts(contracts)

farm = Farm(contracts).status()
farm.start_farming()


# In[ ]:




