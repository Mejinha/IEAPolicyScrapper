# -*- coding: utf-8 -*-
"""
Created on Tue May 25 15:49:06 2021

@author: amejd
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
from multiprocessing import Pool
import numpy as np

def GetData(page):
    
    '''Access main page and search data in each row'''
    
    url = 'https://www.iea.org/policies'

    page_data = []
    
    raw = requests.get(f'{url}?page={page}')
    soup = BeautifulSoup(raw.content, 'html.parser')
    
    rows = soup.findAll(class_ = 'm-policy-listing-item__row')
    
    try:
        for row in rows:  
            page_data.append(Scrape(row))   
        
        return page_data
    
    except:
        pass

def Scrape(row):
    
    '''Access each policy and gather useful information'''
    
    temp_dict = {
                'Country': row.findAll(class_ = 'm-policy-listing-item__col m-policy-listing-item__col--country')[0]['data-sortable-value'],
                'Year': row.findAll(class_ = 'm-policy-listing-item__col m-policy-listing-item__col--year')[0]['data-sortable-value'],
                'Policy': row.findAll(class_ = 'm-policy-listing-item__link')[0].text.strip()
                }
    
    policy_url = 'https://www.iea.org{}'.format(row.findAll(class_ = 'm-policy-listing-item__link')[0]['href'])
    raw_internal = requests.get(policy_url)
    soup_internal = BeautifulSoup(raw_internal.text, 'html.parser')

    categories = soup_internal.findAll('div', {'class': 'o-policy-content__list'})

    for category in categories:
        catname = category.find(class_ = 'o-policy-content-list__title').text
        temp_dict[catname] = [item.text for item in category.findAll(class_ = 'a-tag__label')]
    
    return temp_dict
    
    
if __name__ == '__main__':
    
    pages = range(1, 186)
    
    records = []
    
    # Process the function GetData in batches of 10 parallel functions
    
    with Pool(10) as p:
        records = p.map(GetData, pages) # Execute GetData 
        p.close()   # Closes the application to avoid it from keep running as a background task
        p.join()    # Wait for tasks before start a new batch
    
    # Organize data 
    
    df = pd.concat([pd.DataFrame(i) for i in records])    

    df = df.reset_index()
    
    # Get Unique values for each column
    
    Topics = []
    Policies = []
    Sectors = []
    Technologies = []
    
    for row in df.iterrows():
        try:
            for topic in row[1]['Topics']:
                Topics.append(topic)
        except:
            pass
        
        try:
            for policy in row[1]['Policy types']:
                Policies.append(policy)
        except:
            pass
        
        try:
            for sector in row[1]['Sectors']:
                Sectors.append(sector)
        except:
            pass
                
        try:
            for techs in row[1]['Technologies']:
                Technologies.append(techs)
        except:
            pass
                
    Topics = np.unique(Topics)
    Policies = np.unique(Policies)
    Sectors = np.unique(Sectors)
    Technologies = np.unique(Technologies)
    
    # Convert each value into unique boolean columns
    # 1 Check if the variable is a list, otherwise return False
    # 2 Check if the list contains the key, if so, return True
    
    for key in Topics:
        df['Topic_'+key] = df['Topics'].apply(lambda x: key in x if type(x) is list else False)
    
    for key in Policies:
        df['Policy_'+key] = df['Policy types'].apply(lambda x: key in x if type(x) is list else False)
    
    for key in Sectors:
        df['Sector_'+key] = df['Sectors'].apply(lambda x: key in x if type(x) is list else False)
        
    for key in Technologies:
        df['Tech_'+key] = df['Technologies'].apply(lambda x: key in x if type(x) is list else False)
    
    # Export to excel
    
    df.to_excel('IEA_Policies.xlsx')
    
    
