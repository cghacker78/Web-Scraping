import asyncio
import aiohttp
from pandas import json_normalize
from urllib.parse import quote ## encodes special characters like / and space in the url parameter
import time
import nest_asyncio
import pandas as pd

start_time = time.time()
nest_asyncio.apply()


cookies = {
    '_fbp': 'fb.2.1749107605325.850820154720544394',
    '_gid': 'GA1.3.1749107606',
    'G_ENABLED_IDPS': 'google',
    '_clck': 'sdbzpw|2|fwj|0|1982',
    'countryTag': '5f02e38c6f3de87babe20cd2',
    '_clsk': 's4zbo|1749195429293|7|1|b.clarity.ms/collect',
    '_ga': 'GA1.1.1139995524.1749106545',
    '_ga_42QP5D4V7R': 'GS2.1.s1749195243$o9$g1$t1749195471$j40$l0$h1934351063',
    '_ga_PSV566ZLWP': 'GS2.1.s1749195466$o11$g1$t1749195488$j38$l0$h0',
}

headers = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7',
    'content-type': 'application/json',
    'origin': 'https://www.startupindia.gov.in',
    'priority': 'u=1, i',
    'referer': 'https://www.startupindia.gov.in/digital-map/maps',
    'sec-ch-ua': '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',
}

startup_ids_urls = 'https://www.startupindia.gov.in/maps/startup/filter'
service_url = 'https://api.startupindia.gov.in/sih/api/noauth/resource/startupServices/all'
cin_contact_url =  'https://api.startupindia.gov.in/sih/api/noauth/dpiit/services/cin/info'

startup_columns = ['id', 'name','country', 'state', 'city', 'industries', 'sectors', 'stages']
profile_cin_columns = ['uniqueId','startup_pan','startup_cin','startup_services','startup_website']
cin_contact_columns = ['data_cin','data_companyStatus', 'data_email', 'data_incorpdate', 'data_registeredAddress','data_registeredContactNo']


async def startup(session,page_no):
    json_data = {
        "industries": [],
        "sectors": [],
        "states": [],
        "stages": [],
        "badges": [],
        "roles": ["Startup"],
        "counts": [],
        "registrationFrom": "",
        "registrationTo": "",
        "page":page_no
    }
    
    async with session.post(startup_ids_urls,headers = headers,json = json_data) as response:
        #return await response.json()
         return await response.json()
     
semaphore = asyncio.Semaphore(100)   
async def profile_cin(session,cin_no):
    async with semaphore:
        profile_cin_url= f'https://api.startupindia.gov.in/sih/api/common/replica/user/profile/{cin_no}'
        async with session.get(profile_cin_url) as response:
           cin_dt =  await response.json()
           cin_norm =  json_normalize(cin_dt['user'],sep = '_')
           #print(cin_dt)
    
           return cin_norm[profile_cin_columns]


async def service_name(session,servc_id):
    async with session.post(service_url,headers = headers,json = servc_id ) as response:
      return await response.json()


'''async def cin_contact(session,params_):
    params = {'cin':params_ }
    async with session.get(cin_contact_url,params =params ,headers = headers) as response:
        dt1 = await response.json()
       # if dt1.get('status') is True:
        print(dt1)

        if dt1['status'] is True:
            dt1_norm = json_normalize(dt1,sep = '_')
            #print(dt1)
            return dt1_norm[cin_contact_columns]'''
        
Seam = asyncio.Semaphore(100)
async def cin_contact(session,params_):
    async with Seam:
        safe_cin = quote(params_, safe='')
    
        params = {'cin':safe_cin }
        async with session.get(cin_contact_url,params =params ,headers = headers) as response:
            try: 
              dt1 = await response.json()
            except Exception as e:
              text = await response.text()
              print(f"JSON decoding failed. Raw response:\n{text} and error is {e}")
              
              #Return an empty DataFrame for failed cases (safer for concat() later).
              #Without returning pd.DataFrame() on failure, df could be None, which would raise an error in pd.concat(df_list)
             
              return pd.DataFrame() 
           # if dt1.get('status') is True:
            if dt1['status'] is True:
                dt1_norm = json_normalize(dt1,sep = '_')
                #print(dt1)
                return dt1_norm[cin_contact_columns]
        
        
        
        
                    
        
async def main():
    async with aiohttp.ClientSession(headers = headers) as session:
        # STEP 1: Fetch all startup pages
        page_no = 0
        st_lst= []

        while True:
           page_data  = await startup(session,page_no) 
           if not page_data:
              break
           st_lst.extend(page_data)
           #print(page_data)

           page_no += 1
        df1 = pd.DataFrame(st_lst)[startup_columns]
        st_id = df1['id'].tolist()
        # st_id = [item['id']for item in st_list]
        
        
        # STEP 2: Fetch user profiles of cin
        cin_nos = [profile_cin(session,id) for id  in st_id if id]
        cin_data = await asyncio.gather(*cin_nos)
        df2 = pd.concat(cin_data,ignore_index=True)


        
        # STEP 3: Fetch service names
        #unq_ser_id = df2['startup_services'].explode().dropna().unique().tolist()  why this not working dont knnow to be checked

        unq_ser_sub_id = df2['startup_services'].tolist()
        lst3 = [item for sublist in unq_ser_sub_id for item in sublist if item]
        unq_ser_id = list(set(lst3))
 
    
        ser_id_name =  await service_name(session,unq_ser_id)
        clmsn = ['id','serviceName']
        serv_nm_df = pd.DataFrame(ser_id_name)[clmsn]

        
        # STEP 4: Map service names

        serv_exp_df = df2[['uniqueId','startup_services']].explode('startup_services')
        serv_name_df = serv_exp_df.merge(serv_nm_df,left_on ='startup_services', right_on = 'id', how = 'left')
        serve_name_final = serv_name_df.groupby('uniqueId')['serviceName'].agg(list).reset_index()
        # print(serve_name_final)


        # STEP 5: Fetch CIN bassed contact details

        cin_list = df2['startup_cin'].tolist()
        lst2  =  [cin_contact(session,cin) for cin in cin_list]
        cin_data_list = await asyncio.gather(*lst2)
        df3 = pd.concat(cin_data_list,ignore_index = True)
        #print(df3.columns)
        
        # =============================================================================
        # cin_data_df = pd.DataFrame(cin_data_list)
        # This tries to create a DataFrame from a list of smaller DataFrames.
        # If each item in cin_data_list is a single-row DataFrame, this will work.
        # If not, you might need to use pd.concat(cin_data_list, ignore_index=True) instead (which is safer and handles multiple rows per response).
        # =============================================================================

        df12merge = df1.merge(df2,left_on = 'id',right_on ='uniqueId' ,how = 'left')
        df123merge = df12merge.merge(df3,left_on = 'startup_cin',right_on = 'data_cin',how = 'left')
        dffinal = df123merge.merge(serve_name_final,left_on = 'uniqueId',right_on = 'uniqueId',how = 'left')
        dffinal.to_csv('startup_final.csv')
        
asyncio.run(main())
end_time = time.time()
total_seconds = end_time - start_time
hour = total_seconds // 3600 
remaing_seconds = total_seconds % 3600
minutes = remaing_seconds // 60
seconds =  remaing_seconds % 60
print(f'file successfully written as startup_final.csv and time_taken {hour:.2f} : {minutes:.2f} :{ seconds:.2f} ')


        