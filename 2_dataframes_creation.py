# -*- coding: utf-8 -*-
"""
### **<span style='color:darkred'>SBWL Data Science <br/> Course: 5159 - Data Science Lab, Part 2</span>**

Company: **BDO**

Data Science Specialization - Institute for Data, Process and Knowledge Management

**Philipp Markopulos** (h12030674)

## **2. Dataframes Creation**
At this moment, we are having the preprocessed data, stored in several pickle files. One is the all_tokens.pkl which holds the fully preprocessed text and the ISIN for it.  The other one is called token_stopwords.pkl and holds the same data with the difference that the text was not preprocessed- only stopwords have been removed. I am thinking of using the one where only stopwords are being used to compare the output of the search engines and maybe find out how the output changes and the scores change if the text is not preprocessed.   
However, I ended up not using that file as it seemed more interesting, not compare the outputs of preprocessed data and not preprocessed data, but compare different search engines with one another (this was discussed with the project coach). Additionally, I preprocessed the queries as well and, therefore, the only dataframe used was in the end all_data.     
So what is done in this notebook:  
- To the all_token.pkl file I added the company name according to the ISIN and stored the data as a dataframe that is called all_data. 
- At first, all_data is the dataframe holding the information relevant for the search engines, which is the text of the preprocessed PDFs and the company name.
- On this dataframe, all the search engines are working and giving relevant output for the queries.
- As BDO was asking for an additional information, I added another column to the dataframe called "Description".
- "Description" holds, as the name already explains itself, the description of the companies. 
- For the data retrieval I used the "wikipedia" package for python.
- This data was first stored into a dataframe called "data_and_description".
- Unfortunately, not all the companies where found by wikipedia so I could not retrieve 92 descriptions.
- Some of them because there was no wikipedia page in english and some because there was no page at all. 
- For the search engine, I use up-to-date data which is directly retrieved via the wikipedia package after the search.
- The last step was adding the beta values give to us by BDO to the dataframe and thus, adding another column.
- The resulting dataframe is now holding all the relevant information.
"""

# Import all the relevant packages
import numpy as np
import pickle
import os
import re
import pandas as pd
from pandas import ExcelWriter
from pandas import ExcelFile
import wikipedia

"""### Create a DF where all the data is stored with the right Company name and ISIN
Below, all the company ISIN numbers where extracted from the files available and stored into a list "editFiles".
"""

dirListing = os.listdir("./data/files/")
editFiles = []
  
for item in dirListing:
    if ".txt" in item:
        editFiles.append(item)
editFiles.sort()

"""As the editFiles list included the .txt, the next code cell is supposed to delete the last 4 characters so that only the ISIN is left. We simply call the data frame created and the column name is changed to ISIN."""

file_names= pd.DataFrame(editFiles)
file_names.columns=["ISIN"]
file_names= file_names["ISIN"].str[:-4]

file_names=pd.DataFrame(file_names)
file_names
# print out the date frame just to check

"""install the module needed for opening the excel file

"""

#df_stock = pd.read_excel('/content/gdrive/My Drive/DSLab-BDO/companies.xlsx').drop(index = [601, 0]).reset_index()
df_stock = pd.read_excel('./data/companies.xlsx').drop(index = [594, 0]).reset_index()
df_stock = df_stock.drop(columns = "index")
df_stock

"""A new excel sheet was created with only name and ISIN number to name the companies accordingly, when uploading it the dataframes last rows where empty, therefore, deleted."""

beta_values = df_stock[["ISIN","Beta Value"]]

#df_stock = pd.read_excel('/content/gdrive/My Drive/DSLab-BDO/companies.xlsx').drop(index = [601, 0]).reset_index()
names_ISIN = pd.read_excel('./data/names_ISIN_companies.xlsx')
names_ISIN= names_ISIN.drop(names_ISIN.index[598:601])
names_ISIN
# the file held more rows than companies and, therefore, the last rows were deleted

merger=pd.merge(names_ISIN, file_names, on='ISIN', how="right")
merger
# merge the dataframes with our file names and the excel sheet where the ISIN and the companienames are in

merger.loc[merger.isnull().any(axis=1)]
# after running the rest of the code, there were no missing values anymore
# the dataframe containing all the data is now called all_data and is stored as a pickle file in the pickle folder

"""After merging the two dataframes, you can see that for the given ISIN there is no name in the excel companies sheet - therefore we searched the file for the name.   
203 : Nordea Bank Abp   
278 : Dassault Aviation     
Now the names are inserted into the DF in order to being able to attach the df we build to the data frame with all the text files.
"""

merger.at[203, 'Name'] = 'Nordea Bank Abp'

merger.at[278, "Name"] ="Dassault Aviation"

# rename the missing company names at the correct position according to the information in their annual reports

dfcorpus = pd.DataFrame(corpus)

# dfcorpus.columns=["Text"]  # note: commented out by Sara, reason: the column already seems to be named "Text"
dfcorpus = dfcorpus["Text"].str.lower()

# rename the column with the text in it - it is now called Text

dfcorpus.head()

all_data=pd.merge(merger, dfcorpus, left_index=True, right_index=True)
# finally, we merge all the dataframes by index and create the dataframe with all the data to store it and being able to reuse it as easily as possible

all_data
# as you can see, we got a nice and clean dataframe with three columns: Name, ISIN and Text

#all_data.to_pickle("./data/pickle/all_data.pkl")

# you only need to run it once as the file is just overwriting itself every time you run it, the file does not change anymore and should not change anymore anyways
# store the dataframe into the pickle folder that all of us can easily open it and use it for the search engines
# also the filepath doesn't change after merging the notebooks
# when the risk analysis is relevant another column is going to be added to this dataframe which is going to be the beta value or what ever risk measure we choose
# that value is going to be added and I am going to use an API for getting the data from the internet

"""As the company would like to have additional information about the companies, I am trying to import those with the wikipedia API and add the information either in the dataframe, or whenever the output of the 10 most similar companies is printed only for those.  
Using the latter approach would keep the information updated at all times as the data extraction of Wikipedia would happen only if there was a search done and only for those 10 companies that are most similar.   
If that is easily implemented in the notebook it would be the prefered approach in order to secure the information being up to date.
If not, we will add the information to the dataframe and would only have the information from the date when we extracted it using the API. However, as the Wikipedia API is for free it should be no problem to access it after the search has been conducted.
"""

all_data["Name"]= all_data['Name'].map(lambda x: x.rstrip('(publ)'))

all_data
# in order to make it easier for Wikipedia we extract the "(publ)" from the "Name" column as Wikipedia seems to stumble over that information and can't work with it
# afterward, it worked a lot smoother

name_list = all_data['Name'].tolist()
# in order to easily loop through the company names the column "Name" was transformed into a list

descriptions=[]

for i in name_list:
    try:
        descriptions.append(wikipedia.summary(i), sentences=1)
    except:
        descriptions.append("NaN")
        print(i)
# we are looping through the list and extract all the information on the companies wikipedia could give us, if there is a company without information the list should hold the value "NaN"
# below, the companies from which we did not get a description are listed

descriptions.count("NaN")

"""We can see that Wikipedia was not able to find the description of 92 companies. We could either add company descriptions by hand or if BDO has some descriptions we just extract the information from an excel sheet if possible. Otherwise maybe some Google API for the search but we are not sure if the desciptions are of use.
Most of these companies are in the wrong language and the information does not exist in English. When trying to search them on Wikipedia by hand it was not possible either. On the other hand, we got 500 descriptions which is quite good.
"""

description=pd.DataFrame(descriptions)
description.columns=["Description"]
# we turn the list into a dataframe and call the column created "Description", that represents the wikipedia summaries of the companies

data_and_description=pd.merge(all_data, description, left_index=True, right_index=True)
data_and_description
# at last, the dataframes where merged and named "data_and_description"
# as you can see, we got a nice dataframe with 4 columns that hold the information we want so far
# for adding information on any stock values, we are planning on either using the information that BDO gave us or, also for having relevant up-to-date data, use a stock API

data_and_description.to_pickle("./data/pickle/all_data.pkl")
# store the dataframe with the company descriptions as a pickle file, we call it data_and_description

#data_and_description["Description"]= data_and_description['Description'].map(lambda x: x.rstrip('NaN'))

tokenpath="./data/pickle/"

with open(tokenpath + 'all_data.pkl', 'rb') as f:
    data_and_description = pickle.load(f)
#dataframe with the company name, ISIN and text

#data_and_description=pd.merge(data_and_description, beta_values, on='ISIN', how="right")

all_data=all_data.astype({"Text": str}, errors='raise')
all_data
# in the final dataframe, the text column was not a string type and, therefore, it was changed
