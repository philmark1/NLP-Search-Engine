# -*- coding: utf-8 -*-
"""
### **<span style='color:darkred'>SBWL Data Science <br/> Course: 5159 - Data Science Lab, Part 1</span>**

Company: **BDO**

Data Science Specialization - Institute for Data, Process and Knowledge Management

**Philipp Markopulos** (h12030674)

## **1. Data Processing**
The working environment I chose for the preprocessing was Google Colab as it offered the possibility to upload the already filtered data (10GB instead of 74GB) and further work with it in a shared space. That is quite useful as one does not have to download and the upload the notebook again and again if you are working on it. Furthermore, the sharing of the data went quite well with it.

For being able to access the data I used the "drive" package that links the shared folder with the data to the notebook.   
For further working with the data I switched to the WU Jupyterhub where the Jupyter lab folder was located
"""

# Upload the data from Google Drive
from google.colab import drive
drive.mount('/content/gdrive')

!ls "/content/gdrive/My Drive/DSLab-BDO"
!ls "/content/gdrive/My Drive/DSLab-BDO/Data_Filtered/AT0000606306"

filepath="https://drive.google.com/drive/folders/178zm2aKOoNMZx1FiJXlEVrT4Lkrpq-Zf?usp=sharing"

"""This code is a modified version of the code submitted by the BDO group last semester. We were allowed to use this code since our scope of work is beyond what the previous group did.
For our project I will focus on search engines as BDO wants a list of similar companies to some search terms and not the companies themselves. The endgoal is creating a GUI that allows to search through the companies and finds the 10 most similar companies. Therefore, I am using the code of the previous group for preprocessing only. In terms of taking a look whether the similar companies found by our search engines are to be found in the same cluster as well.
"""

# Install pdfplumber
!pip install pdfplumber

# Import packages
import pdfplumber
import csv
import os
import re
import time
import matplotlib

"""A function, that was partially taken from the previous group was created to clean the text files. The name is `clean_text(txt)`.
This function is supposed to:

- Gets rid of extra spaces and new lines next to each other.
- Gets rid of html pages (both starting with `html:` and `www.`.
- Gets rid of more than 1 dot in a row (it was often the case that durring the conversion of the contents page the dots between the name of the section and page number were taken as symbols).

We didn't want to remove too much in this stage as we will do more cleaning later on as well durring the stemming/lametization process.

And a function to convert the pdf to text using teh `pdfplumber` package. This function is called `pdf_text(filePath, num)` and takes the PDFs file path and a number (to use in its name) as it's inputs. Keep in mind this function is not perfect.
"""

def clean_text(txt):
    txt = txt.strip()
    txt = re.sub("\s+", " ", txt)
    txt = re.sub(r"http:\S+", "", txt)
    txt = re.sub(r"www.\S+", "", txt)
    txt = re.sub("[.][.]+", "", txt)
    # txt = re.sub(r'\([^\]]*\)', "", txt) I think this one is important
    return txt

def pdf_text(filePath, num):
    txt = ""
    try:
        with pdfplumber.open(filePath) as pdf:
            for page in pdf.pages:
                if page.extract_text():
                    txt = txt + " " + page.extract_text()
    except:
        txt = ""
        err_msg = "Error at: " + filePath
        print(err_msg)

    fP_split = filePath.split("/")[1:]
    txt_name = "/content/gdrive/My Drive/DSLab-BDO/Data_Clean/" + ("_".join(fP_split[6:8])) + "_" + str(num) + ".txt"
    
    txt = clean_text(txt)

    text_file = open(txt_name, "w")
    n = text_file.write(txt)
    text_file.close()
    
    #if num % 20 == 0: print(txt_name)

"""This function outputs the directory of every non-folder file in a given folder. This will be useful for looping over durring the pdf creation."""

def absoluteFilePaths(directory):
    all_files = []
    for root, dirs, files in os.walk(os.path.abspath(directory)):
        for file in files:
            if (file != './gdrive/My Drive/DSLab-BDO/Data_Clean') and not ("checkpoint" in file):
                all_files.append(os.path.join(root, file))
    return all_files
#absoluteFilePaths("./gdrive/My Drive/DSLab-BDO/Data_Filtered/AT0000606306")

all_firms = [x.split("/")[6] for x in absoluteFilePaths("./gdrive/My Drive/DSLab-BDO/Data_Filtered")]
all_firms = sorted(list(set(all_firms)))

cur_firms = [x.split("/")[5].replace(".txt", "") for x in absoluteFilePaths("./gdrive/My Drive/DSLab-BDO/Data_Clean")]
cur_firms = sorted(list(set(cur_firms)))
tbd = []
for i in all_firms:
    if i not in cur_firms:
        tbd.append(i)

#len(all_firms), len(cur_firms), tbd
len(all_firms), len(cur_firms), len(tbd)

import pandas as pd
import os
from pandas import ExcelWriter
from pandas import ExcelFile

#df_stock = pd.read_excel('/content/gdrive/My Drive/DSLab-BDO/companies.xlsx').drop(index = [601, 0]).reset_index()
#df_stock = pd.read_excel('/content/gdrive/My Drive/DSLab-BDO/companies.xlsx').drop(index = [594, 0]).reset_index()
df_stock = pd.read_excel('./data/companies.xlsx').drop(index = [594, 0]).reset_index()
df_stock = df_stock.drop(columns = "index")
df_stock

new_name = [re.sub("/", "", i) for i in df_stock.Name]
temp_dict = dict(zip(df_stock.ISIN, new_name))

"""### Extraction - .pdf to .txt conversion:

The next step finaly combines all that we have construced so far.  This function which we loop over to convert all 592 companies' reports and save their pdf's to .txt files.
"""

def FirmFolderName(directory):
    all_firms = []
    for root, dirs, files in os.walk(os.path.abspath(directory)):
        split_root = root.split("/")
        if len(split_root) == 7:
            all_firms.append(split_root[6])
    return sorted(all_firms)

"""- `comp_pdf_to_text` was made to convert each .pdf to its own .txt file.
- `all_to_one` was made to aggregate all of the .txt files for each company to 1 .txt file.
"""

all_firm_dir = FirmFolderName("/content/gdrive/My Drive/DSLab-BDO/Data_Filtered/")
n = 0
for i in all_firm_dir:
    n = n + len(absoluteFilePaths("/content/gdrive/My Drive/DSLab-BDO/Data_Filtered/" + i))
len(all_firm_dir), n

"""There are 6812 files in total spread across 592 companies."""

def comp_pdf_to_text(company_Paths, n):
    company_filePath = "/content/gdrive/My Drive/DSLab-BDO/Data_Filtered/" + company_Paths[n]
    company_pdf_filePath = absoluteFilePaths(company_filePath)
    for num, i in enumerate(company_pdf_filePath):
        pdf_text(i, num)

comp_pdf_to_text(all_firm_dir, 0)

def all_to_one(company_Paths, n):
    dc = "/content/gdrive/My Drive/DSLab-BDO/Data_Filtered/" + company_Paths[n] + ".txt"
    temp_clean_path = absoluteFilePaths("/content/gdrive/My Drive/DSLab-BDO/Data_Clean/")

    with open(dc, 'w') as outfile:
        for name in temp_clean_path:
            if len(name.split("_")) != 1:
                with open(name) as infile:
                    outfile.write(infile.read())
                outfile.write("\n")

                os.remove(name)

#all_to_one(all_firm_dir, 0)

#for num, i in enumerate(temp_pdf_path):
#    temp = i.split("/")[6:8]
#    del_temp = "./Data Clean/" + "_".join(temp) + "_" + str(num) + ".txt"
#    #os.remove(del_temp)
#    break

def comp_pdf_to_text(company_Paths, n):
    company_filePath = "/content/gdrive/My Drive/DSLab-BDO/Data_Filtered/" + company_Paths[n]
    company_pdf_filePath = absoluteFilePaths(company_filePath)
    for num, i in enumerate(company_pdf_filePath):
        pdf_text(i, num)

outp= comp_pdf_to_text(all_firm_dir, 0)
#print(outp)

"""After being able to reduce the amount of files to the most recent ones per companies we were quite happy with the outcome as there are only 6812 files left which leaves us with a decent amount of data (around 7GB) to work with. That is now done by extracting the pdf files from the folder and convert them into text files. Furthermore, all the files of one company are going to be merged into this text file in order to be left with 592 text files of which every file holds the information of the pdfs.

The below loop would run through it all but we decided to keep the method the previous group introduced and split the data up in order to have a smaller amount of data to work with at once . All of the relevant company file paths are storedd in "vals_holder" and were split up into "digestable" pieces. Then they were extracted one after the other to end up with the plain textfiles.
This took us around two days but the alogrithm run through quite smoothly. In the mean time we focused on research for the search engine approach we chose to implement.
"""

n = 0
m = 1
all_n = 0
vals_holder = ""
for i in all_firm_dir:
    temp_len = len(absoluteFilePaths("/content/gdrive/My Drive/DSLab-BDO/Data_Filtered/" + i))
    n = n + temp_len
    all_n = all_n + temp_len
    
    if n <= 1578:
        vals_holder = vals_holder + i + "_"
    
    else:
        vals_holder = vals_holder + "/"
        n = temp_len
        m = m + 1
        vals_holder = vals_holder + i + "_"

vals_holder = [x.rstrip("_").split("_") for x in vals_holder.split("/")]

list_folder_paths = vals_holder[4]

list_folder_paths= vals_holder[2][162:166]

#@title
for i in range(len(list_folder_paths)):
    #break
    try:
        comp_pdf_to_text(list_folder_paths, i)
        #time.sleep(2)
        all_to_one(list_folder_paths, i)
        #time.sleep(2)
        print("Firm", list_folder_paths[i], "is done.", str(i+1) + "/" + str(len(list_folder_paths)))
    except:
        print("Something went wrong :(")

"""This code has essentially moved us from having the filings and reports of all 592 companies in the form of pdfs, into having 592 individual text files for each company holding the contents of all of the afore mentioned filings and reports.

That being done, I still have to clean up each of the text file. The cleaning is done in five steps.

1) **Removal of Stopwords** - These are all the words that are commonly used and have no value to them in terms of context. for example: he/she/it, by, for etc. 

2) **Tokenization** - In this step, one splits each sentence into its composite words, making them lower case and getting rid of the most common words in the english language, contained within "stopwords".

3) **Lemmatization** - This changes words into their most basic form based on the context they appear in, reducing the total number of unique words ("is" -> "be", "was" -> "be").

4) **Stemming** - This changes words into their smallest form possible, also reducing total number of unique words ("Population" -> "popul", "populated" -> "popul").
I am not quite sure if the stemming is useful as the inserted words when searching for matches would have to be stemmed after the insertion as well in order to find companies most similar to the searchwords. To tackle this problem I will simply do two attemps in the preprocessing and compare the output. The first run will not use the stemming of the words-neither in the text nor in the search insertion. The second run will then include the stemming in both the processes. Afterwards I am going to compare the outcome in order to find out which one works better or if there is no difference. In the latter case the stemming is going to be included as it reduces the amount of data and therefore makes the algorithm faster. I later decided to preprocess the entered queries, as the results were best when having them in the same form as the data in the files that are searched. 

5) **Final filtering** to get rid of any words with numbers in them, or which are longer than 20 leters (3sd for english lamnguage in terms of word lenght) as these are likely miss translations
 from the pdf conversion part.

### Corpus Clean Up (Tokenization/ removal of stopwords / Lemmatization / Stemming):
"""

# Install sklearn and seaborn 
!pip install sklearn
!pip install seaborn

# Import packages
import sklearn
import string
import nltk
nltk.download('stopwords')
nltk.download('words')
nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')
nltk.download('wordnet')
from nltk.stem import WordNetLemmatizer 
from nltk.corpus import wordnet
from nltk.stem import PorterStemmer 
from nltk.corpus import stopwords
from nltk.corpus import shakespeare

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pandas as pd

import pickle
import time
import datetime

lemmatizer = WordNetLemmatizer()
ps = PorterStemmer() 
en_stopwords= stopwords.words('english')
words = set(nltk.corpus.words.words())

def get_wordnet_pos(token):
    #"""Helper function: map POS tag to first character lemmatize() accepts
   # Taken from: https://www.machinelearningplus.com/nlp/lemmatization-examples-python/
   # """
    tag = nltk.pos_tag([token])[0][1][0].upper() #extract the information
    tag_dict = {"J": wordnet.ADJ, #map
                "N": wordnet.NOUN,
                "V": wordnet.VERB,
                "R": wordnet.ADV}
    return tag_dict.get(tag, wordnet.NOUN) #guess noun if unknown

def processing_pipeline(text):
    '''takes a sentence and returns a lowercased, lemmatized, and stemmed list of tokens'''
    tokens=nltk.word_tokenize(text)
    lemmatized_tokens=[lemmatizer.lemmatize(token, get_wordnet_pos(token)) for token in tokens]
    stemmed_tokens =[ps.stem(token) for token in lemmatized_tokens]
    processed_tokens = [token for token in stemmed_tokens if token not in en_stopwords]
    processed_tokens = [token for token in processed_tokens if token not in string.punctuation]
    
    return processed_tokens

"""The preprocessing took around 1 1/2 hours and was done in one. afterwards the finished data is stored in a pickle file called all_token.pkl on our shared drive. This is all loaded and saved into one list, which can finaly be used for the NLP analysis.
This gave me the possibility to always access the preprocessed data and not having to run the code every time. 

"""

current_comps = sorted(absoluteFilePaths("/content/gdrive/My Drive/DSLab-BDO/Data_Clean/"))

#current_comps = sorted(absoluteFilePaths("/content/gdrive/My Drive/DSLab-BDO/Data_Clean/"))[0:100]
corpus = []

def read_text_file(file_path):
    with open(file_path, 'r') as f:
        return f.read()

for file in current_comps:
    temp_read = read_text_file(file)
    temp_read = re.sub(r'\([^\]]*\)', "", temp_read)
    temp_read = re.sub(r'\([^\]]*\)', "", temp_read)
    corpus.append(temp_read)

#a = [x.split("/")[len(x.split("/"))-1].split(".")[0] for x in current_comps]

def token_filter(tokens):
    filtered_tokens = []
    for token in tokens:
        if re.search('[a-zA-Z]', token) and len(token) < 20:
            if not re.search('\d', token) and "/" not in token and "-" not in token:
                filtered_tokens.append(token)
    return filtered_tokens

t_time = 0
for num, text in enumerate(corpus):
    t0 = time.perf_counter()
    
    play_tokens = processing_pipeline(corpus[num])
    play_tokens = token_filter(play_tokens)
    corpus[num] = ' '.join(play_tokens)
    
    t1 = round((time.perf_counter() - t0), 0)
    t_time = t_time + t1
    
    print("Corpus" , str(num+1) + "/" + str(len(corpus)), "done. \t Time taken:", datetime.timedelta(seconds = t1))

#print("Total time taken:", datetime.timedelta(seconds = t_time))

tokenpath="/content/gdrive/MyDrive/DSLab-BDO/data_drop/"

with open(tokenpath + 'all_token.pkl', 'wb') as f:
    pickle.dump(corpus, f)

with open(tokenpath + 'all_token.pkl', 'rb') as f:
    corpus = pickle.load(f)

"""In the pickle file all_token.pkl the preprocessed datafiles are stored. However we are preprocessing the data again and only remove stopwords to compare the outcome and leave the words as they are. This gives us exact matches however the number of matches might be smaller."""

from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
 
stopwords = set(stopwords.words('english'))

def remove_stopwords (text):
    #takes a file and removes stopwords
    tokens=nltk.word_tokenize(text)

    ready_token=  [token for token in tokens if token not in stopwords]
    
    return ready_token

corpus_1 = []

def read_text_file(file_path):
    with open(file_path, 'r') as f:
        return f.read()

for file in current_comps:
    temp_read = read_text_file(file)
    temp_read = re.sub(r'\([^\]]*\)', "", temp_read)
    temp_read = re.sub(r'\([^\]]*\)', "", temp_read)
    corpus_1.append(temp_read)

t_time = 0
for num, text in enumerate(corpus_1):
    t0 = time.perf_counter()
    
    play_token1 = remove_stopwords(corpus_1[num])
    corpus_1[num] = ' '.join(play_token1)
    
    t1 = round((time.perf_counter() - t0), 0)
    t_time = t_time + t1
    
   #print("Corpus_1" , str(num+1) + "/" + str(len(corpus_1)), "done. \t Time taken:", datetime.timedelta(seconds = t1))

#print("Total time taken:", datetime.timedelta(seconds = t_time))

tokenpath1="/content/gdrive/MyDrive/DSLab-BDO/data_drop/"

with open(tokenpath1 + 'token_stopwords.pkl', 'wb') as f:
    pickle.dump(corpus_1, f)

with open(tokenpath1 + 'token_stopwords.pkl', 'rb') as f:
    corpus_1 = pickle.load(f)

"""## What did I achieve so far?
- I converted the .pdf files into .txt files and cleaned them.
- Afterward, I stored them in the folder data_clean on Google Colab.
- For the transfer of the data into the WU Jupyter Lab, I downloaded the .txt files and uploaded them again in the Jupyter environment.
"""
