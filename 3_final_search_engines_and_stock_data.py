# -*- coding: utf-8 -*-
"""
### **<span style='color:darkred'>SBWL Data Science <br/> Course: 5159 - Data Science Lab, Part 3</span>**

Company: **BDO**

Data Science Specialization - Institute for Data, Process and Knowledge Management

**Philipp Markopulos** (h12030674)

## **3. Search Engines**
"""

# Install all the relevant libraries/packages
!pip install sklearn
!pip install nltk
!pip install whoosh
!pip install openpyxl
!pip install wikipedia
!pip install yfinance

# Import all the relevant libraries/packages
import sklearn
import string
import nltk
import numpy as np
import pickle
import csv
import os
import re
import pandas as pd
import wikipedia
import yfinance as yf

from pandas import ExcelFile, ExcelWriter

from whoosh import index, scoring
from whoosh.analysis import StandardAnalyzer
from whoosh.fields import Schema, TEXT, ID
from whoosh.qparser import QueryParser

from numpy.linalg import norm
from nltk import pos_tag
from collections import defaultdict

from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from nltk.corpus import shakespeare, stopwords
from nltk.corpus import wordnet #as net
from nltk.stem import LancasterStemmer, PorterStemmer, WordNetLemmatizer 
from nltk.tokenize import sent_tokenize, word_tokenize

"""As, said in the last notebook, I am planning to implement three different search enginges and compare the outcome. The first one is going to be a very simple one, just using the relative number of each term in a document. The second is going to use a term-document matrix with TF-IDF weighting. For that matter I am going to convert the data I have into a matrix and later use cosine similarity to calculate the similarities between the queries and the documents.  The third one will be the BM25 search engine which will be implemented using the Whoosh library. The output is going to be the 10-15 most similar companies (companies with the highest similarity)."""

# Create a path called "tokenpath" leading to the folder that holds all the pickled files - with that path all the needed files can be opened
tokenpath="./data/pickle/"

"""The all_data.pkl is a full dataframe with all the preprocessed text files of the companies. Additionally, I added the company names and their Wikipedia description if it was found on Wikipedia. If not, I added a NaN to the df and asked whether there is  additional information on the companies."""

with open(tokenpath + 'all_data.pkl', 'rb') as f:
    all_data = pickle.load(f)
#dataframe with the company name, ISIN and text

# with open(tokenpath + 'all_token.pkl', 'rb') as f:
#     corpus = pickle.load(f)
# #pickle file with all the preprocessed extracted txt. files

# with open(tokenpath + 'token_stopwords.pkl', 'rb') as f:
#     corpus1 = pickle.load(f)
# #pickle file with all the extracted text but only stopwords removed

"""### 1. Simple search engine
We will start out with a very basic search engine, that weights the words in the document. So the absolute number of occurencies per word are divided by the number of words in total in order to compare the companies.   
There is no weighting included and you can only search for one word in the query.  
Comparing the results, when we enter only one word in the other engines as well the output is quite similar but one word is simply not sufficient for a proper search.
"""

from operator import itemgetter

#query = input("Enter your search word: ")
query= "paper"

#convert the pickle file to a pandas_dataframe
unpickled_data = pd.read_pickle(tokenpath + 'all_data.pkl')
with open(tokenpath + 'all_data.pkl', 'rb') as f:
#read content of file to string
    data = pickle.load(f)
    
#find the matches, where the text equals the input word in each document
matches = (unpickled_data['Text'].str.count(query))

#count words per document
count = (unpickled_data['Text'].apply(len))

#a list of all relative word weightings per company
list_of_matches = matches/count

#get all the company names from the pd. dataframe as list
list_of_single_column = unpickled_data['Name'].tolist()

#merge the elements of the two lists, company names and relative word weighting
unsorted_list = (list(zip(list_of_single_column,list_of_matches)))

#print(unsorted_list)
#sort the list elements after relative weighting
sorted_values = sorted(unsorted_list, key = itemgetter(1))

#get the 10 top elements 
final = (sorted_values[-10:])

#bring them into descending order after highest relative weighting
d=0
y = 0
x = len(final)-1
while y < x:
    final[y],final[x]=final[x],final[y]
    y+=1
    x-=1
for i in final:
    d+=1
    print("The top", d, "matching company is:", i)

"""### 2. TF-IDF

TF-IDF is a keyword analysis system used by search engines to determine the importance and relevancy of a keyword for a given web page. It takes into account two factors: term frequency and inverse document frequency. By evaluating these factors, search engines can assign keyword scores to pages. The more important and relevant a keyword is to a page, the higher its TF-IDF score will be.
"""

vectorizer = TfidfVectorizer(max_df=0.8, 
                             max_features=20000, 
                             min_df=0.1, 
                             lowercase = True)
X = vectorizer.fit_transform(all_data["Text"])

"""- max_features, are the different words, we set maximum at 20.000 but you can choose whatever you want.
- max_df means ignore the words that occur in more than 90% of the documents, we chose 90% as the stopwords have already been removed and therefore there shouldn't be too irrelevant words left
- min_df means ignore the words that occure in less than 5% of the documents- default would be 0. but the lower this is the more features are there so you have to play around a little
- the output however, is better when the min and max are set.
- the number of features increases with the size of the span, so 0.05-0.9 has more features in it than a smaller range.   
- The goal of MIN_DF is to ignore words that have very few occurrences to be considered meaningful. For example, in your text you may have names of people that may appear in only 1 or two documents. In some applications, this may qualify as noise and could be eliminated from further analysis. Similarly, you can ignore words that are too common with MAX_DF.
"""

print(X.toarray())
print(X.shape)
features = vectorizer.get_feature_names_out()
similarities = cosine_similarity(X)
dist = 1 - similarities
# take a look at the matrix produced
len(features)

X=X.T.toarray()
df=pd.DataFrame(X)
#turn the matrix into a dataframe for the search engine

def get_similar_companies(q, df):
    #print("query:", q)
  # Convert the query become a vector
    q = [q]
    q_vec = vectorizer.transform(q).toarray().reshape(df.shape[0],)
    sim = {}
  # Calculate the similarity
    for i in range (592):
        sim[i] = np.dot(df.loc[:, i].values, q_vec) / np.linalg.norm(df.loc[:, i]) * np.linalg.norm(q_vec)
  
  # Sort the values 
    sim_sorted = sorted(sim.items(), key=lambda x: x[1], reverse=True)
    sim_sorted= sim_sorted[:10]
  # Print the articles and their similarity values
    for k, v in sim_sorted:
        if v >= 0.0:
           
            print(all_data["Name"].iloc[k])
            print("similarity:", v)
            print()
        try:
            print(wikipedia.summary(all_data["Name"].iloc[k], sentences=1))
            print()
        except:
            print("description was not found on wikipedia")
            
# search function, calculates the similarity between the query and the dataframe and outputs the 10 companies with the highest similarity accordingly

#lancaster=LancasterStemmer()
porter= PorterStemmer()
def stem_query(query):
    token_words=word_tokenize(query)
    token_words
    stem_query=[]
    for word in token_words:
        stem_query.append(porter.stem(word))
        #stem_query.append(lancaster.stem(word))
        stem_query.append(" ")
    return "".join(stem_query)

# source: https://www.datacamp.com/tutorial/stemming-lemmatization-python create a function using either the lancaster or porter stemmer, lancaster stems a lot more from the word
# therefore, we suggest sticking to the porter stemmer as the word is not modified to much and the search engine works better with it
# when using the lancaster, the output is mostly not usable anymore
# porter works perfectly fine though

q= "pulp paper"
q=stem_query(q)
print(q)

result=get_similar_companies(q,df)
result

"""#### Another attempt at the TF IDF search engine (including bigrams)
The df used is again the all_data one which is already fully preprocessed and has the names of companies, the ISIN and the beta value. The beta value, however, was the one that was given me by BDO is certainly out-dated as stock prices can fluctuate everytime a new trade is settled.
(https://medium.com/@kartheek_akella/implementing-the-tf-idf-search-engine-5e9a42b1d30b)
"""

with open(tokenpath + 'all_data.pkl', 'rb') as f:
    corpus = pickle.load(f)

#corpus=corpus.astype({"Text": str}, errors='raise')
#corpus

vector=TfidfVectorizer(max_df=0.8, 
                             max_features=20000, 
                             min_df=0.1,
                             ngram_range=(1,2),
                             lowercase = True)
# apparently, it makes a huge difference in output when you change the vectorizer
# the "normal" one without any specification performs worse than the one that is defined
# additionally, the ngram_range was added to implement a try-out version of the feedback we got after the final presentation

Y = vector.fit_transform(corpus['Text'])

query = "paper pulp sweden"
query= stem_query(query)
query_vec = vector.transform([query])

results = cosine_similarity(Y,query_vec)

results=pd.DataFrame(results)
results.columns=["similarity"]

df1=pd.merge(results, corpus, left_index=True, right_index=True)
df1=df1.sort_values(by='similarity', ascending=False)

result = df1.head(10)
print("10 most similar companies are:")
print (result.iloc[:10,[0,1,4,5]])

"""As vectorizing was done already, one can simply use the data from above using the TF-IDS vectorizer again for the attempt, doing this to compare the outcome of the search engines using the same method.

### 3. BM25
The last search engine I used, is the BM25 (best matches) scoring. "It is a collection of algorithms for querying a set of documents and returning the ones most relevant to the query" (https://pypi.org/project/rank-bm25/).
I used Whoosh library to implement this search engine. With Whoosh, it is also possible to use the TF-IDF weighting, but surprisingly, the output is more similiar to the one above when using BM25.
"""

# Create a schema
# Source: https://whoosh.readthedocs.io/en/latest/schema.html
search_schema = Schema(
    Name=TEXT(stored=True),
    ISIN=ID(stored=True),
    Text=TEXT(analyzer=StandardAnalyzer())
)

def add_dataframe_to_index(df, index):
    # Add the file to the index
    writer = index.writer()
    for _, doc in df.iterrows():
        writer.add_document(
            Name=str(doc.Name),
            ISIN=str(doc.ISIN),
            Text=str(doc.Text)
        )
    writer.commit()

# Load the file with company names and details
tokenpath="./data/pickle/"

with open(tokenpath + 'all_data.pkl', 'rb') as f:
    all_data = pickle.load(f)

# Create index
# Source: https://whoosh.readthedocs.io/en/latest/indexing.html
def create_search_index(search_schema):
    if not os.path.exists('index'):
        # Create a new index
        os.mkdir('index')
        ix = index.create_in('index', schema=search_schema)
        add_dataframe_to_index(all_data, ix)
    else:           
        # Load an existing index
        ix = index.open_dir('index', schema=search_schema)
    return ix

ix = create_search_index(search_schema)

query_str = 'bank europe'
match_limit = 10

porter=PorterStemmer()
def stem_query(query):
    token_words=word_tokenize(query)
    token_words
    stem_query=[]
    for word in token_words:
        stem_query.append(porter.stem(word))
        stem_query.append(" ")
    return "".join(stem_query)

query_str=stem_query(query_str)
query_str

# Set a custom B value for the "content" field
w = scoring.BM25F
#w = scoring.TF_IDF
#w = scoring.Frequency

# Search
# Source: https://whoosh.readthedocs.io/en/latest/searching.html
#with ix.searcher(weighting=scoring.Frequency) as searcher:
with ix.searcher(weighting=w) as searcher:
    query = QueryParser("Text", ix.schema).parse(query_str)
    results = searcher.search(query, limit=match_limit)

    # print(results)
    if not results:
        print('No matches')
    
    for result in results:
        print(result.score)
        print(result['Name'])

#all_data.set_index('Name', drop=True, inplace=True)

"""## **Stock Data API**"""

# the list of top-10 stocks we like to search, enter e.g. each stock into the list
mystocks = ['STAN.L']
# empty array we will later return
stockdata = []

def getData(symbol):
    #get info for the ticker (summary)
    ticker = yf.Ticker(symbol)
    stock_info = ticker.info
    # from summary we take the ticker
    # if you only want the beta value you can delete 'longBusinessSummary' or 'marketCap' (vice versa)
    stock = {stock_info['marketCap'], stock_info['longBusinessSummary'], stock_info['beta']}
    return stock

# for each item in our list we apply the function getData
for item in mystocks:
        stockdata.append(getData(item))
        print('Getting: ', item)
        
print(stockdata)
