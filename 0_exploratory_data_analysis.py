# -*- coding: utf-8 -*-
"""
# First Exploratory Analysis of BDO Dataset

Exploratory Analysis of Dataset: Generating Portfolios Based on Company Filings and Clustering Shared Risks

Project Partner: BDO

By Philipp Markopulos (h12030674@s.wu.ac.at)

# Loading in files and initial clean up:

Import some key packages:
"""

#!pip install pdfplumber
import pdfplumber
import csv
import os
import re
import time
import matplotlib

"""Lets set up a function that cleans the text: `clean_text(txt)` which:

- Gets rid of extra spaces and new lines next to each other.
- Gets rid of html pages (both starting with `html:` and `www.`.
- Gets rid of more than 1 dot in a row (it was often the case that durring the conversion of the contents page the dots between the name of the section and page number were taken as symbols).

I didn't want to remove too much in this stage as I will need to do more cleaning later on as well durring the stemming/lametization process.

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
    txt_name = "/Users/PeterNovak/Desktop/DS Lab/Data Clean/" + ("_".join(fP_split[6:8])) + "_" + str(num) + ".txt"
    
    txt = clean_text(txt)

    text_file = open(txt_name, "w")
    n = text_file.write(txt)
    text_file.close()
    
    #if num % 20 == 0: print(txt_name)

#pdf_text("./Back Up/DS Lab_19102021/AT0000606306/Filings/Form_Interim_Report(Aug-09-2007) (90).pdf", 1)

"""Now lets make a function that ouputs the directory of every non-folder file in a given folder. This will be useful for looping over durring the pdf creation."""

def absoluteFilePaths(directory):
    all_files = []
    for root, dirs, files in os.walk(os.path.abspath(directory)):
        for file in files:
            if (file != '.DS_Store') and not ("checkpoint" in file):
                all_files.append(os.path.join(root, file))
    return all_files
#absoluteFilePaths("./DS Lab_19102021/AT0000606306")

all_firms = [x.split("/")[7] for x in absoluteFilePaths("./Back Up/DS Lab_19102021")]
all_firms = sorted(list(set(all_firms)))

cur_firms = [x.split("/")[6].replace(".txt", "") for x in absoluteFilePaths("./Data Clean")]
cur_firms = sorted(list(set(cur_firms)))
tbd = []
for i in all_firms:
    if i not in cur_firms:
        tbd.append(i)

len(all_firms), len(cur_firms), tbd

"""# Stock Name, Value & Financial Info Data Frame:"""

import pandas as pd
import os
from pandas import ExcelWriter
from pandas import ExcelFile

df_stock = pd.read_excel('./companies.xlsx', header = 6).drop(index = [601, 0]).reset_index()
df_stock = df_stock.drop(columns = "index")
df_stock

new_name = [re.sub("/", "", i) for i in df_stock.Name]

temp_dict = dict(zip(df_stock.ISIN, new_name))

"""# Extraction - .pdf to .txt conversion:

The next step finaly combines all that we have so far to construct a function which we loop over to convert all 597 companies' reports and save their pdf's to .txt files.
"""

def FirmFolderName(directory):
    all_firms = []
    for root, dirs, files in os.walk(os.path.abspath(directory)):
        split_root = root.split("/")
        if len(split_root) == 8:
            all_firms.append(split_root[7])
    return sorted(all_firms)

"""- `comp_pdf_to_text` was made to convert each .pdf to its own .txt file.
- `all_to_one` was made to aggregate all of the .txt files for each company to 1 .txt file.
"""

all_firm_dir = FirmFolderName("/Users/PeterNovak/Desktop/DS Lab/Back Up/DS Lab_19102021")
n = 0
for i in all_firm_dir:
    n = n + len(absoluteFilePaths("/Users/PeterNovak/Desktop/DS Lab/Back Up/DS Lab_19102021/" + i))
len(all_firm_dir), n

"""There are 48678 files in total spread across 597 companies."""

def comp_pdf_to_text(company_Paths, n):
    company_filePath = "./Back Up/DS Lab_19102021/" + company_Paths[n]
    comapny_pdf_filePath = absoluteFilePaths(company_filePath)
    for num, i in enumerate(comapny_pdf_filePath):
        pdf_text(i, num)

#comp_pdf_to_text(all_firm_dir, 0)

def all_to_one(company_Paths, n):
    dc = "./Data Clean/" + company_Paths[n] + ".txt"
    temp_clean_path = absoluteFilePaths("./Data Clean")

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

"""Took 21.82 second/file -> There are 48678 files -> This will take 295 hours = 12.30 days. In the end I only had time to run it overnight as it uses up 99% CPU on the virtual machine provided. It took rougly 2 weeks. 

The below loop would run through it all but I don't think its a good idea to run accidentaly. All of the relevant company file paths are stroed in "vals_holder" and were split into 3 parts for each one of us to run.
"""

n = 0
m = 1
all_n = 0
vals_holder = ""
for i in all_firm_dir:
    temp_len = len(absoluteFilePaths("/Users/PeterNovak/Desktop/DS Lab/Back Up/DS Lab_19102021/" + i))
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

list_folder_paths = vals_holder

for i in range(len(list_folder_paths)):
    break
    try:
        comp_pdf_to_text(list_folder_paths, i)
        time.sleep(2)
        all_to_one(list_folder_paths, i)
        time.sleep(2)
        print("Firm", list_folder_paths[i], "is done.", str(i+1) + "/" + str(len(list_folder_paths)))
    except:
        print("Something went wrong :(")

"""This code has essentially managed to get me from having the filings and reports of all 500+ companies in the form of pdfs, into having 500+ individual text files for each company holding the contents of all of the before mentioned filings and reports.

That being done, I still have to clean up each of the text file. The cleaning is done in four steps.

1) **Tokenization** - In this step I split each sentence into its composite words, making them lower case and getting rid of the most common words in the english langauge, contained within "stopwords".

2) **Lamentization** - This changes words into their most basic form based on the context they appear in, reducing the total number of unique words ("is" -> "be", "was" -> "be").

3) **Stemming** - This changes words into their smallest form possible, also reducing total number of unique words ("Population" -> "popul", "populated" -> "popul").

4) **Final filtering** to get rid of any words with numbers in them, or which are longer than 20 leters (3sd for english lamnguage in terms of word lenght) as these are likely miss transaltions from the pdf conversion part.

# Corpus Clean Up (Tokenization / Lamentization / Stemming):
"""

#!pip install sklearn
#!pip install seaborn
import sklearn
import string
import nltk
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
    """Helper function: map POS tag to first character lemmatize() accepts
    Taken from: https://www.machinelearningplus.com/nlp/lemmatization-examples-python/
    """
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

"""The data is split into 6 parts as each takes more than 2 hours to complete (this way if there is a crash, which often happened due to full RAM, we dont have to re run the whole code). And the processing procedure is applied to it and saved in a pickle as a list of corpuses. Where each entry in the list is all of the tokenized text in a given text file (so for a company)."""

current_comps_p1  = sorted(absoluteFilePaths("./Data Clean"))[0:100]
current_comps_p23 = sorted(absoluteFilePaths("./Data Clean"))[100:300]
current_comps_p4  = sorted(absoluteFilePaths("./Data Clean"))[300:400]
current_comps_p5  = sorted(absoluteFilePaths("./Data Clean"))[400:500]
current_comps_p6  = sorted(absoluteFilePaths("./Data Clean"))[500:600]
current_comps = sorted(absoluteFilePaths("./Data Clean"))[0:2]

#current_comps = sorted(absoluteFilePaths("./Data Clean"))[0:100]
corpus = []

def read_text_file(file_path):
    with open(file_path, 'r') as f:
        return f.read()

for file in current_comps:
    temp_read = read_text_file(file)
    temp_read = re.sub(r'\([^\]]*\)', "", temp_read)
    temp_read = re.sub(r'\([^\]]*\)', "", temp_read)
    corpus.append(temp_read)

# a = [x.split("/")[len(x.split("/"))-1].split(".")[0] for x in current_comps]

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
    
    print("Courpus" , str(num+1) + "/" + str(len(corpus)), "done. \t Time taken:", datetime.timedelta(seconds = t1))

print("Total time taken:", datetime.timedelta(seconds = t_time))

#with open('tokens_test_p1.pkl', 'wb') as f:
#    pickle.dump(corpus, f)
#with open('tokens_test_p23.pkl', 'wb') as f:
#    pickle.dump(corpus, f)
#with open('tokens_test_p4.pkl', 'wb') as f:
#    pickle.dump(corpus, f)
#with open('tokens_test_p5.pkl', 'wb') as f:
#    pickle.dump(corpus, f)
#with open('tokens_test_p6.pkl', 'wb') as f:
#    pickle.dump(corpus, f)

"""This is all loaded and saved into one list, which can finaly be used for our NLP analysis."""

#with open('tokens_test_p1.pkl', 'rb') as f:
#    corpus_p1 = pickle.load(f)
#with open('tokens_test_p23.pkl', 'rb') as f:
#    corpus_p23 = pickle.load(f)
#with open('tokens_test_p4.pkl', 'rb') as f:
#    corpus_p4 = pickle.load(f)
#with open('tokens_test_p5.pkl', 'rb') as f:
#    corpus_p5 = pickle.load(f)
#with open('tokens_test_p6.pkl', 'rb') as f:
#    corpus_p6 = pickle.load(f)

#corpus = []
#for i in corpus_p1:  corpus.append(i)
#for i in corpus_p23: corpus.append(i)
#for i in corpus_p4:  corpus.append(i)
#for i in corpus_p5:  corpus.append(i)
#for i in corpus_p6:  corpus.append(i)

#with open('tokens_test_all.pkl', 'wb') as f:
#    pickle.dump(corpus, f)

with open('tokens_test_all.pkl', 'rb') as f:
    corpus = pickle.load(f)

corpus[0][0:382]

"""# Conversion to Document-Term-Frequency Table:

In this step I convert the list of corpuses into a document term frequency matrix, and then into a cosine similarities matrix. Later on I also tried using euclidean distance as a metric.

### Talk about different distance measures. (Why these?)
"""

with open('tokens_test_all.pkl', 'rb') as f:
    corpus = pickle.load(f)

vectorizer = TfidfVectorizer(max_df=0.9, max_features=200000, min_df=0.1, lowercase = True)

t0 = time.perf_counter()
DTM = vectorizer.fit_transform(corpus)
t1 = round((time.perf_counter() - t0), 0)
print(len(corpus), "Corpus' took:", datetime.timedelta(seconds = t1))

print(DTM.toarray())
print(DTM.shape)
features = vectorizer.get_feature_names_out()
similarities = cosine_similarity(DTM)
dist = 1 - similarities

mask = np.triu(np.ones_like(similarities, dtype=bool))

# Set up the matplotlib figure
f, ax = plt.subplots(figsize=(32, 30))

labels = [i for i in range(0,len(similarities))]
cmap = sns.diverging_palette(230, 20, as_cmap=True)

plt.title("Pairwise Cosine Similarities Matrix", size=20)

# Draw the heatmap with the mask and correct aspect ratio
sns.heatmap(similarities, mask=mask, cmap=cmap, vmax=0.8, vmin=0.2,
            square=True, linewidths=1, cbar_kws={"shrink": 0.5},
            xticklabels=labels, yticklabels=labels)
plt.show()

"""As we can see, due to the very high number of varriables, the correlation matrix does not tell us much, in most cases the correlation is < 0.5 (blue). And correlation values > 0.5 (orange) are very sparse and in block like structures. Meaning that companies with similar names (thus also the same country code) tend to have similar word usage.

Intro to clustering:...

# K-means clustering

Intro to k-means...
"""

# Commented out IPython magic to ensure Python compatibility.
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
import matplotlib.pyplot as plt
import datetime
# %matplotlib inline

sum_of_squared_distances = []
silhouette_scores = []
K = range(2,30)

for k in K:
    km = KMeans(n_clusters=k, max_iter=200, n_init=10, random_state=0)
    km = km.fit(DTM)
    
    silhouette_scores.append(silhouette_score(DTM, km.labels_, metric='euclidean'))
    sum_of_squared_distances.append(km.inertia_)

fig, ax = plt.subplots(figsize = [10,7])
plt.title('Cluster Number Heuristics')
ax.plot(K, sum_of_squared_distances, 'gx-', alpha = 0.75)
ax.set_xlabel("k", fontsize=14)
ax.set_ylabel("Sum of Squared Distances", color="green", fontsize=14)
ax.vlines(21, min(sum_of_squared_distances), max(sum_of_squared_distances), "r", "--", alpha = 0.5)
ax.set_xticks(list(ax.get_xticks()) + [21])

ax2=ax.twinx()
ax2.plot(K, silhouette_scores, 'bx-', alpha = 0.75)
ax2.set_ylabel("Silhouette Score", color="blue", fontsize=14)
plt.show()

"""What does this tell us..."""

unique_countries = set([x.split("/")[6].replace(".txt", "")[0:2] for x in absoluteFilePaths("./Data Clean")])
len(unique_countries)

num_clusters = 21

km = KMeans(n_clusters = num_clusters, n_init=100, random_state=0)
km.fit(DTM)
clusters = km.labels_.tolist()

firm_id = [x.split("/")[len(x.split("/"))-1].split(".")[0] for x in sorted(absoluteFilePaths("./Data Clean"))]
films = {'firm': labels, 'id': firm_id, 'cluster': clusters}

frame = pd.DataFrame(films, index = [clusters] , columns = ['firm', 'id', 'cluster'])

"""What does frame hold..."""

n_top = 5
order_centroids = km.cluster_centers_.argsort()[:, ::-1]
for i in range(num_clusters):
    print("Cluster %d:" % i)
    
    print("Key words:", end='')
    for ind in order_centroids[i, :n_top]:
        print(' %s' % features[ind], end=',')
    print()
    
    print("Number of Firms:", len(frame.loc[i]['firm'].values.tolist()))
    
    temp_set_cc = []
    for cc in frame.loc[i]['id'].values.tolist():
        if cc[0:2] not in temp_set_cc:
            temp_set_cc.append(cc[0:2])
    print("Unique Markets:   Total: %s   " % len(temp_set_cc), end='')
    for cc in temp_set_cc:
        print(' %s,' % cc, end='')
    print("\n")

id_3 = []
for i in range(num_clusters):
    temp_l = []
    for ind in order_centroids[i, :3]:
        temp_l.append(features[ind])
    id_3.append(", ".join(temp_l))

"""What does this tell us...
Whats next...
"""

#for k in range(0,true_k):
#    s=result[result.cluster==k]
#    text=s['wiki'].str.cat(sep=' ')
#    text=text.lower()
#    text=' '.join([word for word in text.split()])
#    wordcloud = WordCloud(max_font_size=50, max_words=100, background_color="white").generate(text)
#    print('Cluster: {}'.format(k))
#    print('Titles')
#    titles=wiki_cl[wiki_cl.cluster==k]['title']         
#    print(titles.to_string(index=False))
#    plt.figure()
#    plt.imshow(wordcloud, interpolation="bilinear")
#    plt.axis("off")
#    plt.show()

for i in range(num_clusters):
    print("word cloud package not working currently for mac try on Jancis!")
    break

"""What does this tell us..."""

current_comps = sorted(absoluteFilePaths("./Data Clean"))

for i in range(num_clusters):
    print("Cluster %s Firms:" % i)
    for title in frame.loc[i]['firm'].values.tolist():
        title = " ".join(current_comps[title].split("/")[6:]).replace(".txt", "")
        try: print('%s, ' % temp_dict[title], end='')
        except: print("N/A", end='')
    print("\n")

"""What does this tell us..."""

for i in range(num_clusters):
    clust_country = [x[0:2] for x in frame.loc[i]['id'].values.tolist()]
    clust_country = pd.Series(clust_country).value_counts()
    (keys,values) = zip(*clust_country.items())
    plt.bar(keys, values)
    plt.title("Cluster %d: Countries" % i)
    plt.ylabel("Frequency")
    plt.show()

"""# Visualising Clusters Using Multidimentional Scaling:

MLPD3 and MATPLOTLIB
"""

#! pip install mpld3
import mpld3
import matplotlib as mpl

from sklearn.manifold import MDS

MDS()

# two components as we're plotting points in a two-dimensional plane
# "precomputed" because we provide a distance matrix
# we will also specify `random_state` so the plot is reproducible.
mds = MDS(n_components=2, dissimilarity="precomputed", random_state=1)

pos = mds.fit_transform(dist)  # shape (n_components, n_samples)

xs, ys = pos[:, 0], pos[:, 1]

#set up colors per clusters using a dict
kelly_colors_hex = [
    "#FFB300", # Vivid Yellow
    "#803E75", # Strong Purple
    "#FF6800", # Vivid Orange
    "#A6BDD7", # Very Light Blue
    "#C10020", # Vivid Red
    "#CEA262", # Grayish Yellow
    "#817066", # Medium Gray
    "#007D34", # Vivid Green
    "#F6768E", # Strong Purplish Pink
    "#00538A", # Strong Blue
    "#FF7A5C", # Strong Yellowish Pink
    "#53377A", # Strong Violet
    "#FF8E00", # Vivid Orange Yellow
    "#B32851", # Strong Purplish Red
    "#F4C800", # Vivid Greenish Yellow
    "#7F180D", # Strong Reddish Brown
    "#93AA00", # Vivid Yellowish Green
    "#593315", # Deep Yellowish Brown
    "#F13A13", # Vivid Reddish Orange
    "#232C16", # Dark Olive Green
    "#AAB300",
    "#F1EA13",
    "#AABBCC",
    "#ABCDEF",
    "#FEDCBA",
    "#CCCCCC"
    ]

#set up cluster names using a dict
cluster_names = {}
cluster_colors = {}
for i in range(num_clusters):
    cluster_names[i] = id_3[i]
    cluster_colors[i] = kelly_colors_hex[i]

#create data frame that has the result of the MDS plus the cluster numbers and titles
df = pd.DataFrame(dict(x=xs, y=ys, label=clusters, title=labels)) 

#group by cluster
groups = df.groupby('label')


# set up plot
fig, ax = plt.subplots(figsize=(22, 17)) # set size
ax.margins(0.05) # Optional, just adds 5% padding to the autoscaling

#iterate through groups to layer the plot
#note that I use the cluster_name and cluster_color dicts with the 'name' lookup to return the appropriate color/label
for name, group in groups:
    ax.plot(group.x, group.y, marker='o', linestyle='', ms=12, label=cluster_names[name],
            color=cluster_colors[name], mec='none')
    ax.set_aspect('auto')
    ax.tick_params(\
        axis= 'x',         # changes apply to the x-axis
        which='both',      # both major and minor ticks are affected
        bottom='off',      # ticks along the bottom edge are off
        top='off',         # ticks along the top edge are off
        labelbottom='off')
    ax.tick_params(\
        axis= 'y',         # changes apply to the y-axis
        which='both',      # both major and minor ticks are affected
        left='off',        # ticks along the bottom edge are off
        top='off',         # ticks along the top edge are off
        labelleft='off')

ax.legend(numpoints=1)  #show legend with only 1 point

#add label in x,y position with the label as the film title
for i in range(len(df)):
    ax.text(df.loc[i]['x'] + 0.002, df.loc[i]['y'] + 0.002, str(df.loc[i]['title']).replace(".0", ""), size=8)  

plt.show() #show the plot

#uncomment the below to save the plot if need be
#plt.savefig('clusters_small_noaxes.png', dpi=200)

plt.close()

"""What does this tell us..."""

#define custom toolbar location
class TopToolbar(mpld3.plugins.PluginBase):
    """Plugin for moving toolbar to top of figure"""

    JAVASCRIPT = """
    mpld3.register_plugin("toptoolbar", TopToolbar);
    TopToolbar.prototype = Object.create(mpld3.Plugin.prototype);
    TopToolbar.prototype.constructor = TopToolbar;
    function TopToolbar(fig, props){
        mpld3.Plugin.call(this, fig, props);
    };

    TopToolbar.prototype.draw = function(){
      // the toolbar svg doesn't exist yet, so first draw it
      this.fig.toolbar.draw();

      // then change the y position to be at the top of the figure
      this.fig.toolbar.toolbar.attr("x", 150);
      this.fig.toolbar.toolbar.attr("y", 400);

      // then remove the draw function, so that it is not called again
      this.fig.toolbar.draw = function() {}
    }
    """
    def __init__(self):
        self.dict_ = {"type": "toptoolbar"}

#define custom css to format the font and to remove the axis labeling
css = """
text.mpld3-text, div.mpld3-tooltip {
  font-family:Arial, Helvetica, sans-serif;
}

g.mpld3-xaxis, g.mpld3-yaxis {
display: none; }
"""

# Plot 
fig, ax = plt.subplots(figsize=(20,15)) #set plot size
ax.margins(0.03) # Optional, just adds 5% padding to the autoscaling

#iterate through groups to layer the plot
#note that I use the cluster_name and cluster_color dicts with the 'name' lookup to return the appropriate color/label
for name, group in groups:
    points = ax.plot(group.x, group.y, marker='o', linestyle='', ms=18, label=cluster_names[name],
                     mec='none', color=cluster_colors[name])
    ax.set_aspect('auto')
    
    labels = []
    for i in group.title:
        i = " ".join(current_comps[i].split("/")[6:]).replace(".txt", "")
        try: labels.append(temp_dict[i])
        except: labels.append("N/A")
    
    #set tooltip using points, labels and the already defined 'css'
    tooltip = mpld3.plugins.PointHTMLTooltip(points[0], labels,
                                       voffset=10, hoffset=10, css=css)
    #connect tooltip to fig
    mpld3.plugins.connect(fig, tooltip, TopToolbar())    
    
    #set tick marks as blank
    ax.axes.get_xaxis().set_ticks([])
    ax.axes.get_yaxis().set_ticks([])
    
    #set axis as blank
    ax.axes.get_xaxis().set_visible(False)
    ax.axes.get_yaxis().set_visible(False)

    
ax.legend(numpoints=1) #show legend with only one dot

mpld3.display() #show the plot

#uncomment the below to export to html
#html = mpld3.fig_to_html(fig)
#print(html)

"""# Hierarchical Clustering

Hc:
"""

from scipy.cluster.hierarchy import ward, dendrogram, fcluster

new_labs = []
for title in range(0, len(corpus)):
    title = " ".join(current_comps[title].split("/")[6:]).replace(".txt", "")
    try: new_labs.append(temp_dict[title])
    except: new_labs.append(temp_dict)

linkage_matrix = ward(dist) #define the linkage_matrix using ward clustering pre-computed distances

"""Fcluster:"""

fl = fcluster(linkage_matrix, 9, criterion='maxclust')

df = pd.DataFrame(dict(x=xs, y=ys, label=fl, title=[i for i in range(0, 597)]))
groups = df.groupby('label')

cluster_colors = {}
for i in range(len(set(fl))+1):
    cluster_colors[i] = kelly_colors_hex[i]

# set up plot
fig, ax = plt.subplots(figsize=(22, 17)) # set size
ax.margins(0.05)

for name, group in groups:
    ax.plot(group.x, group.y, marker='o', linestyle='', ms=12, label=name, color = cluster_colors[name], mec='none')
    ax.set_aspect('auto')
    ax.tick_params(axis='x', which='both', bottom='off', top='off', labelbottom='off')
    ax.tick_params(axis='y', which='both', left='off', top='off', labelleft='off')

ax.legend(numpoints=1)

for i in range(len(df)):
    ax.text(df.loc[i]['x'] + 0.002, df.loc[i]['y'] + 0.002, str(df.loc[i]['title']).replace(".0", ""), size=8)  

plt.show() #show the plot

#uncomment the below to save the plot if need be
#plt.savefig('clusters_small_noaxes.png', dpi=200)

# Add cluster analysis like done above (see output of dendogram when not plotted

"""Next, look at the individualized clusters:"""

labels = [i for i in range(0,len(similarities))]
firm_id = [x.split("/")[len(x.split("/"))-1].split(".")[0] for x in sorted(absoluteFilePaths("./Data Clean"))]
films = {'firm': labels, 'id': firm_id, 'cluster': list(fl)}

frame = pd.DataFrame(films, index = [list(fl)] , columns = ['firm', 'id', 'cluster'])

len(set(fl))

n_top = 5
order_centroids = km.cluster_centers_.argsort()[:, ::-1]
for i in range(1, len(set(fl))+1):
    print("Cluster %d:" % i)
    
    print("Number of Firms:", len(frame.loc[i]['firm'].values.tolist()))
    
    temp_set_cc = []
    for cc in frame.loc[i]['id'].values.tolist():
        if cc[0:2] not in temp_set_cc:
            temp_set_cc.append(cc[0:2])
    print("Unique Markets:   Total: %s   " % len(temp_set_cc), end='')
    for cc in temp_set_cc:
        print(' %s,' % cc, end='')
    print("\n")

"""Company clusters, unique markets and number of firms."""

current_comps = sorted(absoluteFilePaths("./Data Clean"))

for i in range(1, len(set(fl))+1):
    print("Cluster %s Firms:" % i)
    for title in frame.loc[i]['firm'].values.tolist():
        title = " ".join(current_comps[title].split("/")[6:]).replace(".txt", "")
        try: print('%s, ' % temp_dict[title], end='')
        except: print("N/A", end='')
    print("\n")

"""What does this tell us..."""

for i in range(1, len(set(fl))+1):
    clust_country = [x[0:2] for x in frame.loc[i]['id'].values.tolist()]
    clust_country = pd.Series(clust_country).value_counts()
    (keys,values) = zip(*clust_country.items())
    plt.bar(keys, values)
    plt.title("Cluster %d: Countries" % i)
    plt.ylabel("Frequency")
    plt.show()

"""This tells us some more facts about the frequency of countries in the dataset."""

# Plot 
fig, ax = plt.subplots(figsize=(20,15))
ax.margins(0.03)

for name, group in groups:
    points = ax.plot(group.x, group.y, marker='o', linestyle='', ms=18, label = name,
                     mec='none', color=cluster_colors[name])
    ax.set_aspect('auto')
    
    labels = []
    for i in group.title:
        i = " ".join(current_comps[i].split("/")[6:]).replace(".txt", "")
        try: labels.append(temp_dict[i])
        except: labels.append("N/A")
    
    tooltip = mpld3.plugins.PointHTMLTooltip(points[0], labels,
                                       voffset=10, hoffset=10, css=css)
    mpld3.plugins.connect(fig, tooltip, TopToolbar())    

    ax.axes.get_xaxis().set_ticks([])
    ax.axes.get_yaxis().set_ticks([])
    
    ax.axes.get_xaxis().set_visible(False)
    ax.axes.get_yaxis().set_visible(False)

ax.legend(numpoints=1)

mpld3.display()

"""# DB Scan Clustering:

Intro to DB Scan...
"""

from sklearn.cluster import DBSCAN

sort_dist = np.sort(similarities, axis=0)
sort_dist = sort_dist[:,1]
plt.plot(sort_dist)

def abline(slope, intercept):
    """Plot a line from slope and intercept"""
    axes = plt.gca()
    x_vals = np.array(axes.get_xlim())
    y_vals = intercept + slope * x_vals
    plt.plot(x_vals, y_vals, '--')

abline(0, 0.09)

clustering = DBSCAN(eps = 0.09, min_samples = 597-2, metric = "precomputed").fit(similarities)
db_clust = clustering.labels_ + 1

labels = [i for i in range(0,len(similarities))]
df = pd.DataFrame(dict(x=xs, y=ys, label=db_clust, title=labels))
groups = df.groupby('label')

cluster_colors = {}
for i in range(len(set(db_clust))):
    cluster_colors[i] = kelly_colors_hex[i]

# set up plot
fig, ax = plt.subplots(figsize=(22, 17)) # set size


for name, group in groups:
    ax.plot(group.x, group.y, marker='o', linestyle='', ms=12, label=name, color = cluster_colors[name], mec='none')
    ax.set_aspect('auto')
    ax.tick_params(axis= 'x', which='both', bottom='off', top='off', labelbottom='off')
    ax.tick_params(axis= 'y', which='both', left='off', top='off', labelleft='off')

ax.legend(numpoints=1)

for i in range(len(df)):
    ax.text(df.loc[i]['x'] + 0.002, df.loc[i]['y'] + 0.002, str(df.loc[i]['title']).replace(".0", ""), size=8) 

plt.show()

"""Now db_clust"""

firm_id = [x.split("/")[len(x.split("/"))-1].split(".")[0] for x in sorted(absoluteFilePaths("./Data Clean"))]
films = {'firm': labels, 'id': firm_id, 'cluster': list(db_clust)}

frame = pd.DataFrame(films, index = [list(db_clust)] , columns = ['firm', 'id', 'cluster'])

for i in range(0, len(set(db_clust))):
    print("Cluster %d:" % i)
    
    print("Number of Firms:", len(frame.loc[i]['firm'].values.tolist()))
    
    temp_set_cc = []
    for cc in frame.loc[i]['id'].values.tolist():
        if cc[0:2] not in temp_set_cc:
            temp_set_cc.append(cc[0:2])

    print("Unique Markets:   Total: %s   " % len(temp_set_cc), end='')
    for cc in temp_set_cc:
        print(' %s,' % cc, end='')
    print("\n")

"""What does this tell us..."""

current_comps = sorted(absoluteFilePaths("./Data Clean"))

print("Cluster 0 Firms:")
for title in frame.loc[0]['firm'].values.tolist():
    title = " ".join(current_comps[title].split("/")[6:]).replace(".txt", "")
    print('%s, ' % temp_dict[title])

print()
print("Cluster 1 Firms:")
print("The rest of the 597 firms")

"""Rest of the firms in 1 cluster

# Portfolio creation:
"""

#!pip install pandas_datareader
from pandas_datareader import data as web
from functools import reduce
from itertools import islice
import numpy as np
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd

instruments_orig = {}
for i in range(0, 600):
    instruments_orig[df_stock.RIC[i]] = df_stock.Name[i].replace("/", "")
instruments = instruments_orig

#0-50
instruments["IAG.L"] = instruments.pop("ICAG.L")
instruments["ROSE.SW"] = instruments.pop("ROSEG.S")
instruments["FR.PA"] = instruments.pop("VLOF.PA")
instruments["FAU0.F"] = instruments.pop("EPED.PA")
instruments["CBK.DE"] = instruments.pop("CBKG.DE")
instruments["DUFN.SW"] = instruments.pop("DUFN.S")
instruments["CON.DE"] = instruments.pop("CONG.DE")
instruments["SOP.PA"] = instruments.pop("SOPR.PA")
instruments["BC8.DE"] = instruments.pop("BC8G.DE")
instruments["DB"] = instruments.pop("DBKGn.DE")
instruments["RYAAY"] = instruments.pop("RYA.I")
instruments["TUI1.DE"] = instruments.pop("TUIT.L")
instruments["CS"] = instruments.pop("CSGN.S")
instruments["TS"] = instruments.pop("TENR.MI")
instruments["0RTS.IL"] = instruments.pop("RUBF.PA")
instruments["EVR.L"] = instruments.pop("EVRE.L")
instruments["EVO.ST"] = instruments.pop("EVOG.ST")
instruments["CFRUY"] = instruments.pop("CFR.S")
instruments["ATO.PA"] = instruments.pop("ATOS.PA")
instruments["LHA.DE"] = instruments.pop("LHAG.DE")
instruments["KIND-SDB.ST"] = instruments.pop("KINDsdb.ST")
instruments["EQT.ST"] = instruments.pop("EQTAB.ST")
instruments["AMP.VI"] = instruments.pop("AMPF.MI")

#50-100
instruments["ALO.PA"] = instruments.pop("ALSO.PA")
instruments["TKA.DE"] = instruments.pop("TKAG.DE")
instruments["BNDSF"] = instruments.pop("SABE.MC")
instruments["PDYPY"] = instruments.pop("FLTRF.I")
instruments["GLE.VI"] = instruments.pop("SOGN.PA")
instruments["AMADY"] = instruments.pop("AMA.MC")
instruments["ADE.OL"] = instruments.pop("ADEV.OL")
instruments["SRT3.DE"] = instruments.pop("SATG_p.DE")
instruments["UBS"] = instruments.pop("UBSG.S")
instruments["UCG.MI"] = instruments.pop("CRDI.MI")
instruments["BPT.L"] = instruments.pop("BPTB.L")
instruments["ASM.AS"] = instruments.pop("ASMI.AS")
instruments["LPP.WA"] = instruments.pop("LPPP.WA")
instruments["BNP.PA"] = instruments.pop("BNPP.PA")
instruments["FLDAY"] = instruments.pop("FLUI.MC")
instruments["0RG8.IL"] = instruments.pop("STDM.PA")
instruments["SIE.DE"] = instruments.pop("SIEGn.DE")
instruments["BIRG.IR"] = instruments.pop("BIRG.I")
instruments["KER.PA"] = instruments.pop("PRTP.PA")
instruments["ZAL.DE"] = instruments.pop("ZALG.DE")

#100-150
instruments["BBL"] = instruments.pop("BHPB.L")
instruments["CRARY"] = instruments.pop("CAGR.PA")
instruments["ENEL.MI"] = instruments.pop("ENEI.MI")
instruments["HELN.SW"] = instruments.pop("HELN.S")
instruments["0RMT.IL"] = instruments.pop("SOIT.PA")
instruments["SCR.PA"] = instruments.pop("SCOR.PA")
instruments["HLFFF"] = instruments.pop("HFGG.DE")
instruments["JBAXY"] = instruments.pop("BAER.S")
instruments["AKRBP.OL"] = instruments.pop("AKERBP.OL")
instruments["SFZN.SW"] = instruments.pop("SFZN.S")
instruments["WISE.L"] = instruments.pop("WISEa.L")
instruments["AMS.SW"] = instruments.pop("AMS.S")
instruments["NETC.CO"] = instruments.pop("NETCG.CO")
instruments["GIFLF"] = instruments.pop("GRLS.MC")
instruments["WIE.VI"] = instruments.pop("WBSV.VI")
instruments["UHR.SW"] = instruments.pop("UHR.S")
instruments["DSRLF"] = instruments.pop("DIAS.MI")
instruments["DAI.DE"] = instruments.pop("DAIGn.DE")
instruments["BMW.DE"] = instruments.pop("BMWG.DE")
instruments["SCHA.OL"] = instruments.pop("SBSTA.OL")
instruments["AGS.BR"] = instruments.pop("AGES.BR")
instruments["HNR1.DE"] = instruments.pop("HNRGn.DE")
instruments["FMS"] = instruments.pop("FMEG.DE")
instruments["SU.PA"] = instruments.pop("SCHN.PA")
instruments["MC.PA"] = instruments.pop("LVMH.PA")

#150-200
instruments["NEM.DE"] = instruments.pop("NEKG.DE")
instruments["RNO.PA"] = instruments.pop("RENA.PA")
instruments["POAHY"] = instruments.pop("PSHG_p.DE")
instruments["EBS.VI"] = instruments.pop("ERST.VI")
instruments["CDI.PA"] = instruments.pop("DIOR.PA")
instruments["MUV2.DE"] = instruments.pop("MUVGn.DE")
instruments["CBG.L"] = instruments.pop("CBRO.L")
instruments["AG1.DE"] = instruments.pop("AG1G.DE")
instruments["VWAGY"] = instruments.pop("VOWG_p.DE")
instruments["HEI.DE"] = instruments.pop("HEIG.DE")
instruments["IFNNY"] = instruments.pop("IFXGn.DE")
instruments["ADEN.SW"] = instruments.pop("ADEN.S")
instruments["STMN.SW"] = instruments.pop("STMN.S")
instruments["SHL.DE"] = instruments.pop("SHLG.DE")
instruments["ATLC"] = instruments.pop("ATCOa.ST")
instruments["BANB.SW"] = instruments.pop("BANB.S")
instruments["0F4I.IL"] = instruments.pop("LOIM.PA")
instruments["VOE.VI"] = instruments.pop("VOES.VI")
instruments["GXI.DE"] = instruments.pop("GXIG.DE")
instruments["ENR.F"] = instruments.pop("ENR1n.DE")
instruments["PUM.DE"] = instruments.pop("PUMG.DE")

#200-250
instruments["SGRE.MC"] = instruments.pop("SGREN.MC")
instruments["UBI.PA"] = instruments.pop("UBIP.PA")
instruments["OMV.VI"] = instruments.pop("OMVV.VI")
instruments["SOON.SW"] = instruments.pop("SOON.S")
instruments["AFX.DE"] = instruments.pop("AFXG.DE")
instruments["AED.BR"] = instruments.pop("AOO.BR")
instruments["ARCAD.AS"] = instruments.pop("ARDS.AS")
instruments["0N54.IL"] = instruments.pop("A2.MI")
instruments["VER.VI"] = instruments.pop("VERB.VI")
instruments["LDSVF"] = instruments.pop("LISN.S")
instruments["CLZNY"] = instruments.pop("CLN.S")
instruments["VACN.SW"] = instruments.pop("VACN.S")
instruments["GFTU.L"] = instruments.pop("GFTU_u.L")
instruments["NEXXY"] = instruments.pop("NEXII.MI")
instruments["SEB-A.ST"] = instruments.pop("SEBa.ST")
instruments["LOGI"] = instruments.pop("LOGN.S")
instruments["EVO"] = instruments.pop("EVTG.DE")
instruments["EPI-B.ST"] = instruments.pop("EPIRa.ST")
instruments["DPW.DE"] = instruments.pop("DPWGn.DE")

#250-300
instruments["0RUG.IL"] = instruments.pop("BIOX.PA")
instruments["GRPTF"] = instruments.pop("GETP.PA")
instruments["RDSMY"] = instruments.pop("DSMN.AS")
instruments["DLG.L"] = instruments.pop("DLGD.L")
instruments["DOM.ST"] = instruments.pop("DOMETIC.ST")
instruments["0EWD.IL"] = instruments.pop("ITPG.MI")
instruments["EXXRF"] = instruments.pop("EXOR.MI")
instruments["RBI.VI"] = instruments.pop("RBIV.VI")
instruments["0NZM.IL"] = instruments.pop("OREP.PA")
instruments["CRH"] = instruments.pop("CRH.I")
instruments["RI.PA"] = instruments.pop("PERP.PA")
instruments["SREN.SW"] = instruments.pop("SRENH.S")
instruments["NDA-FI.HE"] = instruments.pop("NDASE.ST")
instruments["TECN.SW"] = instruments.pop("TECN.S")
instruments["SLHN.SW"] = instruments.pop("SLHN.S")
instruments["0RG6.IL"] = instruments.pop("FHZN.S")
#instruments["ROO.L"] = instruments.pop("ROO.L")
instruments["ALV.DE"] = instruments.pop("ALVG.DE")
instruments["ALE.WA"] = instruments.pop("ALEP.WA")
instruments["BAS.DE"] = instruments.pop("BASFn.DE")
instruments["ABN.AS"] = instruments.pop("ABNd.AS")
instruments["RAA.DE"] = instruments.pop("RAAG.DE")
instruments["REC.MI"] = instruments.pop("RECI.MI")
instruments["0HV2.IL"] = instruments.pop("HRMS.PA")
instruments["AENF.F"] = instruments.pop("AEGN.AS")
instruments["0QP4.IL"] = instruments.pop("FIN.S")
instruments["ADM.L"] = instruments.pop("ADML.L")
instruments["PGHN.SW"] = instruments.pop("PGHN.S")
instruments["SK3.IR"] = instruments.pop("SKG.I")
instruments["SGO.VI"] = instruments.pop("SGOB.PA")

#300-350
instruments["TTE"] = instruments.pop("TTEF.PA")
instruments["BG.VI"] = instruments.pop("BAWG.VI")
instruments["LXS.DE"] = instruments.pop("LXSG.DE")
instruments["0NPT.IL"] = instruments.pop("FOUG.PA")
instruments["DG.VI"] = instruments.pop("SGEF.PA")
instruments["BNR.DE"] = instruments.pop("BNRGn.DE")
instruments["KHNGF"] = instruments.pop("KNIN.S")
instruments["AUTO.L"] = instruments.pop("AUTOA.L")
instruments["RCO.PA"] = instruments.pop("RCOP.PA")
instruments["HOLN.SW"] = instruments.pop("HOLN.S")
instruments["TIIAY"] = instruments.pop("TLIT.MI")
instruments["LR.PA"] = instruments.pop("LEGD.PA")
instruments["ERF.PA"] = instruments.pop("EUFI.PA")
instruments["CS.PA"] = instruments.pop("AXAF.PA")
instruments["TEG.DE"] = instruments.pop("TEGG.DE")
instruments["DBOEY"] = instruments.pop("DB1Gn.DE")
instruments["EVD.DE"] = instruments.pop("EVDG.DE")
instruments["SY1.DE"] = instruments.pop("SY1G.DE")
instruments["STJ.L"] = instruments.pop("SJP.L")
instruments["0HB4.IL"] = instruments.pop("DAST.PA")
instruments["POLY.L"] = instruments.pop("POLYP.L")
instruments["SPSN.SW"] = instruments.pop("SPSN.S")
instruments["ALLN.SW"] = instruments.pop("ALLN.S")
instruments["G24.SG"] = instruments.pop("G24n.DE")
instruments["KRX.IR"] = instruments.pop("KSP.I")

#350-400
instruments["BT-A.L"] = instruments.pop("BT.L")
instruments["CAP.PA"] = instruments.pop("CAPP.PA")
instruments["PSM.DE"] = instruments.pop("PSMGn.DE")
instruments["INW.MI"] = instruments.pop("INWT.MI")
instruments["FRE.DE"] = instruments.pop("FREG.DE")
instruments["AOX.DE"] = instruments.pop("AOXG.DE")
instruments["WIS.DU"] = instruments.pop("MWDP.PA")
instruments["LZAGY"] = instruments.pop("LONN.S")
instruments["BUCN.SW"] = instruments.pop("BUCN.S")
instruments["BBOX.L"] = instruments.pop("BBOXT.L")
instruments["DTE.DE"] = instruments.pop("DTEGn.DE")
instruments["CA.PA"] = instruments.pop("CARR.PA")
instruments["SHB-A.ST"] = instruments.pop("SHBa.ST")
instruments["CSP.L"] = instruments.pop("CSPC.L")
instruments["ABB"] = instruments.pop("ABBN.S")
instruments["MRO.L"] = instruments.pop("MRON.L")
instruments["PSPN.SW"] = instruments.pop("PSPN.S")
instruments["TEP.PA"] = instruments.pop("TEPRF.PA")
instruments["VCT.L"] = instruments.pop("VCTX.L")
instruments["0NVV.IL"] = instruments.pop("HRA.MI")
instruments["ML.PA"] = instruments.pop("MICP.PA")
instruments["ADS.DE"] = instruments.pop("ADSGn.DE")
instruments["AC.PA"] = instruments.pop("ACCP.PA")

#400-450
instruments["BEAN.SW"] = instruments.pop("BEAN.S")
instruments["MB.MI"] = instruments.pop("MDBI.MI")
instruments["WDP.BR"] = instruments.pop("WDPP.BR")
instruments["TEMN.SW"] = instruments.pop("TEMN.S")
instruments["RWE.DE"] = instruments.pop("RWEG.DE")
instruments["G1A.DE"] = instruments.pop("G1AG.DE")
instruments["ZURVY"] = instruments.pop("ZURN.S")
instruments["FINMY"] = instruments.pop("LDOF.MI")
instruments["SWED-A.ST"] = instruments.pop("SWEDa.ST")
instruments["TIGO"] = instruments.pop("TIGOsdb.ST")
instruments["AZA.ST"] = instruments.pop("AVANZ.ST")
instruments["DVDCF"] = instruments.pop("CPRI.MI")
instruments["MRK.DE"] = instruments.pop("MRCG.DE")
instruments["SAP"] = instruments.pop("SAPG.DE")
instruments["0O8V.IL"] = instruments.pop("VOPA.AS")
instruments["ENGI.PA"] = instruments.pop("ENGIE.PA")
instruments["ARZGY"] = instruments.pop("GASI.MI")
instruments["COV.PA"] = instruments.pop("CVO.PA")
instruments["SCHN.SW"] = instruments.pop("SCHP.S")
instruments["GL9.IR"] = instruments.pop("GL9.I")

#450-500
instruments["EVK.DE"] = instruments.pop("EVKn.DE")
instruments["VTY.L"] = instruments.pop("VTYV.L")
instruments["VNA.DE"] = instruments.pop("VNAn.DE")
instruments["SCT.L"] = instruments.pop("SCTS.L")
instruments["SK.PA"] = instruments.pop("SEBF.PA")
instruments["0H13.IL"] = instruments.pop("INDUa.ST")
instruments["AM.PA"] = instruments.pop("AVMD.PA")
instruments["BARN.SW"] = instruments.pop("BARN.S")
instruments["AKZOY"] = instruments.pop("AKZO.AS")
instruments["PUB.PA"] = instruments.pop("PUBP.PA")
instruments["BLHEY"] = instruments.pop("BALN.S")
instruments["ENGGY"] = instruments.pop("ENAG.MC")
instruments["SIKA.SW"] = instruments.pop("SIKA.S")
instruments["HAS.L"] = instruments.pop("HAYS.L")
instruments["GIVN.SW"] = instruments.pop("GIVN.S")
instruments["EMSN.SW"] = instruments.pop("EMSN.S")
instruments["SIGN.SW"] = instruments.pop("SIGNC.S")
instruments["THLLY"] = instruments.pop("TCFP.PA")
instruments["CHR.CO"] = instruments.pop("CHRH.CO")
instruments["LEG.DE"] = instruments.pop("LEGn.DE")
instruments["AN3.F"] = instruments.pop("LTEN.PA")

#500-550
instruments["GALE.SW"] = instruments.pop("GALE.S")
instruments["FUPBY"] = instruments.pop("FPE3_p.DE")
instruments["ALC"] = instruments.pop("ALCC.S")
instruments["RF.PA"] = instruments.pop("EURA.PA")
instruments["RHM.DE"] = instruments.pop("RHMG.DE")
instruments["MTX.DE"] = instruments.pop("MTXGn.DE")
instruments["COFB.BR"] = instruments.pop("EONGn.DE")
instruments["BOL.PA"] = instruments.pop("BOLL.PA")
instruments["GFC.PA"] = instruments.pop("GFCP.PA")
instruments["KRZ.IR"] = instruments.pop("KYGa.I")
instruments["LMP.L"] = instruments.pop("LMPL.L")
instruments["0NMU.IL"] = instruments.pop("WLSNc.AS")
instruments["AGR.L"] = instruments.pop("AGRP.L")
instruments["SW.VI"] = instruments.pop("EXHO.PA")
instruments["EL.PA"] = instruments.pop("ESLX.PA")
instruments["GEBN.SW"] = instruments.pop("GEBN.S")
instruments["HL.L"] = instruments.pop("HRGV.L")
instruments["BMRRY"] = instruments.pop("BMEB.L")
instruments["LIN"] = instruments.pop("LINI.DE")
instruments["BKG.L"] = instruments.pop("BKGH.L")
instruments["VIFN.SW"] = instruments.pop("VIFN.S")

#550-600
instruments["BAYN.DE"] = instruments.pop("BAYGn.DE")
instruments["SGSN.SW"] = instruments.pop("SGSN.S")
instruments["ICA.ST"] = instruments.pop("ICAA.ST")
instruments["DOCS.L"] = instruments.pop("DOCS.L")
instruments["RHHBY"] = instruments.pop("ROG.S")
instruments["SCMN.SW"] = instruments.pop("SCMN.S")
instruments["EN.PA"] = instruments.pop("BOUY.PA")
instruments["GJF.OL"] = instruments.pop("GJES.OL")
instruments["0QHL.IL"] = instruments.pop("CORB.AS")
instruments["HEN3.DE"] = instruments.pop("HNKG_p.DE")
instruments["0KFX.IL"] = instruments.pop("DANO.PA")
instruments["FNTN.DE"] = instruments.pop("FNTGn.DE")
instruments["SNW.F"] = instruments.pop("SASY.PA")
instruments["RYLPF"] = instruments.pop("PHG.AS")
instruments["BAESY"] = instruments.pop("BAES.L")
instruments["ORA.VI"] = instruments.pop("ORAN.PA")
instruments["HEINY"] = instruments.pop("HEIN.AS")
instruments["SGBAF"] = instruments.pop("SESFd.PA")
instruments["NVS"] = instruments.pop("NOVN.S")
instruments["BEI.DE"] = instruments.pop("BEIG.DE")
instruments["NSRGY"] = instruments.pop("NESN.S")
instruments["DWNI.DE"] = instruments.pop("DWNG.DE")
instruments["SOBI.ST"] = instruments.pop("SOBIV.ST")
instruments["SEV.PA"] = instruments.pop("SEVI.PA")
instruments["UMG.AS"] = instruments.pop("UMG.AS")

# Extra
#instruments[""] = instruments.pop("ROO.L")
#instruments[""] = instruments.pop("ALLFG.AS")
#instruments[""] = instruments.pop("INPST.AS")
#instruments[""] = instruments.pop("AIRP.PA")
#instruments[""] = instruments.pop("MRW.L")
#instruments[""] = instruments.pop("FAU0.F")
#instruments[""] = instruments.pop("BPT.L")
#instruments[""] = instruments.pop("FLDAY")
#instruments[""] = instruments.pop("WISE.L")
#instruments[""] = instruments.pop("AG1.DE")
#instruments[""] = instruments.pop("DOCS.L")
#instruments[""] = instruments.pop("UMG.AS")

current_comps = sorted(absoluteFilePaths("./Data Clean"))

comps_cluster = []
for i in range(num_clusters):
    temp_comps = []
    for title in frame.loc[i]['firm'].values.tolist():
        title = " ".join(current_comps[title].split("/")[6:]).replace(".txt", "")
        try: temp_comps.append(temp_dict[title])
        except: pass
    comps_cluster.append(temp_comps)

dict_cluster = []
for i in range(num_clusters):
    dict_cluster.append({ key: instruments[key] for key in instruments if instruments[key] in comps_cluster[i] })

#with open('dict_cluster_final.pkl', 'wb') as f:
#    pickle.dump(dict_cluster, f)
with open('dict_cluster_final.pkl', 'rb') as f:
    dict_cluster = pickle.load(f)

def do_all2(instruments, cluster, plot = False, start = datetime.datetime(2015,1,1), end = datetime.datetime(2020,12,31)):
    ##############################################
    ############    AssetSelection    ############
    ##############################################
    tickers = list(instruments.keys())
    instruments_data = {}
    for ticker, instrument in instruments.items():
        try: instruments_data[ticker] = web.DataReader(ticker, data_source = 'yahoo', start = start, end = end)['Adj Close']
        except: pass
    
    tr_days = [] ; nums = []
    for ticker, instrument in instruments.items():
        try:
            tr_days.append(instruments_data[ticker].shape[0])
            nums.append(True)
        except: nums.append(False)
    tickers = [x for x, y in zip(tickers, nums) if y]
    tr_days = pd.DataFrame(tr_days, index = tickers, columns = ["Trading Days"])
    
    tr_days_per_year = instruments_data[tickers[0]].groupby([instruments_data[tickers[0]].index.year]).agg('count')
    tr_days_per_year = pd.DataFrame([tr_days_per_year], index = ["Stocks"])
    
    data = list(instruments_data.values())
    data_df = reduce(lambda x, y: pd.merge(x, y, left_index=True, right_index=True, how='outer'), data)
    data_df.columns = tickers
    
    fig, ax = plt.subplots(figsize=(22,11))
    data_df.plot(ax = plt.gca(), grid = True)
    ax.set_title('Adjusted Close for all Stocks')
    
    if not plot: plt.close()
    else: plt.show()
    
    ###############################################
    #########    DescriptiveStatistics    #########
    ###############################################  
    log_returns = data_df.pct_change()
    
    fig, ax = plt.subplots(figsize=(22,11))
    log_returns.plot(ax = plt.gca(), grid = True)
    ax.set_title('Log Returns for all Stocks')
    ax.axhline(y = 0, color = "black", lw = 2)
    if not plot: plt.close()
    else: plt.show()
    
    APR = log_returns.groupby([log_returns.index.year], dropna=False).sum(numeric_only = None)
    APR_avg = APR.mean()
    
    N = np.array(list(tr_days_per_year.T.Stocks))[:, None]
    N_total = np.sum(N)
    APY = (1 + APR / N)**N - 1
    APY_avg = (1 + APR_avg / N_total)**N_total - 1
    
    STD = log_returns.groupby([log_returns.index.year], dropna=False).agg('std') * np.sqrt(252)
    STD_avg = STD.mean()
    
    fig, ax = plt.subplots(figsize = (22,11))
    STD.plot(ax = plt.gca(), grid = True)
    ax.set_title("$\sigma$ of all stocks for all years")
    ax.set_ylabel("$\sigma$") ; ax.set_xlabel("Year")
    if not plot: plt.close()
    else: plt.show()
    
    c = [y + x for y, x in zip(APY_avg, STD_avg)]
    c = list(map(lambda x : x / max(c), c))
    s = list(map(lambda x : x * 600   , c))
    
    fig, ax = plt.subplots(figsize = (18,11))
    ax.scatter(STD_avg, APY_avg, s = s , c = c , cmap = "Blues", alpha = 0.4, edgecolors = "grey")
    ax.grid()
    ax.axhline(y = 0, c = "plum") ; ax.axvline(x = 0, c = "plum")
    ax.set_title("Risk ($\sigma$) vs Return ($APY$) of all stocks")
    ax.set_xlabel("$\sigma$") ; ax.set_ylabel("Annualized Percetage Returns $APY$ or $R_{effective}$")
    for idx, instr in enumerate(list(STD.columns)):
        ax.annotate(instr, (STD_avg[idx] + 0.005, APY_avg[idx]))
    
    
    if not plot: plt.close()
    else: plt.show()
    
    ##############################################
    ##############    PortCreate    ##############
    ##############################################
    risk_free = float(web.DataReader('^IRX', data_source = 'yahoo', start = end, end = end)['Adj Close'])
    cov = APR.cov()
    
    portfolios = {"MaxSR"  : {"E(R)" : 0, "SD" : 0, "SR" : 0, "beta" : 0},
                  "MinVar" : {"E(R)" : 0, "SD" : 0, "SR" : 0, "beta" : 0}}
    
    weights = np.array([1/len(log_returns.columns)] * len(log_returns.columns))
    
    # Bigger portfolios need more optimisation, this makes it more fair as smaller portfolios are more optimised if its a flat rate then larger portfolios
    num_portfolios = 1000 * len(log_returns.columns)
    generated_portfolios = [] # store the results
    for _ in range(num_portfolios) :
        # 1 - select random weights for portfolio holdings &  rebalance weights to sum to 1
        weights = np.array(np.random.random(len(APR.columns)))
        weights /= np.sum(weights) 
        # 2 - calculate return, risk, sharpe ratio
        expected_return = np.sum(APR_avg * weights)
        expected_risk   = np.sqrt(np.dot(weights.T,np.dot(cov,weights)))
        sharpe_ratio    = (expected_return - risk_free) / expected_risk
        # 3 - store the result
        generated_portfolios.append([expected_return, expected_risk, sharpe_ratio, weights])
    
    maximum_sr_portfolio   = sorted(generated_portfolios, key = lambda x : -x[2])[0]
    minimum_risk_portfolio = sorted(generated_portfolios, key = lambda x : x[1])[0]
    max_sr = maximum_sr_portfolio[2]
    
    max_sr_weights   = pd.DataFrame(maximum_sr_portfolio[3],   index = log_returns.columns, columns = [cluster]).T
    min_risk_weights = pd.DataFrame(minimum_risk_portfolio[3], index = log_returns.columns, columns = [cluster]).T

    # TOTAL PORTFOLIOS
    total_expected_return = 0.9 * maximum_sr_portfolio[0] + 0.1 * risk_free
    total_expected_risk   = 0.9 * maximum_sr_portfolio[1]
    portfolios["MaxSR"]["E(R)"]  = total_expected_return
    portfolios["MaxSR"]["SD"]    = total_expected_risk
    portfolios["MaxSR"]["SR"]    = (total_expected_return - risk_free) / total_expected_risk
    
    total_expected_return = 0.9 * minimum_risk_portfolio[0] + 0.1 * risk_free
    total_expected_risk   = 0.9 * minimum_risk_portfolio[1]
    portfolios["MinVar"]["E(R)"] = total_expected_return
    portfolios["MinVar"]["SD"]   = total_expected_risk
    portfolios["MinVar"]["SR"]   = (total_expected_return - risk_free) / total_expected_risk
    
    ##############################################
    #############    PlottingFunc    #############
    ##############################################
    if plot:
        fig, ax = plt.subplots(figsize = (22,11))
        ax.grid()
        ret, risk, sr = [x[0] for x in generated_portfolios], [x[1] for x in generated_portfolios], [x[2] for x in generated_portfolios]
        ax.scatter(risk, ret, c = sr, cmap = 'viridis', marker = 'o', s = 10, alpha = 0.5)
        ax.scatter(maximum_sr_portfolio[1], maximum_sr_portfolio[0], marker = (5,1), color = 'orange',   s = 700, label = 'Max SR Portfolio')
        ax.scatter(minimum_risk_portfolio[1], minimum_risk_portfolio[0], marker = (5,1), color = 'plum', s = 700, label = 'Min $\sigma$ Portfolio')
        tit = str(num_portfolios) + ' Simulated Portfolios'
        ax.set_title(tit) ; ax.set_xlabel('Annualized Risk ($\sigma$)') ; ax.set_ylabel('Annualized Returns ($APR_{avg}$)')
        ax.legend(labelspacing = 1.2)
        plt.show()
        
        cal_x = np.linspace(0.0, 0.5, 50) ; cal_y = risk_free + cal_x * max_sr
        fig, ax = plt.subplots(figsize = (22,11))
        ax.grid()
        ax.scatter(risk, ret, c = sr, cmap = 'viridis', marker = 'o', s = 10, alpha = 0.5)
        ax.scatter(maximum_sr_portfolio[1], maximum_sr_portfolio[0], marker = (5,1), color = 'orange',   s = 700, label = 'Max SR Portfolio')
        ax.scatter(minimum_risk_portfolio[1], minimum_risk_portfolio[0], marker = (5,1), color = 'plum', s = 700, label = 'Min $\sigma$ Portfolio')
        ax.plot(cal_x, cal_y, linestyle = '-', color = 'red', label = 'CAL')
        ax.scatter(STD_avg, APR_avg, s = s , c = c , cmap = "Blues", alpha = 0.4, edgecolors = "grey", linewidth = 2)
        for idx, instr in enumerate(list(STD.columns)): ax.annotate(instr, (STD_avg[idx] + 0.01, APR_avg[idx]))
        ax.set_title(tit) ; ax.set_xlabel('Annualized Risk ($\sigma$)') ; ax.set_ylabel('Annualized Returns ($APR_{avg}$)')
        ax.legend(labelspacing = 1.2)
        plt.show()
    
    ##############################################
    ###############    BetaCalcs   ###############
    ##############################################
    market             = web.DataReader('^STOXX', data_source = 'yahoo', start = start, end = end)['Adj Close'].rename("^STOXX")
    market_log_return  = pd.concat([market.pct_change()], axis = 1)
    STD_total          = market_log_return.groupby([market.index.year]).agg('std') * np.sqrt(N)
    corr = log_returns.corrwith(market.pct_change())
    beta  = corr * STD_avg / STD_total.mean()['^STOXX']
    portfolios["MaxSR"]["beta"] = float((beta * max_sr_weights).T.sum())
    
    ##############################################
    ###############    TableOut    ###############
    ##############################################
    portfolio = pd.DataFrame([[portfolios["MaxSR"]['E(R)'], portfolios["MaxSR"]['SD'], portfolios["MaxSR"]['SR'], portfolios["MaxSR"]["beta"]]],
                             columns = ['E(R)', '$\sigma$', 'Sharpe Ratio', "beta"], index = [cluster])
    
    return(portfolio, max_sr_weights)

for i in range(0,21):
    print(len(dict_cluster[i]), end = ", ")

try: port0, w0  = do_all2(instruments = dict_cluster[0],  cluster = 0,  plot = False)
except: print("Mistake", 0)
try: port1, w1  = do_all2(instruments = dict_cluster[1],  cluster = 1,  plot = False)
except: print("Mistake", 1)
try: port2, w2  = do_all2(instruments = dict_cluster[2],  cluster = 2,  plot = False)
except: print("Mistake", 2)
try: port3, w3  = do_all2(instruments = dict_cluster[3],  cluster = 3,  plot = False)
except: print("Mistake", 3)
try: port4, w4  = do_all2(instruments = dict_cluster[4],  cluster = 4,  plot = False)
except: print("Mistake", 4)

print("5/21 done")

try: port5, w5  = do_all2(instruments = dict_cluster[5],  cluster = 5,  plot = False)
except: print("Mistake", 5)
try: port6, w6  = do_all2(instruments = dict_cluster[6],  cluster = 6,  plot = False)
except: print("Mistake", 6)
try: port7, w7  = do_all2(instruments = dict_cluster[7],  cluster = 7,  plot = False)
except: print("Mistake", 7)
try: port8, w8  = do_all2(instruments = dict_cluster[8],  cluster = 8,  plot = False)
except: print("Mistake", 8)
try: port9, w9  = do_all2(instruments = dict_cluster[9],  cluster = 9,  plot = False)
except: print("Mistake", 9)

print("10/21 done")

try: port10, w10 = do_all2(instruments = dict_cluster[10], cluster = 10, plot = False)
except: print("Mistake", 10)
try: port11, w11 = do_all2(instruments = dict_cluster[11], cluster = 11, plot = False)
except: print("Mistake", 11)
try: port12, w12 = do_all2(instruments = dict_cluster[12], cluster = 12, plot = False)
except: print("Mistake", 12)
try: port13, w13 = do_all2(instruments = dict_cluster[13], cluster = 13, plot = False)
except: print("Mistake", 13)
try: port14, w14 = do_all2(instruments = dict_cluster[14], cluster = 14, plot = False)
except: print("Mistake", 14)

print("15/21 done")

try: port15, w15 = do_all2(instruments = dict_cluster[15], cluster = 15, plot = False)
except: print("Mistake", 15)
try: port16, w16 = do_all2(instruments = dict_cluster[16], cluster = 16, plot = False)
except: print("Mistake", 16)
try: port17, w17 = do_all2(instruments = dict_cluster[17], cluster = 17, plot = False)
except: print("Mistake", 17)
try: port18, w18 = do_all2(instruments = dict_cluster[18], cluster = 18, plot = False)
except: print("Mistake", 18)
try: port19, w19 = do_all2(instruments = dict_cluster[19], cluster = 19, plot = False)
except: print("Mistake", 19)

print("20/21 done")

try: port20, w20 = do_all2(instruments = dict_cluster[20], cluster = 20, plot = False)
except: print("Mistake", 20)

port_clust_all = pd.concat([port0,  port1,  port3,  port4,  port5,  port6,  port7,  port8,  port9, #,  port2
                            port10, port11, port12, port13, port14, port15, port16, port17, port18, port19,
                            port20])

#with open('all_portfolios_desc2.pkl', 'wb') as f:
#    pickle.dump(port_clust_all, f)

with open('all_portfolios_desc2.pkl', 'rb') as f:
    port_clust_all = pickle.load(f)

port_clust_all

"""# Portfolio performance"""

#!pip install plotly
#!pip install yfinance
#!pip install pandas_market_calendars

import pandas as pd
import numpy as np
import datetime
import plotly.express as px
import yfinance as yf
import pandas_market_calendars as mcal
from plotly.offline import init_notebook_mode, plot

def port_test(stocks, weights, start = datetime.datetime(2021,1,1), end = datetime.datetime(2021,12,31)):
    for i in range(21):
        try:
            intruments_temp = stocks[i]
            w_temp = weights[i][0]
    
            tickers = list(intruments_temp.keys())
            instruments_data = {}
            n = []
            for ticker, instrument in intruments_temp.items():
                try:
                    instruments_data[ticker] = web.DataReader(ticker, data_source = 'yahoo', start = datetime.datetime(2021,1,1), end = datetime.datetime(2021,12,31))['Adj Close']
                    n.append(True)
                except: n.append(False)
        
            w_temp = w_temp / sum(w_temp) # Normalize
        
            data = list(instruments_data.values())
            data_df = reduce(lambda x, y: pd.merge(x, y, left_index=True, right_index=True, how='outer'), data).dropna()
        
            portfolio_df = data_df * w_temp
            portfolio_df = portfolio_df.sum(axis = 1)
        
            if i == 0:
                portfolio_daily = pd.DataFrame(portfolio_df, columns = [i])
            else:
                y = pd.DataFrame(portfolio_df, columns = [i])
                portfolio_daily = pd.merge(portfolio_daily, y, left_index=True, right_index=True, how='outer')
        except: print("Mistake at", i)
    
    fig, ax = plt.subplots(figsize=(18,8))
    portfolio_daily.plot(ax = plt.gca(), grid = True)
    ax.set_title('Portfolio Performance')
    plt.show()
    return("Done")

w_all = [w0.values, w1.values, w3.values, w4.values, w5.values, w6.values, w7.values, w8.values, w9.values,
         w10.values, w11.values, w12.values, w13.values, w14.values, w15.values, w16.values, w17.values, w18.values, w19.values, w20.values]
#do_all2(instruments = dict_cluster[0], w = , cluster = 0)

port_test(stocks = dict_cluster, weights = w_all)

"""## Plot all:"""

for i in range(21):
    try:
        print("Cluster", i, "plots:")
        do_all2(instruments = dict_cluster[i], cluster = i, plot = True)
        print()
        print()
    except: print(i)



"""# Dif try"""

def plot_simulation(generated_portfolios, maximum_sr_portfolio, minimum_risk_portfolio, STD, STD_avg, APR_avg, s, c, CAL = None, INSTRUMENTS = None) :
    fig, ax = plt.subplots(figsize = (22,11))
    ax.grid()
    
    ret, risk, sr = [x[0] for x in generated_portfolios], [x[1] for x in generated_portfolios], [x[2] for x in generated_portfolios]
    
    ax.scatter(risk, ret, c = sr, cmap = 'viridis', marker = 'o', s = 10, alpha = 0.5)
    ax.scatter(maximum_sr_portfolio[1], maximum_sr_portfolio[0], marker = (5,1), color = 'orange',   s = 700, label = 'Max SR Portfolio')
    ax.scatter(minimum_risk_portfolio[1], minimum_risk_portfolio[0], marker = (5,1), color = 'plum', s = 700, label = 'Min $\sigma$ Portfolio')

    if CAL:
        ax.plot(CAL[0], CAL[1], linestyle = '-', color = 'red', label = 'CAL')
    if INSTRUMENTS:
        ax.scatter(STD_avg, APR_avg, s = s , c = c , cmap = "Blues", alpha = 0.4, edgecolors = "grey", linewidth = 2)
        for idx, instr in enumerate(list(STD.columns)): ax.annotate(instr, (STD_avg[idx] + 0.01, APR_avg[idx]))
    
    ax.set_title('2000 Simulated Portfolios')
    ax.set_xlabel('Annualized Risk ($\sigma$)') ; ax.set_ylabel('Annualized Returns ($APR_{avg}$)')
    ax.legend(labelspacing = 1.2)
    
    plt.show()


def do_all3(instruments, cluster, plot = False, start = datetime.datetime(2018,1,1), end = datetime.datetime(2021,12,31)):#, data_df = data_df, tr_days_per_year = tr_days_per_year, log_returns = log_returns):    
    ##############################################
    ############    AssetSelection    ############
    ##############################################
    tickers = list(instruments.keys())
    instruments_data = {}
    for ticker, instrument in instruments.items():
        try: instruments_data[ticker] = web.DataReader(ticker, data_source = 'yahoo', start = start, end = end)['Adj Close']
        except: pass
    
    tr_days = [] ; nums = []
    for ticker, instrument in instruments.items():
        try:
            tr_days.append(instruments_data[ticker].shape[0])
            nums.append(True)
        except: nums.append(False)
    tickers = [x for x, y in zip(tickers, nums) if y]
    tr_days = pd.DataFrame(tr_days, index = tickers, columns = ["Trading Days"])
    
    tr_days_per_year = instruments_data[tickers[0]].groupby([instruments_data[tickers[0]].index.year]).agg('count')
    tr_days_per_year = pd.DataFrame([tr_days_per_year], index = ["Stocks"])
    
    data = list(instruments_data.values())
    data_df = reduce(lambda x, y: pd.merge(x, y, left_index=True, right_index=True, how='outer'), data)
    data_df.columns = tickers
    
    fig, ax = plt.subplots(figsize=(22,11))
    data_df.plot(ax = plt.gca(), grid = True)
    ax.set_title('Adjusted Close for all Stocks')
    
    if not plot: plt.close()
    else: plt.show()
    
    ###############################################
    #########    DescriptiveStatistics    #########
    ###############################################  
    log_returns = data_df.pct_change()
    
    fig, ax = plt.subplots(figsize=(22,11))
    log_returns.plot(ax = plt.gca(), grid = True)
    ax.set_title('Log Returns for all Stocks')
    ax.axhline(y = 0, color = "black", lw = 2)
    if not plot: plt.close()
    else: plt.show()
    
    APR = log_returns.groupby([log_returns.index.year], dropna=False).sum(numeric_only = None)
    APR_avg = APR.mean()
    
    N = np.array(list(tr_days_per_year.T.Stocks))[:, None]
    N_total = np.sum(N)
    APY = (1 + APR / N)**N - 1
    APY_avg = (1 + APR_avg / N_total)**N_total - 1
    
    STD = log_returns.groupby([log_returns.index.year], dropna=False).agg('std') * np.sqrt(252)
    STD_avg = STD.mean()
    
    fig, ax = plt.subplots(figsize = (22,11))
    STD.plot(ax = plt.gca(), grid = True)
    ax.set_title("$\sigma$ of all stocks for all years")
    ax.set_ylabel("$\sigma$") ; ax.set_xlabel("Year")
    if not plot: plt.close()
    else: plt.show()
    
    c = [y + x for y, x in zip(APY_avg, STD_avg)]
    c = list(map(lambda x : x / max(c), c))
    s = list(map(lambda x : x * 600   , c))
    
    fig, ax = plt.subplots(figsize = (18,11))
    ax.scatter(STD_avg, APY_avg, s = s , c = c , cmap = "Blues", alpha = 0.4, edgecolors = "grey")
    ax.grid()
    ax.axhline(y = 0, c = "plum") ; ax.axvline(x = 0, c = "plum")
    ax.set_title("Risk ($\sigma$) vs Return ($APY$) of all stocks")
    ax.set_xlabel("$\sigma$") ; ax.set_ylabel("Annualized Percetage Returns $APY$ or $R_{effective}$")
    for idx, instr in enumerate(list(STD.columns)):
        ax.annotate(instr, (STD_avg[idx] + 0.005, APY_avg[idx]))
    
    
    if not plot: plt.close()
    else: plt.show()
    
    ##############################################
    ##############    PortCreate    ##############
    ##############################################
    risk_free = float(web.DataReader('^IRX', data_source = 'yahoo', start = end, end = end)['Adj Close'])
    cov = APR.cov()
    
    portfolios = { "#2 optimized max sr (total)" : {"Return E[R]" : 0, "Risk σ" : 0, "Sharpe Ratio SR" : 0},
        "#2 optimized min σ (total)" : {"Return E[R]" : 0, "Risk σ" : 0, "Sharpe Ratio SR" : 0} }
    
    weights = np.array([1/len(log_returns.columns)] * len(log_returns.columns))
    num_portfolios = 2000
    generated_portfolios = [] # store the results
    for _ in range(num_portfolios) :
        # 1 - select random weights for portfolio holdings &  rebalance weights to sum to 1
        weights = np.array(np.random.random(len(APR.columns))) / np.sum(weights)  ######################### PROBLEM HERE #######################
        # 2 - calculate return, risk, sharpe ratio
        expected_return = np.sum(APR_avg * weights)
        expected_risk   = np.sqrt(np.dot(weights.T,np.dot(cov,weights)))
        sharpe_ratio    = (expected_return - risk_free) / expected_risk
        # 3 - store the result
        generated_portfolios.append([expected_return, expected_risk, sharpe_ratio, weights])
    
    maximum_sr_portfolio   = sorted(generated_portfolios, key = lambda x : -x[2])[0]
    minimum_risk_portfolio = sorted(generated_portfolios, key = lambda x : x[1])[0]
    max_sr = maximum_sr_portfolio[2]
    max_sr_weights   = pd.DataFrame(maximum_sr_portfolio[3],   index = log_returns.columns ,columns = ["Optimal Weights  #2 optimized max sr "]).T
    min_risk_weights = pd.DataFrame(minimum_risk_portfolio[3], index = log_returns.columns, columns = ["Optimal Weights  #2 optimized min σ "]).T
    
    # TOTAL PORTFOLIOS
    total_expected_return = 0.9 * maximum_sr_portfolio[0] + 0.1 * risk_free
    total_expected_risk   = 0.9 * maximum_sr_portfolio[1]
    portfolios["#2 optimized max sr (total)"]["Return E[R]"]     = total_expected_return
    portfolios["#2 optimized max sr (total)"]["Risk σ"]          = total_expected_risk
    portfolios["#2 optimized max sr (total)"]["Sharpe Ratio SR"] = (total_expected_return - risk_free) / total_expected_risk
    
    total_expected_return = 0.9 * minimum_risk_portfolio[0] + 0.1 * risk_free
    total_expected_risk   = 0.9 * minimum_risk_portfolio[1]
    portfolios["#2 optimized min σ (total)"]["Return E[R]"]      = total_expected_return
    portfolios["#2 optimized min σ (total)"]["Risk σ"]           = total_expected_risk
    portfolios["#2 optimized min σ (total)"]["Sharpe Ratio SR"]  = (total_expected_return - risk_free) / total_expected_risk
    
    
    if plot:
        plot_simulation(generated_portfolios, maximum_sr_portfolio, minimum_risk_portfolio, STD, STD_avg, APR_avg, s, c, CAL = None, INSTRUMENTS = None)
        cal_x = np.linspace(0.0, 0.5, 50) ; cal_y = risk_free + cal_x * max_sr
        plot_simulation(generated_portfolios, maximum_sr_portfolio, minimum_risk_portfolio, STD, STD_avg, APR_avg, s, c, CAL = [cal_x, cal_y] , INSTRUMENTS = 'yes')
    
    ##############################################
    ##############    UtilityOut    ##############
    ##############################################
    A = np.linspace(0, 10, 10)
    utility_max_sr   = portfolios["#2 optimized max sr (total)"]["Return E[R]"] - 1/2 * A * portfolios["#2 optimized max sr (total)"]["Risk σ"] ** 2
    utility_min_risk = portfolios["#2 optimized min σ (total)"]["Return E[R]"] - 1/2 * A * portfolios["#2 optimized min σ (total)"]["Risk σ"] ** 2
    
    fig, ax = plt.subplots(figsize = (22,11))
    ax.plot(A, [risk_free] * 10, color = 'green',  label = 'risk free', linewidth = 4)
    ax.plot(A, utility_max_sr,   color = 'plum',   label = 'Max SR Portfolio')
    ax.plot(A, utility_min_risk, color = 'orange', label = 'Min $\sigma$ Portfolio')

    ax.set_title('Utility Function $U=E(r)- 0.5 * A * \sigma^2$')
    ax.set_xlabel('Risk Aversion (A)') ; ax.set_ylabel('Utility (U)')
    ax.set_ylim([0, 0.4])
    ax.legend(labelspacing = 1.2)
    if not plot: plt.close()
    else: plt.show()
    
    ret       = portfolios["#2 optimized max sr (total)"]['Return E[R]']
    risk      = portfolios["#2 optimized max sr (total)"]['Risk σ']
    sr        = portfolios["#2 optimized max sr (total)"]['Sharpe Ratio SR']
    utility   = ret - 1/2 * 3 * risk ** 2

    portfolio = pd.DataFrame([str(round(ret * 100, 2)) + "%", str(round(risk * 100, 2)) + "%", sr, str(round(utility * 100, 2) ) + "%", cluster],
                             index = ['E[R]', '$\sigma$', 'Sharpe Ratio SR', 'Utility U', "cluster_id"],
                             columns = ["Max SR Portfolio"]).T
    
    return(portfolio)

port_clust_all = pd.concat([port_clust0,  port_clust1,  port_clust2,  port_clust3,  port_clust5,  port_clust6,  port_clust8,  port_clust9,
                            port_clust11, port_clust12, port_clust13, port_clust14, port_clust15, port_clust16, port_clust17, port_clust18, port_clust19,
                            port_clust20])#, port_clust4, port_clust7, port_clust10])
port_clust_all

def AssetSelection(instruments, start, end, plot):
    global tr_days_per_year, data_df
    
    tickers = list(instruments.keys())
    instruments_data = {}
    for ticker, instrument in instruments.items():
        try: instruments_data[ticker] = web.DataReader(ticker, data_source = 'yahoo', start = start, end = end)['Adj Close']
        except: pass
    
    
    tr_days = [] ; nums = []
    for ticker, instrument in instruments.items():
        try:
            tr_days.append(instruments_data[ticker].shape[0])
            nums.append(True)
        except: nums.append(False)
    tickers = [x for x, y in zip(tickers, nums) if y]
    tr_days = pd.DataFrame(tr_days, index = tickers, columns = ["Trading Days"])
    
    
    tr_days_per_year = instruments_data[tickers[0]].groupby([instruments_data[tickers[0]].index.year]).agg('count')
    tr_days_per_year = pd.DataFrame([tr_days_per_year], index = ["Stocks"])
    
    
    data = list(instruments_data.values())
    data_df = reduce(lambda x, y: pd.merge(x, y, left_index=True, right_index=True, how='outer'), data)
    data_df.columns = tickers
    
    
    fig, ax = plt.subplots(figsize=(22,11))
    data_df.plot(ax = plt.gca(), grid = True)
    ax.set_title('Adjusted Close for all Stocks')
    
    
    if not plot: plt.close()
    else: plt.show()

#AssetSelection(instruments, start = datetime.datetime(2018,1,1), end = datetime.datetime(2020,12,31), plot = True)

def DescriptiveStatistics(data_df, tr_days_per_year, plot):
    global log_returns, APR, STD, N, APR_avg, STD_avg, s, c
    
    log_returns = data_df.pct_change()
    
    
    fig, ax = plt.subplots(figsize=(22,11))
    log_returns.plot(ax = plt.gca(), grid = True)
    ax.set_title('Log Returns for all Stocks')
    ax.axhline(y = 0, color = "black", lw = 2)
    if not plot: plt.close()
    else: plt.show()
    
    
    APR = log_returns.groupby([log_returns.index.year], dropna=False).sum(numeric_only = None)
    APR_avg = APR.mean()
    
    
    N = np.array(list(tr_days_per_year.T.Stocks))[:, None]
    N_total = np.sum(N)
    APY = (1 + APR / N)**N - 1
    APY_avg = (1 + APR_avg / N_total)**N_total - 1
    
    
    STD = log_returns.groupby([log_returns.index.year], dropna=False).agg('std') * np.sqrt(252)
    STD_avg = STD.mean()
    
    
    fig, ax = plt.subplots(figsize = (22,11))
    STD.plot(ax = plt.gca(), grid = True)
    ax.set_title("$\sigma$ of all stocks for all years")
    ax.set_ylabel("$\sigma$") ; ax.set_xlabel("Year")
    if not plot: plt.close()
    else: plt.show()
    
    
    c = [y + x for y, x in zip(APY_avg, STD_avg)]
    c = list(map(lambda x : x / max(c), c))
    s = list(map(lambda x : x * 600   , c))
    
    fig, ax = plt.subplots(figsize = (18,11))
    ax.scatter(STD_avg, APY_avg, s = s , c = c , cmap = "Blues", alpha = 0.4, edgecolors = "grey")
    ax.grid()
    ax.axhline(y = 0, c = "plum") ; ax.axvline(x = 0, c = "plum")
    ax.set_title("Risk ($\sigma$) vs Return ($APY$) of all stocks")
    ax.set_xlabel("$\sigma$") ; ax.set_ylabel("Annualized Percetage Returns $APY$ or $R_{effective}$")
    for idx, instr in enumerate(list(STD.columns)):
        ax.annotate(instr, (STD_avg[idx] + 0.005, APY_avg[idx]))
    
    
    if not plot: plt.close()
    else: plt.show()

#DescriptiveStatistics(data_df, tr_days_per_year, plot = True)

def plot_simulation(CAL = None, INSTRUMENTS = None, plot = True) :
    fig, ax = plt.subplots(figsize = (22,11))
    ax.grid()
    
    ret, risk, sr = [x[0] for x in generated_portfolios], [x[1] for x in generated_portfolios], [x[2] for x in generated_portfolios]
    
    ax.scatter(risk, ret, c = sr, cmap = 'viridis', marker = 'o', s = 10, alpha = 0.5)
    ax.scatter(maximum_sr_portfolio[1], maximum_sr_portfolio[0], marker = (5,1), color = 'orange',   s = 700, label = 'Max SR Portfolio')
    ax.scatter(minimum_risk_portfolio[1], minimum_risk_portfolio[0], marker = (5,1), color = 'plum', s = 700, label = 'Min $\sigma$ Portfolio')

    if CAL:
        ax.plot(CAL[0], CAL[1], linestyle = '-', color = 'red', label = 'CAL')
    if INSTRUMENTS:
        ax.scatter(STD_avg, APR_avg, s = s , c = c , cmap = "Blues", alpha = 0.4, edgecolors = "grey", linewidth = 2)
        for idx, instr in enumerate(list(STD.columns)): ax.annotate(instr, (STD_avg[idx] + 0.01, APR_avg[idx]))
    
    ax.set_title('2000 Simulated Portfolios')
    ax.set_xlabel('Annualized Risk ($\sigma$)') ; ax.set_ylabel('Annualized Returns ($APR_{avg}$)')
    ax.legend(labelspacing = 1.2)
    
    
    if not plot: plt.close()
    else: plt.show()


def PortCreate(log_returns, start, end, plot):
    global generated_portfolios, maximum_sr_portfolio, minimum_risk_portfolio, portfolios, risk_free
    
    risk_free = float(web.DataReader('^IRX', data_source = 'yahoo', start = end, end = end)['Adj Close'])
    cov = APR.cov()
    
    
    portfolios = {
        "#2 optimized max sr (total)" : {"Return E[R]" : 0, "Risk σ" : 0, "Sharpe Ratio SR" : 0},
        "#2 optimized min σ (total)" : {"Return E[R]" : 0, "Risk σ" : 0, "Sharpe Ratio SR" : 0}
    }
    
    weights = np.array([1/len(log_returns.columns)] * len(log_returns.columns))
    num_portfolios = 2000
    generated_portfolios = [] # store the results
    for _ in range(num_portfolios) :
        # 1 - select random weights for portfolio holdings &  rebalance weights to sum to 1
        weights = np.array(np.random.random(len(APR.columns))) / np.sum(weights)  ######################### PROBLEM HERE #######################
        # 2 - calculate return, risk, sharpe ratio
        expected_return = np.sum(APR_avg * weights)
        expected_risk   = np.sqrt(np.dot(weights.T,np.dot(cov,weights)))
        sharpe_ratio    = (expected_return - risk_free) / expected_risk
        # 3 - store the result
        generated_portfolios.append([expected_return, expected_risk, sharpe_ratio, weights])
    
    maximum_sr_portfolio   = sorted(generated_portfolios, key = lambda x : -x[2])[0]
    minimum_risk_portfolio = sorted(generated_portfolios, key = lambda x : x[1])[0]
    max_sr = maximum_sr_portfolio[2]
    min_risk_weights = pd.DataFrame(minimum_risk_portfolio[3], index = log_returns.columns, columns = ["Optimal Weights  #2 optimized min σ "]).T
    
    
    # TOTAL PORTFOLIOS
    total_expected_return = 0.9 * maximum_sr_portfolio[0] + 0.1 * risk_free
    total_expected_risk   = 0.9 * maximum_sr_portfolio[1]
    portfolios["#2 optimized max sr (total)"]["Return E[R]"]     = total_expected_return
    portfolios["#2 optimized max sr (total)"]["Risk σ"]          = total_expected_risk
    portfolios["#2 optimized max sr (total)"]["Sharpe Ratio SR"] = (total_expected_return - risk_free) / total_expected_risk
    
    total_expected_return = 0.9 * minimum_risk_portfolio[0] + 0.1 * risk_free
    total_expected_risk   = 0.9 * minimum_risk_portfolio[1]
    portfolios["#2 optimized min σ (total)"]["Return E[R]"]      = total_expected_return
    portfolios["#2 optimized min σ (total)"]["Risk σ"]           = total_expected_risk
    portfolios["#2 optimized min σ (total)"]["Sharpe Ratio SR"]  = (total_expected_return - risk_free) / total_expected_risk

    
    plot_simulation(CAL = None, INSTRUMENTS = None, plot = plot)
    
    
    cal_x = np.linspace(0.0, 0.5, 50) ; cal_y = risk_free + cal_x * max_sr
    plot_simulation(CAL = [cal_x, cal_y] , INSTRUMENTS = 'yes', plot = plot)


#PortCreate(log_returns, start = datetime.datetime(2018,1,1), end = datetime.datetime(2020,12,31), plot = True)

def UtilityOut(plot, cluster):
    A = np.linspace(0, 10, 10)
    utility_max_sr   = portfolios["#2 optimized max sr (total)"]["Return E[R]"] - 1/2 * A * portfolios["#2 optimized max sr (total)"]["Risk σ"] ** 2
    utility_min_risk = portfolios["#2 optimized min σ (total)"]["Return E[R]"] - 1/2 * A * portfolios["#2 optimized min σ (total)"]["Risk σ"] ** 2
    
    
    fig, ax = plt.subplots(figsize = (22,11))
    ax.plot(A, [risk_free] * 10, color = 'green',  label = 'risk free', linewidth = 4)
    ax.plot(A, utility_max_sr,   color = 'plum',   label = 'Max SR Portfolio')
    ax.plot(A, utility_min_risk, color = 'orange', label = 'Min $\sigma$ Portfolio')

    ax.set_title('Utility Function $U=E(r)- 0.5 * A * \sigma^2$')
    ax.set_xlabel('Risk Aversion (A)') ; ax.set_ylabel('Utility (U)')
    ax.set_ylim([0, 0.4])
    ax.legend(labelspacing = 1.2)
    if not plot: plt.close()
    else: plt.show()
    
    ret       = portfolios["#2 optimized max sr (total)"]['Return E[R]']
    risk      = portfolios["#2 optimized max sr (total)"]['Risk σ']
    sr        = portfolios["#2 optimized max sr (total)"]['Sharpe Ratio SR']
    utility   = ret - 1/2 * 3 * risk ** 2

    portfolio = pd.DataFrame([str(round(ret * 100, 2)) + "%", str(round(risk * 100, 2)) + "%", sr, str(round(utility * 100, 2) ) + "%", cluster],
                             index = ['E[R]', '$\sigma$', 'Sharpe Ratio SR', 'Utility U', "cluster_id"],
                             columns = ["Max SR Portfolio"]).T
    return(portfolio)

#UtilityOut(plot = True)

def do_all(instruments, cluster, plot = False, start = datetime.datetime(2018,1,1), end = datetime.datetime(2020,12,31), data_df = data_df, tr_days_per_year = tr_days_per_year, log_returns = log_returns):
    AssetSelection(instruments, start, end, plot)
    DescriptiveStatistics(data_df, tr_days_per_year, plot)
    PortCreate(log_returns, start, end, plot)
    return(UtilityOut(plot, cluster))

#do_all(instruments = dict_cluster[0], cluster = 0, plot = True)
