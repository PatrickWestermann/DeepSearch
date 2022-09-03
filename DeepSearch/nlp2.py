import pandas as pd
import numpy as np
import string
import warnings
import pymongo
import csv
import scispacy
import spacy
import gensim
import gensim.corpora as corpora

from gensim.models import TfidfModel, LdaModel, CoherenceModel

from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer

from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.neighbors import NearestNeighbors
from sklearn.utils import parallel_backend

nlp = spacy.load("en_core_sci_lg")
warnings.filterwarnings('ignore')

def get_data():
    """import data from MongoDB"""

    client = "mongodb+srv://lucas-deepen:DSIqP935gtFobYc2@cluster0.ixkyxa7.mongodb.net/?retryWrites=true&w=majority"
    myclient = pymongo.MongoClient(client)
    mydb = myclient["cleanpapers"]
    mycol = mydb["cleanedf"]
    mydoc = mycol.find({}, {"_id":1,
                            "articleTitle":1,
                            "abstract":1,
                            "pubDate":1,
                            "affiliations":1})

    print('----------Data imported----------')

    return mydoc

def dataframe(mydoc,length=132820):
    """convert mongodb data to dataframe (full = 132820 rows)"""
    # data to dataframe and limit length
    df = pd.DataFrame(list(mydoc)).set_index(['_id'])
    df = df[df.abstract != '.'].iloc[:length,:]
    # extract year from the pubDate column
    df['pubDate'] = df['pubDate'].str.extract(r'(\d{4})')

    print ('----------DataFrame created----------')
    print (df.head(15))

    return df

def cleaning(text):
    """cleaning function for the abstract"""
    # extract medical terms
    doc = nlp(text)
    doc_string = " ".join(str(a) for a in doc.ents)
    # transform abtract words into lower case
    words = doc_string.lower()
    # remove punctuations
    for punctuation in string.punctuation:
        words = words.replace(punctuation,'')
    # remove digits
    words = ''.join(char for char in words if not char.isdigit())
    # tokenize sentences
    tokenized_text = word_tokenize(words)
    # remove stop words
    stop_words = set(stopwords.words('english'))
    tokenized_sentence_cleaned = [w for w in tokenized_text
                                if not w in stop_words]
    # standardize verbs
    verb_lemmatized = [WordNetLemmatizer().lemmatize(word, pos = "v") # v --> verbs
            for word in tokenized_sentence_cleaned]
    # standardize nouns
    noun_lemmatized = [WordNetLemmatizer().lemmatize(word, pos = "n")  # n --> nouns
            for word in verb_lemmatized]
    # only words longer than 3 charachters:
    length_3 = [ word for word in noun_lemmatized if len(word) > 3 ]
    # re-join list into sentence
    cleaned_txt = " ".join(length_3)

    return cleaned_txt

def clean(df):
    """clean abstract"""
    df_ = df.copy()
    # apply clean function to abstracts
    df_.abstract = df_.abstract.astype(str).apply(cleaning)

    return df_

def tokenize(df):
    """generate tokenized dataframe"""
    # intitialize vectorizer model
    tfidf_vectorizer = TfidfVectorizer(use_idf=True,
                                       analyzer='word',
                                       stop_words='english')
    # fit_transform abstract
    tfidf_abstract = tfidf_vectorizer.fit_transform(df.abstract)
    # create data frame with columns names
    weighted_words = pd.DataFrame(tfidf_abstract.toarray(),
                columns = tfidf_vectorizer.get_feature_names(),index=df.index).round(2)

    print ('----------Abstract tokenized----------')
    print (weighted_words.head(15))

    return weighted_words

def rank(token,words=['brain','mouse','animal',
                      'image','vivo','injury',
                      'intravital','voltage',
                      'circuit','neuronal',
                      'multiphoton','optogenetics',
                      'preclinical']):

    """rank abstracts based on chosen words"""

    token_df = token.copy()

    # clean tokenized data frame

    selected_tokens = token_df[words].replace('',0).astype(float)

    # remove rows with only 0 results

    selected_tokens = selected_tokens.loc[~(selected_tokens==0).all(axis=1)]

    # create count columns (1 - chosen word was encountered / 0 - chosen word was not encountered)

    columns = selected_tokens.columns

    length_words = selected_tokens.shape[1]

    for index, row in selected_tokens.iterrows():

        for column in columns:

            new_column = f'{column}_count'

            if row[column] > 0:

                selected_tokens.loc[index, new_column] = 1

            elif row[column] == 0:

                selected_tokens.loc[index, new_column] = 0

    # get average frequency of chosen words

    selected_tokens['mean'] = (selected_tokens[list(columns)].sum(axis=1)) / length_words

    # get how many chosen words were encountered

    selected_tokens['count'] = selected_tokens.iloc[: , length_words:-1].sum(axis=1)

    # sort rank by 1. how many chosen words were encountered and 2. average frequency of chosen words

    df_rank = selected_tokens.sort_values(by=['count','mean'],ascending=False)[['count','mean']]

    # remove rows with only 0 values

    df_rank = df_rank.loc[~(df_rank==0).all(axis=1)]

    print ('----------Abstracts ranked----------')

    print (df_rank.head(15))

    return df_rank

def clean_abstracts():
    "generate cleaned abstracts"
    data = get_data()
    df = dataframe(data)
    clean_abstract = clean(df)

    return clean_abstract

def main():

    mydoc = get_data()
    df = dataframe(mydoc)
    token = tokenize(df)

    return token


if __name__ == '__main__':

    clean_abstracts()
