#!/usr/bin/python
import pandas as pd
from numba import jit, cuda
from timeit import default_timer as timer
import numpy as np
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
import string
import warnings
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.neighbors import NearestNeighbors
import dtale
import pymongo
import csv
import scispacy
import spacy
from sklearn.utils import parallel_backend
nlp = spacy.load("en_core_sci_lg")
warnings.filterwarnings('ignore')


class NLP:
    """Preparing data for natural language processing via scispacy model"""

    def __init__(self):
        print("NPL functions: ")
        print("     get_data()")
        print("     dataframe(data, length=132820)")
        print("     cleaning(text)")
        print("     cleaning_ginsem(text)")
        print("     clean(df)")
        print("     clean_ginsem(df)")
        print("     tokenize(df)")
        print(" Use saved weighted_words.csv to enter LDA ")

    def get_data(self):
        """import data from MongoDB"""

        self.myclient = pymongo.MongoClient(
            "mongodb+srv://lucas-deepen:DSIqP935gtFobYc2@cluster0.ixkyxa7.mongodb.net/?retryWrites=true&w=majority")
        self.mydb = self.myclient["cleanpapers"]
        self.mycol = self.mydb["cleanedf"]
        self.mydoc = self.mycol.find({}, {"_id": 1, "articleTitle": 1,
                                          "abstract": 1, "pubDate": 1, "affiliations": 1})

        print('----------Data imported----------')

        return self.mydoc

    def dataframe(self, mydoc, length=132820):
        """convert mongodb data to dataframe (full = 132820 rows)"""
        self.mydoc = mydoc
        self.length = length
        # data to dataframe and limit length
        self.df = pd.DataFrame(list(self.mydoc)).set_index(['_id'])
        self.df = self.df[self.df.abstract != '.'].iloc[:self.length, :]

        # extract year from the pubDate column
        self.df['pubDate'] = self.df['pubDate'].str.extract(r'(\d{4})')

        print('----------DataFrame created----------')
        print(self.df.head(15))
        return self.df

    @jit(target_backend='cuda')
    def cleaning(self, text):
        """cleaning function for the abstract"""
        self.text = text
        # extract medical terms
        self.doc = nlp(self.text)

        self.doc_string = " ".join(str(a) for a in self.doc.ents)

        # transform abtract words into lower case
        self.words = self.doc_string.lower()

        # remove punctuations
        for punctuation in string.punctuation:
            self.words = self.words.replace(punctuation, '')

        # remove digits
        self.words = ''.join(char for char in self.words if not char.isdigit())

        # tokenize sentences
        self.tokenized_text = word_tokenize(self.words)

        # remove stop words
        self.stop_words = set(stopwords.words('english'))

        self.tokenized_sentence_cleaned = [
            w for w in self.tokenized_text if not w in self.stop_words
        ]

        # standardize verbs
        self.verb_lemmatized = [
            WordNetLemmatizer().lemmatize(word, pos="v") for word in self.tokenized_sentence_cleaned
        ]

        # standardize nouns
        self.noun_lemmatized = [
            WordNetLemmatizer().lemmatize(word, pos="n") for word in self.verb_lemmatized
        ]

        # only words longer than 3 charachters:
        self.length_3 = [
            word for word in self.noun_lemmatized if len(word) > 3]

        # re-join list into sentences
        self.cleaned_txt = " ".join(length_3)

        self.cleaned_txt.to_csv(
                    'cleaned_txt.csv', encoding='utf-8', index=False)

        print('-----Saved as cleaned_txt.csv------')

        return self.cleaned_txt

    @jit(target_backend='cuda')
    def tokenize(self, df):
        """generate tokenized dataframe"""
        self.df = df
        # intitialize vectorizer model
        self.tfidf_vectorizer = TfidfVectorizer(use_idf=True,
                                                analyzer='word',
                                                stop_words='english',
                                                max_df=0.6, min_df=0.01)  # ,
        #max_features=10000)

        # fit_transform abstract
        self.tfidf_abstract = self.tfidf_vectorizer.fit_transform(
            self.df.abstract)

        # create data frame with columns names
        self.weighted_words = pd.DataFrame(self.tfidf_abstract.toarray(),
                                           columns=self.tfidf_vectorizer.get_feature_names(), index=self.df.index).round(2)

        print('----------Abstract tokenized----------')

        print(self.weighted_words.head(15))

        self.weighted_words.to_csv(
            'weighted_words.csv', encoding='utf-8', index=False)

        print('-----Saved as weighted_words.csv------')

        return self.weighted_words

    def print_to_file(self, message, log_file='nlp_gpu_output.txt'):
        print(message)
        with open(log_file, 'a') as of:
            of.write(message + '\n')


if __name__ == "__main__":
    nlp_prep = NLP()

    data = nlp_prep.get_data()

    df = nlp_prep.dataframe(data)

    start = timer()
    clean_abstract = nlp_prep.clean(df)
    time = timer()-start
    message = f"Cleaning with GPU: {time}"
    nlp_prep.print_to_file(message)

    start = timer()
    token = nlp_prep.tokenize(clean_abstract)
    time = timer()-start
    message = f"Tokenize with GPU: {time}"
    nlp_prep.print_to_file(message)
