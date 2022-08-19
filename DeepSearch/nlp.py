import pandas as pd
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


class NLP():

    def __init__(self,length=20000):

        """import data from MongoDB"""

        myclient = pymongo.MongoClient("mongodb+srv://lucas-deepen:DSIqP935gtFobYc2@cluster0.ixkyxa7.mongodb.net/?retryWrites=true&w=majority")
        mydb = myclient["cleanpapers"]
        mycol = mydb["cleanedf"]
        mydoc = mycol.find({}, {"_id":1,"abstract":1})
        self.df = pd.DataFrame(list(mydoc)).set_index(['_id'])
        self.length = length

        print('----------Data imported----------')

    def cleaning(self,text):

        """cleaning function for the abstract"""

        # transform abtract words into lower case

        text = text.lower()

        # remove punctuations

        for punctuation in string.punctuation:

            text = text.replace(punctuation,'')

        # remove digits

        text = ''.join(char for char in text if not char.isdigit())

        # tokenize sentences

        tokenized_text = word_tokenize(text)

        # remove stop words

        stop_words = set(stopwords.words('english'))


        tokenized_sentence_cleaned = [w for w in tokenized_text
                                    if not w in stop_words]

        # standardize verbs

        verb_lemmatized = [WordNetLemmatizer().lemmatize(word, pos = "v")
                for word in tokenized_sentence_cleaned]

        # standardize nouns

        noun_lemmatized = [WordNetLemmatizer().lemmatize(word, pos = "n")  # n --> nouns
                for word in verb_lemmatized]

        # re-join list into sentence

        cleaned_txt = " ".join(noun_lemmatized)

        return cleaned_txt

    def tokenize(self):

        """generate tokenized dataframe"""

        df = self.df

        # apply clean function to abstracts

        df.abstract = df.abstract.astype(str).apply(self.cleaning)

        print ('----------Abstract cleaned----------')

        # intitialize vectorizer model

        tfidf_vectorizer = TfidfVectorizer(use_idf=False,
                                   analyzer='word',
                                   stop_words='english',
                                   max_df=0.6,min_df=15,
                                   token_pattern=r'(?u)\b[A-Za-z]{4,}\b',
                                   max_features=10000)

        # fit_transform abstract

        tfidf_abstract = tfidf_vectorizer.fit_transform(df.abstract)

        # create data frame with columns names

        self.weighted_words = pd.DataFrame(tfidf_abstract.toarray(),
                 columns = tfidf_vectorizer.get_feature_names(),index=df.index)

        print ('----------Abstract tokenized----------')

        return self.weighted_words

    def rank(self,words=['brain','mouse','animal','image','vivo','injury','intravital','voltage','circuit','neuronal','multiphoton','optogenetics','preclinical']):

        """rank abstracts based on chosen words"""

        self.tokenize()

        # clean tokenized data frame

        selected_tokens = self.weighted_words[words].replace('',0).astype(float)

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

        self.df_rank = df_rank.loc[~(df_rank==0).all(axis=1)]

        print ('----------Abstracts ranked----------')

        return self.df_rank

if __name__ == '__main__':

    nlp = NLP()

    print(nlp.rank())
