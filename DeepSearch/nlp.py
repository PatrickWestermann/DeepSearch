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

    def __init__(self,length=50000,words=['brain','mouse','animal','image','vivo','injury','intravital','voltage','circuit','neuronal','multiphoton','optogenetics','preclinical']):

        myclient = pymongo.MongoClient("mongodb+srv://lucas-deepen:DSIqP935gtFobYc2@cluster0.ixkyxa7.mongodb.net/?retryWrites=true&w=majority")
        mydb = myclient["papers"]
        mycol = mydb["researchpapers"]
        mydoc = mycol.find({}, {"_id":1,"PMID":1,"abstract":1})
        self.df = pd.DataFrame(list(mydoc))
        self.length = length
        self.words = words

    def precleaning(self):

        pre_clean_1 = self.df.dropna(subset=['abstract']).astype(str).set_index(['_id','PMID'])
        pre_clean_2 = pre_clean_1[~pre_clean_1.abstract.str.contains("{",na=False)]
        self.df_length = pre_clean_2[pre_clean_2.abstract != '.'].iloc[:self.length,:]

        return self.df_length

    def cleaning(self,text):

        text = text.lower()

        for punctuation in string.punctuation:

            text = text.replace(punctuation,'')

        text = ''.join(char for char in text if not char.isdigit())

        tokenized_text = word_tokenize(text)
        stop_words = set(stopwords.words('english'))
        tokenized_sentence_cleaned = [w for w in tokenized_text
                                    if not w in stop_words]

        verb_lemmatized = [WordNetLemmatizer().lemmatize(word, pos = "v")
                for word in tokenized_sentence_cleaned]

        noun_lemmatized = [WordNetLemmatizer().lemmatize(word, pos = "n")  # n --> nouns
                for word in verb_lemmatized]

        cleaned_txt = " ".join(noun_lemmatized)

        return cleaned_txt

    def tokenize(self):

        self.precleaning()

        df_length = self.df_length

        df_length.abstract = df_length.abstract.astype(str).apply(self.cleaning)

        tfidf_vectorizer = TfidfVectorizer(use_idf=False,
                                   analyzer='word',
                                   stop_words='english',
                                   max_df=0.6,min_df=15,
                                   token_pattern=r'(?u)\b[A-Za-z]{4,}\b',
                                   max_features=10000)

        tfidf_abstract = tfidf_vectorizer.fit_transform(df_length.abstract)

        self.weighted_words = pd.DataFrame(tfidf_abstract.toarray(),
                 columns = tfidf_vectorizer.get_feature_names(),index=df_length.index)

        return self.weighted_words

    def rank(self):

        self.tokenize()

        selected_tokens = self.weighted_words[self.words].replace('',0).astype(float)

        selected_tokens = selected_tokens.loc[~(selected_tokens==0).all(axis=1)]

        columns = selected_tokens.columns

        length_words = selected_tokens.shape[1]

        for index, row in selected_tokens.iterrows():

            for column in columns:

                new_column = f'{column}_count'

                if row[column] > 0:

                    selected_tokens.loc[index, new_column] = 1

                elif row[column] == 0:

                    selected_tokens.loc[index, new_column] = 0

        selected_tokens['mean'] = (selected_tokens[list(columns)].sum(axis=1)) / length_words

        selected_tokens['count'] = selected_tokens.iloc[: , length_words:-1].sum(axis=1)

        df_rank = selected_tokens.sort_values(by=['count','mean'],ascending=False)[['count','mean']]

        self.df_rank = df_rank.loc[~(df_rank==0).all(axis=1)]

        return self.df_rank

if __name__ == '__main__':

    nlp = NLP()

    nlp.rank()
