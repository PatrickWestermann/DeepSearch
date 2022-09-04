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
import nlp2

from gensim.models import TfidfModel, LdaModel, CoherenceModel

from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer

from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.neighbors import NearestNeighbors
from sklearn.utils import parallel_backend

warnings.filterwarnings('ignore')

class LDA():

    def __init__(self,df,num_topics=8):
        self.df_ = df.copy() # df = Dataframe with cleaned abstract
        self.num_topics = num_topics
        self.texts = list(self.df_.clean_abstr.str.split())
        self.id2word = corpora.Dictionary(self.texts)
        self.corpus = [self.id2word.doc2bow(text) for text in self.texts]
        self.tfidf = TfidfModel(self.corpus)
        self.tfidf_corpus = [self.tfidf[self.corpus[c]] for c in range(len(self.corpus))]
        self.model = ''

    def model(self):
        model = LdaModel(
            corpus=self.tfidf_corpus,
            id2word=self.id2word,
            num_topics=self.num_topics,
            random_state=1,
            iterations=100
            )

        self.model = model

        return self.model

    def topics(self):
        shown_topics = self.model.show_topics(
            num_topics=self.num_topics,
            num_words=30,
            formatted=False
            )

        topics_ = [[word[0] for word in topic[1]] for topic in shown_topics]

        return topics_

    def abstract_topics(self):
        most_relevant_topics = self.model.get_document_topics(self.tfidf_corpus)
        max_scored_topic = list()
        for i in range(len(most_relevant_topics)):
            max_tuple = max(most_relevant_topics[i], key=lambda x:x[1])
            max_scored_topic.append(max_tuple[0])

        topics_df = pd.DataFrame(
            max_scored_topic,
            index=self.df_.index,
            columns=['topic_number']
            )

        topics_df['topic_name'] = topics_df['topic_number'].map({
            1: 'Cancer Research',
            2: 'Imaging Methods',
            3: 'Treatment of Illnesses',
            4: 'Genetic Varation',
            5: 'Vaccinations',
            6: 'Parasites and Cannabis',
            7: 'Impact of Drugs',
            8: 'Nanomedicine'
            })

        return topics_df

    def top_words(self):
        top10_words = {1:[],2:[],3:[],4:[],
                       5:[],6:[],7:[],8:[]}

        for i in range(len(self.model.print_topics())):
            sep1 = self.model.print_topics()[i][1].split(' + ')
            for b in range(len(sep1)):
                sep2 = sep1[b].replace('"','').split('*')
                top10_words[i+1].append(sep2[1])

        topwords = pd.DataFrame(top10_words)

        topwords.rename(columns={
            1: 'Parasites and Cannabis',
            2: 'Treatment of Illnesses',
            3: 'Impact of Drugs',
            4: 'Genetic Varation',
            5: 'Vaccinations',
            6: 'Cancer Research',
            7: 'Nanomedicine',
            8: 'Imaging Methods',
            },
            inplace=True)

        topwords.index = np.arange(1, len(topwords) + 1)

        return top10_words

if __name__=='__main__':
    data = nlp2.get_data()
    df = nlp2.dataframe(data)
    lda = LDA(df)
    model = lda.model()
    topics_ = lda.topics()
    abs_topics = lda.abstract_topics()
    top = lda.top_words()

    print(topics_)
    print(abs_topics)
    print(top)
