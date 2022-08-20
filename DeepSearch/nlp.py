import pandas as pd
import numpy as np
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
import string
from sklearn.feature_extraction.text import TfidfVectorizer
import pymongo

def get_data():

    """import data from MongoDB"""

    myclient = pymongo.MongoClient("mongodb+srv://lucas-deepen:DSIqP935gtFobYc2@cluster0.ixkyxa7.mongodb.net/?retryWrites=true&w=majority")
    mydb = myclient["cleanpapers"]
    mycol = mydb["cleanedf"]
    mydoc = mycol.find({}, {"_id":1,"articleTitle":1,"abstract":1,"pubDate":1,"affiliations":1})

    print('----------Data imported----------')

    return mydoc

def cleaning(text):

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

def dataframe(mydoc,length=132820):

    """convert mongodb data to dataframe"""

    # data to dataframe and limit length

    df = pd.DataFrame(list(mydoc)).set_index(['_id'])

    df = df[df.abstract != '.'].iloc[:length,:]

    # extract year from the pubDate column

    df['pubDate']=df['pubDate'].str.extract(r'(\d{4})')

    print ('----------DataFrame created----------')

    print (df.head(15))

    return df

def tokenize(df):

    """generate tokenized dataframe"""

    df_ = df.copy()

    # apply clean function to abstracts

    df_.abstract = df_.abstract.astype(str).apply(cleaning)

    print ('----------Abstract cleaned----------')

    # intitialize vectorizer model

    tfidf_vectorizer = TfidfVectorizer(use_idf=False,
                                analyzer='word',
                                stop_words='english',
                                max_df=0.6,min_df=15,
                                token_pattern=r'(?u)\b[A-Za-z]{4,}\b',
                                max_features=10000)

    # fit_transform abstract

    tfidf_abstract = tfidf_vectorizer.fit_transform(df_.abstract)

    # create data frame with columns names

    weighted_words = pd.DataFrame(tfidf_abstract.toarray(),
                columns = tfidf_vectorizer.get_feature_names(),index=df_.index).round(2)

    print ('----------Abstract tokenized----------')

    print (weighted_words.head(15))

    return weighted_words

def rank(token,words=['brain','mouse','animal','image','vivo','injury','intravital','voltage','circuit','neuronal','multiphoton','optogenetics','preclinical']):

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

def main():

    mydoc = get_data()
    df = dataframe(mydoc)
    token = tokenize(df)
    ranked = rank(token)

    return ranked


if __name__ == '__main__':

    main()
