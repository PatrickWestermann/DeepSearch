# making a frontend that can be run locally and allows userinput"
import streamlit as st
import pandas as pd
import requests
import nlp

#token = pd.read_csv('data/tokenized_df.csv')

st.markdown('''
Hey, here you can use our amazing research locator!

Just put in some topics you are interested in, we take up to 5!

''')
search_terms = st.text_area('Put in the words')

search_term_list = list(search_terms.split(" "))

article_count = st.slider('How many articles?', 1, 200, 1)
article_count

#list_of_results = nlp.rank(token, search_term_list)

st.map()
