# making a frontend that can be run locally and allows userinput"
import streamlit as st
import pandas as pd
import requests

st.markdown('''
Hey, here you can use our amazing research locator
''')

search_terms = st.text_area('Put in topics you are interested in!')

article_count = st.slider('How many articles?', 1, 200, 1)
