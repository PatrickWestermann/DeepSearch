# making a frontend that can be run locally and allows userinput"
from lib2to3.pgen2 import token
import streamlit as st
import pandas as pd
import nlp
import timeit
import matplotlib.pyplot as plt
from wordcloud import WordCloud

st.header('Prototype Research Locator')
#token = pd.read_csv('data/tokenized_df.csv')
st.markdown('''
Hey, here you can use our amazing research locator!
First you need to wait a bit until the most recent dataset is pulled from the mongo db cloud! This might take a minute but your data will be nice and fresh ;)
''')
#loads the dataframe when the session is started
start = timeit.timeit()
@st.cache(allow_output_mutation=True)
def get_df():
    mydoc = nlp.get_data()
    df = nlp.dataframe(mydoc)
    tokenized_df = nlp.tokenize(df)
    return df, tokenized_df
df, tokenized_df = get_df()
end = timeit.timeit()
st.write(f"It took {end - start} to load the data")

#let the user put in the search terms and start ranking process
st.markdown('''
Okay, you got access to our database. Now, give us some medical or neuroscience related topics you are interested and see where in the world they are researched!
''')
search_terms = st.text_area('Put in the words, sparate them with an empty space')
search_term_list = list(search_terms.split(" "))
load = st.button("Let's find the most relevant articles!")


#initialize session state
if "load_state" not in st.session_state:
    st.session_state.load_state = False
#ranking
if load or st.session_state.load_state:
    st.session_state.load_state = True
    @st.cache(allow_output_mutation=True)
    def load_ranked_df():
        ids_ranked = nlp.rank(tokenized_df, words = search_term_list)
        return ids_ranked
    ids_ranked = load_ranked_df()
    article_count = st.slider('How many articles should be displayed?', 1, 2000, 1)

   #ids_ranked[:article_count]
    df_ranked = df.loc[ids_ranked[:article_count].index]
    df_ranked

st.map(df_ranked[[float('lat'), float('lat')]])

#making a cloud of words
data = df_ranked.iloc[0]['abstract']
fig = plt.subplots(figsize = (8,8))
wordcloud = WordCloud(
                    background_color = 'white',
                    width = 512,
                    height = 384
                        ).generate(data)
plt.imshow(wordcloud) # image show
plt.axis('off')
st.pyplot(fig[0])
