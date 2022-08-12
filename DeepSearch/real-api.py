import requests
import pandas as pd
import numpy as np
import xmltodict
import timeit

api_key = "857be3304fd504d7d4901fc8a7d12d221408"

query = 'mouse+brain+imaging'
base = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/'
url = base + "esearch.fcgi?db=pubmed&term=" + query + "&usehistory=y"

#execute the esearch URL to create a list of all search results
#store it online behind the query key
output_esearch = requests.get(url)
dict_output_esearch = xmltodict.parse(output_esearch.content)

#parse WebEnv, QueryKey and Count (# records retrieved)
web = dict_output_esearch['eSearchResult']['WebEnv']
key = dict_output_esearch['eSearchResult']['QueryKey']
count = dict_output_esearch['eSearchResult']['Count']

retmax = 50
retstart = 0
count_test = 500

output_list = []
article_list = []
start = timeit.timeit()

while retstart < count_test:
    efetch_url = base + "efetch.fcgi?db=pubmed&WebEnv="+web+"&query_key="+key+"&retstart="+str(retstart)+"&retmax="+str(retmax)+"&rettype=xml&retmode=xml"+"&api_key"+api_key
    output = requests.get(efetch_url)
    output_list.append(xmltodict.parse(output.content))
    retstart += retmax

end = timeit.timeit()
print(end - start)

for article_set in output_list:
    divided_dict = article_set['PubmedArticleSet']['PubmedArticle']
    for i in range(len(divided_dict)):
        publication_date = divided_dict[i]['MedlineCitation']['Article']['Journal']['JournalIssue']['PubDate']
        ## abstract:
        try:
            abstract = divided_dict[i]['MedlineCitation']['Article']['Abstract']['AbstractText']['#text']
        except:
            abstract = divided_dict[i]['MedlineCitation']['Article']['Abstract']['AbstractText']

        try:
            volume= divided_dict[i]['MedlineCitation']['Article']['Journal']['JournalIssue']['Volume']
        except:
            volume = '1'

        # authors list
        names = ''
        try:
            authors = divided_dict[i]['MedlineCitation']['Article']['AuthorList']['Author']
        except:
            authors = 'None'
            names = authors
        if authors!= "None":

            for j in range(len(authors)):
                try:
                    fullname = authors[j]['LastName'] + ' ' + authors[j]['ForeName']
                    if j==0:
                        names = fullname
                    else:
                        names+= ", " + fullname
                except:
                    pass
        ## Mesh Words
        try:
            meshs = divided_dict[i]['MedlineCitation']['MeshHeadingList']['MeshHeading']
        except:
            meshs = "None"

        keywords = ''
        if meshs!="None":
            for mesh in range(len(meshs)):
                if mesh == 0:
                    keywords+= meshs[mesh]['DescriptorName']['#text']
                else:
                    keywords+= ", " + meshs[mesh]['DescriptorName']['#text']
        else:
            keywords="None"
        article = {
            'PMID':divided_dict[i]['MedlineCitation']['PMID']['#text'],
            'abstract':abstract,
            'title':divided_dict[i]['MedlineCitation']['Article']['Journal']['Title'],
            'volume':volume,
            'pubDate':''.join([f'{value}-' for key, value in divided_dict[i]['MedlineCitation']['Article']['Journal']['JournalIssue']['PubDate'].items()])[:-1],
            'authors': names,
            'MeSh':keywords
        }
        article_list.append(article)

pd.DataFrame(article_list)
