import requests
import pandas as pd
import numpy as np
import xmltodict
import timeit

api_key = "857be3304fd504d7d4901fc8a7d12d221408"

query = input('Give me terms: ').replace(' ','+')
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
count_test = int(input('Give me how many: '))

output_list = []
article_list = []
start = timeit.timeit()
bad_lines_counter = 0

while retstart < count_test:
    efetch_url = base + "efetch.fcgi?db=pubmed&WebEnv="+web+"&query_key="+key+"&retstart="+str(retstart)+"&retmax="+str(retmax)+"&rettype=xml&retmode=xml"+"&api_key"+api_key
    output = requests.get(efetch_url)
    output_list.append(xmltodict.parse(output.content))
    retstart += retmax

end = timeit.timeit()
print(end - start)

for article_set in output_list:
    try:
        divided_dict = article_set['PubmedArticleSet']['PubmedArticle']
    except:
        print("last set had too many failures to process. Breaking FOR Loop")
        break
    for i in range(len(divided_dict)):
        try:
            publication_date = divided_dict[i]['MedlineCitation']['Article']['Journal']['JournalIssue']['PubDate']
            ## abstract:
            try:
                abstract = divided_dict[i]['MedlineCitation']['Article']['Abstract']['AbstractText']['#text']
            except:
                abstract = divided_dict[i]['MedlineCitation']['Article']['Abstract']['AbstractText']
                if isinstance(abstract,list):
                    grouped_texts= ''
                    for subabstract in abstract:
                        grouped_texts += subabstract['#text'] + " "
                    abstract = grouped_texts

            try:
                volume= divided_dict[i]['MedlineCitation']['Article']['Journal']['JournalIssue']['Volume']
            except:
                volume = '1'
            ## Title
            try:
                articletitle = divided_dict[i]['MedlineCitation']['Article']['ArticleTitle']
            except:
                articletitle = 'No title'
            # authors list
            names = ''

            try:
                authors = divided_dict[10]['MedlineCitation']['Article']['AuthorList']['Author']
            except:
                authors = 'None'
                names = authors
            if authors!= "None":
                for j in range(len(authors)):
                    try:
                        fullname = authors[j]['LastName'] + ' ' + authors[j]['ForeName']
                        affiliationinfo = authors[j]['AffiliationInfo']
                        if isinstance(affiliationinfo, list):
                            for aff in affiliationinfo:
                                location = aff['Affiliation']
                        else:
                            location = affiliationinfo['Affiliation']
                        if j==0:
                            names = fullname
                            affiliations = location
                        else:
                            names+= ", " + fullname
                            affiliations+= ", " + location
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
            ## citations
            try:
                citations = len(divided_dict[i]['PubmedData']['ReferenceList']['Reference'])
            except:
                citations = 0

            ## keywords

            secondary_keywords = 'None'
            try:
                keys = divided_dict[i]['MedlineCitation']['KeywordList']['Keyword']
                for key in range(len(keys)):
                    if key == 0:
                        secondary_keywords = keys[key]['#text']
                    else:
                        secondary_keywords += ', ' + keys[key]['#text']
            except:
                pass
            # Is review article?
            pubtypes = divided_dict[i]['MedlineCitation']['Article']['PublicationTypeList']['PublicationType']
            isreview = 0
            if isinstance(pubtypes,list):
                for elem in pubtypes:
                    if "Review" in elem['#text']:
                        isreview = 1
                        break
            else:
                for key,value in pubtypes.items():
                    if "Review" in value:
                        isreview = 1
                        break
            article = {
                'PMID':divided_dict[i]['MedlineCitation']['PMID']['#text'],
                'abstract':abstract,
                'articleTitle': articletitle,
                'Journaltitle':divided_dict[i]['MedlineCitation']['Article']['Journal']['Title'],
                'volume':volume,
                'pubDate':''.join([f'{value}-' for key, value in divided_dict[i]['MedlineCitation']['Article']['Journal']['JournalIssue']['PubDate'].items()])[:-1],
                'authors': names,
                'MeSh':keywords,
                'citations':citations,
                'affiliations':affiliations,
                'keywords': secondary_keywords,
                'IsReviewArticle':isreview
            }
            article_list.append(article)
        except:
            bad_lines_counter += 1
print("there were ",bad_lines_counter," line failures, we are ommiting them from the output")
df = pd.DataFrame(article_list)
name = "data/" + query.replace("+","_") + "_data_" + str(count_test) + ".csv"
df.to_csv(name)
