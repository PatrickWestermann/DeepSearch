from bs4 import BeautifulSoup
import requests
import urllib.request, urllib.parse, urllib.error
import ssl
import json
import calendar
import pandas as pd
from functools import partial
from geopy.geocoders import Nominatim

url="https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&retmode=json&retmax=NUM&sort=relevance&term=KEYWORD"

geolocator = Nominatim(user_agent="denem")

# We ask the user to provide the keyword and number of results and subsequently replace these elements in the url string
keyword = str(input('Please enter the keyword '))
num = int(input('Please enter the number of results '))
url = url.replace('NUM', str(num))
url = url.replace('KEYWORD', keyword)

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    # Legacy Python that doesn’t verify HTTPS certificates by default
    pass
else:
    # Handle target environment that doesn’t support HTTPS verification
    ssl._create_default_https_context = _create_unverified_https_context

webpage = urllib.request.urlopen(url).read()
dict_page =json.loads(webpage)
idlist = dict_page["esearchresult"]["idlist"]



# We create a function to delete brackets from titles
def strip_brackets(s):
    # initialization of string to ""
    no_bracktes = ""
    dont_want = ['[',']']
    # traverse in the string
    for char in s:
        if char not in dont_want:
            no_bracktes += char
    # return string
    return no_bracktes


# We create a function which takes the soup and extracts all needed elements for the bibliography and abstract
# Example output: A. Bester, R. Zelazny, and H. Ellison, “On the Role of Viruses in Future Epidemics,” Journal of Irreproducible Results 3(4) pp. 29–35 (Mar. 2103). PUBMED: 23456789; DOI 12.1119/2847595.

def get_bibliography(soup):

    # This function creates a empty variable for each needed element and subsequently fills in the true value if it exists

    article = soup.find('article')
    journal = soup.find('journal')
    citations = ''
    meshs = ''
    authorlist = article.find('authorlist')
    try:
        citations = soup.find('pubmeddata').find("referencelist").find_all("reference")
    except:
        pass

    citation_list = ''

    if citations!= '':
        number_citations = len(citations)
        for i in range(len(citations)):
            citation = citations[i].find('citation').text
            if i == 0:
                citation_list += citation
            else:
                citation_list += ", " + citation
    else:
        number_citations = 0
        citation_list = "No citations"
    mesh_words = ''
    mesh_ids = ''
    try:
        meshs = soup.find('meshheadinglist').find_all('meshheading')
    except:
        pass
    if meshs!='':
        for i in range(len(meshs)):
            word = meshs[i].find('descriptorname').text
            attributes = meshs[i].find("descriptorname").attrs
            meshid = attributes['ui']
            majortopic = attributes['majortopicyn']
            if i == 0:
                mesh_words += word
                mesh_ids += meshid + ", Major topic: " + majortopic
            else:
                mesh_words += ", " + word
                mesh_ids += ", " + meshid + ", Major topic: " + majortopic

    authors = ""
    if authorlist:
        for i in range(len(authorlist.find_all('lastname'))):
            initial = authorlist.find_all('initials')[i].text
            authors+= initial
            authors+= '. '
            last_name = authorlist.find_all('lastname')[i].text
            authors+= last_name
            if i == len(authorlist.find_all('lastname'))-2:
                authors += ' and '
            elif i != len(authorlist.find_all('lastname'))-1:
                authors += ', '
        authors += ", "

    geocodes = ''
    try:
        geoaffiliations = authorlist.find_all("affiliation")
        if geoaffiliations:
            for i in range(len(geoaffiliations)):
                geosplit = geoaffiliations[i].text.split()
                city,country = geosplit[-2],geosplit[-1]
                location = geolocator.geocode(city + " " +country)
                lat,lon = location.latitude, location.longitude
                if i == 0:
                    geocodes += str(lat) + ", " + str(lon)
                else:
                    geocodes += "; " + str(lat) + ", " + str(lon)
    except:
        pass

    ArticleTitle = ''
    if article.find('articletitle'):
            ArticleTitle = '"'
            title_str = article.find('articletitle').text
            title_str = strip_brackets(title_str)
            ArticleTitle += title_str
            # If that is in the title, please leave it and put the comma after the quotation marks. - Professor Bishop
            if ArticleTitle[-1] == '.':
                ArticleTitle += '", '
            else:
                ArticleTitle += '," '

    volume = ''
    if journal.find('volume'):
        volume = journal.find('volume').text
        if soup.find('issue'):
            volume += '('
            volume += soup.find('issue').text
            volume += ')'
        volume += ' '

    page = ''
    if article.find('pagination'):
        if '-' in article.find('pagination').text:
            page = 'pp. '
            page_str = article.find('pagination').text
            page_str = page_str.strip('\n')
            page += page_str
            page += ' '
        else:
            page = 'p. '
            page_str = article.find('pagination').text
            page_str = page_str.strip('\n')
            page += page_str
            page += ' '

    journal_title = ''
    if journal.find('title'):
        journal_title = journal.find('title').text
        journal_title += ' '

    JournalIssue = journal.find('journalissue')

    month = JournalIssue.find('month')
    date = ''
    if month:
        month = JournalIssue.find('month').text
        if len(month)<3:
            month_int = int(str(month))
            month = calendar.month_abbr[month_int]

        year = JournalIssue.find('year').text
        date = '('
        date += month
        date += '. '
        date += year
        date += '). '
    elif JournalIssue.find('year'):
        date = '('
        date+= JournalIssue.find('year').text
        date += '). '
    else: ''

    pubmed = ''
    if soup.find('articleid'):
        pubmed = 'PUBMED: '
        pubmed += soup.find('articleid').text
        pubmed += '; '
        doi_pii = article.find_all('elocationid')
        doi_pii_str = ""
        if len(doi_pii)>1:
            if 'doi' in str(doi_pii[0]):
                doi_pii = doi_pii[0].text
                doi_pii_str += "DOI "
                doi_pii_str += doi_pii
                doi_pii_str += "."
            elif 'doi' in str(doi_pii[1]):
                doi_pii = doi_pii[1].text
                doi_pii_str += "DOI "
                doi_pii_str += doi_pii
                doi_pii_str += "."
        elif len(doi_pii) == 1:
            if 'doi' in str(doi_pii[0]):
                doi_pii = doi_pii[0].text
                doi_pii_str += "DOI "
                doi_pii_str += doi_pii
                doi_pii_str += "."
            elif 'pii' in str(doi_pii[0]):
                doi_pii = doi_pii[0].text
                doi_pii_str += "PII "
                doi_pii_str += doi_pii
                doi_pii_str += "."

    abstract = ''
    if article.find('abstracttext'):
        abstract = article.find('abstracttext').text

    result = [authors,ArticleTitle,journal_title,volume,date,pubmed,doi_pii_str,abstract,number_citations,mesh_words,mesh_ids,geocodes]
    return result


articles_list = []

# We loop over each element in the idlist to get the soup and feed it into our function
for link in idlist:
    url = "http://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&retmode=xml&id=idlist"
    url = url.replace('idlist', link)

    try:
        _create_unverified_https_context = ssl._create_unverified_context
    except AttributeError:
        # Legacy Python that doesn’t verify HTTPS certificates by default
        pass
    else:
        # Handle target environment that doesn’t support HTTPS verification
        ssl._create_default_https_context = _create_unverified_https_context

    r = requests.get(url)
    soup = BeautifulSoup(r.content, "html.parser")
    article = get_bibliography(soup)
    articles_list.append(article)

df = pd.DataFrame(articles_list)
df.columns = ['authors', 'ArticleTitle', 'journal_title', 'volume', 'date', 'pubmed', 'doi_pii_str', 'Abstract','Citations', 'MeSH Words','MeSH ids','Geocodes']
file_name = keyword + '_' + str(num) + '.csv'
df.to_csv("data/" + file_name)
