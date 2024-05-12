import requests
from bs4 import BeautifulSoup
import csv
import re
"""from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer"""

from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

#===================
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 5, 11),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

#================Extracting Data via Web Scraping=================
sources = ['https://www.dawn.com/', 'https://www.bbc.com/'] 
def extract_dawn():
    reqs = requests.get(sources[0])
    soup = BeautifulSoup(reqs.text, 'html.parser')
    urls = []
    #extracting links from the main page
    for link in soup.find_all('a'):
        urls.append(link.get('href'))

    #extracting titles and descriptions from articles    
    titles = []
    descriptions = []
        
    for article in soup.find_all('article'):
        title = article.find('a', class_ = 'story__link')
        if title:
            titles.append(title.text)

        description = article.find('div', class_='story__excerpt')
        if description:
            descriptions.append(description.text)
           
        else:
            descriptions.append("None")  

    for title, description in zip(titles, descriptions):
        print(f"{title}, {description}")   

    return titles, descriptions

def extract_bbc():
    reqs = requests.get(sources[1])
    soup = BeautifulSoup(reqs.text, 'html.parser')
    urls = []
    #extracting links from the main page
    for link in soup.find_all('a'):
        urls.append(link.get('href'))

    #extracting titles and descriptions from articles    
    titles = []
    descriptions = []
        
    for article in soup.find_all('div'):
        title = [title.text for title in article.find_all('h2', attrs={'data-testid': 'card-headline'})]
        if title:
            titles.append(title)

        description = [description.text for description in article.find_all('p', attrs={'data-testid': 'card-description'})]
        if description:
            descriptions.append(description)
        else:
            descriptions.append("None")  

    for title, description in zip(titles, descriptions):
        print(f"{title}, {description}") 

    return titles, descriptions

def preprocess_text(text):
    #Remove HTML tags
    text = re.sub(r'<[^>]+>', '', text)
    
    # Remove special characters, punctuation, and symbols
    text = re.sub(r'[^\w\s]', '', text)
    
    # Convert text to lowercase
    text = text.lower()
    
    # Tokenization: Splitting the text into individual words or tokens
    """tokens = word_tokenize(text)
    
    # Remove stopwords
    stop_words = set(stopwords.words('english'))
    tokens = [word for word in tokens if word not in stop_words]
    
    # Lemmatization: Reduce words to their base or root form
    lemmatizer = WordNetLemmatizer()
    tokens = [lemmatizer.lemmatize(word) for word in tokens]
    
    # Join tokens back into a single string
    processed_text = ' '.join(tokens)"""
    
    return text

def transform():
    str = sources[0]
    if str.find("bbc") != -1:
        titles, descriptions = extract_bbc()
        source = "bbc"
    elif str.find("dawn") != -1:
        titles, descriptions = extract_dawn()
        source = "dawn"
    else:
        return   
    #================Starting Pre-processing of Data=================  
    # Preprocess titles and descriptions
    preprocessed_titles = [preprocess_text(title) for title in titles]
    preprocessed_descriptions = [preprocess_text(description) if description != "None" else "None" for description in descriptions]
           
    #remove duplicates
    unique_titles = set()
    unique_descriptions = []
    for title, description in zip(preprocessed_titles, preprocessed_descriptions):    
        #if title is not a duplicate
        if title not in unique_titles:
            unique_titles.add(title)
            unique_descriptions.append(description)
        else:
            continue   

    #save as csv
    with open(f'preprocessed_data_{source}.csv', 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Title', 'Description'])
        for title, description in zip(unique_titles, unique_descriptions):
            writer.writerow([title, description])


def process_sources(**kwargs):  
    for source in sources:
        transform(source=source)

#==================Defining the DAG===================
with DAG('mlops_workflow', default_args=default_args) as dag:

    extract_task_1 = PythonOperator(
        task_id='extract_data_dawn',
        python_callable=extract_dawn
    )

    extract_task_2 = PythonOperator(
        task_id='extract_data_bbc',
        python_callable=extract_bbc
    )

    preprocess_task = PythonOperator(
        task_id='preprocess_data',
        python_callable=transform,
        #provide_context=True
    )

    load_task = BashOperator(
        task_id='load_data',
        bash_command = "dvc add data/preprocessed_data_dawn.csv"
    )


#=================Defining Order of DAG=========================
extract_task_1 >> extract_task_2 >> preprocess_task 