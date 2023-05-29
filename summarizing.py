import openai
import os
from dotenv import load_dotenv, find_dotenv
from mysql_connect import connection
import pymysql
import time
import logging

logging.basicConfig(filename="summarizing.log", filemode="w", level=logging.DEBUG, format=f"%(asctime)s :: %(levelname)-8s: \t %(filename)s %(lineno)s - %(message)s")



_ = load_dotenv(find_dotenv()) # read local .env file
openai.api_key = os.getenv('OPENAI_API_KEY')

def get_completion(prompt, model="gpt-3.5-turbo"):
	
    messages = [{"role": "user", "content": prompt}]

    response = openai.ChatCompletion.create(

        model=model,

        messages=messages,

        temperature=0, # this is the degree of randomness of the model's output

    )

    return response.choices[0].message["content"]


news_sources = ['nytimes','cnbc','washingtonpost','sky','smh','chicagotribune','csmonitor','latimes'] #'theguardian','independent'

# Text to summarize

news_summary = ''

for source in news_sources:
    logging.info(f"""Creating text summaries for source: {source}""")

    sql = f"""
        SELECT DISTINCT description
        FROM story_categories AS c
            JOIN story_feed AS f
                ON c.story_id = f.story_id
        WHERE story_datetime >= CURDATE()
            AND news_source = '{source}'
            AND (c.category_1 = 'Government' OR category_3 IN ('Politics', 'Conflict'));
    """

    try:
        cursor = connection.cursor()
        cursor.execute(sql)
        news_stories = cursor.fetchall()
        cursor.close()
        #connection.close()

    except pymysql.Error as e:
        logging.error(f"""Select text descriptions from story feed table. Error while connecting to MySQL: {e}""")

    text_to_summarize = ''

    for item in news_stories:
        text_to_summarize += ''.join(item[0] + ' ')

    try:
        prompt1 = f"""

        Your task is to generate a short summary of a news items from rss feeds. 
        In at most 100 words summarize the text below that is delimited by triple backticks.  
        Break each topic area in the summary into it's own section.
        For example, in summarizing all of the sentences about China into one block of text, make a header that says CHINA then provide the summary of the news items about China.  
        Do this for the top 5 topics in the text based on keyword frequency omitting stopwords from the frequency count.

        Sentences: ```{text_to_summarize}```

        """

        response1 = get_completion(prompt1)
        news_summary += ''.join(response1 + ' ')
        time.sleep(30)

    except:
        logging.info(f"""openapi did not return a response for {source}""")
        time.sleep(30)

# Summarize with a word/sentence/character limit

prompt2 = f"""

Your task is to generate a short summary of a news items from rss feeds. 
Summarize the text below, delimited by triple backticks, in at most 500 words. 
Group all of the sentences summarized that are in the same topic area under a heading that says what the topic is.
For example, the summary of all of the sentences about the Russia-Ukraine conflict should be under the header RUSSIA-UKRAINE.
For example, the summary of all of the sentences about political news items should be under the header POLITICS.
At the end of the summary provide a list of the top 10 topics being discussed in the original text blob
with the most frequent, relevant topic of the day at the top of the list and the least frequent, relevant
topic at the bottom.  Label this list as TODAY'S TOP TOPICS.

News items: ```{news_summary}```

"""

time.sleep(30)
response2 = get_completion(prompt2)

logging.info(f"""Text summary created: {response2}""")

inserts = [(response2,)]

try:
    cursor = connection.cursor()
    cursor.execute("INSERT INTO daily_summary VALUES (CURDATE(), %s)", inserts)
    connection.commit()
    cursor.close()
    connection.close()
except pymysql.Error as e:
    logging.error(f"""Insert into daily summary table step. Error while connecting to MySQL: {e}""")


