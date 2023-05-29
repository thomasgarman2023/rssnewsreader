import openai
import os
from dotenv import load_dotenv, find_dotenv
from mysql_connect import connection
import pymysql
import time
import logging
from random import randint


logging.basicConfig(filename="categorizing.log", filemode="w", level=logging.DEBUG, format=f"%(asctime)s :: %(levelname)-8s: \t %(filename)s %(lineno)s - %(message)s")


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


def count_texts_to_categorize():

    sql = f"""
        SELECT 
            COUNT(*)
        FROM story_feed AS f
        WHERE story_datetime >= CURDATE()
            AND story_id NOT IN (SELECT story_id FROM story_categories AS c WHERE f.story_id = c.story_id)
        ;
    """

    try:
        cursor = connection.cursor()
        cursor.execute(sql)
        count_texts = cursor.fetchall()
        cursor.close()

        return count_texts[0][0]

    except pymysql.Error as e:
        logging.error(f"""Select 10 rows to start step. Error while connecting to MySQL: {e}""")



def get_texts_to_categorize(limit):

    sql = f"""
        SELECT 
            story_id, 
            description 
        FROM story_feed AS f
        WHERE story_datetime >= CURDATE()
            AND story_id NOT IN (SELECT story_id FROM story_categories AS c WHERE f.story_id = c.story_id)
        LIMIT {limit};
    """


    try:
        cursor = connection.cursor()
        cursor.execute(sql)
        texts = cursor.fetchall()
        cursor.close()

        return texts

    except pymysql.Error as e:
        logging.error(f"""Select 10 rows to start step. Error while connecting to MySQL: {e}""")


i = count_texts_to_categorize()
logging.info(f"""There are {i} text descriptions to categorize.""")


while i > 0:
    
    news_stories = get_texts_to_categorize(10)
    inserts = []

    for item in news_stories:

        prompt = f"""

        Your task is to categorize the topic of the text below delimited below with three backticks.  
        You should output three categories, i.e. category 1, category 2, category 3, that the text is about.
        Every text should be categorized into one choice from among the following labels:
               - category 1: Business, Government, Technology, Sports, Entertainment
               - category 2: Europe, Asia, United States, Africa, International
               - category 3: Economy, Conflict, Crime, Legal, Politics
        For example the sentence 'US Lawmakers reach deal on Debt Ceiling' would be categorized as:  Goverment, United States, Economy
        For example the sentence 'DeSantis announces candidacy for President' would be categorized as: Government, United States, Politics
        For example the sentence 'Ukraine plans counteroffensive against Russia' would be categorized as: Government, Europe, Conflict
        Format your response following the examples just given:  category 1, category 2, category 3
        If no text is provided, respond with: blank, blank, blank

        Text to categorize: ```{item[1]}```

        """

        try:

            response = get_completion(prompt)
        
        except openai.error as e:
            
            logging.error(e)
            time.sleep(randint(30,60))

        result = response.split(', ')
        result.insert(0, item[0])
        insert_string = tuple(result)
        inserts.append(insert_string)

        logging.info(f"""Categorization result: {insert_string}""")
        time.sleep(randint(15,35))


    try:
        cursor = connection.cursor()
        cursor.executemany("INSERT INTO story_categories VALUES (%s, %s, %s, %s)", inserts)
        connection.commit()
        cursor.close()


    except pymysql.Error as e:
        logging.error(f"""Insert categorizations step. Error while connecting to MySQL: {e}""")


    i = count_texts_to_categorize()
    logging.info(f"""There are {i} text descriptions remaining to categorize.""")
    time.sleep(randint(20,40))

connection.close()

