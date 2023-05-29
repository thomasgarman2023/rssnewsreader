from urllib.request import urlopen
from bs4 import BeautifulSoup
import unicodedata
from rss_sources import news_urls_simple
from shared_functions import standardize_published_dates, source_extractor, end_sentence_with_period
from mysql_connect import connection
import pymysql
import logging

logging.basicConfig(filename="newsreader1.log", filemode="w", level=logging.DEBUG, format=f"%(asctime)s :: %(levelname)-8s: \t %(filename)s %(lineno)s - %(message)s")


def newsreader1(rss_link):

    fetch_xml = urlopen(rss_link)
    xml_response = fetch_xml.read()
    fetch_xml.close()

    soup_page = BeautifulSoup(xml_response, "xml")
    news_items = soup_page.findAll("item")

    list_of_stories = []

    for story in news_items:
    	
    	try:

            description = unicodedata.normalize("NFKD", end_sentence_with_period(BeautifulSoup(story.description.text, features="html.parser").text.encode('utf-8', errors='ignore').decode('utf-8')))
            pubdate = story.pubDate.text
            story_date = standardize_published_dates(pubdate)
            link = story.link.text
            result = [newsreader1.__name__, source_extractor(link), description, link, story_date.strftime('%Y-%m-%d %H:%M:%S')]
            list_of_stories.append(result)

    	except:

    		logging.info(f"""Error in the newsreader1 function at: {story}""")


    return list_of_stories




if __name__ == '__main__':


    try:
        cursor = connection.cursor()
        cursor.execute("DROP TABLE IF EXISTS raw_feed;")
        cursor.close()

    except pymysql.Error as e:
        logging.error(f"""Drop raw feed table step. Error while connecting to MySQL: {e}""")



    try:
        create_statement = '''
            CREATE TABLE raw_feed ( 
                script_name VARCHAR(15), 
                news_source VARCHAR(15), 
                description TEXT, 
                link TEXT, 
                story_date VARCHAR(20)
        ); '''
        cursor = connection.cursor()
        cursor.execute(create_statement)
        cursor.close()

    except pymysql.Error as e:
        logging.error(f"""Create raw feed table step. Error while connecting to MySQL: {e}""")



    inserts = []

    for url in news_urls_simple:

        logging.info(f"""Getting the rss feed for {url}""")

        news = newsreader1(url)
        for item in news:
            insert_string = (str(item[0]), str(item[1]), str(item[2]).replace('\'', '').replace('\"', ''), str(item[3]), str(item[4]))
            inserts.append(insert_string)


    try:
        cursor = connection.cursor()
        cursor.executemany("INSERT INTO raw_feed VALUES (%s, %s, %s, %s, %s)", inserts)
        connection.commit()
        cursor.close()

    except pymysql.Error as e:
        logging.error(f"""Insert into raw feed table step. Error while connecting to MySQL: {e}""")


    # Example Inserts
    # INSERT INTO raw_feed VALUES ('newsreader1', 'smh', 'Australia is clever enough to understand that not taking a stand against a brutal authoritarian in Europe will provide encouragement for another of a similar vein to engage in similar behaviour in our region.', 'https://www.smh.com.au/world/europe/we-ve-become-a-bystander-in-the-ukraine-war-and-china-will-notice-20230518-p5d9de.html?ref=rss&utm_medium=rss&utm_source=rss_world', '2023-05-22 05:00:00');
    # INSERT INTO raw_feed VALUES ('newsreader1', 'smh', 'Ukrainian President Volodymyr Zelensky has denied claims Bakhmut has fallen to Russia after Yevgeny Prigozhin, the chief of the mercenary Wagner group, said his forces had taken complete control of the eastern Ukrainian city following months of brutal fighting.', 'https://www.smh.com.au/world/zelenskyy-responds-to-wagner-chiefs-claims-about-bakhmut-20230522-p5da4n.html?ref=rss&utm_medium=rss&utm_source=rss_world', '2023-05-22 07:14:18');
    # INSERT INTO raw_feed VALUES ('newsreader1', 'smh', 'The Ukrainian president compared Bakhmut’s “total destruction” to the Japanese city of Hiroshima after the nuclear strike of 1945.', 'https://www.smh.com.au/world/asia/zelensky-makes-impassioned-plea-for-arms-at-g7-after-bombing-of-bakhmut-20230521-p5da35.html?ref=rss&utm_medium=rss&utm_source=rss_world', '2023-05-22 05:36:12');
    # INSERT INTO raw_feed VALUES ('newsreader1', 'smh', 'Australia is clever enough to understand that not taking a stand against a brutal authoritarian in Europe will provide encouragement for another of a similar vein to engage in similar behaviour in our region.', 'https://www.smh.com.au/world/europe/we-ve-become-a-bystander-in-the-ukraine-war-and-china-will-notice-20230518-p5d9de.html?ref=rss&utm_medium=rss&utm_source=rss_world', '2023-05-22 05:00:00');
    # INSERT INTO raw_feed VALUES ('newsreader1', 'smh', 'The renowned Australian artist has taken centre stage in one of London’s showcase venues.', 'https://www.smh.com.au/world/europe/why-acclaimed-aboriginal-activist-richard-bell-won-t-vote-yes-20230521-p5d9yk.html?ref=rss&utm_medium=rss&utm_source=rss_world', '2023-05-22 05:00:00');
    # INSERT INTO raw_feed VALUES ('newsreader1', 'washingtonpost', "Users on the livestreaming site Niconico are closely following the Group of Seven summit in Hiroshima. Many were moved by a tribute to the city's dark history.", 'https://www.washingtonpost.com/world/2023/05/21/japan-g7-summit-livestream-niconico/', '2023-05-21 13:38:30');



    try:
        story_feed_sql = """
            INSERT INTO story_feed
            SELECT
                NULL AS story_id,
                script_name,
                news_source,
                description,
                link,
                STR_TO_DATE(story_date, '%Y-%m-%d %T') AS story_datetime,
                CURDATE() AS insert_date
            FROM raw_feed AS r
            WHERE NOT EXISTS (SELECT 1 FROM story_feed AS s WHERE STRCMP(r.description, s.description) = 0);
        """
        cursor = connection.cursor()
        cursor.execute(story_feed_sql)
        connection.commit()
        cursor.close()
        connection.close()

    except pymysql.Error as e:
        logging.error(f"""Insert into story feed table step. Error while connecting to MySQL: {e}""")



