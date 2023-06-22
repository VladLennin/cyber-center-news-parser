import time
import requests
from bs4 import BeautifulSoup
import psycopg2
import hashlib
from datetime import datetime

hostname = 'localhost'
port = '5432'
database = 'cyber-center'
username = 'postgres'
password = '0000'

connection = psycopg2.connect(
    host=hostname,
    port=port,
    database=database,
    user=username,
    password=password
)

count_requests = 0


def parse_rss():
    cursor = connection.cursor()

    url = "https://tsn.ua/rss/ato"
    response = requests.get(url)

    soup = BeautifulSoup(response.text, "xml")

    category_war = soup.findAll("item")

    query = "SELECT * FROM news"
    cursor.execute(query)
    all_news = cursor.fetchall()

    for item in category_war:
        title = item.title.text
        link = item.find("link").text
        description = item.find("description").text
        content = item.find("fulltext").text
        img_href = item.find("enclosure").get("url")
        pubDate = item.find("pubDate").text
        date_format = "%a, %d %b %Y %H:%M:%S %z"

        link_bytes = link.encode("utf-8")
        hash_obj = hashlib.sha256(link_bytes)
        hash_link = hash_obj.hexdigest()

        already_exist = False
        for i in all_news:
            if i[1] == hash_link:
                already_exist = True
                break

        if not already_exist:
            query = "INSERT INTO news (title,description, content, date_pub, img_href, hashed_link, created_at) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(query, (
            str(title), str(description), str(content), datetime.strptime(pubDate, date_format), str(img_href),
            str(hash_link), datetime.now()))
            print(str(title), str(description), str(content), pubDate, str(img_href), str(hash_link), datetime.now())
            connection.commit()


while True:
    parse_rss()
    count_requests += 1
    print(f"Count requests at the {datetime.now()} is {count_requests}")
    for sec in range(600):
        print(f"{sec} s", end="", flush=True)
        time.sleep(1)
        print('\b' * len(str(sec)), end="\r", flush=True)
