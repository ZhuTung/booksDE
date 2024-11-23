from bs4 import BeautifulSoup
import requests
import pandas as pd
import boto3, json
from pymongo import MongoClient

def books_etl():
    html_text = requests.get('https://books.toscrape.com/catalogue/page-1.html').text
    soup = BeautifulSoup(html_text, 'lxml')

    l_titles = []
    l_prices = []
    l_ratings = []
    l_status = []

    total_page = soup.find('li', class_="current").text.strip().split()[-1]
    total_page = int(total_page)

    for i in range(1, total_page + 1):
        link = f"https://books.toscrape.com/catalogue/page-{i}.html"
        page = requests.get(link).text
        soup = BeautifulSoup(page, 'lxml')
        books = soup.find_all('li', class_='col-xs-6 col-sm-4 col-md-3 col-lg-3')

        rating_num = ["one","two","three","four","five"]

        for book in books:
            title = book.find('h3')
            price = book.find('p', class_='price_color').text.replace('Â£','')
            rating = book.find('p', class_='star-rating').get('class')[-1].lower()
            status = book.find('p', class_='instock availability').text.strip()
            
            if "..." in title.text:
                title = title.find('a').get("title")
            else:
                title = title.text
            
            for x in range(0,len(rating_num)):
                if rating == rating_num[x]:
                    rating = x+1
                    
            l_titles.append(title)
            l_prices.append(price)
            l_ratings.append(rating)
            l_status.append(status)

    data = {
            "Title" : l_titles,
            "Price" : l_prices,
            "Rating" : l_ratings,
            "Status" : l_status
            }

    df = pd.DataFrame(data)

    df.to_csv("/home/airflow/books.csv", index=False)

def transferToS3():
    with open("configuration.json","r") as output:
        configuration = json.load(output)

    s3_client = boto3.client(
        service_name = configuration["service_name"],
        region_name = configuration["region_name"],
        aws_access_key_id = configuration["aws_access_key_id"],
        aws_secret_access_key = configuration["aws_secret_access_key"]
    )

    s3_client.upload_file("/home/airflow/books.csv", configuration["bucket_name"], configuration["file_name"])

def s3ToMongoDB():
    with open("configuration.json","r") as output:
        configuration = json.load(output)

        s3_client = boto3.client(
            service_name = configuration["service_name"],
            region_name = configuration["region_name"],
            aws_access_key_id = configuration["aws_access_key_id"],
            aws_secret_access_key = configuration["aws_secret_access_key"]
        )

        obj = s3_client.get_object(Bucket = configuration["bucket_name"], Key = configuration["file_name"])
        
        mongoClient = MongoClient("mongodb+srv://<username>:<password>@bookscluter.5n7ub.mongodb.net/")

        db = mongoClient["booksDB"]
        dbCol = db["books_collection"]

        data = pd.read_csv(obj['Body'])
        data = data.to_dict('records')

        dbCol.insert_many(data)