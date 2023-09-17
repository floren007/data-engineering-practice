import requests
import pandas
from selenium import webdriver
from bs4 import BeautifulSoup
import pandas as pd
import sys

def main():
# Step 1: Web Scraping
    url = 'https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/'

    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Step 2: Finding the corresponding file

    target_date = '2022-02-07 14:03'

    file_link = None

    for row in soup.find_all('tr'):
        cells = row.find_all('td')
        if len(cells) >= 2 and target_date in cells[1].text:
            file_link = cells[0].find('a').get('href')
            print(file_link)
            break

    if file_link is None:
        print('File not found for the specified date.')
        exit()

    ### Join the url + the name of csv
    download_csv=url + file_link
    ## call the API, then download the csv
    call_api=requests.get(download_csv)
    with open('Exercises/Exercise-2/data33.csv', 'wb') as w:
        w.write(call_api.content)
    df=pd.read_csv('Exercises\Exercise-2\data33.csv')
    print(df['HourlyDryBulbTemperature'].max(), file=sys.stdout) 
    

if __name__ == "__main__":
    main()

