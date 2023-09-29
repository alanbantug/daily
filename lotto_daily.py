import requests
import psycopg2
import json
import os

from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.firefox.options import Options as options
from selenium.webdriver.firefox.service import Service
from bs4 import BeautifulSoup
from time import sleep

''' set the parms that will be used in each game
'''
driver_path = r'c:\users\alan\gecko\geckodriver.exe'
binary_path = r'c:\program files\mozilla firefox\firefox.exe'
ops = options()
ops.binary_location = binary_path
serv = Service(driver_path)

month_name = {
    'JAN': 'January',
    'FEB': 'February',
    'MAR': 'March',
    'APR': 'April',
    'MAY': 'May',
    'JUN': 'June',
    'JUL': 'July',
    'AUG': 'August',
    'SEP': 'September',
    'OCT': 'October',
    'NOV': 'November',
    'DEC': 'December'
}

def create_connection():
    ''' Create connection to PostgreSQL database
    '''
    with open(r"c:\users\alan\creds\credentials.json", "r") as credentials:
        creds = json.loads(credentials.read())

    conn = psycopg2.connect(database=creds['database'],
    user=creds['user'],
    password=creds['password'],
    host=creds['host'],
    port=creds['port'])

    return conn

def parse_dates(soup):

    winners = soup.find_all('div', {'id': lambda L: L and L.startswith('PastDrawsRow')})

    winner_dates = []

    for winner in winners:

        text = winner.get_text()
        
        ''' get the draw date
        '''
        d = text.split('#')[0].split()
        d[0] = month_name[d[0]]
        rd = ' '.join(d)
        f = '%B %d, %Y'
        nd = datetime.strptime(rd, f).strftime("%Y-%m-%d")

        winner_dates.append(nd)

    return winner_dates

def scrape_fantasy_winners():
    ''' Scrape data from Fantasy Five results
    '''

    site = 'https://www.calottery.com/draw-games/fantasy-5#section-content-2-3'
    browser = webdriver.Firefox(service=serv, options=ops)
    browser.get(site)

    html = browser.page_source
    sleep(5)
    browser.close()

    soup = BeautifulSoup(html, 'html.parser')

    winners = soup.find_all('li', class_='list-inline-item')

    winner_list = []
    winning = []

    for winner in winners:
        winning.append(int(winner.get_text()))

        if len(winning) == 5:
            if winning in winner_list:
                pass
            else:
                winner_list.append(winning)
            winning = []

    winner_dates = parse_dates(soup)

    return winner_list, winner_dates

def insert_fantasy_item(cursor, data):
    ''' Insert the scraped data one at a time
    '''

    draw_date, numa, numb, numc, numd, nume = data

    fantasy_data = (draw_date, numa, numb, numc, numd, nume)

    insert_sql = '''
    insert into fantasy_five (draw_date, numa, numb, numc, numd, nume)
    values (%s, %s, %s, %s, %s, %s)
    '''

    try:
        cursor.execute(insert_sql, fantasy_data)
        return True
    except Exception as e:
        return False


def insert_fantasy_records(winners):
    ''' Putting all the Fantasy Five processing together
    '''

    good = 0
    bad = 0

    conn = create_connection()

    cursor = conn.cursor()

    for winner in winners:

        if insert_fantasy_item(cursor, winner):
            good += 1
        else:
            bad += 1

        conn.commit()

    print(f"Inserted     : {good}")
    print(f"Not inserted : {bad}")

def process_fantasy():

    count = 0
    while True:
        winners, dates = scrape_fantasy_winners()

        if len(winners) == 20:
            break
        else:
            count += 1
            if count > 10:
                break

    fantasy_data = []
    for d, w  in zip(dates, winners):
        data = w
        data.insert(0, d)
        fantasy_data.append(data)

    insert_fantasy_records(fantasy_data)

def insert_mps_item(cursor, table_name, data):
    ''' Insert the scraped data one at a time used for Super Lotto, Mega Lotto and Powerball
    '''

    draw_date, numa, numb, numc, numd, nume, numx = data

    mps_data = (draw_date, numa, numb, numc, numd, nume, numx)

    insert_sql = f'''
    insert into {table_name} (draw_date, numa, numb, numc, numd, nume, numx)
    values (%s, %s, %s, %s, %s, %s, %s)
    '''

    try:
        cursor.execute(insert_sql, mps_data)
        return True
    except Exception as e:
        return False

def scrape_super_winners():
    ''' Scrape data from Super Lotto results
    '''

    site = 'https://www.calottery.com/draw-games/superlotto-plus#section-content-2-3'
    browser = webdriver.Firefox(service=serv, options=ops)
    browser.get(site)

    html = browser.page_source
    sleep(5)
    browser.close()

    soup = BeautifulSoup(html, 'html.parser')

    winners = soup.find_all('li', class_='list-inline-item')

    winner_list = []
    winning = []

    for winner in winners:

        text = winner.get_text()

        win = ''.join([t for t in text if t.isdigit()])

        winning.append(int(win))

        if len(winning) == 6:
            if winning in winner_list:
                pass
            else:
                winner_list.append(winning)
            winning = []

    winner_dates = parse_dates(soup)

    return winner_list, winner_dates

def insert_super_records(winners):
    ''' Putting all the Super Lotto processing together
    '''

    good = 0
    bad = 0

    conn = create_connection()

    cursor = conn.cursor()

    for winner in winners:

        if insert_mps_item(cursor, 'super_lotto', winner):
            good += 1
        else:
            bad += 1

        conn.commit()

    print(f"Inserted     : {good}")
    print(f"Not inserted : {bad}")

def process_super():

    count = 0
    while True:
        winners, dates = scrape_super_winners()

        if len(winners) == 20:
            break
        else:
            count += 1
            if count > 10:
                break

    super_data = []
    for d, w  in zip(dates, winners):
        data = w
        data.insert(0, d)
        super_data.append(data)

    insert_super_records(super_data)

def scrape_mega_winners():
    ''' Scrape data from Mega Lotto results
    '''

    site = 'https://www.calottery.com/draw-games/mega-millions#section-content-2-3'
    browser = webdriver.Firefox(service=serv, options=ops)
    browser.get(site)

    html = browser.page_source
    sleep(5)
    browser.close()

    soup = BeautifulSoup(html, 'html.parser')

    winners = soup.find_all('li', class_='list-inline-item')

    winner_list = []
    winning = []

    for winner in winners:

        text = winner.get_text()

        win = ''.join([t for t in text if t.isdigit()])

        winning.append(int(win))

        if len(winning) == 6:
            if winning in winner_list:
                pass
            else:
                winner_list.append(winning)
            winning = []

    winner_dates = parse_dates(soup)

    return winner_list, winner_dates

def insert_mega_records(winners):
    ''' Putting all the Mega Lotto processing together
    '''

    good = 0
    bad = 0

    conn = create_connection()

    cursor = conn.cursor()

    for winner in winners:

        if insert_mps_item(cursor, 'mega_lotto', winner):
            good += 1
        else:
            bad += 1

        conn.commit()

    print(f"Inserted     : {good}")
    print(f"Not inserted : {bad}")

def process_mega():

    count = 0
    while True:
        winners, dates = scrape_mega_winners()

        if len(winners) == 20:
            break
        else:
            count += 1
            if count > 10:
                break

    mega_data = []
    for d, w  in zip(dates, winners):
        data = w
        data.insert(0, d)
        mega_data.append(data)

    insert_mega_records(mega_data)

def scrape_power_winners():
    ''' Scrape data from Powerball results
    '''

    site = 'https://www.calottery.com/draw-games/powerball#section-content-2-3'
    browser = webdriver.Firefox(service=serv, options=ops)
    browser.get(site)

    html = browser.page_source
    sleep(5)
    browser.close()

    soup = BeautifulSoup(html, 'html.parser')

    winners = soup.find_all('li', class_='list-inline-item')

    winner_list = []
    winning = []

    for winner in winners:

        text = winner.get_text()

        win = ''.join([t for t in text if t.isdigit()])

        winning.append(int(win))

        if len(winning) == 6:
            if winning in winner_list:
                pass
            else:
                winner_list.append(winning)
            winning = []

    winner_dates = parse_dates(soup)

    return winner_list, winner_dates

def insert_power_records(winners):
    ''' Putting all the Powerball processing together
    '''

    good = 0
    bad = 0

    conn = create_connection()

    cursor = conn.cursor()

    for winner in winners:

        if insert_mps_item(cursor, 'power_ball', winner):
            good += 1
        else:
            bad += 1

        conn.commit()

    print(f"Inserted     : {good}")
    print(f"Not inserted : {bad}")

def process_power():

    count = 0
    while True:
        winners, dates = scrape_power_winners()

        if len(winners) == 20:
            break
        else:
            count += 1
            if count > 10:
                break

    power_data = []
    for d, w  in zip(dates, winners):
        data = w
        data.insert(0, d)
        power_data.append(data)

    insert_power_records(power_data)

if __name__ == '__main__':
    res = input('Scrape Fantasy Five game (Y/N)? : ')
    if res.lower() == 'y':
        process_fantasy()
    
    res = input('Scrape Super Lotto game  (Y/N)? : ')
    if res.lower() == 'y':
        process_super()

    res = input('Scrape Mega Lotto game   (Y/N)? : ')
    if res.lower() == 'y':
        process_mega()

    res = input('Scrape Power Ball game   (Y/N)? : ')
    if res.lower() == 'y':
        process_power()
