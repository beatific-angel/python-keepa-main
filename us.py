import csv
import io
import os
import time
import sys
import time
from datetime import datetime, timedelta
import json
from threading import Thread
import re
import threading
import psycopg2
import requests
import yagmail
# import config
from queue import Queue
import random

conn = None
params = {

}
conn = psycopg2.connect(**params)


def telegram_bot_send(chat_id, bot_message):

    bot_chatID = chat_id


    requests.get(send_text)
    return


def request(asin, domain):  # pass in (asin, domain)
    global json_string, tokens_left
    url = 'https://api.keepa.com/product'

    headers = {
        'Content-Type': 'application/json',
        'Connection': 'keep-alive'
    }
    try:
        response = requests.post(url=url, data=params, json=headers)
        # res = response.text.encode('latin-1')
        json_string = json.loads(response.text, encoding='latin-1')
    except:
        json_string = 'error'
        pass
    return json_string


def get_asins():
    asin_list1 = []
    returned = []
    cur = conn.cursor()
    try:
        cur.execute("")
        asin_list1 = cur.fetchall()
        for asin in asin_list1:
            returned.append(asin[0])
        print('asins retrieved. ')
    except Exception as e:
        print(str(e))
    conn.commit()
    cur.close()
    return returned


def get_db_seller_name(sellerId):
    seller_name = ''
    cur = conn.cursor()
        cur.execute("")
    listq = cur.fetchone()
    if listq is None:
        if str(sellerId) == 'ATVPDKIKX0DER':
            seller_nm = 'Amazon'
        else:
            seller_nm = 'None'
    else:
        seller_name = listq[0]
        if seller_name == 'Missing':
            seller_nm = 'None'
        else:
            seller_nm = seller_name.replace(',', '')
    return seller_nm


def get_keepa_name(sellerId):
    global name_response, keepa_seller_name
    url = 'https://api.keepa.com/seller'

    headers = {
        'Content-Type': 'application/json',
        'Connection': 'keep-alive'
    }
    try:
        response = requests.post(url=url, data=params, json=headers)
        # res = response.text.encode('latin-1')
        name_response = json.loads(response.text, encoding='latin-1')
        keepa_seller_name = name_response['sellers'][sellerId]['sellerName']
    except:
        name_response = 'error'
        keepa_seller_name = ''
        pass
    return keepa_seller_name


def get_seller_country(sellerId):
    country = ''
    cur = conn.cursor()
        cur.execute("")
    listq = cur.fetchone()
    if listq is None:
        country = 'None'
    else:
        country = listq[0]
        # country = seller_country.replace(',', '')
    return country


def parsing_function(data):
    return_list = []
    timestamp = data['timestamp']
    # timestamp2 = (timestamp1 + 21564000) * 60
    # timestamp = datetime.utcfromtimestamp(timestamp2).strftime('%Y-%m-%d %H:%M:%S')
    tokens_left = data['tokensLeft']

    # products
    try:
        products = data['products']
        if products is not None:
            for product in products:
                **** products definition
                oneTimeAbsolute = ''
                oneTimePercentage = ''
                SNSpercentage = ''
                if coupon is not None:
                    if str(isSNS) == 'True':  # then there is NO one time
                        if coupon[0] < 0:
                            SNSpercentage = coupon[0]
                            if coupon[1] < 0:
                                SNSpercentage = coupon[1]
                        else:
                            SNSpercentage = coupon[1]
                    else:
                        if coupon[0] < 0:
                            oneTimePercentage = coupon[0]
                            if coupon[1] < 0:
                                SNSpercentage = coupon[1]
                            else:
                                oneTimeAbsolute = coupon[1]
                                pass
                        else:
                            oneTimeAbsolute = coupon[0] / 100
                            SNSpercentage = coupon[1]
                else:
                    pass
                try:
                    salesRankReference = product['salesRankReference']
                    salesRank = product['salesRanks']
                except:
                    salesRankReference = ''
                    salesRank = ''
                try:
                    rating1 = len(csvObject[16])
                    reviewCount1 = len(csvObject[17])
                    rating = (csvObject[16][rating1 - 1]) / 10
                    reviewCount = csvObject[17][reviewCount1 - 1]
                    rootSalesRankLength = len(csvObject[3])
                    rootSalesRank = (csvObject[3][rootSalesRankLength - 1])
                except:
                    rootSalesRank = 'None'
                    rating = 'None'
                    reviewCount = 'None'
                audit_ts = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
                rootCat = 'None'
                subCat = 'None'
                description = 'None'
                salesRank1 = ''
                try:
                    if rootCategory is not None:
                      condition clause 
                    
                    subcatRank = 'None'
                    if salesRank is not None:
                        for i in salesRank.keys():
                            length = len(salesRank[i])
                            subcatRank = salesRank[i][length - 1]
                    else:
                        subcatRank = 'None'
                    try:
                        avg = str(avg).replace(',', ';').replace('|', '')
                        avg30 = str(avg30).replace(',', ';').replace('|', '')
                        avg90 = str(avg90).replace(',', ';').replace('|', '')
                        avg180 = str(avg180).replace(',', ';').replace('|', '')
                        avg365 = str(avg365).replace(',', ';').replace('|', '')
                        salesRankDrops30 = str(salesRankDrops30).replace(',', ';').replace('|', '')
                        salesRankDrops90 = str(salesRankDrops90).replace(',', ';').replace('|', '')
                        salesRankDrops180 = str(salesRankDrops180).replace(',', ';').replace('|', '')
                        salesRankDrops365 = str(salesRankDrops365).replace(',', ';').replace('|', '')
                    except:
                        avg = ''
                        avg30 = ''
                        avg90 = ''
                        avg180 = ''
                        avg365 = ''
                        salesRankDrops30 = ''
                        salesRankDrops90 = ''
                        salesRankDrops180 = ''
                        salesRankDrops365 = ''
                except Exception as e:
                    print(str(e))
                    pass
                offers = product['offers']
                if offers is not None:
                    for offer in offers:
                        lastSeen1 = offer['lastSeen']
                        try:
                            lastSeen2 = (lastSeen1 + 21564000) * 60
                            lastSeen = datetime.utcfromtimestamp(lastSeen2).strftime('%Y-%m-%d %H:%M:%S')
                        except:
                            lastSeen = 'None'
                        sellerId = offer['sellerId']
                        seller_name = get_db_seller_name(sellerId)
                        seller_country = "AmazonCA"
                        # seller_country = get_seller_country(sellerId)
                        if seller_name == 'None':
                            seller_name = get_keepa_name(sellerId)
                        else:
                            pass
                        if seller_country == 'None':
                            seller_country = 'Unknown'
                        else:
                            pass
                        offerCSV = offer['offerCSV']
                        offerCSVlength = len(offerCSV)
                        try:
                            price1 = offerCSV[offerCSVlength - 2]
                            price = price1 / 100
                        except:
                            price = 'None'
                        condition = offer['condition']
                        isPrime = offer['isPrime']
                        isMap = offer['isMAP']
                        isShippable = offer['isShippable']
                        isAddOnItem = offer['isAddonItem']
                        isPreOrder = offer['isPreorder']
                        isWarehouseDeal = offer['isWarehouseDeal']
                        isScam = offer['isScam']
                        isAmazon = offer['isAmazon']
                        isPrimeExcl = offer['isPrimeExcl']
                        isFba = offer['isFBA']
                        shipsFromChina = offer['shipsFromChina']
                        row cluase
                        if str(seller_name) == 'Amazon Warehouse':
                            pass
                        else:
                            if condition == 1:
                                return_list.append(row)
                            else:
                                pass
                else:
                   row1 clause 
                    return_list.append(row1)
        else:
            print('hi')
    except Exception as e:
        print(str(e) + ': Inactive')
    return return_list, tokens_left


def insert_to_database(filename):
    print('starting csv insert')
    cur = conn.cursor()
    f = io.open(filename, 'r', encoding='latin-1')
    try:
        cur.copy_from(f, 'stg.keepa_product_daily_run_all', sep=',')
        conn.commit()
        print('CSV Inserted')
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        cur.close()
    cur.close()
    f.close()


def sendLog(filename, asins_in, asins_out):
    yag_smtp_connection = yagmail.SMTP(user="autojobs@potoosolutions.com", password="Potoo2021!", host='smtp.gmail.com')
    subject = 'Keepa Log File'
    # attachment = ['Logs/Log_' + str(s1) + '.log']
    html_str = """  """ + str(asins_in) + """  """ + str(asins_out) + """  """ + str(filename) + """  """

    Html_file = open('./log/log_Email.html', "w")
    Html_file.write(str(html_str.encode('UTF-8')).replace('\\n', '').replace('\\t', '')[2:-1])
    Html_file.close()
    # os.remove('./log/log_Email.html')


def get_out_asins(date):
    print('retrieving out asins')
    cur = conn.cursor()
    asin_count = 0
    try:

        asin = cur.fetchone()
        asin_count = asin[0]
        print('asisns retirevce')
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        cur.close()
    cur.close()
    return asin_count


def run_request(taskQueue, resultQueue, marketplace, goalTime, s1, s2, asin_count, total_asin_count):
    chunkstart = datetime.now()
    response_time = ''
    parsing_time = ''
    csv_time = ''
    db_time = ''
    chunk_count = 0
    filename = '_ca/' + str(s1) + f"_ca_{random.randint(100, 999)}.csv"
   
        while not taskQueue.empty():
            chunk = taskQueue.get()
            chunk_count = len(chunk)
            print(f"Start Thread:  {chunkstart}")
            response_time = datetime.now()
            response = request(chunk, marketplace)
            response_time = datetime.now() - response_time
            if response == 'error':
                print('response error')
                pass
            else:
                ttt = 0
                parsing_time = datetime.now()
                returned, tokens = parsing_function(response)
                parsing_time = datetime.now() - parsing_time
                for row in returned:
                    try:
                        csv_time = datetime.now()
                        spamwriter.writerow(row)
                        csv_time = datetime.now() - csv_time
                        tt = 0
                    except Exception as e:
                        print(str(row))
                        print(str(e) + 'hi')
                        pass
                if tokens < 1000:
                    print('pasuing for token refresh')
                    time.sleep(1000)
                    print('tokens refreshed.')
                else:
                    pass
            taskQueue.task_done()
            resultQueue.put(response['tokensConsumed'])

        # if datetime.now() > goalTime:
        # continue
        time.sleep(2)
        # insert_to_database(filename)
        # inserted_asins = get_out_asins(s2)
        sendLog(filename, total_asin_count, 100)
     
    return True





def main():
    marketplace = '6'
    start = time.time()

    startTime = datetime.now()
    goalTime = datetime.now() + timedelta(hours=12)
    now = datetime.now()
    s1 = now.strftime("%Y-%m-%d")
    s2 = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
    asin_list = get_asins()
    total_asin_count = len(asin_list)
    print('asins: ' + str(total_asin_count))
    asin_chunks = [asin_list[x:x + 100] for x in
                   range(0, total_asin_count, 100)]
    asin_count = 0

    print(datetime.now())

    taskQueue = Queue()
    resultQueue = Queue()
    for chunk in asin_chunks:
        taskQueue.put(chunk)

    consumed_token = 0
    print(f"Unfinished Task: {taskQueue.unfinished_tasks}")
    thread_count = min(10, taskQueue.unfinished_tasks)
    for i in range(thread_count):
        worker = Thread(target=run_request, daemon=True, args=(
            taskQueue, resultQueue, marketplace, goalTime, s1, s2, asin_count, total_asin_count))
        worker.start()
    taskQueue.join()

    while not resultQueue.empty():
        consumed_token += resultQueue.get()

    for f in os.listdir('./_ca/'):
        if not f.endswith(".csv"):
            continue
        os.remove(os.path.join('_ca/', f))
 if __name__ == '__main__':
    main()
