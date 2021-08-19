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
    'host': 'localhost',
    'port': 5432,
    'user': 'postgres',
    'password': 'admin123123',
    'database': 'potoo_db',
}
conn = psycopg2.connect(**params)


def telegram_bot_send(chat_id, bot_message):
    bot_token = '1466615990:AAGBCGqf6NVM4ZQ-7nMGnwAlVSnT5qD8uFA'
    bot_chatID = '-552890666'
    bot_chatID = chat_id
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + str(
        bot_message)

    requests.get(send_text)
    return


def request(asin, domain):  # pass in (asin, domain)
    global json_string, tokens_left
    url = 'https://api.keepa.com/product'
    params = {
        'key': '1tvha8e4oh00otudtrpt1uusn9if0au8lomuh44glek8imqavl8rjaaupnovjv7v',
        'asin': ','.join(asin),
        'domain': str(domain),
        'offers': '40',
        'stock': '1',
        'rating': '1',
        'stats': '1',
        'only-live-offers': '1',
        'update': '1'
    }
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
        cur.execute("""select product_asin from missing_asin_daily_run
                        where country_code =  'CA' group by product_asin limit 230
                        """)
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
    cur.execute(
        """select seller_nm from sellers
            where external_id = '""" + str(sellerId) + """'
                    """)
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
    params = {
        'key': '1tvha8e4oh00otudtrpt1uusn9if0au8lomuh44glek8imqavl8rjaaupnovjv7v',
        'seller': str(sellerId),
        'domain': '6',
        'storefront': '1',
        'update': '1'
    }
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
    cur.execute(
        """select seller_country_1 from potoo_db.seller_info_log
            where external_id = '""" + str(sellerId) + """'
                        """)
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
                categories = product['categoryTree']
                manufacturer = product['manufacturer']
                title = product['title']
                lastUpdate1 = product['lastUpdate']
                lastUpdate2 = (lastUpdate1 + 21564000) * 60
                lastUpdate = datetime.utcfromtimestamp(lastUpdate2).strftime('%Y-%m-%d %H:%M:%S')
                rootCategory = product['rootCategory']
                productType = product['productType']
                parentAsin = product['parentAsin']
                variationCSV = product['variationCSV']
                asin = product['asin']
                domainId = product['domainId']
                hasReviews = product['hasReviews']
                trackingSince1 = product['trackingSince']
                trackingSince2 = (trackingSince1 + 21564000) * 60
                trackingSince = datetime.utcfromtimestamp(trackingSince2).strftime('%Y-%m-%d %H:%M:%S')
                brand = product['brand']
                productGroup = product['productGroup']
                partNumber = product['partNumber']
                model = product['model']
                color = product['color']
                size = product['size']
                edition = product['edition']
                format = product['format']
                packageHeight = product['packageHeight']
                packageLength = product['packageLength']
                packageWidth = product['packageWidth']
                packageWeight = product['packageWeight']
                isAdultProduct = product['isAdultProduct']
                isEligibleForTradeIn = product['isEligibleForTradeIn']
                isEligibleForSuperSaverShipping = product['isEligibleForSuperSaverShipping']
                isRedirectASIN = product['isRedirectASIN']
                isSNS = product['isSNS']
                author = product['author']
                binding = product['binding']
                numberOfItems = product['numberOfItems']
                numberOfPages = product['numberOfPages']
                publicationDate = product['publicationDate']
                releaseDate = product['releaseDate']
                ebayListingIds = product['ebayListingIds']
                eanList = product['eanList']
                upcList = product['upcList']
                frequentlyBoughtTogether = product['frequentlyBoughtTogether']
                description1 = product['description']
                stats = product['stats']
                avg = stats['avg']
                avg30 = stats['avg30']
                avg90 = stats['avg90']
                avg180 = stats['avg180']
                avg365 = stats['avg365']
                stockAmazon = stats['stockAmazon']
                stockBuyBox = stats['stockBuyBox']
                totalOfferCount = stats['totalOfferCount']
                salesRankDrops30 = stats['salesRankDrops30']
                salesRankDrops90 = stats['salesRankDrops90']
                salesRankDrops180 = stats['salesRankDrops180']
                salesRankDrops365 = stats['salesRankDrops365']
                buyBoxPrice1 = stats['buyBoxPrice']
                buyBoxPrice = buyBoxPrice1 / 100
                buyBoxShipping = stats['buyBoxShipping']
                buyBoxIsFBA = stats['buyBoxIsFBA']
                buyBoxIsAmazon = stats['buyBoxIsAmazon']
                buyBoxIsMAP = stats['buyBoxIsMAP']
                buyBoxIsUsed = stats['buyBoxIsUsed']
                buyBoxCondition = stats['buyBoxCondition']
                buyBoxSellerId = stats['buyBoxSellerId']
                buyBoxMaxOrderQuantity = stats['buyBoxMaxOrderQuantity']
                csvObject = product['csv']
                coupon = product['coupon']
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
                        rootCategory = str(rootCategory).replace(r'\|', '').replace("\\", "").replace("\n", "").replace(
                            "\r", "").replace(",", "").replace('®', '').replace('�', '').replace("'", "").replace('%',
                                                                                                                  '').replace(
                            '⁄', '')
                    if productType is not None:
                        productType = str(productType).replace(r'\|', '').replace("\\", "").replace("\n", "").replace(
                            "\r", "").replace(",", "").replace('®', '').replace('�', '').replace("'", "").replace('%',
                                                                                                                  '').replace(
                            '⁄', '')
                    if parentAsin is not None:
                        parentAsin = str(parentAsin).replace(r'\|', '').replace("\\", "").replace("\n", "").replace(
                            "\r", "").replace(",", "").replace('®', '').replace('�', '').replace("'", "").replace('%',
                                                                                                                  '').replace(
                            '⁄', '')
                    if productGroup is not None:
                        productGroup = str(productGroup).replace(r'\|', '').replace("\\", "").replace("\n", "").replace(
                            "\r", "").replace(",", "").replace('®', '').replace('�', '').replace("'", "").replace('%',
                                                                                                                  '').replace(
                            '⁄', '')
                    if model is not None:
                        model = str(model).replace(r'\|', '').replace("\\", "").replace("\n", "").replace("\r",
                                                                                                          "").replace(
                            ",", "").replace('®', '').replace('�', '').replace("'", "").replace('%', '').replace('⁄',
                                                                                                                 '')
                    if edition is not None:
                        edition = str(edition).replace(r'\|', '').replace("\\", "").replace("\n", "").replace("\r",
                                                                                                              "").replace(
                            ",", "").replace('®', '').replace('�', '').replace("'", "").replace('%', '').replace('⁄',
                                                                                                                 '')
                    if format is not None:
                        format = str(format).replace(r'\|', '').replace("\\", "").replace("\n", "").replace("\r",
                                                                                                            "").replace(
                            ",", "").replace('®', '').replace('�', '').replace("'", "").replace('%', '').replace('⁄',
                                                                                                                 '')
                    if packageHeight is not None:
                        packageHeight = str(packageHeight).replace(r'\|', '').replace("\\", "").replace("\n",
                                                                                                        "").replace(
                            "\r", "").replace(",", "").replace('®', '').replace('�', '').replace("'", "").replace('%',
                                                                                                                  '').replace(
                            '⁄', '')
                    if packageLength is not None:
                        packageLength = str(packageLength).replace(r'\|', '').replace("\\", "").replace("\n",
                                                                                                        "").replace(
                            "\r", "").replace(",", "").replace('®', '').replace('�', '').replace("'", "").replace('%',
                                                                                                                  '').replace(
                            '⁄', '')
                    if packageWidth is not None:
                        packageWidth = str(packageWidth).replace(r'\|', '').replace("\\", "").replace("\n", "").replace(
                            "\r", "").replace(",", "").replace('®', '').replace('�', '').replace("'", "").replace('%',
                                                                                                                  '').replace(
                            '⁄', '')
                    if packageWeight is not None:
                        packageWeight = str(packageWeight).replace(r'\|', '').replace("\\", "").replace("\n",
                                                                                                        "").replace(
                            "\r", "").replace(",", "").replace('®', '').replace('�', '').replace("'", "").replace('%',
                                                                                                                  '').replace(
                            '⁄', '')
                    if author is not None:
                        author = str(author).replace(r'\|', '').replace("\\", "").replace("\n", "").replace("\r",
                                                                                                            "").replace(
                            ",", "").replace('®', '').replace('�', '').replace("'", "").replace('%', '').replace('⁄',
                                                                                                                 '')
                    if binding is not None:
                        binding = str(binding).replace(r'\|', '').replace("\\", "").replace("\n", "").replace("\r",
                                                                                                              "").replace(
                            ",", "").replace('®', '').replace('�', '').replace("'", "").replace('%', '').replace('⁄',
                                                                                                                 '')
                    if partNumber is not None:
                        partNumber = str(partNumber).replace(r'\|', '').replace("\\", "").replace("\n", "").replace(
                            "\r", "").replace(",", "").replace('®', '').replace('�', '').replace("'", "").replace('%',
                                                                                                                  '').replace(
                            '⁄', '')
                    if title is not None:
                        title = str(title).replace(r'\|', '').replace("\\", "").replace("\n", "").replace("\r",
                                                                                                          "").replace(
                            ",", "").replace('®', '').replace('�', '').replace("'", "").replace('%', '').replace('⁄',
                                                                                                                 '')
                    if manufacturer is not None:
                        manufacturer = str(manufacturer).replace(r'\|', '').replace("\\", "").replace("\n", "").replace(
                            "\r", "").replace(",", "").replace('®', '').replace('�', '').replace("'", "").replace('%',
                                                                                                                  '').replace(
                            '⁄', '')
                    if brand is not None:
                        brand = str(brand).replace(r'\|', '').replace("\\", "").replace("\n", "").replace("\r",
                                                                                                          "").replace(
                            ",", "").replace('®', '').replace('�', '').replace("'", "").replace('%', '').replace('⁄',
                                                                                                                 '')
                    if categories is not None:
                        # categories = str(categories).replace("{'catId':", "").replace("'name'", "")
                        rootCat = str(categories[0]).replace('{', '').replace('}', '').replace(r'\|', '').replace("\\",
                                                                                                                  "").replace(
                            "\n", "").replace("\r", "").replace(",", "").replace('name', '').replace('catId',
                                                                                                     '').replace("'",
                                                                                                                 "").replace(
                            ':', '').replace('⁄', '').replace('  ', '-')
                        subCat = str(categories[(len(categories) - 1)]).replace('{', '').replace('}', '').replace(r'\|',
                                                                                                                  '').replace(
                            "\\", "").replace("\n", "").replace("\r", "").replace(",", "").replace('name', '').replace(
                            'catId', '').replace("'", "").replace(':', '').replace('⁄', '').replace('  ', '-')
                        # categories = str(categories).replace(',', '').replace('|', '').replace('[', '').replace(']', '').replace("{'catId':", "").replace("'name'", "")
                        # cats = categories.split('}')
                        # cat1 = cats[0]
                        # cat2 = str(cats[1])
                    if variationCSV is not None:
                        variationCSV = str(variationCSV).replace(r'\|', '').replace("\\", "").replace("\n", "").replace(
                            "\r", "").replace(",", "").replace('®', '').replace('�', '').replace("'", "").replace('%',
                                                                                                                  '').replace(
                            '⁄', '')
                    if ebayListingIds is not None:
                        ebayListingIds = str(ebayListingIds).replace(r'\|', '').replace("\\", "").replace("\n",
                                                                                                          "").replace(
                            "\r", "").replace(",", "").replace('®', '').replace('�', '').replace("'", "").replace('%',
                                                                                                                  '').replace(
                            '⁄', '')
                    if eanList is not None:
                        eanList = str(eanList).replace(r'\|', '').replace("\\", "").replace("\n", "").replace("\r",
                                                                                                              "").replace(
                            ",", "").replace('®', '').replace('�', '').replace("'", "").replace('%', '').replace('⁄',
                                                                                                                 '')
                    if eanList is not None:
                        upcList = str(upcList).replace(r'\|', '').replace("\\", "").replace("\n", "").replace("\r",
                                                                                                              "").replace(
                            ",", "").replace('®', '').replace('�', '').replace("'", "").replace('%', '').replace('⁄',
                                                                                                                 '')
                    if color is not None:
                        color = str(color).replace(r'\|', '').replace("\\", "").replace("\n", "").replace("\r",
                                                                                                          "").replace(
                            ",", "").replace('®', '').replace('�', '').replace("'", "").replace('%', '').replace('⁄',
                                                                                                                 '')
                        color = "".join(color)
                    if size is not None:
                        size1 = str(size).replace(r'\|', '').replace("\\", "").replace("\n", "").replace("\r",
                                                                                                         "").replace(
                            ",", "").replace('®', '').replace('�', '').replace("'", "").replace('%', '').replace('⁄',
                                                                                                                 '')
                        size = "".join(size1)
                    if description1 is not None:
                        description2 = str(description1).replace(r'\|', '').replace("\\", "").replace("\n", "").replace(
                            "\r", "").replace(",", "").replace('®', '').replace('�', '').replace("'", "").replace('%',
                                                                                                                  '').replace(
                            '⁄', '')
                        description = "".join(description2)
                    if frequentlyBoughtTogether is not None:
                        frequentlyBoughtTogether = str(frequentlyBoughtTogether).replace(r'\|', '').replace("\\",
                                                                                                            "").replace(
                            "\n", "").replace("\r", "").replace(",", "").replace('®', '').replace('�', '').replace("'",
                                                                                                                   "").replace(
                            '%', '').replace('⁄', '')
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
                        row = [str(timestamp), 'ACTIVE', str(rootCat), str(manufacturer), str(title),
                               str(lastUpdate),
                               str(rootCategory),
                               str(productType), str(parentAsin), str(variationCSV), str(asin), str(domainId),
                               str(hasReviews),
                               str(trackingSince), str(brand), str(productGroup), str(partNumber), str(model),
                               str(color), str(size),
                               str(edition), str(format), str(packageHeight), str(packageLength), str(packageWidth),
                               str(packageWeight), str(isAdultProduct),
                               str(isEligibleForTradeIn), str(isEligibleForSuperSaverShipping), str(rating),
                               str(reviewCount),
                               str(lastSeen),
                               str(sellerId), str(seller_name), str(seller_country),
                               str(condition), str(price), str(isPrime), str(isMap), str(isShippable),
                               str(isAddOnItem),
                               str(isPreOrder),
                               str(isWarehouseDeal), str(isScam), str(isAmazon), str(isPrimeExcl), str(isFba),
                               str(shipsFromChina), str(isRedirectASIN), str(isSNS), str(author),
                               str(binding),
                               str(numberOfItems),
                               str(numberOfPages), str(publicationDate), str(releaseDate), str(ebayListingIds),
                               str(eanList), str(upcList),
                               str(frequentlyBoughtTogether), str(description), str(avg),
                               str(avg30),
                               str(avg90), str(avg180),
                               str(avg365), str(stockAmazon), str(stockBuyBox), str(totalOfferCount),
                               str(salesRankDrops30), str(salesRankDrops90),
                               str(salesRankDrops180), str(salesRankDrops365), str(buyBoxPrice),
                               str(buyBoxShipping),
                               str(buyBoxIsFBA), str(buyBoxIsUsed),
                               str(buyBoxCondition), str(buyBoxSellerId), str(buyBoxIsAmazon), str(buyBoxIsMAP),
                               str(buyBoxMaxOrderQuantity),
                               str(rootSalesRank), str(subcatRank), str(oneTimeAbsolute), str(oneTimePercentage),
                               str(SNSpercentage),
                               str(audit_ts), str(subCat)]
                        if str(seller_name) == 'Amazon Warehouse':
                            pass
                        else:
                            if condition == 1:
                                return_list.append(row)
                            else:
                                pass
                else:
                    row1 = [str(timestamp), 'INACTIVE', str(rootCat), str(manufacturer), str(title),
                            str(lastUpdate),
                            str(rootCategory),
                            str(productType), str(parentAsin), str(variationCSV), str(asin), str(domainId),
                            str(hasReviews),
                            str(trackingSince), str(brand), str(productGroup), str(partNumber), str(model),
                            str(color), str(size),
                            str(edition), str(format), str(packageHeight), str(packageLength), str(packageWidth),
                            str(packageWeight), str(isAdultProduct),
                            str(isEligibleForTradeIn), str(isEligibleForSuperSaverShipping), str(rating),
                            str(reviewCount),
                            'lastSeen',
                            'sellerId', 'seller_name', 'seller_country',
                            'condition', 'price', 'isPrime', 'isMap', 'isShippable',
                            'isAddOnItem',
                            'isPreOrder',
                            'isWarehouseDeal', 'isScam', 'isAmazon', 'isPrimeExcl', 'isFba',
                            'shipsFromChina', str(isRedirectASIN), str(isSNS), str(author),
                            str(binding),
                            str(numberOfItems),
                            str(numberOfPages), str(publicationDate), str(releaseDate), str(ebayListingIds),
                            str(eanList), str(upcList),
                            str(frequentlyBoughtTogether), str(description), str(avg),
                            str(avg30),
                            str(avg90), str(avg180),
                            str(avg365), str(stockAmazon), str(stockBuyBox), str(totalOfferCount),
                            str(salesRankDrops30), str(salesRankDrops90),
                            str(salesRankDrops180), str(salesRankDrops365), str(buyBoxPrice),
                            str(buyBoxShipping),
                            str(buyBoxIsFBA), str(buyBoxIsUsed),
                            str(buyBoxCondition), str(buyBoxSellerId), str(buyBoxIsAmazon), str(buyBoxIsMAP),
                            str(buyBoxMaxOrderQuantity),
                            str(rootSalesRank), str(subcatRank), str(oneTimeAbsolute), str(oneTimePercentage),
                            str(SNSpercentage),
                            str(audit_ts), str(subCat)]
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
    # html_str = """
    #                             <html lang="en">
    #                             <head>
    #                                 <meta charset="UTF-8">
    #                             </head>
    #                             <body>
    #                                 <p>
    #                                 Good morning, <br><br>
    #                                 """ + str(asins_in) + """ - CA asins loaded into Keepa <br>
    #                                 """ + str(asins_out) + """ - CA dsitinct asins OUT
    #                                 <br>""" + str(filename) + """ "finished" .<br>
    #                                 </p>
    #                             </body>
    #                             </html>
    #                         """
    Html_file = open('./log/log_Email.html', "w")
    Html_file.write(str(html_str.encode('UTF-8')).replace('\\n', '').replace('\\t', '')[2:-1])
    Html_file.close()
    yag_smtp_connection.send('datateam@potoosolutions.com', subject, 'log_Email.html')  # send from autojobs
    # os.remove('./log/log_Email.html')


def get_out_asins(date):
    print('retrieving out asins')
    cur = conn.cursor()
    asin_count = 0
    try:
        cur.execute("""select count(distinct(product_asin)) from stg.keepa_product_daily_run_all
                        where domainid = 'Amazon.ca'
                        and lastseen like '""" + str(date) + """%'
                                """)
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
    filename = '_de/' + str(s1) + f"_de_{random.randint(100, 999)}.csv"
    with io.open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        spamwriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        spamwriter.writerow(['timestamp', 'status', 'rootCat', 'manufacturer', 'title', 'lastUpdate',
                             'rootCategory',
                             'productType', 'parentAsin', 'variationCSV', 'asin', 'domainId',
                             'hasReviews',
                             'trackingSince', 'brand', 'productGroup', 'partNumber', 'model',
                             'color', 'size',
                             'edition', 'format', 'packageHeight', 'packageLength', 'packageWidth',
                             'packageWeight', 'isAdultProduct',
                             'isEligibleForTradeIn', 'isEligibleForSuperSaverShipping', 'rating', 'reviewCount',
                             'lastSeen',
                             'sellerId', 'seller_name', 'seller_country',
                             'condition', 'price', 'isPrime', 'isMap', 'isShippable', 'isAddOnItem',
                             'isPreOrder',
                             'isWarehouseDeal', 'isScam', 'isAmazon', 'isPrimeExcl', 'isFBA',
                             'shipsFromChina',
                             'isRedirectASIN', 'isSNS', 'author', 'binding',
                             'numberOfItems',
                             'numberOfPages', 'publicationDate', 'releaseDate', 'ebayListingIds',
                             'eanList', 'upcList',
                             'frequentlyBoughtTogether', 'description', 'avg', 'avg30',
                             'avg90', 'avg180',
                             'avg365', 'stockAmazon', 'stockBuyBox', 'totalOfferCount',
                             'salesRankDrops30', 'salesRankDrops90',
                             'salesRankDrops180', 'salesRankDrops365', 'buyBoxPrice', 'buyBoxShipping',
                             'buyBoxIsFBA', 'buyBoxIsUsed',
                             'buyBoxCondition', 'buyBoxSellerId', 'buyBoxIsAmazon', 'buyBoxIsMAP',
                             'buyBoxMaxOrderQuantity',
                             'rootSalesRank', 'subcatRank', 'oneTimeAbsolute', 'oneTimePercentage',
                             'SNSpercentage',
                             'audit_ts', 'subcats'])
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
                    telegram_bot_send('-558961676',
                                      f"****   Token Waning - Amazon DE   ***** +%0A+Token Left :  {tokens}")
                    time.sleep(1000)
                    print('tokens refreshed.')
                else:
                    pass
            taskQueue.task_done()
            resultQueue.put(response['tokensConsumed'])
        print(response['refillRate'])
        print(response['refillIn'])
        print(response['tokensLeft'])
        print(response['tokensConsumed'])
        asin_count += 1
        print(str(asin_count) + ' - chunk finished in - ' + str(datetime.now() - chunkstart))
        print(str(asin_count) + ' - chunk finished in - ' + str(datetime.now()))
        # if datetime.now() > goalTime:
        # continue
        time.sleep(2)
        # insert_to_database(filename)
        # inserted_asins = get_out_asins(s2)
        sendLog(filename, total_asin_count, 100)
        telegram_bot_send('-552890666',
                          f"DE -- 100 ASIN: {chunk_count}+%0A+ConsumedToken: {response['tokensConsumed']}+%0A+RemainToken: {response['tokensLeft']}+%0A+KeepaResponseTime: {response_time}+%0A+ParsingTime: {parsing_time}+%0A+TotalTIme:  {str(datetime.now() - chunkstart)}")

    return True


# def run_request_none_thread(chunk, spamwriter, marketplace, goalTime):
#     chunkstart = datetime.now()
#     print(f"{len(chunk)}")
#     response = request(chunk, marketplace)
#     print(response['refillRate'])
#     print(response['refillIn'])
#     print(response['tokensLeft'])
#     print(response['tokensConsumed'])
#     print(response['tokenFlowReduction'])
#     # exit(0)
#     if response == 'error':
#         print('response error')
#         pass
#     else:
#         returned, tokens = parsing_function(response)
#         for row in returned:
#             try:
#                 # spamwriter.writerow(row)
#                 tt = 0
#             except Exception as e:
#                 print(str(row))
#                 print(str(e) + 'hi')
#                 pass
#         if tokens < 1000:
#             print('pasuing for token refresh')
#             time.sleep(3000)
#             print('tokens refreshed.')
#         else:
#             pass
#     print(response['refillRate'])
#     print(response['refillIn'])
#     print(response['tokensLeft'])
#     print(response['tokensConsumed'])
#     exit(0)
#     asin_count += 1
#     sendLog(filename, len(asin_list), len(asin_list))
#     print(str(asin_count) + ' - chunk finished in - ' + str(datetime.now() - chunkstart))
#     print(str(asin_count) + ' - chunk finished in - ' + str(datetime.now()))
#     if datetime.now() > goalTime:
#         continue
#     insert_to_database(filename)
#     inserted_asins = get_out_asins(s2)


def main():
    marketplace = '3'
    start = time.time()
    if not os.path.exists('./_de/'):
        os.makedirs('./_de/')
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

    print(f"ConsumedToken:{consumed_token}")
    print(f"Unfinished Task: {taskQueue.unfinished_tasks}")
    print(str(datetime.now() - startTime))
    for f in os.listdir('./_de/'):
        if not f.endswith(".csv"):
            continue
        os.remove(os.path.join('_de/', f))
    telegram_bot_send('-558961676',
                      f"DE Report+%0A+Total ASIN: {total_asin_count}+%0A+ConsumedToken: {consumed_token}+%0A+TotalTIme:  {str(datetime.now() - startTime)}")


if __name__ == '__main__':
    main()