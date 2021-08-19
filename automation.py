from datetime import datetime, timedelta
import psycopg2
import os
import yagmail
import time
from config import config


conn = None
params = config()
conn = psycopg2.connect(**params)


def truncate_table():
    print('truncating temp table')
    cur = conn.cursor()
    try:
        cur.execute("""truncate stg.keepa_product_daily_run_tmp""")
        conn.commit()
        print('table truncated')
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        cur.close()
    cur.close()


def insert_to_tmp(today):
    print('starting insert to tmp')
    cur = conn.cursor()
    try:
        cur.execute("""insert into stg.keepa_product_daily_run_tmp (
                        select * from stg.keepa_product_daily_run
                        where lastseen like '""" + str(today) + """%')
                    """)
        conn.commit()
        print('data inserted')
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        cur.close()
    cur.close()


def update_tmp(yesterday):
    print('starting update to tmp')
    cur = conn.cursor()
    try:
        cur.execute("""update stg.keepa_product_daily_run_tmp 
                        set lastseen = '""" + str(yesterday) + """'
                    """)
        conn.commit()
        print('data updated')
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        cur.close()
    cur.close()


def run_function():
    print('starting insert to production tables')
    cur = conn.cursor()
    try:
        cur.execute("""select potoo_db.keepa_pull()""")
        conn.commit()
        print('data inserted to production')
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        cur.close()
    cur.close()


def run_function2():
    print('starting insert to production tables')
    cur = conn.cursor()
    try:
        cur.execute("""select potoo_db.etl_load()""")
        conn.commit()
        print('data inserted to production')
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        cur.close()
    cur.close()


def run_function3():
    print('starting insert to production tables')
    cur = conn.cursor()
    try:
        cur.execute("""select potoo_db.dashboard_auto_c()""")
        conn.commit()
        print('data inserted to production')
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        cur.close()
    cur.close()


def select_inventory():
    cur = conn.cursor()
    try:
        cur.execute("""SELECT country_code, count(*)
                        FROM potoo_db.sellerinventory_asin_seller
                        group by country_code 
                        """)
        output = cur.fetchall()
        print('data fetched')
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        output = 'error'
        cur.close()
    cur.close()
    return output


def sendLog(output1):
    yag_smtp_connection = yagmail.SMTP(user="autojobs@potoosolutions.com", password="Potoo2021!", host='smtp.gmail.com')
    subject = 'Keepa Log File1'
    # attachment = ['Logs/Log_' + str(s1) + '.log']
    html_str = """ 
                            <html lang="en">
                            <head>
                                <meta charset="UTF-8">
                            </head>
                            <body>
                                <p>
                                Good morning, <br><br> 
                                """ + str(output1) + """
                                </p> 
                            </body>
                            </html>                   
                        """
    Html_file = open('log_Email1.html', "w")
    Html_file.write(str(html_str.encode('UTF-8')).replace('\\n', '').replace('\\t', '')[2:-1])
    Html_file.close()
    yag_smtp_connection.send('datateam@potoosolutions.com', subject, 'log_Email1.html')  # send from autojobs
    os.remove('log_Email1.html')


def main():
    now = datetime.now()
    s1 = now.strftime("%Y-%m-%d")
    s2 = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    # truncate_table()
    insert_to_tmp(s1)
    update_tmp(s2)
    run_function()
    #try:
#    	run_function2()
#    except:
#    	pass
#    try:
#    	run_function3()
    #except:
    	#pass
    output = select_inventory()
    sendLog(output)
    # s3_control will execute at 10:30am


if __name__ == '__main__':
    main()