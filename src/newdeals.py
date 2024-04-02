from threading import Thread
import asyncio
import time , os

from libs.commonlib.db_insist import get_db
from libs.commonlib.pymongo_paginated_cursor import PaginatedCursor as mpcur

from libs.commonlib.graph_funcs import get_user_with_sellrequest_name

SEARCH_SLEEP_TIME = int(os.getenv('SEARCH_SLEEP_TIME' , 1))

num_new_deals : dict = {}
sellreq_name_to_email : dict = {}
has_old_deals : dict = {}

def get_num_new_deals(email : str) -> int :
    return num_new_deals.get(email, 0)

def get_has_old_deals(email : str) -> int :
    return has_old_deals.get(email, 0)

def numdeals_search() :
    db = get_db()
    pr_it = db.insist_on_find('planned_routes')
    updated_num_new_deals : dict = {}
    for pr in mpcur(pr_it) :
        for sellreq_name , deal in pr.get('deals' , {}).items() :
            if not sellreq_name in sellreq_name_to_email :
                seller_graph = get_user_with_sellrequest_name(sellreq_name)
                if seller_graph :
                    sellreq_name_to_email[sellreq_name] = seller_graph[0][0]['email']
            if sellreq_name in sellreq_name_to_email :
                corresponding_email = sellreq_name_to_email[sellreq_name]
                if not corresponding_email in updated_num_new_deals :
                    updated_num_new_deals[corresponding_email] = 1
                else:
                    updated_num_new_deals[corresponding_email] = updated_num_new_deals[corresponding_email] + 1
    global num_new_deals
    num_new_deals = updated_num_new_deals

def has_old_deals_search() :
    db = get_db()
    pay_it = db.insist_on_find('vipps_payments_out' , {
        'target' : 'seller'
    })
    updated_has_old_deals : dict = {}
    for pay_obj in mpcur(pay_it) :
        seller_graph = get_user_with_sellrequest_name(pay_obj.get('receiving_user' , {}).get('name' , ''))
        if not seller_graph :
            continue
        email = seller_graph[0][0].get('email' , '')
        if email and not email in updated_has_old_deals :
            updated_has_old_deals[email] = 1
    global has_old_deals
    has_old_deals = updated_has_old_deals

def numdeals_search_loop(asyncLoop) :
    asyncio.set_event_loop(asyncLoop)
    while True:
        try:
            numdeals_search()
            has_old_deals_search()
        except Exception as e :
            print('Exception during driver_notifications_search : ' , e)
        time.sleep(SEARCH_SLEEP_TIME)

def start_search_for_numdeals_loop() :
    asyncLoop = asyncio.new_event_loop()
    _thread = Thread(target=numdeals_search_loop, args=(asyncLoop,))
    _thread.start()