from threading import Thread
import asyncio
import time , os

from libs.commonlib.db_insist import get_db
from libs.commonlib.pymongo_paginated_cursor import PaginatedCursor as mpcur

from libs.commonlib.graph_funcs import get_user_with_sellrequest_name, get_user_with_driverequest_name

SEARCH_SLEEP_TIME = int(os.getenv('SEARCH_SLEEP_TIME' , 1))

num_ongoing_deals_deals : dict = {}
finished_ongoing_ids : dict = {}
sellreq_name_to_email : dict = {}
drivereq_name_to_email : dict = {}

def get_num_num_ongoing_deals(email : str) -> int :
    return num_ongoing_deals_deals.get(email, 0)

def get_ongoing_isfinished(email : str) -> list :
    return finished_ongoing_ids.get(email, [])

def num_ongoing_deals_search() :
    db = get_db()
    pr_it = db.insist_on_find('ongoing_routes')
    updated_num_new_deals : dict = {}
    updated_finished_ongoing_ids : dict = {}
    for pr in mpcur(pr_it) :

        if 'wrapup' in pr :

            drivereq_name = pr.get('driveRequestName' , '')
            if not drivereq_name in drivereq_name_to_email :
                drive_graph = get_user_with_driverequest_name(drivereq_name)
                if drive_graph :
                    drivereq_name_to_email[drivereq_name] = drive_graph[0][0]['email']
                else:
                    continue
            driver_email = drivereq_name_to_email[drivereq_name]
            if not driver_email in updated_finished_ongoing_ids :
                updated_finished_ongoing_ids[driver_email] = []
            updated_finished_ongoing_ids[driver_email].append(str(pr['_id']))

        else:

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

    global num_ongoing_deals_deals
    num_ongoing_deals_deals = updated_num_new_deals
    global finished_ongoing_ids
    finished_ongoing_ids = updated_finished_ongoing_ids

def num_ongoing_deals_search_loop(asyncLoop) :
    asyncio.set_event_loop(asyncLoop)
    while True:
        try:
            num_ongoing_deals_search()
        except Exception as e :
            print('Exception during num_ongoing_deals_search : ' , e)
        time.sleep(SEARCH_SLEEP_TIME)

def start_search_for_num_ongoing_deals_loop() :
    asyncLoop = asyncio.new_event_loop()
    _thread = Thread(target=num_ongoing_deals_search_loop, args=(asyncLoop,))
    _thread.start()