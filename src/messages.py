from threading import Thread
import asyncio
import time , os

from libs.commonlib.db_insist import get_db
from libs.commonlib.pymongo_paginated_cursor import PaginatedCursor as mpcur

SEARCH_SLEEP_TIME = int(os.getenv('SEARCH_SLEEP_TIME' , 1))

grabbable_messages : dict = {}

def get_messages(email : str) :
    if email in grabbable_messages:
        ret : list = []
        for _in, msg in grabbable_messages[email].items():
            if msg.get('has_been_notified' , False) == False :
                ret.append(msg)
        return ret
    return []

def set_messages_seen(email : str) :
    if email in grabbable_messages :
        for _in , msg in grabbable_messages[email].items() :
            msg['has_been_notified'] = True

def messages_search() :
    db = get_db()
    messages_it = db.insist_on_find('notifications')
    existing: dict = {}
    for msg in mpcur(messages_it) :
        email = msg['email']
        if not email in grabbable_messages:
            grabbable_messages[email] = {}
        if not str(msg['_id']) in grabbable_messages[email]:
            grabbable_messages[email][str(msg['_id'])] = {
                'email' : msg.get('email' , '') ,
                'ongoing_routes' : str(msg.get('ongoing_routes' , '')) ,
                'status' : msg.get('status' , '') ,
                'contentType' : msg.get('contentType' , '') ,
                'text' : msg.get('text' , '') ,
                'has_been_notified' : False ,
                'canceller' : msg.get('canceller' , '') ,
                'route_index' : msg.get('route_index' , -1)
            }
        existing[email] = 1

    remove_these : dict = {}
    for email, noti in grabbable_messages.items() :
        if not email in existing :
            remove_these[email] = 1
    for email , _ in remove_these.items() :
        if email in grabbable_messages :
            print('messages :: Removing messages for ', email)
            del grabbable_messages[email]

def messages_search_loop(asyncLoop) :
    asyncio.set_event_loop(asyncLoop)
    while True:
        try:
            messages_search()
        except Exception as e :
            print('Exception during driver_notifications_search : ' , e)
        time.sleep(SEARCH_SLEEP_TIME)

def start_search_for_messages_loop() :
    asyncLoop = asyncio.new_event_loop()
    _thread = Thread(target=messages_search_loop, args=(asyncLoop,))
    _thread.start()