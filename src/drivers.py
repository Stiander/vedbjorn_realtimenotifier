from threading import Thread
import asyncio
import time , os

from libs.commonlib.db_insist import get_db
from libs.commonlib.pymongo_paginated_cursor import PaginatedCursor as mpcur

SEARCH_SLEEP_TIME = int(os.getenv('SEARCH_SLEEP_TIME' , 1))

grabbable_notifications : dict = {}

def get_driver_found_notification(email : str) :
    if email in grabbable_notifications:
        ret : list = []
        for _in, noti in grabbable_notifications[email].items():
            if noti.get('has_been_notified' , False) == False :
                ret.append(noti)
        return ret
    return []

def set_driver_found_notification(email : str) :
    if email in grabbable_notifications :
        for _in , noti in grabbable_notifications[email].items() :
            noti['has_been_notified'] = True

def driver_notifications_search() :
    db = get_db()
    notifications_it = db.insist_on_find('notifications' , {'contentType' : 'driver'})
    existing : dict = {}
    for notification in mpcur(notifications_it) :
        email = notification['email']
        if not email in grabbable_notifications:
            grabbable_notifications[email] = {}
        if not str(notification['_id']) in grabbable_notifications[email]:
            grabbable_notifications[email][str(notification['_id'])] = {
                'email' : notification.get('email' , '') ,
                'ongoing_routes' : str(notification.get('ongoing_routes' , '')) ,
                'status' : notification.get('status' , '') ,
                'contentType' : notification.get('contentType' , '') ,
                'text' : notification.get('text' , '') ,
                'has_been_notified' : False ,
                'canceller' : notification.get('canceller' , '') ,
                'route_index' : notification.get('route_index' , -1)
            }
        existing[email] = 1

    remove_these : dict = {}
    for email, noti in grabbable_notifications.items() :
        if not email in existing :
            remove_these[email] = 1

    for email , _ in remove_these.items() :
        if email in grabbable_notifications :
            print('drivers :: Removing notification for ', email)
            del grabbable_notifications[email]

def driver_notifications_search_loop(asyncLoop) :
    asyncio.set_event_loop(asyncLoop)
    while True:
        try:
            driver_notifications_search()
        except Exception as e :
            print('Exception during driver_notifications_search : ' , e)
        time.sleep(SEARCH_SLEEP_TIME)

def start_search_for_driver_notifications_loop() :
    asyncLoop = asyncio.new_event_loop()
    _thread = Thread(target=driver_notifications_search_loop, args=(asyncLoop,))
    _thread.start()