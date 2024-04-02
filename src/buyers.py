from threading import Thread
import asyncio
import time , os

from libs.commonlib.db_insist import get_db
from libs.commonlib.db_insist import all_objectids_to_str
from libs.commonlib.pymongo_paginated_cursor import PaginatedCursor as mpcur

SEARCH_SLEEP_TIME = int(os.getenv('SEARCH_SLEEP_TIME' , 1))

grabbable_notifications : dict = {}
delivery_notifications : dict = {}
payment_statuses : dict = {}

def get_found_notification(email : str) :
    if email in grabbable_notifications and grabbable_notifications[email]['has_been_notified'] == False:
        return grabbable_notifications[email]['ongoing_routes']
    return ''

def get_found_delivery_notification(email : str) :
    if email in delivery_notifications and delivery_notifications[email]['has_been_notified'] == False:
        return all_objectids_to_str(delivery_notifications[email]['delivery'])
    return ''

def set_found_notification(email : str) :
    if email in grabbable_notifications :
        grabbable_notifications[email]['has_been_notified'] = True

def set_found_delivery_notification(email : str) :
    if email in delivery_notifications :
        delivery_notifications[email]['has_been_notified'] = True

def get_payment_status(email : str , pay_id : str) :
    return payment_statuses.get(email + pay_id, '')

def payment_status_search() :
    db = get_db()
    payment_it = db.insist_on_find('vipps_payments_in')
    for pay in mpcur(payment_it):
        email = pay.get('paying_user' , {}).get('email' , '')
        status = pay.get('status' , '')
        if email and status :
            payment_statuses[email + str(pay['_id'])] = status

def notifications_search() :
    db = get_db()
    notifications_it = db.insist_on_find('notifications' , {'contentType' : 'delivery'})
    existing_grabbable : dict = {}

    for notification in mpcur(notifications_it) :
        email = notification['email']

        delivery = db.insist_on_find_one('deliveries', notification.get('ref_id', None))
        if not delivery:
            continue

        payment = db.insist_on_find_one('vipps_payments_in', delivery.get('payment_ref', None))
        if not payment:
            continue

        if payment.get('status', '') == 'unpaid':

            notification['_id'] = str(notification['_id'])
            # if email in delivery_notifications :
            #     already_delivery = delivery_notifications[email].get('delivery' , {})
            #     if str(already_delivery.get('_id', '')) == str(notification['_id']) :
            #         # there is already a notification for this, present, possibly one which has already been seen
            #         continue

            delivery_notifications[email] = {
                'delivery' : all_objectids_to_str(notification) ,
                'has_been_notified' : False
            }

        else:
            if not email in grabbable_notifications and notification.get('status' , '') == 'requested':
                grabbable_notifications[email] = {
                    'ongoing_routes' : str(notification['ongoing_routes']) ,
                    'has_been_notified' : False
                }

            existing_grabbable[email] = 1

    remove_these : dict = {}
    for email, noti in grabbable_notifications.items() :
        if not email in existing_grabbable :
            remove_these[email] = 1
    for email , _ in remove_these.items() :
        if email in grabbable_notifications :
            print('buyers :: Removing notification for ' , email)
            del grabbable_notifications[email]

    remove_these = {}
    for email, noti in delivery_notifications.items() :
        if delivery_notifications.get('has_been_notified' , False) :
            remove_these[email] = 1
    for email , _ in remove_these.items() :
        if email in delivery_notifications :
            print('buyers :: Removing delivery-notification for ' , email)
            del delivery_notifications[email]

def notifications_search_loop(asyncLoop) :
    asyncio.set_event_loop(asyncLoop)
    while True:
        try:
            notifications_search()
            payment_status_search()
        except Exception as e :
            print('Exception during notifications_search : ' , e)
        time.sleep(SEARCH_SLEEP_TIME)

def start_search_for_notifications_loop() :
    asyncLoop = asyncio.new_event_loop()
    _thread = Thread(target=notifications_search_loop, args=(asyncLoop,))
    _thread.start()