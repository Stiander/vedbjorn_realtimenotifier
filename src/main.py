__author__ = 'Stiander'

import sys, os
myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath + '/libs/')
sys.path.insert(0, myPath + '/libs/commonLib')
sys.path.insert(0, myPath + '/libs/matchLib')
sys.path.insert(0, myPath + '/libs/qrpcclientlib')

import socketio
import auth
from aiohttp import web
from aiohttp_middlewares import cors_middleware
from aiohttp_middlewares.cors import DEFAULT_ALLOW_HEADERS
import jwt
import requests
import datetime
import asyncio
from urllib import parse

from stats import start_market_stats_loop , get_type_users_stat
from assignments import start_assignment_search_loop , get_assignment , get_time_to_due , get_must_refresh , set_has_refreshed
from buyers import get_found_notification , set_found_notification , start_search_for_notifications_loop , \
    get_found_delivery_notification , set_found_delivery_notification , get_payment_status
from drivers import start_search_for_driver_notifications_loop, set_driver_found_notification, get_driver_found_notification
from messages import start_search_for_messages_loop, get_messages, set_messages_seen
from newdeals import start_search_for_numdeals_loop, get_num_new_deals, get_has_old_deals
from ongoingdeals import start_search_for_num_ongoing_deals_loop, get_num_num_ongoing_deals, get_ongoing_isfinished

start_market_stats_loop()
start_assignment_search_loop()
start_search_for_notifications_loop()
start_search_for_driver_notifications_loop()
start_search_for_messages_loop()
start_search_for_numdeals_loop()
start_search_for_num_ongoing_deals_loop()

HOST=os.getenv("HOST", "0.0.0.0")
PORT= int(os.getenv("PORT",5000))

sio = socketio.AsyncServer(
    async_mode='aiohttp' ,

    # Warning : https://1library.net/article/cross-origin-controls-python-socketio-documentation.y4wo6d75
    cors_allowed_origins='*'
)

# Unsecure configuration to allow all CORS requests
app = web.Application(
    middlewares=[cors_middleware(allow_all=True)]
)
sio.attach(app)
routes = web.RouteTableDef()
subscribers : dict = {}

@sio.event
async def ongoing_isfinished(sid, data = None) :
    while sid in subscribers:
        query = subscribers[sid]
        if 'email' in query :
            finished_id_list = get_ongoing_isfinished(query['email'])
            if len(finished_id_list) > 0:
                await sio.emit('ongoing_isfinished', finished_id_list, room=sid)
        await asyncio.sleep(1)

@sio.event
async def getnongoingdeals(sid, data = None) :
    while sid in subscribers:
        query = subscribers[sid]
        if 'email' in query :
            num = get_num_num_ongoing_deals(query['email'])
            await sio.emit('getnongoingdeals', num, room=sid)
        await asyncio.sleep(1)

@sio.event
async def getnewdeals(sid, data = None) :
    while sid in subscribers:
        query = subscribers[sid]
        if 'email' in query :
            num = get_num_new_deals(query['email'])
            await sio.emit('getnewdeals', num, room=sid)
        await asyncio.sleep(1)

@sio.event
async def gethasolddeals(sid, data = None) :
    while sid in subscribers:
        query = subscribers[sid]
        if 'email' in query :
            num = get_has_old_deals(query['email'])
            if num > 0:
                await sio.emit('gethasolddeals', num, room=sid)
        await asyncio.sleep(1)

@sio.event
async def getdelivery(sid, data = None) :
    while sid in subscribers:
        query = subscribers[sid]
        if 'email' in query :
            delivery_id = get_found_delivery_notification(query['email'])
            if delivery_id != '' :
                set_found_delivery_notification(query['email'])
                await sio.emit('getdelivery', delivery_id, room=sid)
        await asyncio.sleep(1)

@sio.event
async def getmessages(sid, data = None) :
    while sid in subscribers:
        query = subscribers[sid]
        if 'email' in query :
            messages = get_messages(query['email'])
            if len(messages) > 0 :
                set_messages_seen(query['email'])
                for msg in messages :
                    await sio.emit('getmessages', msg, room=sid)
        await asyncio.sleep(1)

@sio.event
async def getdrivernotification(sid, data = None) :
    while sid in subscribers:
        query = subscribers[sid]
        if 'email' in query :
            notifications = get_driver_found_notification(query['email'])
            if len(notifications) > 0 :
                set_driver_found_notification(query['email'])
                await sio.emit('getdrivernotification', notifications, room=sid)
        await asyncio.sleep(1)

@sio.event
async def getstats(sid, data = None):
    while sid in subscribers:
        query = subscribers[sid]
        stats_obj : dict = {}
        if 'county' in query :
            county_stats = get_type_users_stat(query['county'])
            if county_stats :
                stats_obj['county'] = county_stats
        if 'municipality' in query :
            muni_stats = get_type_users_stat(query['county'] , query['municipality'])
            if muni_stats :
                stats_obj['municipality'] = muni_stats
        if 'county' in stats_obj and 'municipality' in stats_obj :
            await sio.emit('getstats', stats_obj, room=sid)
        await asyncio.sleep(1)
    print('Unsubscribed : ', sid)

@sio.event
async def getnewassignments(sid, data = None) :
    while sid in subscribers:
        query = subscribers[sid]
        if 'driverName' in query :
            countdown = get_assignment(query['driverName'])
            await sio.emit('getnewassignments', countdown, room=sid)
        await asyncio.sleep(1)

@sio.event
async def getongoingassignments(sid, data = None) :
    while sid in subscribers:
        query = subscribers[sid]
        if 'driverName' in query :
            countdown = get_time_to_due(query['driverName'])
            await sio.emit('getongoingassignments', countdown, room=sid)
        await asyncio.sleep(1)

@sio.event
async def getmustrefresh(sid, data = None) :
    while sid in subscribers:
        query = subscribers[sid]
        if 'driverName' in query :
            must_refresh = get_must_refresh(query['driverName'])
            if must_refresh :
                set_has_refreshed(query['driverName'])
                await sio.emit('getmustrefresh', must_refresh, room=sid)
        await asyncio.sleep(1)

@sio.event
async def getfoundbagstobuy(sid, data = None) :
    while sid in subscribers:
        query = subscribers[sid]
        if 'email' in query :
            ongoing_route = get_found_notification(query['email'])
            if ongoing_route != '' :
                set_found_notification(query['email'])
                await sio.emit('getfoundbagstobuy', ongoing_route, room=sid)
        await asyncio.sleep(1)

@sio.event
async def current_payment_changes(sid, data = None) :
    while sid in subscribers :
        query = subscribers[sid]
        if 'email' in query:
            status = get_payment_status(query.get('email' , '_') , query.get('payment_id' , ''))
            if status :
                await sio.emit('current_payment_changes', status, room=sid)
        await asyncio.sleep(1)

@sio.event
def connect(sid, environ, token : dict):

    if token :
        token_payload = token.get('token' , '')
        if not token_payload :
            return
        auth_content = auth.decode_auth_header(token_payload)
        if not auth.let_me_in(auth_content):
            return
        else:
            print('NEW CONNECTION : ' , auth_content.get('email' , '__UNKNOWN__'))
    else:
        return

    query = environ.get("QUERY_STRING")
    query = dict(parse.parse_qsl(query))
    if query.get('email' , '_') != auth_content.get('email' , '__UNKNOWN__') :
        return

    if not sid in subscribers :
        subscribers[sid] = query

@sio.event
def disconnect(sid):
    if sid in subscribers :
        del subscribers[sid]

app.add_routes(routes)
if __name__ == "__main__":
    web.run_app(app , port=PORT)