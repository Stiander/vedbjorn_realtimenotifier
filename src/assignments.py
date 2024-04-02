import datetime
from threading import Thread
import asyncio
import time , os

from libs.commonlib.db_insist import get_db
from libs.commonlib.pymongo_paginated_cursor import PaginatedCursor as mpcur
from libs.qrpcclientlib.grpcClient import SetDriverNotAvailable , NotifyDriverOnNewMission

SEARCH_SLEEP_TIME = int(os.getenv('SEARCH_SLEEP_TIME' , 1))
ASSIGNMENT_ACCEPT_TIME  = int(os.getenv('ASSIGNMENT_ACCEPT_TIME' , 86400))

grabbable_assignments : dict = {}
grabbable_ongoing_assignments : dict = {}

def get_assignment(driveName : str) :
    if driveName in grabbable_assignments and 'accept_deadline' in grabbable_assignments[driveName] :
        return grabbable_assignments[driveName]['accept_deadline'] - datetime.datetime.utcnow().timestamp()
    return -1

def get_time_to_due(driveName : str) :
    if driveName in grabbable_ongoing_assignments and 'due' in grabbable_ongoing_assignments[driveName] :
        return grabbable_ongoing_assignments[driveName]['due'] - datetime.datetime.utcnow().timestamp()
    return -1

def get_must_refresh(driveName : str) :
    if driveName in grabbable_assignments and 'must_refresh' in grabbable_assignments[driveName] and \
            grabbable_assignments[driveName]['must_refresh'] == True :
        return True
    return False

def set_has_refreshed(driveName : str) :
    if driveName in grabbable_assignments :
        grabbable_assignments[driveName]['must_refresh'] = False

def assignment_search() :
    db = get_db()
    planned_routes_it = db.insist_on_find('planned_routes' , {} , {
        '_id'                  : 1 ,
        'driveRequestName'     : 1 ,
        'accepted'             : 1 ,
        'accept_deadline'      : 1 ,
        'updated'              : 1 ,
        'num_sellers'          : 1 ,
        'num_sellers_accepted' : 1
    })
    for planned_route in mpcur(planned_routes_it) :
        driveRequestName = planned_route['driveRequestName']
        if 'accepted' in planned_route :
            if driveRequestName in grabbable_assignments :
                del grabbable_assignments[driveRequestName]
            continue

        """
        Check that all sellers has accepted the mission
        """
        num_sellers = planned_route.get('num_sellers' , 0)
        num_sellers_accepted = planned_route.get('num_sellers_accepted' , 0)
        if num_sellers <= 0 or num_sellers_accepted <= 0:
            continue
        if num_sellers_accepted < num_sellers :
            continue

        if not 'accept_deadline' in planned_route :
            accept_deadline = datetime.datetime.utcnow().timestamp() + ASSIGNMENT_ACCEPT_TIME
            db.insist_on_update_one(planned_route, 'planned_routes' , 'accept_deadline' , accept_deadline)
            planned_route['accept_deadline'] = accept_deadline
            NotifyDriverOnNewMission(driveRequestName)

        if not driveRequestName in grabbable_assignments :
            grabbable_assignments[driveRequestName] = {
                '_id' : planned_route['_id'] ,
                'accept_deadline' : planned_route['accept_deadline'] ,
                'updated' : planned_route.get('updated' , 0)
            }

    ongoing_routes_it = db.insist_on_find('ongoing_routes', {}, {
        '_id': 1,
        'driveRequestName': 1,
        'due': 1
    })
    for ongoing_route in mpcur(ongoing_routes_it) :
        driveRequestName = ongoing_route['driveRequestName']
        if not driveRequestName in grabbable_ongoing_assignments :
            grabbable_ongoing_assignments[driveRequestName] = ongoing_route

def update_grabbable_assignments() :
    db = get_db()
    _now = datetime.datetime.utcnow().timestamp()
    remove_these : list = []
    try:
        for driveRequestName , assignment in grabbable_assignments.items() :
            routeObj = db.insist_on_find_one('planned_routes', assignment['_id'], {'_id' : 1 , 'updated' : 1})
            if not routeObj:
                del grabbable_assignments[driveRequestName]
                continue
            if assignment['accept_deadline'] < _now :
                routeObj = db.insist_on_find_one('planned_routes', assignment['_id'])
                if not routeObj :
                    pass
                elif not 'accepted' in routeObj :
                    del routeObj['_id']
                    db.insist_on_insert_one('planned_routes_not_accepted' , routeObj)
                    db.insist_on_delete_one('planned_routes' , assignment['_id'])
                    SetDriverNotAvailable(driveRequestName)
                remove_these.append(driveRequestName)
            if 'updated' in routeObj :
                updated = routeObj['updated']
                last_known_update = assignment.get('updated' , 0)
                if updated > last_known_update :
                    assignment['updated'] = updated
                    assignment['must_refresh'] = True
    except Exception as e:
        print('Exception when iterating grabbable_assignments : ')
        print(e)
        return

    try:
        for driveRequestName, assignment in grabbable_assignments.items():
            routeObj = db.insist_on_find_one('planned_routes', assignment['_id'], {'_id': 1})
            if not routeObj:
                remove_these.append(driveRequestName)
        for removeMe in remove_these :
            if removeMe in grabbable_assignments :
                print('assignments :: Removing assignments for ', removeMe)
                del grabbable_assignments[removeMe]
    except Exception as e:
        print('Exception when iterating grabbable_assignments (2) : ')
        print(e)
        return

    try:
        remove_these = []
        for driveRequestName, assignment in grabbable_ongoing_assignments.items():
            routeObj = db.insist_on_find_one('ongoing_routes', assignment['_id'], {'_id': 1})
            if not routeObj:
                remove_these.append(driveRequestName)
        for removeMe in remove_these :
            if removeMe in grabbable_ongoing_assignments :
                print('assignments :: Removing ongoing assignments for ', removeMe)
                del grabbable_ongoing_assignments[removeMe]
    except Exception as e:
        print('Exception when iterating grabbable_ongoing_assignments : ')
        print(e)
        return


def assignment_search_loop(asyncLoop) :
    asyncio.set_event_loop(asyncLoop)
    while True:
        try:
            assignment_search()
        except Exception as e :
            print('Exception during assignment_search : ' , e)
        try:
            update_grabbable_assignments()
        except Exception as e :
            print('Exception during update_grabbable_assignments : ' , e)
        time.sleep(SEARCH_SLEEP_TIME)

def start_assignment_search_loop() :
    asyncLoop = asyncio.new_event_loop()
    _thread = Thread(target=assignment_search_loop, args=(asyncLoop,))
    _thread.start()