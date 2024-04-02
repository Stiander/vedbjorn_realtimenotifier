import datetime
from threading import Thread
import asyncio
import time , os

from qrpcclientlib.grpcClient import ShortCountryInfo , GetMarketInfo
from commonlib.db_insist import get_db

STAT_CALC_SLEEP_TIME = int(os.getenv('STAT_CALC_SLEEP_TIME' , 1))

type_users_stat : dict = {}

def get_type_users_stat(county : str , municipality : str = '') -> dict :
    if not county in type_users_stat :
        return {}
    if not municipality :
        return type_users_stat[county]['stats']
    elif not municipality in type_users_stat[county]['munistats'] :
        return {}
    else:
        return type_users_stat[county]['munistats'][municipality]['stats']

def lists_are_the_same(l1 : list,l2 : list) -> bool:
   l1.sort()
   l2.sort()
   return l1.sort() == l2.sort()

def calc_type_users_stat() :
    db = get_db()
    cnties = ShortCountryInfo('Norway')
    for countyName , munis in cnties.items() :
        if not countyName in type_users_stat :
            already_county_stats = db.insist_on_find_one_q('type_users_stat' , {
                'level' : 'county' ,
                'county' : countyName ,
                'latest' : True
            })
            if already_county_stats :
                already_county_stats['munistats'] = {}
                type_users_stat[countyName] = already_county_stats

        countyInfo = GetMarketInfo({'county' : countyName})
        if not countyName in type_users_stat :
            stats_obj : dict = {
                'level' : 'county' ,
                'county' : countyName ,
                'latest' : True ,
                'time' : datetime.datetime.utcnow().timestamp() ,
                'stats' : countyInfo ,
                'municipalities' : munis
            }
            inserted_id = db.insist_on_insert_one('type_users_stat' , stats_obj)
            stats_obj['_id'] = inserted_id
            stats_obj['munistats'] = {}
            type_users_stat[countyName] = stats_obj
        else :
            compary_county = type_users_stat[countyName]
            if compary_county['stats']['num_sellers'] != countyInfo['num_sellers'] or \
            compary_county['stats']['num_buyers'] != countyInfo['num_buyers'] or \
            compary_county['stats']['num_drivers'] != countyInfo['num_drivers'] or \
            not lists_are_the_same(compary_county['municipalities'] , munis) :
                db.insist_on_update_one(compary_county, 'type_users_stat', 'latest' , False)
                munistats = compary_county['munistats']
                stats_obj: dict = {
                    'level': 'county',
                    'county': countyName,
                    'latest': True,
                    'time': datetime.datetime.utcnow().timestamp(),
                    'stats': countyInfo,
                    'municipalities': munis
                }
                inserted_id = db.insist_on_insert_one('type_users_stat', stats_obj)
                stats_obj['_id'] = inserted_id
                stats_obj['munistats'] = munistats
                type_users_stat[countyName] = stats_obj

        for muni in munis :
            if not muni in type_users_stat[countyName]['munistats'] :
                already_muni_stats = db.insist_on_find_one_q('type_users_stat', {
                    'level': 'municipality',
                    'county': countyName,
                    'municipality' : muni,
                    'latest': True
                })
                if already_muni_stats :
                    type_users_stat[countyName]['munistats'][muni] = already_muni_stats
            muniInfo = GetMarketInfo({'county': countyName, 'municipality': muni})
            if not muni in type_users_stat[countyName]['munistats']:
                stats_obj: dict = {
                    'level': 'municipality',
                    'county': countyName,
                    'municipality': muni,
                    'latest': True,
                    'time': datetime.datetime.utcnow().timestamp(),
                    'stats': muniInfo
                }
                inserted_id = db.insist_on_insert_one('type_users_stat', stats_obj)
                stats_obj['_id'] = inserted_id
                type_users_stat[countyName]['munistats'][muni] = stats_obj
            else:
                compare_muni = type_users_stat[countyName]['munistats'][muni]
                if compare_muni['stats']['num_sellers'] != muniInfo['num_sellers'] or \
                compare_muni['stats']['num_buyers'] != muniInfo['num_buyers'] or \
                compare_muni['stats']['num_drivers'] != muniInfo['num_drivers'] :
                    db.insist_on_update_one(compare_muni, 'type_users_stat', 'latest', False)
                    stats_obj: dict = {
                        'level': 'municipality',
                        'county': countyName,
                        'municipality': muni,
                        'latest': True,
                        'time': datetime.datetime.utcnow().timestamp(),
                        'stats': muniInfo
                    }
                    inserted_id = db.insist_on_insert_one('type_users_stat', stats_obj)
                    stats_obj['_id'] = inserted_id
                    type_users_stat[countyName]['munistats'][muni] = stats_obj

def market_stats_loop(asyncLoop) :
    asyncio.set_event_loop(asyncLoop)
    while True:
        try:
            calc_type_users_stat()
        except Exception as e:
            print('Exception in calc_type_users_stat :' , e)
        time.sleep(STAT_CALC_SLEEP_TIME)

def start_market_stats_loop() :
    asyncLoop = asyncio.new_event_loop()
    _thread = Thread(target=market_stats_loop, args=(asyncLoop,))
    _thread.start()