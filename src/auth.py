
import jwt
import os
from libs.commonlib.db_insist import get_db


REACT_APP_JWT_SECRET  = os.getenv('REACT_APP_JWT_SECRET', '')
ALWAYS_LET_ME_IN      = os.getenv('ALWAYS_LET_ME_IN', 'false') == 'true'

def decode_auth_header(auth_content) :
    # TODO ##############
    # TODO
    # TODO : We are using symmetric signature HS256, which is not the most secure (but its faster). Should switch to assymetric
    # TODO   with RS256, using a public+private key.
    # TODO
    # TODO  NOTE : Signing IS NOT encryption, so don't lean on it for security, only for data integrity
    # TODO
    # TODO ##############
    try :
        return jwt.decode(auth_content, REACT_APP_JWT_SECRET, algorithms=["HS256"])
    except Exception as e:
        return {}

def let_me_in(decoded_jwt : dict, db = get_db()) -> bool :
    if ALWAYS_LET_ME_IN == True :
        return True
    try:
        access_token = decoded_jwt.get('access_token' , '')
        phone = decoded_jwt.get('phone' , '')
        if not access_token or not phone:
            return False
        confirmed_token = db.insist_on_find_one_q('access_tokens' , {'num' : phone})
        return confirmed_token and confirmed_token['access_token'] == access_token
    except Exception as e:
        return False


