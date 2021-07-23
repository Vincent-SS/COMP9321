import urllib.request as req
import json
import sqlite3
from flask import Flask, request, send_file
from flask_restx import Resource, Api, fields
import re
import time
import math
import datetime
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt

app = Flask(__name__)
api = Api(app, version='1.0', title='Film Data Service', description='A Data Service for TV Shows, fetched from https://api.tvmaze.com/.\n Completed by Vincent Shi.')
ns = api.namespace('tv-shows', description='tv-shows-db')


'''def get_arg():
    parser = api.parser()
    parser.add_argument('name', required=True, type=str, help='Q1 Post', location='args')
    parser.add_argument('aa', required=True, type=str, help='Q1 Post', location='args')
    return parser
'''

def import_arg():
    parser = api.parser()
    parser.add_argument('name', required=False, type=str, help='Q1 Post', location='args')
    return parser

# Get last update
def get_last_update():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

# Get the biggest id in db
def get_db_id(cur, del_id):
    r = cur.execute('''SELECT MAX(stored_id) from StoredId''').fetchone()[0]
    if (r is None):
        return del_id+1 if del_id != -1 else 0
    else:
        return del_id+1 if del_id > r else r+1

# join genres and schedule-days
def get_genres_or_scheduleDays(v):
    return ','.join(map(str, v))

# Store current tv info into DB
def store_tv(tv_info, del_id):
    # Store into Table Tv
    con = sqlite3.connect('z5182291.db')
    cur = con.cursor()
    cus_id = get_db_id(cur, del_id)
    #print(cus_id)
    cus_last_update = get_last_update()
    cus_genres = get_genres_or_scheduleDays(tv_info['genres'])
    cus_schedule_days = get_genres_or_scheduleDays(tv_info['schedule']['days'])
    links_self = "http://127.0.0.1:5000/tv-shows/"+str(cus_id)
    cur.execute("INSERT INTO Tv (tvmaze_id, id, last_update, name, type, language, genres, status, runtime, premiered, officialSite, schedule_time, schedule_days, rating_average, weight, summary, links_self) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (tv_info['id'], cus_id, cus_last_update, tv_info['name'], tv_info['type'], tv_info['language'], cus_genres, tv_info['status'], tv_info['runtime'], tv_info['premiered'], tv_info['officialSite'], tv_info['schedule']['time'], cus_schedule_days, tv_info['rating']['average'], tv_info['weight'], tv_info['summary'], links_self))
    

    # Store into Table Network
    cur.execute("INSERT INTO Network (network_id, tv_id, network_name, country_name, country_code, country_timezone) VALUES (?,?,?,?,?,?)", (tv_info['network']['id'], cus_id, tv_info['network']['name'], tv_info['network']['country']['name'], tv_info['network']['country']['code'], tv_info['network']['country']['timezone']))

    # Store into Table StoredId
    #print(type(cus_id))
    cur.execute("INSERT INTO StoredId (stored_id) VALUES (?)", (cus_id,))

    con.commit()
    con.close()

# Check if DB already stored the tv show
def check_already_stored(n):
    con = sqlite3.connect('z5182291.db')
    cur = con.cursor()
    for name_db in cur.execute('''SELECT name from Tv'''):
        name_db = re.sub('[^a-zA-Z0-9]', '', name_db[0]).lower()
        if (n == name_db):
            return True
    return False

# Delete from DB by given tv name
def delete_tv(name):
    con = sqlite3.connect('z5182291.db')
    cur = con.cursor()
    del_id = cur.execute('''SELECT id FROM Tv where name = ?''', (name,)).fetchone()[0]
    cur.execute('''DELETE FROM Tv where id = ?''', (del_id,))
    cur.execute('''DELETE FROM Network where tv_id = ?''', (del_id,))
    cur.execute('''DELETE FROM StoredId where stored_id = ?''', (del_id,))
    con.commit()
    con.close()
    return del_id

# Delete from DB by given id
def delete_tv_by_id(id):
    con = sqlite3.connect('z5182291.db')
    cur = con.cursor()
    cur.execute('''DELETE FROM Tv where id = ?''', (id,))
    cur.execute('''DELETE FROM Network where tv_id = ?''', (id,))
    cur.execute('''DELETE FROM StoredId where stored_id = ?''', (id,))
    con.commit()
    con.close()
    return

# Store the data into db
def store_data(best_match, name_tvmaze):
    con = sqlite3.connect('z5182291.db')
    cur = con.cursor()
    cur.execute('''SELECT count(name) FROM sqlite_master WHERE type='table' AND name='Tv' ''')
    # db hasn't been created
    if cur.fetchone()[0] != 1:
        cur.execute('''CREATE TABLE Tv
                    (tvmaze_id INTEGER,
                    id INTEGER NOT NULL PRIMARY KEY,
                    last_update DATETIME,
                    name TEXT,
                    type TEXT,
                    language TEXT,
                    genres TEXT,
                    status TEXT,
                    runtime INTEGER,
                    premiered DATE,
                    officialSite text,
                    schedule_time TEXT,
                    schedule_days TEXT,
                    rating_average DOUBLE,
                    weight INTEGER,
                    summary TEXT,
                    links_self TEXT)''')
        cur.execute('''CREATE TABLE Network
                    (network_id INTEGER NOT NULL,
                    tv_id INTEGER NOT NULL,
                    network_name TEXT,
                    country_name TEXT,
                    country_code TEXT,
                    country_timezone TEXT,
                    FOREIGN KEY (tv_id) REFERENCES Tv(id))''')
        cur.execute('''CREATE TABLE StoredId
                    (stored_id INTEGER NOT NULL,
                    FOREIGN KEY (stored_id) REFERENCES Tv(id))''')
        con.commit()
        con.close()

    # check if db already stored this tv
    already = check_already_stored(name_tvmaze)
    if (already):
        # Delete this tv then store
        del_id = delete_tv(best_match['name'])
        store_tv(best_match, del_id)
        return False
    else:
        # store tv info into db
        store_tv(best_match, -1)
        return True

# Get previous id, if no previous return -1
def get_previous_id(curr_id, cur):
    pre = -1
    for i in cur.execute('''SELECT * from StoredId'''):
        if (i[0] < curr_id and i[0] > pre):
            pre = i[0]
    return pre

def q1_output():
    con = sqlite3.connect('z5182291.db')
    cur = con.cursor()
    max_id = cur.execute('''SELECT MAX(stored_id) from StoredId''').fetchone()[0]
    q1_tv = cur.execute('''SELECT * from Tv where id = ?''', (max_id,))
    q1_db = q1_tv.fetchone()
    # Create a return type for q1
    q1_result = {}
    q1_result['id'] = q1_db[1]
    q1_result['last-update'] = q1_db[2]
    q1_result['tvmaze-id'] = q1_db[0]
    q1_result['_links'] = {}
    q1_result['_links']['self'] = {}
    q1_result['_links']['self']['href'] = "http://127.0.0.1:5000/tv-shows/"+str(q1_db[1])
    # Check if it has previous in DB
    previous_id = get_previous_id(q1_db[1], cur)
    # It has previous tv show
    if (previous_id != -1):
        links_previous = cur.execute('''SELECT links_self from Tv where id = ?''', (previous_id,)).fetchone()[0]
        q1_result['_links']['previous'] = {}
        q1_result['_links']['previous']['href'] = links_previous
    con.close()
    return q1_result

@api.route('/tv-shows/import')
@api.response(201, 'Import success')
@api.response(200, 'Update success (The tv show with same name has been stored before)')
@api.response(404, "The tv show NOT FOUND")
class Import(Resource):
    @api.expect(import_arg())
    @api.doc(params={'name': 'Type the title for the tv show'})
    def post(self):
        parser = import_arg()
        query_tv_names = parser.parse_args()['name']
        url_query_names = re.sub(' ', '-', query_tv_names)
        resource = "http://api.tvmaze.com/search/shows?q="
        resource+=url_query_names
        resource = req.Request(resource)
        data = json.loads(req.urlopen(resource).read())
        try:
            best_match = data[0]['show']
        except:
            return {"message":"Not Found!"}, 404

        # Check query name and the best match name

        # remove all other chars in name from tvmaze
        name_tvmaze = best_match['name']
        name_tvmaze = re.sub('[^a-zA-Z0-9]', '', name_tvmaze).lower()
        query_tv_names = re.sub('[^a-zA-Z0-9]', '', query_tv_names).lower()
        # Check if they match
        if name_tvmaze == query_tv_names:
            # store into db
            success = store_data(best_match, name_tvmaze)
            # if success
            if (success):
                # get the output for Q1
                result = q1_output()
                #return result, 200
                return result, 201
            # db already has that. Need to delete first then store
            else:
                #print("db has already stored this, update success")
                result = q1_output()
                return result, 200
        else:
            return {"message":"Not Found!"}, 404

# Get previous and next links if exist Q2 helper
def get_pre_next_link(self_id, cur):
    pre_id = -1
    next_id = 99999999999
    for i in cur.execute('''SELECT * FROM StoredId'''):
        if (pre_id < i[0] < self_id):
            pre_id = i[0]
        if (self_id < i[0] < next_id):
            next_id = i[0]
    try:
        pre_link = cur.execute('''SELECT links_self FROM Tv where id = ?''', (pre_id,)).fetchone()[0]
    except:
        pre_link = -1

    try:
        next_link = cur.execute('''SELECT links_self FROM Tv where id = ?''', (next_id,)).fetchone()[0]
    except:
        next_link = -1
    
    return pre_link, next_link

patch_model = api.model('Q4 Update Payload', {
    #"name": fields.String(required=True, default="hello"),
    #"runtime": fields.Integer(required=False, default=-1),
    #"other": fields.String(required=False),
})

# Q4 helper
# Check if the key is not valid
def update_bad_key(k, v):
    if (k == "schedule"):
        try:
            for k_schedule, v_schedule in v.items():
                if (k_schedule not in ("time", "days")):

                    return True
        except:
            return True
    elif (k == "rating"):
        try:
            for k_rating, v_rating in v.items():
                if (k_rating != "average"):
                    return True
        except:
            return True
    elif (k == "network"):
        try:
            for k_network, v_network in v.items():
                if (k_network not in ("name", "country")):
                    return True
                if (k_network == "country"):
                    try:
                        for k_country, v_country in v_network.items():
                            if (k_country not in ("name", "code", "timezone")):
                                return True
                    except:
                        return True
        except:
            return True
    elif (k == "_links"):
        try:
            for k_link, v_link in v.items():
                #print(k_link," ",v_link)
                if (k_link != "self"):
                    return True
                try:
                    for k_self, v_self in v_link.items():
                        if (k_self != "href"):
                            return True
                except:
                    return True
        except:
            return True
    elif (k == "genres" and isinstance(v, list) is False):
        return True
    if (k not in ("name", "type", "language", "genres", "status", "runtime", "premiered", "officialSite", "schedule", "rating", "weight", "network", "summary", "_links")):
        return True
    return False

# Check if it updates forbidden info
# (i.e. id, tvmaze-id, network['id'], last_update, _links['previous], _links['next'], _links['self'])
def update_critical(k, v):
    if (k in ("id", "tvmaze-id", "last-update")):
        return True
    elif (k == "network"):
        try:
            for k_network, v_n in v.items():
                if (k_network == "id"):
                    return True
        except:
            return True
    elif (k == "_links"):
        return True
    return False

# Check if it contians bad format
def update_bad_format(k, v):
    # Create a list Mon to Sun
    seven_weekdays = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    # String
    if ((k == "name" and type(v) is not str) or (k == "type" and type(v) is not str) or (k == "language" and type(v) is not str) or (k == "status" and type(v) is not str) or (k == "officialSite" and type(v) is not str) or (k == "summary" and type(v) is not str)):
        return True
    # String
    elif (k == "network"):
        if isinstance(v, dict) is False:
            return True
        for k_n, v_network in v.items():
            if (k_n == "name" and type(v_network) is not str):
                return True
            elif (k_n == "country" and isinstance(v_network, dict) is False):
                return True
            elif (k_n == "country" and isinstance(v_network, dict)):
                for k_country, v_country in v_network.items():
                    if (type(v_country) is not str):
                        return True
    # String
    elif (k == "_links"):
        try:
            if (isinstance(v, dict) is True and isinstance(v['self'], dict) is True):
                if (type(v['self']['href']) is not str):
                    return True
        except:
            return True
    # Numeric
    elif (k == "runtime"):
        if (type(v) is not int and type(v) is not float):
            return True
        if ((type(v) is int or type(v) is float) and v < 0):
            return True
    elif (k == "rating"):
        if (type(v['average']) is not int and type(v['average']) is not float):
            return True
    elif (k == "weight"):
        if (type(v) is not int and type(v) is not float):
            return True
    
    # List
    # HH:MM
    elif (k == "genres"):
        for v_genres in v:
            if (type(v_genres) is not str):
                return True
    elif (k == "schedule"):
        if isinstance(v, dict) is False:
            return True
        if ("days" in v):
            if (type(v['days']) is not list):
                return True
            for d in v['days']:
                if ((d in seven_weekdays) is False):
                    return True
        if ("time" in v):
            if (type(v['time']) is not str):
                return True
            match = re.match("[0-9]{2}:[0-9]{2}", v['time'])
            match = bool(match)
            if (match is False):
                return True

    # Date
    elif (k == "premiered"):
        match = re.match("[0-9]{4}-[0-9]{2}-[0-9]{2}", v)
        match = bool(match)
        if (match is False):
            return True

    #print("update_bad_format pass")
    return False

'''
def patch_model():
    parser = api.parser()
    parser.add_argument('update', required=True, type=str, help='Q4 Update', location='args')
    return parser
'''

# Update to DB
def update_to_db(payload, id):
    con = sqlite3.connect('z5182291.db')
    cur = con.cursor()
    for k, v in payload.items():
        # Update Table Tv
        # Update unchanged key names in DB
        if (k in ("name", "type", "language", "status", "runtime", "premiered", "officialSite", "weight", "summary")):
            cur.execute('''UPDATE Tv SET {0} = ? where id = ?'''.format(k), (v, id))
        # Update other modified key names
        elif (k == "genres"):
            genres_list = get_genres_or_scheduleDays(v)
            cur.execute('''UPDATE Tv SET genres = ? where id = ?''', (genres_list, id))
        elif (k == "schedule"):
            if ("time" in v):
                cur.execute('''UPDATE Tv SET schedule_time = ? where id = ?''', (v['time'], id))
            if ("days" in v):
                schedule_days_list = get_genres_or_scheduleDays(v['days'])
                cur.execute('''UPDATE Tv SET schedule_days = ? where id = ?''', (schedule_days_list, id))
        elif (k == "rating"):
            cur.execute('''UPDATE Tv SET rating_average = ? where id = ?''', (v['average'], id))
        elif (k == "_links"):
            cur.execute('''UPDATE Tv SET links_self = ? where id = ?''', (v['self']['href'], id))

        # Update Table Network
        if (k == "network"):
            #print(v)
            if ("name" in v):
                cur.execute('''UPDATE Network SET network_name = ? where tv_id = ?''', (v['name'], id))
            if ("country" in v):
                if ("name" in v['country']):
                    cur.execute('''UPDATE Network SET country_name = ? where tv_id = ?''', (v['country']['name'], id))
                if ("code" in v['country']):
                    cur.execute('''UPDATE Network SET country_code = ? where tv_id = ?''', (v['country']['code'], id))
                if ("timezone" in v['country']):
                    cur.execute('''UPDATE Network SET country_timezone = ? where tv_id = ?''', (v['country']['timezone'], id))

    # Update last_update
    update_last_update = get_last_update()
    cur.execute('''UPDATE Tv SET last_update = ? where id = ?''', (update_last_update, id))

    con.commit()
    con.close()
    return update_last_update

def get_self_link(curr_id, cur):
    return cur.execute('''SELECT links_self FROM Tv where id = ?''', (curr_id,)).fetchone()[0]

# Patch helper: check if id exist
def id_not_exist(patch_id):
	con = sqlite3.connect('z5182291.db')
	cur = con.cursor()
	ids = cur.execute('''SELECT stored_id from StoredId''').fetchall()
	for i in ids:
		if (i[0] == patch_id):
			return False
	return True
	con.close()

@api.route('/tv-shows/<int:id>')
@api.response(200, 'Success')
@api.response(404, 'The tv show does not exist')
@api.response(400, 'Bad request')
class Retrieve(Resource):
    def get(self, id):
        con = sqlite3.connect('z5182291.db')
        cur = con.cursor()
        tv_info = cur.execute('''SELECT * FROM Tv where id = ?''', (id,)).fetchone()
        if (tv_info is None):
            return {}, 404
        # Create the result type
        q2_result = {}
        q2_result['tvmaze-id'] = tv_info[0]
        q2_result['id'] = tv_info[1]
        q2_result['last-update'] = tv_info[2]
        q2_result['name'] = tv_info[3]
        q2_result['type'] = tv_info[4]
        q2_result['language'] = tv_info[5]
        # Genres
        q2_result['genres'] = tv_info[6].split(',')
        q2_result['status'] = tv_info[7]
        q2_result['runtime'] = tv_info[8]
        q2_result['premiered'] = tv_info[9]
        q2_result['officialSite'] = tv_info[10]
        # Schedule
        q2_result['schedule'] = {}
        q2_result['schedule']['time'] = tv_info[11]
        q2_result['schedule']['days'] = tv_info[12].split(',')
        q2_result['rating'] = {}
        # Rating
        q2_result['rating']['average'] = tv_info[13]
        q2_result['weight'] = tv_info[14]
        # Network
        network_info = cur.execute('''SELECT * FROM Network where tv_id = ?''', (id,)).fetchone()
        q2_result['network'] = {}
        q2_result['network']['id'] = network_info[0]
        q2_result['network']['name'] = network_info[2]
        q2_result['network']['country'] = {}
        q2_result['network']['country']['name'] = network_info[3]
        q2_result['network']['country']['code'] = network_info[4]
        q2_result['network']['country']['timezone'] = network_info[5]
        q2_result['summary'] = tv_info[15]
        q2_result['_links'] = {}
        q2_result['_links']['self'] = {}
        q2_result['_links']['self']['href'] = tv_info[16]
        pre_link, next_link = get_pre_next_link(tv_info[1], cur)
        if (pre_link != -1):
            q2_result['_links']['previous'] = {}
            q2_result['_links']['previous']['href'] = pre_link
        if (next_link != -1):
            q2_result['_links']['next'] = {}
            q2_result['_links']['next']['href'] = next_link
        con.close()
        return q2_result, 200

    def delete(self, id):
        delete_tv_by_id(id)
        return {'message': "The tv show with id {} was removed from the database!".format(id),'id': id}, 200

    #@api.expect(patch_model)
    @api.expect(api.model('q4',{}))
    def patch(self, id):
        #print(id)
        #print(api.payload)
        # Check if all valid
        bad_request = 0
        for k, v in api.payload.items():
            #print(k+" "+str(v))
            if (update_bad_key(k, v) or update_bad_format(k, v) or update_critical(k, v)):
                bad_request = 1
                break
        
        if (bad_request == 1):
            return {"message": "Invalid payload, bad request"}, 400
        if (id_not_exist(id)):
        	return {"message": "ID not found, bad request"}, 400
        # Update to DB
        last_update = update_to_db(api.payload, id)

        # Create return type
        # Get previous and next links if exist
        con = sqlite3.connect('z5182291.db')
        cur = con.cursor()
        pre_link, next_link = get_pre_next_link(id, cur)
        self_link = get_self_link(id, cur)
        cur.close()
        if (pre_link == -1 and next_link == -1):
            return {'id': id, 'last-update': last_update, '_links': {'self': {'href': self_link}}}
        elif (pre_link == -1):
            return {'id': id, 'last-update': last_update, '_links': {'self': {'href': self_link}, 'next': {'href': next_link}}}
        elif (next_link == -1):
            return {'id': id, 'last-update': last_update, '_links': {'self': {'href': self_link}, 'previous': {'href': pre_link}}}
        else:
            return {'id': id, 'last-update': last_update, '_links': {'self': {'href': self_link}, 'previous': {'href': pre_link}, 'next': {'href': next_link}}}

def import_arg_q5():
    parser = api.parser()
    parser.add_argument('order_by', required=False, type=str, help='Q5 Get order_by', location='args')
    parser.add_argument('page', required=False, type=int, help='Q5 Get page', location='args')
    parser.add_argument('page_size', required=False, type=int, help='Q5 Get page_size', location='args')
    parser.add_argument('filter', required=False, type=str, help='Q5 Get filter', location='args')
    return parser


# Check if filter has network
def check_filter_network(q_filter):
    f = q_filter.split(",")
    for i in f:
        if (i == "network"):
            return True
    return False

# Get previous and next link for Q5 if exist
def get_pre_next_link_q5(query_page, query_page_size, query_order_by, query_filter, length):
    previous_link = -1
    next_link = -1

    previous_total = (query_page-1)*query_page_size
    remaining_tv_shows = length - previous_total
    start = previous_total
    # Self num is largest (i.e. query_page_size)
    if (remaining_tv_shows >= query_page_size):
        self_page_size = query_page_size
        end = previous_total+query_page_size
        # Also has next_link
        if (remaining_tv_shows > query_page_size):
            next_link = "http://127.0.0.1:5000/tv-shows?order_by="+query_order_by+"&page="+str(query_page+1)+"&page_size="+str(query_page_size)+"&filter="+query_filter
    # Last page
    else:
        self_page_size = remaining_tv_shows
        end = length

    # If query_page != 1
    if (query_page > 1):
        previous_link = "http://127.0.0.1:5000/tv-shows?order_by="+query_order_by+"&page="+str(query_page-1)+"&page_size="+str(query_page_size)+"&filter="+query_filter

    return previous_link, next_link, self_page_size, start, end

@api.route('/tv-shows')
@api.response(200, 'OK')
@api.response(400, 'Bad request')
@api.response(404, 'Tv show not found')
class RetrieveQ5(Resource):
    @api.doc(params={'order_by': 'A comma separated string value to sort the list', 'page': 'The page num', 'page_size': 'The number of TV shows per page', 'filter': 'Comma separated values to show attributes'})
    @api.expect(import_arg_q5())
    def get(self):
        parser = import_arg_q5()
        query_order_by = parser.parse_args()['order_by']
        query_page = parser.parse_args()['page']
        query_page_size = parser.parse_args()['page_size']
        query_filter = parser.parse_args()['filter']
        
        # Apply default value if None
        if (query_order_by == None):
            query_order_by = "+id"
        if (query_page == None):
            query_page = 1
        if (query_page_size == None):
            query_page_size = 100
        if (query_filter == None):
            query_filter = "id,name"

        '''print(query_order_by)
        print(query_page)
        print(query_page_size)
        print(query_filter)'''

        # Split order_by
        query_order_by_mid = query_order_by
        query_order_by_mid = query_order_by_mid.split(",")
        query_order_by_mid

        # query_order_by
        # Create a order_by sqlite3 type
        order_by_sqlite = ""
        # Process order_by into sqlite3 supported language
        for q in query_order_by_mid:
            if (q[1:]=="rating-average"):
                q = q.replace("rating-average", "rating_average")
            if (q[0] == '+'):
                order_by_sqlite+=q[1:]+" ASC, "
            elif (q[0] == '-'):
                order_by_sqlite+=q[1:]+" DESC, "
            else:
                return {"message": "order_by should start with +/-"}, 400
        order_by_sqlite=order_by_sqlite[0:-2]

        # Check if filter has network
        filter_network = check_filter_network(query_filter)
        # Process filter into sqlite3 supported language, remove network
        filter_sqlite = query_filter
        filter_sqlite = filter_sqlite.replace("last-update", "last_update")
        filter_sqlite = filter_sqlite.replace("schedule", "schedule_time,schedule_days")
        filter_sqlite = filter_sqlite.replace("rating", "rating_average")
        filter_sqlite = filter_sqlite.replace("network,", "")
        filter_sqlite = filter_sqlite.replace("network", "")
        if (filter_sqlite != "" and filter_sqlite[-1] != ','):
            filter_sqlite+=','
        
        # Filter check
        for qu in query_filter.split(","):
            if (qu not in ("tvmaze_id", "id", "last-update", "name", "type", "language", "genres", "status", "runtime", "premiered", "officialSite", "schedule", "rating", "weight", "network", "summary")):
                return {"message": "Filter not found, bad request"}, 400
        con = sqlite3.connect('z5182291.db')
        cur = con.cursor()
        # Add Table Network to after_order_tv if requested
        if (filter_network):
            #print(filter_network)
            if (filter_sqlite == ""):
                after_order_tv = cur.execute('''SELECT network_id, network_name, country_name, country_code, country_timezone FROM (SELECT * FROM Tv INNER JOIN Network on Tv.id = Network.tv_id) ORDER BY {1}'''.format(filter_sqlite, order_by_sqlite)).fetchall()
            else:
                after_order_tv = cur.execute('''SELECT {0} network_id, network_name, country_name, country_code, country_timezone FROM (SELECT * FROM Tv INNER JOIN Network on Tv.id = Network.tv_id) ORDER BY {1}'''.format(filter_sqlite, order_by_sqlite)).fetchall()
        else:
            #print(filter_network)
            filter_sqlite=filter_sqlite[0:-1]
            after_order_tv = cur.execute('''SELECT {0} FROM Tv ORDER BY {1}'''.format(filter_sqlite, order_by_sqlite)).fetchall()
        con.close()

        # Error checking before output
        if (len(after_order_tv) == 0):
            return {"message": "No satisfied tv shows"}, 404
        elif (query_page < 1 or query_page_size < 1):
            return {"message": "Invalid query"}, 400
        else:
            previous_total = (query_page-1)*query_page_size
            if (previous_total >= len(after_order_tv)):
                return {"message": "Invalid query"}, 400
            if (math.ceil(len(after_order_tv)/query_page_size) < query_page):
                return {"message": "Invalid query"}, 400

        previous_link, next_link, self_page_size, start, end = get_pre_next_link_q5(query_page, query_page_size, query_order_by, query_filter, len(after_order_tv))
        self_link = "http://127.0.0.1:5000/tv-shows?order_by="+query_order_by+"&page="+str(query_page)+"&page_size="+str(query_page_size)+"&filter="+query_filter
        raw_q5_tv_shows = after_order_tv[start:end]

        # Create tv-shows return dict
        tv_shows_q5_return = []
        for show in raw_q5_tv_shows:
            index = 0
            each_show = {}
            query_filter_sp = query_filter.split(",")
            for q in query_filter_sp:
                if (q == "genres"):
                    each_show['genres'] = show[index].split(",")
                    index+=1
                elif (q == "schedule"):
                    each_show['schedule'] = {}
                    each_show['schedule']['time'] = show[index]
                    index+=1
                    each_show['schedule']['days'] = show[index].split(",")
                    index+=1
                elif (q == "rating"):
                    each_show['rating'] = {}
                    each_show['rating']['average'] = show[index]
                    index+=1
                elif (q == "network"):
                    each_show['network'] = {}
                    each_show['network']['id'] = show[-5]
                    each_show['network']['name'] = show[-4]
                    each_show['network']['country'] = {}
                    each_show['network']['country']['name'] = show[-3]
                    each_show['network']['country']['code'] = show[-2]
                    each_show['network']['country']['timezone'] = show[-1]
                else:
                    each_show[q] = show[index]
                    index+=1
            tv_shows_q5_return.append(each_show)

        q5_output = {}
        q5_output['page'] = query_page
        q5_output['page-size'] = query_page_size
        q5_output['tv-shows'] = tv_shows_q5_return
        q5_output['_links'] = {}
        q5_output['_links']['self'] = {}
        q5_output['_links']['self']['href'] = self_link
        if (previous_link != -1):
            q5_output['_links']['previous'] = {}
            q5_output['_links']['previous']['href'] = previous_link
        if (next_link != -1):
            q5_output['_links']['next'] = {}
            q5_output['_links']['next']['href'] = next_link
        return q5_output, 200

def import_arg_q6():
    parser = api.parser()
    parser.add_argument('format', required=True, type=str, help='Q6 Get format', location='args')
    parser.add_argument('by', required=True, type=str, help='Q6 Get by', location='args')
    return parser

# Calculate percentage
def cal_percentage(stat, total):
    r = round(stat[1]/total, 4)*100
    r = "{:.1f}".format(r)
    return (stat[0], r)

@api.route('/tv-shows/statistics')
@api.response(200, 'OK')
@api.response(400, 'Bad request')
class Statistics(Resource):
    @api.doc(params={'format': 'image or json', 'by': 'a single parameter of language, genres, status, or type'})
    @api.expect(import_arg_q6())
    def get(self):
        parser = import_arg_q6()
        query_format = parser.parse_args()['format']
        query_by = parser.parse_args()['by']

        # Query validation
        if (query_format not in ("json", "image") or query_by not in ("language", "genres", "status", "type")):
            return {"message": "Invalid query"}, 400

        con = sqlite3.connect('z5182291.db')
        cur = con.cursor()
        # Get total
        total = cur.execute('''SELECT COUNT(*) FROM Tv''').fetchone()[0]
        # Get total updated in past 24 hours
        today = datetime.datetime.now()
        yesterday = today-datetime.timedelta(days=1)
        total_updated = cur.execute('''SELECT COUNT(*) FROM Tv where last_update >= ?''', (yesterday,)).fetchone()[0]

        # Calculate values
        stats = cur.execute('''SELECT {0}, count({1}) FROM Tv GROUP BY {2} ORDER BY {3}'''.format(query_by, query_by, query_by, query_by)).fetchall()
        con.close()
        
        stats = [cal_percentage(stat, total) for stat in stats]
        stats = list(stats)

        # Create a values type
        values = {}
        for stat, v in stats:
            values[stat] = float(v)
        matplotlib.use('Agg')
        labels=[]
        value=[]
        for k, v in values.items():
            labels.append(k)
            value.append(v)

        if (query_format == "json"):
            q6_output_json = {}
            q6_output_json['total'] = total
            q6_output_json['total-updated'] = total_updated
            q6_output_json['values'] = values
            return q6_output_json, 200
        else:
            df = pd.DataFrame(list(values.items()), columns=['Language', 'Percentage'])
            df.plot(kind='pie', figsize=(15,10), y="Percentage", labels=labels, autopct='%1.0f%%')
            #df.plot(kind='pie', figsize=(10,10), y="Percentage", labels=labels)
            plt.title("Percentage of TV Shows per {}".format(query_by))
            plt.savefig("q6.jpg")
            #matplotlib.pie(values_plot, labels=labels, autopct='%.2f', figsize(20,20))
            return send_file("q6.jpg", mimetype='image/jpg')



if __name__ == '__main__':
    app.run(debug=True)