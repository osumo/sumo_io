
import pymongo
from bson import json_util
from bson.objectid import ObjectId

def sumo_fetch(spec, **kwargs):
    db = spec['db']
    id = ObjectId(spec['id'])
    key = spec.get('key', 'value')
    collection = spec['collection']
    host = spec.get('host', 'localhost')
    data = pymongo.MongoClient(host)[db][collection].find_one(
            { '_id': id })[key]
    return data

def sumo_push(data, spec, **kwargs):
    db = spec['db']
    id = ObjectId(spec['id'])
    key = spec.get('key', 'value')
    collection = spec['collection']
    host = spec.get('host', 'localhost')
    converter = spec.get('converter')

    if converter == 'int':
        data = int(data)
    elif converter == 'float':
        data = float(data)
    elif converter == 'bool':
        data = bool(data)
    elif converter == 'json':
        data = json_util.loads(data)

    pymongo.MongoClient(host)[db][collection].find_one_and_update(
            { '_id': id },
            { '$set': { key: data } })

def load(params):
    from girder_worker.core import io as core_io
    core_io.register_fetch_handler('sumo', sumo_fetch)
    core_io.register_push_handler('sumo', sumo_push)

