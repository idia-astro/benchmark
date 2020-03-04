from __future__ import absolute_import, division, print_function

from .utils import dbclient, dbclient_tunnel
import collections
import pymongo


class BenchmarkDataManager:
    
    benchmark_list = []
    
    def __init__(self):
        
        self.collection = dbclient_tunnel()
        self.colnames = self.getcolnames()
    
    def query(self, query, sortby = False, columns = ['TestID', 'Description', 'Date', 'CPUMax', 'MemMax', 'IOTotW', 'IOTotR'], ascending = False, limit = 100):
        '''
        query: dictionary of keys and values to search on, example {'TestID' : 'nproc', 'Date' : '2018-07-03'}
        columns: list of keys to be returned by the query, example ['TestID', 'Date', 'Time']
        sortby: key on which to order query, default = False, use natural order
        ascending: boolean to set sortby ascending/descending
        limit: integer number of entries returned by the query, 0 for all
        '''   
        coldict = {}
        if columns:
            colnames = self.getcolnames()
            for key in columns:
                if key in colnames:
                    coldict[key] = 1
                else:
                    print("Error:\tThe column list item: '{}'.\n\tThis is not a columns in this collection.".format(key))
                    return False

            if sortby:
                if ascending:
                    for result in self.collection.find(query,coldict).sort([(sortby, pymongo.ASCENDING)]).limit(limit):
                        self.benchmark_list.append(result)
                else:
                    for result in self.collection.find(query,coldict).sort([(sortby, pymongo.DESCENDING)]).limit(limit):
                        self.benchmark_list.append(result)
            else:
                for result in self.collection.find(query,coldict).limit(limit):
                    self.benchmark_list.append(result)
        else:
            if sortby:
                if not(sortby in self.getcolnames()):
                    print("Error:\tThe sortby column provided was: '{}'.\n\tThis is not a columns in this collection.".format(sortby))
                    return False

                if ascending:
                    for result in self.collection.find(query).sort([(sortby, pymongo.ASCENDING)]).limit(limit):
                        self.benchmark_list.append(result)
                else:
                    for result in self.collection.find(query).sort([(sortby, pymongo.DESCENDING)]).limit(limit):
                        self.benchmark_list.append(result)
            else:
                for result in self.collection.find(query).limit(limit):
                    self.benchmark_list.append(result)

        return self.benchmark_list
    
    def getcolnames(self, query = {}):
        '''
        returns a list of the key values of a document in the mongodb. Because there is no forced structure to the 
        documents in the db, the returned list may sometimes show varying keys. The document from which the keys
        returned is first documented entered into the db that meets the query (natural order).
        query: dictionary of keys and values to search on 
        '''
        l = []
        result = self.collection.find_one(query)
        for key in result.keys():
            l.append(str(key))
        l.sort()
        return l
    
    def convert(self, data):
        if isinstance(data, basestring):
            return str(data)
        elif isinstance(data, self.collections.Mapping):
            return dict(map(self.convert, data.iteritems()))
        elif isinstance(data, self.collections.Iterable):
            return type(data)(map(self.convert, data))
        else:
            return data

    def distinct(self, query):
        '''
        '''
        return self.convert(self.collection.distinct(query))
    
    #def visualize():
        
