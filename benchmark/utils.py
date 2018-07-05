""" Utilities for Benchmarking Code

This module contains utility functions used for benchmarking and testing.

Example: 
	import utils
	omp = utils.OpenMPRuntime()
	print "OMP max threads: {}".format( omp.omp_get_max_threads() )
	# Set the OMP threads to 2
	omp.omp_set_num_threads(2)

"""

# libpath = find_library('gomp')
# openmp_lib = ctypes.cdll.LoadLibrary(libpath)
# openmp_lib.omp_set_num_threads(int(1))

import ctypes 
from ctypes.util import find_library

class OpenMPRuntime(object):
    '''
    Access OpenMP Runtime Library parameters
    
    omp = OpenMPRuntime()
    omp.omp_set_num_threads(3)
    omp.omp_get_max_threads()
    
    https://gcc.gnu.org/onlinedocs/libgomp/Runtime-Library-Routines.html
    '''

    def __init__(self, libname=None):
        if libname:
            try:
                self.libname = libname
                self.lib = ctypes.cdll.LoadLibrary(self.libname)
            except OSError:
                print "lib not found"
        else:
            self.libname = find_library('gomp')
            self.lib = ctypes.cdll.LoadLibrary(self.libname)
 
    def omp_set_num_threads(self, nthreads):
        self.lib.omp_set_num_threads(int(nthreads))

    def omp_get_max_threads(self):
        return self.lib.omp_get_max_threads()


from sshtunnel import SSHTunnelForwarder
from pymongo import MongoClient


def dbclient_tunnel():

    '''
    Set up the sshtunnel connection authenticating with a private key pair.
    '''

    server = SSHTunnelForwarder(
        "10.0.0.169",     # IP address of the database server
        ssh_username="ubuntu",
        ssh_pkey="/users/jbochenek/.ssh/mongodb_keypair.key",
        ssh_private_key_password="",
        remote_bind_address=('127.0.0.1', 27017)   # local address, does not need to be changed
    )
    server.start()
    
    client = MongoClient('127.0.0.1', server.local_bind_port)
    
    db = client['local']
    collection = db['results']
    return collection
    
def dbclient():
    client = MongoClient('10.0.0.169')

    db = client['local']
    collection = db['results']
    return collection
