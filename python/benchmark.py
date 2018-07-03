from __future__ import absolute_import, division, print_function

import os
import subprocess
import csv
from datetime import datetime
import concurrent.futures
from profile import ResourceProfiler
import numpy as np

from utils import dbclient


def profile_function( fn, *args):
        """
        Use the Profile context manager to record the runtime performance profile of a 
        users-suplied function

        Args:
            fn: A callable tat takes as many arguments as passed in 'args'
            
        Returns:
            rprof: An object with a list of arrays, each one representing a runtime 
            profile of a performance variable

        Raises:
            Exception: If fn(*args) raises for any values.
        """

        with ResourceProfiler(dt=0.1) as rprof:
            fn(*args)
        return rprof


class Benchmark:
    """Benchmarking for IDIA Pipeline"""
    
    bench_dict = {}
    path_out = "sysinfo.csv"
    fieldnames = ['Date', 'Time', 'TestID', 'RunTime', 'Container', 'DDIOTestSize (MB)', 
                  'DDIORead (MB/s)', 'DDIOWrite (MB/s)', 'Architecture', 'CPU(s)',
                  'Thread(s) per core', 'CPU MHz', 'MemAvailable', 'MemFree', 'Description', 
                  'MemMax', 'MemMean', 'MemTot', 'CPUMax', 'CPUMean', 'IOMeanR', 'IOMeanW', 
                  'IOTotR', 'IOTotW']
                  # 'Core(s) per socket', 'Socket(s)', 'Model', 'Model name', 
    
    def __init__(self, container_path = None, exec_path = "python", testid = "", description = "", profile=True, dt_profile=1.0):
        self.bench_dict.update(self._sysinfo())
        self.bench_dict.update(self._meminfo())
        self.graphs = []
        self.stats = {}

        # Set test properties
        self.container_path = container_path
        self.exec_path = exec_path
        self.testid = testid
        self.description = description
        self.do_profile = profile
        self.dt_profile = dt_profile

        self.collection = dbclient()

        
    def _sysinfo(self):
        """Capture environment system information"""
        sys = subprocess.check_output(['lscpu'])
        sys_info = sys.split('\n')
        sys_dict = {}
        for info in sys_info[:-1]:
            field, data = info.split(":")
            sys_dict[field] = data.strip()

        return sys_dict

    def _meminfo(self):
        """Capture environment memory information"""
        file_handler = open('/proc/meminfo', 'r')
        mem_info = file_handler.readlines()
        mem_dict = {}
        file_handler.close()
        for info in mem_info:
            field, data = info.split(":")
            mem_dict[field] = data.strip()

        return mem_dict

    def ddio_test(self, n=5, bs='100M'):
        '''
        Simulation of input/output using DD Read and Write
        '''
        self.update_bench_dict({"DDIOTestSize (MB)": bs})
        ddio_dict = {}
        write_times = []
        call = 'rm testfile; dd if=/dev/zero of=testfile bs=' + bs + ' count=1 oflag=direct'
        for i in range(1,n+1):
            proc = subprocess.Popen(call, stdout=subprocess.PIPE,
                    stderr = subprocess.PIPE, shell=True)
            out, err = proc.communicate()
            write_times.append(err.replace(' ','').split(',')[-1].replace('\n',''))
        #first write time is removed
        write_times = write_times[1:]
        write_ave = self._ddio_mem_average(write_times)
        self.update_bench_dict({"DDIOWrite (MB/s)": write_ave})
        read_times = []
        for i in range(1,n+1):
            call = 'dd if=testfile of=/dev/null bs='+bs
            proc = subprocess.Popen(call, stdout=subprocess.PIPE,
                    stderr = subprocess.PIPE, shell=True)
            out, err = proc.communicate()
            read_times.append(err.replace(' ','').split(',')[-1].replace('\n',''))
        #first read time is removed
        read_times = read_times[1:]
        read_ave = self._ddio_mem_average(read_times)
        self.update_bench_dict({"DDIORead (MB/s)": read_ave})


    def _ddio_mem_average(self, rw_times):
        """Convert a list of values from ddio runtimes to an average with units MB/s"""
        r_value = []
        for i in rw_times:
            if (i[-4] == 'k'):
                r_value.append(float(i[:-4])*1024)
            if (i[-4] == 'M'):
                r_value.append(float(i[:-4])*(1024**2))
            if (i[-4] == 'G'):
                r_value.append(float(i[:-4])*(1024**3))
            if (i[-4] == 'T'):
                r_value.append(float(i[:-4])*(1024**4))
        summ = 0.0
        for k in r_value:
            summ = summ + k  

        ave_rw_time = summ/len(r_value)/(1024.**2)
        return ("%0.2f" % ave_rw_time)

    def write_to_csv(self):
        write = False
        data = self.bench_dict
        path = self.path_out
        fieldnames = self.fieldnames
        """Select write/append"""
        if os.path.exists(path):
            append_write = 'ab' # append if already exists
        else:
            append_write = 'wb' # make a new file if not
            write = True
        with open(path, append_write) as out_file:
            writer = csv.DictWriter(out_file, delimiter=',', fieldnames=fieldnames, extrasaction='ignore')
            if write:
                writer.writeheader()
                writer.writerow(data)
            else:
                writer.writerow(data)

    def write_to_database(self):
        # write data to mongodb
        data = self.bench_dict
        dbdict = {}
        for fn in self.fieldnames:
            dbdict[fn] = data.get(fn, '')
        dbdict['graphs'] = self.graphs
        self.collection.insert_one( dbdict )


    def compute_stats(self):
        rdata = self.graphs

        # memory alocation integrated over time [MB * s]
        t_diff = np.diff(rdata['t'])
        tot_memory = np.sum([ v_i*t_i for v_i, t_i in zip(t_diff, rdata['umem'][1:])])
        
        # Memory
        max_memory = np.amax(rdata['umem'])
        mean_memory = np.mean(rdata['umem'])
        
        # mean cpu
        mean_cpu = np.mean(rdata['cpu'])
        max_cpu = np.amax(rdata['cpu'])
        
        # mean IO Reads
        mean_rio = np.average(list(np.diff(rdata['rio'])), axis=0, weights=1./t_diff)
        mean_wio = np.average(list(np.diff(rdata['wio'])), axis=0, weights=1./t_diff)
        
        tot_rio = rdata['rio'][-1] - rdata['rio'][0]
        tot_wio = rdata['wio'][-1] - rdata['wio'][0]
        
        self.update_bench_dict({"MemMax": "{:.2f}".format(max_memory)})
        self.update_bench_dict({"MemMean": "{:.2f}".format(mean_memory)})
        self.update_bench_dict({"MemTot": "{:.2f}".format(tot_memory)})

        self.update_bench_dict({"CPUMax": "{:.2f}".format(max_cpu)})
        self.update_bench_dict({"CPUMean": "{:.2f}".format(mean_cpu)})

        self.update_bench_dict({"IOMeanR": "{:.2f}".format(mean_rio)})
        self.update_bench_dict({"IOMeanW": "{:.2f}".format(mean_wio)})
        
        self.update_bench_dict({"IOTotR": "{:.2f}".format(tot_rio)})
        self.update_bench_dict({"IOTotW": "{:.2f}".format(tot_wio)})
        
        rstats = {"MemMax": max_memory,
                  "MemMean": mean_memory,
                  "MemTot": tot_memory,
                  "CPUMax": max_cpu,
                  "CPUMean": mean_cpu,    
                  "IOMeanR": mean_rio,
                  "IOMeanW": mean_wio,
                  "IOTotR": tot_rio,
                  "IOTotW": tot_wio}
        
        return rstats


    def execute_script(self, script_name ):
        #check container file
        self.update_bench_dict({"TestID": self.testid})
        self.update_bench_dict({"Description": self.description})
        self.update_bench_dict({"Container": self.container_path.split("/")[-1]})
        self.bench_dict["Time"] = datetime.now().strftime('%H:%M:%S')
        self.bench_dict["Date"] = datetime.now().strftime('%Y-%m-%d')

        if self.container_path:
            if os.path.isfile(self.container_path):
                args = 'singularity exec'
                args += ' ' + self.container_path + ' ' + self.exec_path + ' ' + script_name
                print( args )
                time_start = datetime.now()
                proc = subprocess.Popen(args, stdout=subprocess.PIPE,
                        stderr = subprocess.PIPE, shell=True)
                out, err = proc.communicate()
                time_end = datetime.now()
                time_diff = time_end - time_start
                test_time = "{:.3f}".format( (time_diff.seconds + time_diff.microseconds / 1000000.) )

                self.update_bench_dict({'RunTime': test_time })
                print( out, err )
        else:
            args = exec_path
            args += ' ' + script_name
            time_start = datetime.datetime.now()
            proc = subprocess.Popen(args, stdout=subprocess.PIPE,
                    stderr = subprocess.PIPE, shell=True)
            out, err = proc.communicate()
            time_end = datetime.datetime.now()
            time_diff = time_end - time_start
            test_time = "{:.3f}".format( (test_time.seconds + test_time.microseconds / 1000000.) )
            self.update_bench_dict({"Runtime": test_time})
            print( out, err )



    def execute_function(self, function, *args ):
        """Run benchmarking code on a function inside of a python environment.

        Args:
            function (object): a function defined in the current scope, the function can be called with or without arguments.  Use lambda notation to call a function with arguments ('see example below').
            container_path (str, optional): Full path to a singularity container.  The function will be executed in by the defualt python version in the container.  All libraries and other requirements for the function must be available in the container. Defaults to None.  
            testid (str, optional): An optional id which will be passed to the output database. Defaults to empty string.
            description (str, optional): A descriptive string containing any desired information that is not automatically included.  Will be passed as-is to the database.  Defaults to empty string.
            profile (boolean, optional): Turn off runtime profiling.  Defaults to True. 
            dt_profile (float, optional): Change the profiling time interval.  Defaults to 1 second.

        Returns:
            bool: True if successful, False otherwise.

        Raises:
            AttributeError: ...
            ValueError: ...
 
        ex.: 
        def test_function(x):
            sleep(0.25)
            print("Executing test_function({})...".format(x) )
                a = np.random.random(size=(1000,1000))
                q, r = np.linalg.qr(a)
                a2 = q.dot(r)
        mybenchmark = Benchmark()
        mybenchmark.execute_function( lambda: test_function(4), profile=True )

        """

        self.update_bench_dict({"TestID": self.testid})
        self.update_bench_dict({"Description": self.description})
        self.update_bench_dict({"Container": os.environ['SINGULARITY_NAME']})
        self.bench_dict["Time"] = datetime.now().strftime('%H:%M:%S')
        self.bench_dict["Date"] = datetime.now().strftime('%Y-%m-%d')

        time_start = datetime.now()
        if self.do_profile:
#             with ResourceProfiler(dt=dt_profile) as rprof:
#                 function()

            with concurrent.futures.ProcessPoolExecutor() as executor:
                result = executor.submit( profile_function, function, *args ).result()    

#            from bokeh.io import output_notebook
#             rprof.visualize()
        else:
            function(*args)
            rprof = None

        time_end = datetime.now()
        time_diff = time_end - time_start
        test_time = "{:.3f}".format( (time_diff.seconds + time_diff.microseconds / 1000000.) )
        print( 'Test finished - RunTime (s): ' + test_time )

        self.update_bench_dict({ 'RunTime': test_time })
        self.results = result

        res = result.results
        rdata = {
            't': [r.time for r in res],
            'cpu': [r.cpu for r in res],
            'pmem': [r.umem for r in res],
            'rmem': [r.umem for r in res],
            'umem': [r.umem for r in res],
            'smem': [r.umem for r in res],
            'rio': [r.rio for r in res],
            'wio': [r.wio for r in res]
        }
        self.graphs = rdata

        self.stats = self.compute_stats()

        return True


    def visualize():
        self.results.visualize()

    def update_bench_dict(self,dict_):
        self.bench_dict.update(dict_)
    
    def set_testid(self, testid):
        self.testID = testid

