import os
import subprocess
import csv
from datetime import datetime

class Benchmark:
    """Benchmarking for IDIA Pipeline"""
    
    bench_dict = {}
    path_out = "sysinfo.csv"
    fieldnames = ['Date', 'Time', 'TestID','RunTime (s)', 'Container', 'DDIOTestSize (MB)', 
                  'DDIORead (MB/s)', 'DDIOWrite (MB/s)', 'Architecture', 'CPU(s)',
                  'Thread(s) per core', 'Core(s) per socket', 'Socket(s)', 'Model',
                  'Model name', 'MemTotal', 'CPU MHz']
    
    def __init__(self):
        self.bench_dict["Time"] = datetime.now().strftime('%H:%M:%S')
        self.bench_dict["Date"] = datetime.now().strftime('%Y-%m-%d')
        self.bench_dict.update(self._sysinfo())
        self.bench_dict.update(self._meminfo())
        
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

        ave_rw_time = summ/len(r_value)/(1024**2)
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

    def execute_script(self, script_name, container_path = None, exec_path = "python2", testid = "", description = "", profile=True):
        #check container file
        self.update_bench_dict({"TestID": testid})
        self.update_bench_dict({"Desctription": description})
        self.update_bench_dict({"Container": container_path.split("/")[-1]})
        if container_path:
            if os.path.isfile(container_path):
                args = 'singularity exec'
                args += ' ' + container_path + ' ' + exec_path + ' ' + script_name
                print args
                time_start = datetime.now()
                proc = subprocess.Popen(args, stdout=subprocess.PIPE,
                        stderr = subprocess.PIPE, shell=True)
                out, err = proc.communicate()
                time_end = datetime.now()
                test_time = time_end - time_start
                self.update_bench_dict({'RunTime (s)': str(test_time.seconds)})
                print out, err
        else:
            args = exec_path
            args += ' ' + script_name
            time_start = datetime.datetime.now()
            proc = subprocess.Popen(args, stdout=subprocess.PIPE,
                    stderr = subprocess.PIPE, shell=True)

            out, err = proc.communicate()
            time_end = datetime.datetime.now()
            test_time = time_end - time_start
            self.update_bench_dict({"Runtime (s)": str(test_time.seconds)})
            print out, err

    def execute_function(self, function, container_path = None, testid = "", description = "", profile=True, dt_profile=1.0):
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
		print "Executing test_function({})...".format(x)
    		a = np.random.random(size=(1000,1000))
    		q, r = np.linalg.qr(a)
    		a2 = q.dot(r)
	mybenchmark = Benchmark()
	mybenchmark.execute_function( lambda: test_function(4), profile=True )

	"""

        self.update_bench_dict({"TestID": testid})
        self.update_bench_dict({"Desctription": description})
        self.update_bench_dict({"Container": os.environ['SINGULARITY_NAME']})

        time_start = datetime.now()
        if profile:
            from profile import ResourceProfiler
            with ResourceProfiler(dt=dt_profile) as rprof:
                function()
            results = rprof.results
            from bokeh.io import output_notebook
            rprof.visualize()
        else:
            function()
        time_end = datetime.now()
        test_time = time_end - time_start
        print 'Test finished - RunTime (s): ' + str(test_time.seconds)
        self.update_bench_dict({'RunTime (s)': str(test_time.seconds)})
        
				

    def update_bench_dict(self,dict_):
        self.bench_dict.update(dict_)
        
