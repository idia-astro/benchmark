from ConfigParser import SafeConfigParser
from optparse import OptionParser
#import dateutil

import datetime
import subprocess
import sys
import os

config = SafeConfigParser()

usage = "usage: %prog options"
parser = OptionParser(usage=usage)

parser.add_option("--config", "-c", type='string', dest='config',
        default=None, help='Name of config file')
parser.add_option("--mode", "-m", type='string', dest='mode',
        default='run', help='Mode to run [run]/dry')
parser.add_option("--output", "-o", type='string', dest='output', 
        default='simnclean.log', help='File for output messages. [simnclean.log]')

(options, args) = parser.parse_args()

if __name__=="__main__":
        if (len(sys.argv)==1) or (options.config is None):
            parser.print_help()
            dummy = sys.exit(0)

def inouttimes(logtext, time_format="%Y-%m-%d %H:%M:%S", 
              begin_text='Begin Task', end_text='End Task', 
              split_char='\t',
              wsclean=False):
    '''
    Function that searches a log output for start and end times based on begin 
    and end text identifiers.
    '''
    loglist = logtext.split('\n')
    #print mylist
    begin_i = [i for i, s in enumerate(loglist) if begin_text in s]
    end_i = [i for i, s in enumerate(loglist) if end_text in s]
    

    if not wsclean:
        #start_time = dateutil.parser.parse(loglist.split('\n')[0])
        #time_format = "%Y-%m-%d %H:%M:%S"
        t1 = loglist[begin_i[0]].split(split_char)[0]
        s = datetime.datetime.strptime(t1, time_format)
        #end_time = dateutil.parser.parse(loglist.split('\n')[0])
        t2 = loglist[end_i[0]].split(split_char)[0]
        f = datetime.datetime.strptime(t2, time_format)
    else:
        #start_time = dateutil.parser.parse(loglist.split('\n')[0])
        #time_format = "%Y-%m-%d %H:%M:%S"
        t1 = loglist[begin_i[0]].split(split_char)[0]+' '+loglist[begin_i[0]].split(split_char)[1]
        s = datetime.datetime.strptime(t1, time_format)
        #end_time = dateutil.parser.parse(loglist.split('\n')[0])
        t2 = loglist[end_i[0]].split(split_char)[0]+' '+loglist[end_i[0]].split(split_char)[1]
        f = datetime.datetime.strptime(t2, time_format)
    
    dt = f-s
    #dt = end_time-start_time
    return dt
    
def runprocess(call=None, mode='RUN', file=None):
    call = 'time '+call
    proc = subprocess.Popen(call, stdout=subprocess.PIPE,
            stderr = subprocess.PIPE, shell=True)
    if mode.upper()=="RUN":
        print call
        out, err = proc.communicate()
        #print out, err
        if file is not None:
            file.write(out)
            file.write(err)
        return out, err
    else:
        print "Dry Run"
        print call
        return 0, 0

def ddio(n=2, bs='10M'):
    '''
    '''
    print "DD/Write"
    wtimes = []
    call = 'rm testfile; dd if=/dev/zero of=testfile bs='+bs+' count=1 oflag=direct'
    for i in range(1,n+1):
        
        proc = subprocess.Popen(call, stdout=subprocess.PIPE,
                stderr = subprocess.PIPE, shell=True)
        out, err = proc.communicate()
        wtimes.append(err.replace(' ','').split(',')[-1].replace('\n',''))
        #print out, err
    print wtimes
    
    print "DD/Read"
    rtimes = []
    for i in range(1,n+1):
        call = 'dd if=testfile of=/dev/null bs='+bs
        proc = subprocess.Popen(call, stdout=subprocess.PIPE,
                stderr = subprocess.PIPE, shell=True)
        out, err = proc.communicate()
        rtimes.append(err.replace(' ','').split(',')[-1].replace('\n',''))
        #print rtimes
    print rtimes    
        
def simulate(config, file=None):
    call = config.get('execute', 'path2casa')
    call+=' --nologger --log2term --nogui -c "run msim.py '
    # Name of Project
    call+=(' -p '+config.get('simulate', 'project'))
    # Direction
    call+=(' -d '+config.get('simulate', 'direction'))
    # Band
    call+=(' -b '+config.get('simulate', 'band'))
    # Time
    call+=(' -t '+config.get('simulate', 'time'))
    call+=' "'
    out, err = runprocess(call, mode=options.mode, file=file)
    dt = inouttimes(err, begin_text='Begin Task: simobserve',
              end_text="End Task: simobserve")
    return out, err, dt
    
this_script_start = datetime.datetime.now()
   
config.read(options.config)

F = False # This will be overwritten if mode=run.

if options.mode.upper()=='RUN':
    F = open(options.output, 'a+')
    F.write('====SIMNCLEAN.PY '+this_script_start.isoformat()+'====\n')
    outtext = "Check "+options.output+" for full log, after time "+this_script_start.isoformat()+'\n'
    print outtext

# Do the simulation.

if config.getboolean('execute','sim'):
    out, err, dt_sim = simulate(config, file=F)

# Bookkeeping.

project = config.get('simulate','project')
ms = project+'/'+project+'.meerkat.ms'

# Do the imaging.

if config.getboolean('execute', 'image'):
    if config.getboolean('execute','tclean'):
        #Parameters for TCLEAN
        call = config.get('execute', 'path2casa')
        call+=' --nologger --log2term --nogui -c "'
        call+='tclean(vis=\\"'+ms+'\\",imagename=\\"'+ms+'.tclean\\",'
        for c in config.items('tclean'):
            call+=c[0]+'='+c[1]+','
        
        call+=')"'
        out, err = runprocess(call, mode=options.mode, file=F)

        print 'TCLEAN'
        dt_tclean = inouttimes(err)

    if config.getboolean('execute','wsclean'):
        # Parameters for WSCLEAN
        call = config.get('execute', 'path2wsclean')+' -log-time '
        for c in config.items('wsclean'):
            call+=(' -'+c[0]+' '+c[1])

        call+=' -name '+ms+'.wsclean'
        call+=' '+ms 
        out, err = runprocess(call, mode=options.mode, file=F)
        dt_wsclean = inouttimes(out, time_format="%Y-%b-%d %H:%M:%S.%f",
                  begin_text="WSClean version",
                  end_text="Writing restored image",
                  split_char=' ',
                  wsclean=True)
        
if int(config.get('execute', 'ddio').split(',')[0])>0:
    n = int(config.get('execute', 'ddio').split(',')[0])
    bs =  config.get('execute', 'ddio').split(',')[1]
    ddio(n=n, bs=bs)
    
this_script_end = datetime.datetime.now()

this_script_duration = this_script_end - this_script_start
outtext = "simnclean.py ran in "+str(this_script_duration.seconds)+" seconds.\n"
print outtext
if F:
    F.write("====BEGIN SIMNCLEAN.PY REPORT====\n")
if F:
    F.write(outtext)

if config.getboolean('execute','sim'):
    outtext = "casa simobserve ran in "+str(dt_sim.seconds)+" seconds.\n"
    print outtext
    if F:
        F.write(outtext)
        
if config.getboolean('execute', 'image'):
    if config.getboolean('execute','tclean'):
        outtext =  "casa tclean ran in "+str(dt_tclean.seconds)+" seconds.\n"
        print outtext
        if F:
            F.write(outtext)
    if config.getboolean('execute','wsclean'):
        outtext = "wsclean ran in "+str(dt_wsclean.seconds)+" seconds.\n"
        print outtext
        if F:
            F.write(outtext)
        
if F:
    F.write("====END SIMNCLEAN.PY REPORT====\n")
    F.close()
    