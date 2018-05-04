from __future__ import absolute_import, division, print_function

from collections import namedtuple
from itertools import starmap
from timeit import default_timer
from time import sleep
from multiprocessing import Process, Pipe, current_process

from importlib import import_module

def import_required(mod_name, error_msg):
    """Attempt to import a required dependency.
    Raises a RuntimeError if the requested module is not available.
    """
    try:
        return import_module(mod_name)
    except ImportError:
        raise RuntimeError(error_msg)


# Stores execution data for each task
ResourceData = namedtuple('ResourceData', ('time', 'mem', 'cpu', 'pmem', 'rio', 'wio'))


class ResourceProfiler(object):
    """A profiler for resource use.

    Records the following each timestep
        1. Time in seconds since the epoch
        2. Memory usage in MB
        3. % CPU usage

    Examples
    --------

    >>> from operator import add, mul
    >>> from dask.threaded import get
    >>> dsk = {'x': 1, 'y': (add, 'x', 10), 'z': (mul, 'y', 2)}
    >>> with ResourceProfiler() as prof:  # doctest: +SKIP
    ...     get(dsk, 'z')
    22

    These results can be visualized in a bokeh plot using the ``visualize``
    method. Note that this requires bokeh to be installed.

    >>> prof.visualize() # doctest: +SKIP

    You can activate the profiler globally

    >>> prof.register()  # doctest: +SKIP

    If you use the profiler globally you will need to clear out old results
    manually.

    >>> prof.clear()  # doctest: +SKIP

    Note that when used as a context manager data will be collected throughout
    the duration of the enclosed block. In contrast, when registered globally
    data will only be collected while a dask scheduler is active.
    """
    def __init__(self, dt=1):
        print("init rprof")
        self._dt = dt
        self._entered = False
        self._tracker = None
        self.results = []

    def _is_running(self):
        return self._tracker is not None and self._tracker.is_alive()

    def _start_collect(self):
        if not self._is_running():
            self._tracker = _Tracker(self._dt)
            self._tracker.start()
        self._tracker.parent_conn.send('collect')

    def _stop_collect(self):
        if self._is_running():
            self._tracker.parent_conn.send('send_data')
            self.results.extend( starmap(ResourceData, self._tracker.parent_conn.recv()) )

    def __enter__(self):
        self._entered = True
        self.clear()
        self._start_collect()
        print("entering rprof")
        return self

    def __exit__(self, *args):
        self._entered = False
        print("exiting rprof ...")
        self._stop_collect()
        self.close()
        print("exited rprof")
#         super(ResourceProfiler, self).__exit__(*args)

    def _start(self, dsk):
        self._start_collect()

    def _finish(self, dsk, state, failed):
        if not self._entered:
            self._stop_collect()

    def close(self):
        """Shutdown the resource tracker process"""
        if self._is_running():
            self._tracker.shutdown()
            self._tracker = None

    __del__ = close

    def clear(self):
        self.results = []

    def _plot(self, **kwargs):
        from profile_visualize import plot_resources
        return plot_resources(self.results, **kwargs)

    def visualize(self, **kwargs):
        """Visualize the profiling run in a bokeh plot.

        See also
        --------
        dask.diagnostics.profile_visualize.visualize
        """
        from profile_visualize import visualize
        return visualize(self, **kwargs)


class _Tracker(Process):
    """Background process for tracking resource usage"""
    def __init__(self, dt=1):
        psutil = import_required("psutil", "Tracking resource usage requires "
                                           "`psutil` to be installed")
        Process.__init__(self)
        self.daemon = True
        self.dt = dt
        self.parent = psutil.Process(current_process().pid)
        self.parent_conn, self.child_conn = Pipe()

    def shutdown(self):
        if not self.parent_conn.closed:
            self.parent_conn.send('shutdown')
            self.parent_conn.close()
        self.join()

    def _update_pids(self, pid):
        return [self.parent] + [p for p in self.parent.children()
                                if p.pid != pid and p.status() != 'zombie']

    def run(self):
        pid = current_process()
        data = []
        while True:
            try:
                msg = self.child_conn.recv()
            except KeyboardInterrupt:
                continue
            if msg == 'shutdown':
                break
            elif msg == 'collect':
                ps = self._update_pids(pid)
                while not data or not self.child_conn.poll():
                    tic = default_timer()
                    mem = cpu = pmem = ior = iow = 0
                    for p in ps:
                        try:
                            mem1 = p.memory_percent()
                            mem2 = p.memory_info().rss
                            cpu2 = p.cpu_percent()
                            io_read = p.io_counters().read_bytes
                            io_write = p.io_counters().write_bytes
                        except Exception: # could be a few different exceptions
                            pass
                        else:
                            # Only increment if both were successful
                            mem += mem2
                            cpu += cpu2
                            pmem += mem1
                            ior += io_read
                            iow += io_write
                    data.append((tic, mem / 1e6, cpu, pmem, ior / 1e6, iow / 1e6))
                    sleep(self.dt)
            elif msg == 'send_data':
                self.child_conn.send(data)
                data = []
        self.child_conn.close()