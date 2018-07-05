from __future__ import absolute_import, division, print_function

import random
from bisect import bisect_left
from distutils.version import LooseVersion
from itertools import cycle
from operator import itemgetter, add
from numpy import diff

# from ..utils import funcname, import_required
# from ..core import istask
# from ..compatibility import apply

from importlib import import_module

def import_required(mod_name, error_msg):
    """Attempt to import a required dependency.
    Raises a RuntimeError if the requested module is not available.
    """
    try:
        return import_module(mod_name)
    except ImportError:
        raise RuntimeError(error_msg)



_BOKEH_MISSING_MSG = "Diagnostics plots require `bokeh` to be installed"
_TOOLZ_MISSING_MSG = "Diagnostics plots require `toolz` to be installed"


def visualize(profilers, file_path=None, show=True, save=True, **kwargs):
    """Visualize the results of profiling in a bokeh plot.

    If multiple profilers are passed in, the plots are stacked vertically.

    Parameters
    ----------
    profilers : profiler or list
        Profiler or list of profilers.
    file_path : string, optional
        Name of the plot output file.
    show : boolean, optional
        If True (default), the plot is opened in a browser.
    save : boolean, optional
        If True (default), the plot is saved to disk.
    **kwargs
        Other keyword arguments, passed to bokeh.figure. These will override
        all defaults set by visualize.

    Returns
    -------
    The completed bokeh plot object.
    """
    bp = import_required('bokeh.plotting', _BOKEH_MISSING_MSG)
    import bokeh

    if LooseVersion(bokeh.__version__) >= "0.12.10":
        from bokeh.io import state
        in_notebook = state.curstate().notebook
    else:
        from bokeh.io import _state
        in_notebook = _state._notebook

    if not in_notebook:
        file_path = file_path or "profile.html"
        bp.output_file(file_path)

    if not isinstance(profilers, list):
        profilers = [profilers]
    figs = [prof._plot(**kwargs) for prof in profilers]
    # Stack the plots
    if len(figs) == 1:
        p = figs[0]
    else:
        top = figs[0]
        for f in figs[1:]:
            f.x_range = top.x_range
            f.title = None
            f.min_border_top = 20
            f.plot_height -= 30
        for f in figs[:-1]:
            f.xaxis.axis_label = None
            f.min_border_bottom = 20
            f.plot_height -= 30
        for f in figs:
            f.min_border_left = 75
            f.min_border_right = 75
        p = bp.gridplot([[f] for f in figs])
    if show:
        bp.show(p)
    if file_path and save:
        bp.save(p)
    return p


def _get_figure_keywords():
    bp = import_required('bokeh.plotting', _BOKEH_MISSING_MSG)
    o = bp.Figure.properties()
    o.add('tools')
    return o



def plot_resources(results, palette='Viridis', **kwargs):
    """Plot resource usage in a bokeh plot.

    Parameters
    ----------
    results : sequence
        Output of ResourceProfiler.results
    palette : string, optional
        Name of the bokeh palette to use, must be a member of
        bokeh.palettes.all_palettes.
    **kwargs
        Other keyword arguments, passed to bokeh.figure. These will override
        all defaults set by plot_resources.

    Returns
    -------
    The completed bokeh plot object.
    """
    bp = import_required('bokeh.plotting', _BOKEH_MISSING_MSG)
    from bokeh import palettes
    from bokeh.models import LinearAxis, Range1d
    from bokeh.layouts import column

    defaults = dict(title="Profile Results",
                    tools="save,reset,xwheel_zoom,xpan",
                    toolbar_location='above',
                    plot_width=800, plot_height=300)
    defaults.update((k, v) for (k, v) in kwargs.items() if k in
                    _get_figure_keywords())
    if results:
        t, cpu, pmem, rss, uss, pss, rio, wio = zip(*results)
        rio = [0] + list(diff(rio))
        wio = [0] + list(diff(wio))	

        left, right = min(t), max(t)
        t = [i - left for i in t]
        p1 = bp.figure(y_range=fix_bounds(0, 1.1*max(cpu), 100),
                      x_range=fix_bounds(0, right - left, 1),
                      **defaults)
        p2 = bp.figure(y_range=fix_bounds(0, 1.1*max(rss), 100),
                      x_range=fix_bounds(0, right - left, 1),
                      **defaults)
        p3 = bp.figure(y_range=fix_bounds(0, 1.1*max(rio), 100),
                      x_range=fix_bounds(0, right - left, 1),
                      **defaults)

    else:
        t = cpu = pmem = rss = uss = pss = rio = wio = []

        p1 = bp.figure(y_range=(0, 100), x_range=(0, 1), **defaults)
        p2 = bp.figure(y_range=(0, 100), x_range=(0, 1), **defaults)
        p3 = bp.figure(y_range=(0, 100), x_range=(0, 1), **defaults)

    colors = palettes.all_palettes[palette][6]

    p1.line(t, cpu, color=colors[0], line_width=4, legend='% CPU')
    p1.yaxis.axis_label = "% CPU"
#     p1.extra_y_ranges = {'memory': Range1d(*fix_bounds(0, 1.2*max(pmem) if pmem else 100, 100))}
#     p1.line(t, pmem, color=colors[2], y_range_name='memory', line_width=4, legend='Memory')
    p1.xaxis.axis_label = "Time (s)"
#     p1.add_layout(LinearAxis(y_range_name='memory', axis_label='Memory (MB)'), 'right')


    p2.line(t, rss, color=colors[2], line_width=4, legend='Memory')
    p2.yaxis.axis_label = "RSS Memory (MB)"

    p2.extra_y_ranges = {'uss': Range1d(*fix_bounds(0, 1.1*max(uss) if uss else 100, 100))}
    p2.line(t, uss, color=colors[1], y_range_name='uss', line_width=4, legend='USS Memory (MB)')
    p2.add_layout(LinearAxis(y_range_name='uss', axis_label='USS Memory (MB)'), 'right')

    p2.xaxis.axis_label = "Time (s)"


    p3.line(t, rio, color=colors[3], line_width=4, legend='I/O Reads')
    p3.yaxis.axis_label = "I/O Reads (MB)"

    p3.extra_y_ranges = {'iowrites': Range1d(*fix_bounds(0, 1.1*max(wio) if wio else 100, 100))}
    p3.line(t, wio, color=colors[4], y_range_name='iowrites', line_width=4, legend='I/O Writes (MB)')
    p3.add_layout(LinearAxis(y_range_name='iowrites', axis_label='I/O Writes (MB)'), 'right')

    p3.xaxis.axis_label = "Time (s)"

    p = column(p1, p2, p3)
    return p


def fix_bounds(start, end, min_span):
    """Adjust end point to ensure span of at least `min_span`"""
    return start, max(end, start + min_span)
