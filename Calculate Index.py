#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 10 18:06:41 2021

V 1.0.0 -- old version
V 2.0.0 -- 1 feb 2022 by mgr -- fixed some bugs, added better configurability
V 2.1.0 -- 14 jan 2023 by mgr -- steady improvements

TODO:
- fix pandas warnings
- fix openPyXl
- consolidate code for plots


@author: max
"""
import pymarkets as pm
import pymarkets.VIK as VIK
import pymarkets.VIK.output
import configparser as cp
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as ticker
import datetime
from datetime import datetime as dt
import math
import argparse as arg

annotate = False
pad_historical = True

version = '2.1.0'

# command line arguments
 
parser = arg.ArgumentParser(description='VIK-Index -- Version ' + version) 
# add arguments inside parser 
# "help" (description of the argument)
# '--config', '--reload', '--debug', '--trend' are optional arguments in long notation
parser.add_argument('--config', dest='configfile', default='config.ini', help='define the name of the configuration file (default: config.ini)')
# action='store_true' makes default-value false but if we call this flag, it will become true
parser.add_argument('--verbose', dest='verbose', default=False, action='store_true', help='be verbose')
parser.add_argument('--reload', dest='reload', default='Partial', help='Define reload mechanism: Full, Partial, None')
parser.add_argument('--no_ftp', dest='noftp', action='store_true', default=False, help='Do not load data from FTP')
parser.add_argument('--no_output', dest='no_output', action='store_true', default=False, help='Do not output data')
parser.add_argument('--last_year', dest='last_year', default=datetime.datetime.now().year, help='last year to be read for power prices')
parser.add_argument('--eom_override', dest='eom_override', default=False, action='store_true', help='do not perform end-of-month check for index time series.')
args = parser.parse_args()

# read configuration file

config = cp.ConfigParser()
config.sections()
config.read(args.configfile)

year = int(args.last_year)

# Paths
fontpath = config['Paths']['fontpath']
basename = config['Paths']['basename']
datapath = basename + config['Paths']['data']
outpath = basename + config['Paths']['output']

# Filenames
filename_Abgaben = config['Files']['Abgaben']
filename_historical = config['Files']['Historical']

files = [{'r_path': config['Paths']['r_path'] + '/' + str(year) + '/',
          'r_filename': pm.eex.eex_filenames(str(year))[2],
          'l_path': config['Paths']['l_path']}]

if args.verbose:
    print('Getting latest power prices from EEX')

# provide error checking
if not args.noftp:
    EEX = {'host': config['sFTP']['server'], 'user': config['sFTP']['username'], 'passwd': config['sFTP']['password']}
    pm.eex.import_from_sftp(EEX, files)

Hours = list(range(3000, 7000, 1000))
# need one path for placing the output
if args.verbose:
    print('instantiating and initializing VIK-Index object')

V = VIK.VIndex(filename_Abgaben, basename = basename, index_range = ('2002', str(year)), eom_override = args.eom_override)

if args.verbose:
    print('partial reload of power prices from XLS')

# provide error checking in order not to accientially delete csvs
if args.reload == "Full":
    start_year = 2002
else:
    start_year = year

V.Power_Prices(reload = args.reload, basename = datapath, start = start_year, end = year + 1, verbose = args.verbose)

# calculate index
# NB: The base_peak_function_table is only defined for the legacy set of usage hours!
if args.verbose:
    print('Calculating index acc. to CvP')

V.index_CvP(V.base_peak_function_table)

# export to Excel/CSV
# set proper period-type indices
for df in [V.base, V.end]:
    df.index = df.index.to_period()
   
result = pd.concat([V.base, V.end], axis = 1).round(2)

# pad first rows with historical data
if pad_historical:
   #  filename_historical = 'VIK Index CvP 01-2021.xlsx'
    VIK_historical = pd.read_excel(basename + filename_historical, index_col = 0, skiprows = 3, usecols=(0,1,2))
    VIK_historical.columns = ['VIK Base', 'VIK End']
    VIK_historical.index = VIK_historical.index.to_period()
    result = pd.concat([round(VIK_historical[:'2002-06'],2), result], axis = 0).sort_index()

# make sure this is the right index!
if not args.no_output:
    filename = result.index[-1].strftime('%Y-%m') + ' VIK-Index'
    result.to_csv(outpath + filename + '.csv')
#    result.to_csv("/tmp/" + filename + '.csv')

# The OpenPyXl is picky regarding formatting whole columns, thus the decimals aren't done properly.
# writer = pd.ExcelWriter(outpath + filename + '.xlsx', datetime_format = 'YYYY-MM', engine_kwargs = {float_format: '%.2f'})
    writer = pd.ExcelWriter(outpath + filename + '.xlsx', datetime_format = 'YYYY-MM')
    result.to_excel(writer, sheet_name = 'VIK Index', float_format = '%.2f')
    writer.close()

# plot data
# NB: we have to assume that the index is complete, because pandas cannot do dynamic rolling windows
cols = list(result.columns)
# Rolling mean is now centered!
result = pd.concat([result, result.rolling(12, center = True).mean()], axis = 1).round(2)
result.columns = cols + ['VIK Base - Mean', 'VIK End - Mean']

# figures
width = 10
golden_ratio = (5 ** 0.5 - 1) / 2
height = width * golden_ratio
linewidth = 1.8

#fig, ax = plt.subplots(figsize = (width, height))
# line style
colors = [VIK.defs.VIK_corporate_colours['Primary'],
          VIK.defs.VIK_corporate_colours['VIK_Green'],
          VIK.defs.VIK_corporate_colours['Primary'],
          VIK.defs.VIK_corporate_colours['VIK_Green']]
linestyle = ['-', '-', '--', '--']
linewidths = [ linewidth * x for x in [1, 1, 0.6, 0.6]]

# legend
legend_labels = ['Basisindex', 'Endindex', 
                 'gleitendes 12-Monatsmittel Basisindex', 
                 'gleitendes 12-Monatsmittel Endindex']

df2 = result
df2.index = df2.index.to_timestamp()
df2.columns = legend_labels
ticks = [10, 50]
labels = ['', "Index"]
ylims = [0, math.ceil(result.max().max()/ticks[1])*ticks[1]]

if not args.no_output:
    pm.VIK.output.plot(df2,
                          colors = colors,
                          width = 10,
                          linestyle = linestyle,
                          linewidths = linewidths,
                          ticks = ticks,
                          ylims = ylims,
                          labels = labels,
                          fontpath = fontpath,
                          outfile = outpath + filename )

def Direction_string(x, y):
    if y > x:
        return "Anstieg"
    else:
        return "Abfall"


# compile descriptive Markdown
if not args.no_output:
    with open(outpath + filename + '.md', 'w') as md_file:
        Index_date = result.index[-1].strftime("%B %Y")
        Current = result.iloc[-1]
        Before = result.iloc[-2]
        rolling_mean = result.dropna().iloc[-3:]

        # This indexing ensures (not always) that we only take full months into account 
        Power = V.Power_Prices[V.Power_Prices.index < dt.today()].iloc[-2:]
        
        print(f"# VIK-Index für {Index_date}", file = md_file)
        print(f"Base Index: {Current[0]} -- End Index: {Current[1]}", file = md_file)
        print("Gleitender Jahresdurchschnitt:", file = md_file)
        print(f"Base Index: {rolling_mean.iloc[-1,2]} -- End Index: {rolling_mean.iloc[-1,3]}", file = md_file)
        print("## Veränderungen", file = md_file)
        print(f"Base: {(Current[0] - Before[0]).round(2)} bzw. {100 * ((Current[0] - Before[0])/Before[0]).round(2)} %", file = md_file)
        print(f"End: {(Current[1] - Before[1]).round(2)} bzw. {100 * ((Current[1] - Before[1])/Before[1]).round(2)} %", file = md_file)
        print("## Strompreise", file = md_file)
        Direction = Direction_string(Power.iloc[-2]['Price Base'], Power.iloc[-1]['Price Base'])
        print(f"Base: {Direction} von {Power.iloc[-2]['Price Base'].round(2):.2f} EUR/MWh auf {Power.iloc[-1]['Price Base'].round(2):.2f} EUR/MWh", file = md_file)
        Direction = Direction_string(Power.iloc[-2]['Price Peak'], Power.iloc[-1]['Price Peak'])
        print(f"Peak: {Direction} von {Power.iloc[-2]['Price Peak'].round(2):.2f} EUR/MWh auf {Power.iloc[-1]['Price Peak'].round(2):.2f} EUR/MWh", file = md_file)
