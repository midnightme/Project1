import pandas as pd
import datetime
import os
import configparser as cp
from datetime import datetime as dt
import math as m
import numpy as np
import pymarkets as pm
import pymarkets.eex as eex
import seaborn as sns
import matplotlib.pyplot as plt

# ToDo:
# - fill in the blanks
# - EEX-Prices before Jun-2002
# - CO2-Prices
# - Abgaben: 
#   - Pad dates before first datum with zeros
#   - Pad NaNs for merged cells in Excel
# - VIK corporate colors as given in defs.py
# - date functionality, ie. get index from x to y (in z resolution) : but maybe that's just simple slicing and resampling.
#####
# - allow for all longer/more complicated time series to be reassembled from raw data. but write in single file for day-to-day operations
#

class Abgabe(object):
    '''Einfache Klasse, die Ababedaten (jahresscharf) aus XLS/CSV-Dateien lädt
       und in verschiedenen Formaten zur Verfügung stellt'''
       
       
    def __init__(self, name = '', basename = '', pad = False, **kwargs):
        self.basename = basename
        self.name = name
        if 'native_freq' in kwargs:
            self.native_freq = kwargs['native_freq']
        else:
            self.native_freq = '%Y'
        self.indextype = ''
        self.df = None
        self.pad = pad
  
        # call the load-method if enough (and the right) arguments are passed
        if 'xls_filename' in kwargs:
            if 'sheet' in kwargs:
                method = 'ffill', axis = axis)

class NNE(Abgabe):
    ''' Simple class to hold data of all DSOs
        with additional analytics methods
        In order to simplify data management, All DSOs have been given a number (also to account for name changes over the years),
        and only incremental changes have been recorded in the excel sheet
        '''
    
    def __init__(self, Hours, **kwargs):
        if type(Hours) is not list:
            self.Hours = [Hours]
        else:
            self.Hours = Hours
        # use the base class's constructor for data loading
        super().__init__(**kwargs)
        # resample according to frequency (Month) and DSO index
        
        if 'last_date' not in kwargs:
            last_date = dt.today().replace(hour = 0, minute = 0, second = 0, microsecond = 0)
        else:
            last_date = kwargs['last_date']
        for DSO in self.df['VNB Nr'].unique():
            row = list(self.df[self.df['VNB Nr'] == DSO].tail(1).values[0])
            self.df = pd.concat([self.df, pd.DataFrame([row], columns =
                                                       self.df.columns, index =
                                                       [last_date])])
            # self.df = self.df.append(pd.DataFrame([row], columns = self.df.columns, index = [last_date]))
        self.df.index.name = 'Monat'
        G = self.df.groupby('VNB Nr')
        G = G.resample('MS').ffill()
        self.df = G.drop('VNB Nr', axis = 1).reset_index().set_index('Monat').sort_values(['Monat', 'VNB Nr'])
  
    def Price_per_kWh(self,prefix = 'NE_', avg = False):
        ' Netzentgelt pro kWh mittels Jahresbenutzungsstunden aus Arbeitspreis und Leistungspreis'
       
        result = self.df.copy()
        
        for item in self.Hours:
            mask = self.df['Tarifschwelle'] > item
            result.loc[mask, prefix + str(item)] =  round(self.df[mask]['LP_low']/item * 100 + self.df[mask]['AP_low'], 4)
            result.loc[~mask, prefix + str(item)] =  round(self.df[~mask] ['LP_high']/item * 100 + self.df[~mask]['AP_high'], 4)
        
        if avg:
            # !Hardcoded format
            result = result.groupby('Monat').mean(numeric_only = True).drop('Tarifschwelle', axis = 1)
        return result

    def Avg_NE(self, df, prefix = 'NE_'):
        'Durchschnitt der Netzentgelte über die mit Prefix benannten Spalten, ct/kWh'
        result = df[[col for col in df.columns if prefix in col]].groupby(df.index).mean().mean(axis = 1)
        result.name = 'Netzentgelte'
        
        return result

class Stromsteuer(Abgabe):
    ''' include that weird tax calculation '''
    
    def __init__(self, **kwargs):
        ''' Init via the super class and perform CvP's calculations '''
        super().__init__(**kwargs)
        # The values calculated thus are not the same as CvP's hard-coded 20% rates
        self.df['Stromsteuer'] = 0.28 * (self.df['Regelsatz'] - self.df['Abzug prod Gewerbe'])
        X = self.df.index < '2012'
        self.df.loc[X, 'Stromsteuer'] = 0.24 * (self.df.loc[X]['Regelsatz'] - self.df.loc[X]['Abzug prod Gewerbe']) 
        X = self.df.index < '2003'
        self.df.loc[X, 'Stromsteuer'] = 6/25 * (self.df.loc[X]['Regelsatz'] - self.df.loc[X]['Abzug prod Gewerbe']) 

class VIndex(object):
        
    # compile the individual levies
    # NB: some hardcoded index ranges
    def __init__(self, Abgabenfile, basename = '', reload = {'Power': False, 'NNE': 'False', 'CO2': False}, index_range = ('2002', '2021'), eom_override = False):
        kwargs = {'xls_filename': Abgabenfile, 'basename': basename, 'index_range': index_range}
    
        # set variables
        self.Hours = list(range(3000, 7000, 1000))
        self.base = None
        self.end = None
        self.index_range = index_range
        self.eom_override = eom_override
        
        # set DataFrames
        self.KWK = Abgabe(name = 'KWK-Umlage', sheet = 'KWK-Umlage', pad = True, **kwargs)
        self.EEG = Abgabe(name = 'EEG-Umlage', sheet = 'EEG-Umlage', **kwargs)
        self.AblaV = Abgabe(name = 'AblaV-Umlage',sheet = 'AblaV-Umlage', **kwargs)
        self.Offshore = Abgabe(name = 'Offshore-Umlage', sheet = 'Offshore-Umlage', pad = True, **kwargs)
        self.Stromsteuer = Stromsteuer(name = 'Stromsteuer', sheet = 'Stromsteuer', **kwargs)
        self.Konzessionsabgabe = Abgabe(name = 'Konzessionsabgabe', sheet = 'Konzessionsabgabe', **kwargs)
        self.P19 = Abgabe(name = '§ 19(2)-Umlage', sheet = '§ 19(2)-Umlage', **kwargs)
        self.CO2_Intensität = Abgabe(name = 'CO2-Intensität', sheet = 'CO2-Intensität', **kwargs)
        self.NNE = NNE(Hours = self.Hours, xls_filename = 'NE.xlsx', basename = basename, sheet = 'Netzentgelte', format = '%Y-%m')
        
        # self.Abgaben = [self.KWK, self.EEG, self.AblaV, 
        #                 self.Offshore, self.Stromsteuer, self.Konzessionsabgabe, 
        #                 self.P19]
        
        # Power Prices
        # self.Power_Prices(reload['Power'], **kwargs)
        # CO2-Prices
        #self.CO2_Prices(reload['CO2'], **kwargs)
        
    def Power_Prices(self, reload = 'Full', **kwargs):
        
        # load and aggregate Quarter Prices
        basename = kwargs['basename']
        print(reload)
        for C in ['B', 'P']:
            cname = 'F1' + C + 'Q'
            # instantiate the contract from its string name using the classes dictionary
            self.__dict__[cname] = eex.Contract(kwargs['basename'], cname, import_specs = eex.Phelix(self.index_range[1], profile = C, maturity = 'Q'))
            contract = self.__dict__[cname]
            if reload != 'Full':
                contract.read_csv(basename + cname + '.csv')
            if reload in ['Full', 'Partial']:
                load_range = range(kwargs['start'], kwargs['end'])
                if 'verbose' in kwargs:
                    print(load_range)
                    print (cname)
                    print(reload)
                    print("debug")
                    print(self.index_range)
                contract.import_xls(range = load_range, verbose = True)
                contract.sort_me(['Trading Day', 'Maturity'])
                # remove June 2002, as there are dummy prices
                contract.df = contract.df['2002-07':]
                contract.dropna()
                contract.export_csv(basename + cname + '.csv')
            
        Base =self._aggregate_Q_Y(self.F1BQ.df)
        Peak = self._aggregate_Q_Y(self.F1PQ.df)
        
        # self.F1BQ = eex.Contract(kwargs['basename'], 'F1BQ', import_specs = eex.Phelix(self.index_range[1], profile = 'B', maturity = 'Q'))
        # self.F1PQ = eex.Contract(kwargs['basename'], 'F1PQ', import_specs = eex.Phelix(self.index_range[1], profile = 'P', maturity = 'Q'))
        # basename = kwargs['basename']
        # if reload != 'Full':
        #     self.F1BQ.read_csv((basename + 'F1BQ.csv'))
        #     self.F1PQ.read_csv((basename + 'F1PQ.csv'))
        # if reload in ['Full', 'Partial']:
        #     load_range = range(kwargs['start'], kwargs['end'])
        #     self.F1BQ.import_xls(range = load_range, verbose = True)
        #     self.F1PQ.import_xls(range = load_range, verbose = True)
        #     self.F1BQ.sort_me(['Trading Day', 'Maturity'])
        #     self.F1PQ.sort_me(['Trading Day', 'Maturity'])
        #     # replace the legacy dummy of 0.1 in 2002
        #     if kwargs['start'] <= 2002:
        #         self.F1BQ.df.loc['2002'].replace(to_replace = 0.1, value = np.nan, inplace = True)
        #         self.F1PQ.df.loc['2002'].replace(to_replace = 0.1, value = np.nan, inplace = True)
        #     self.F1BQ.dropna()
        #     self.F1PQ.dropna()
        #     self.F1BQ.export_csv(basename + 'F1BQ.csv')
        #     self.F1PQ.export_csv(basename + 'F1PQ.csv')
        # Base =self._aggregate_Q_Y(self.F1BQ.df)
        # Peak = self._aggregate_Q_Y(self.F1PQ.df)
        
        self.Power_Prices = Base.merge(Peak, left_index = True, right_index = True, suffixes = (' Base', ' Peak'))
    
    # aggregate four next quarters to a quasi-front-year
    def _aggregate_Q_Y(self, df):
        # !Hardcoded format
        P = df.groupby('Maturity').resample('1M').mean(numeric_only = True).reset_index(level = 0)
        P2 = P.sort_values(by = ['Trading Day', 'Maturity'])
        P2 = P2.groupby(P2.index).head(4)
        return P2.groupby(P2.index).mean(numeric_only = True)
    
    def Avg_NNE(self):
        return self.NNE.Avg_NE(self.NNE.Price_per_kWh())
    
    def base_peak_function_exp(self, hours):
        ''' Das ist diese komische Base-Peak-Aufteilung, die undokumentiert ist.'''
    
        peak = 310.72 * m.exp(-0.0006*hours)
        return (100 - peak, peak)
    
    def base_peak_function_table(self, hours):
        
        L = [50.50, 71.40, 83.20, 92.00]
        base = L[int(hours/1000) - 3]
        
        return (base, 100 - base)


    def base_index(self, base_peak_function, prefix = 'P_'):
        result = self.Power_Prices.copy()
        Hour_dict = {hour: base_peak_function(hour) for hour in self.Hours} 
        for k, v in Hour_dict.items():
            result[prefix + str(k)] = (v[0] * self.Power_Prices['Price Base'] + v[1] * self.Power_Prices['Price Peak'])/100

        # !Hardcoded format        
        result = result.set_index(result.index.strftime('%Y-%m'))
        # Ich glaube, das hier ist sachlich falsch, weil wir durchschnittliche Strompreise plus durchschnittliche NNE rechnen,
        # statt erst Strom + NNE und dann über die Vollbenutzungsstunden zu mitteln
        # NNE = self.Avg_NNE()
        # besser:
        NNE = self.NNE.Price_per_kWh(avg = True)
        # !Hardcoded format
        NNE.index = NNE.index.strftime('%Y-%m')
        
        result= result.merge(NNE, left_index = True, right_index = True)
        result = result.drop([col for col in result.columns if col[-4:] not in str(self.Hours)], axis = 1)
        for hour in self.NNE.Hours:
            result[str(hour)] = result['P_' + str(hour)] + 10*result['NE_' + str(hour)]
    
        result = result[[str(hour) for hour in self.Hours]].mean(axis = 1)

        # Todo:
        # - we'e missing the early-2002-data
        # - normalize to Jan-2002
        result.name = 'VIK Base'
        result.index = pd.to_datetime(result.index)
        self.base = result

    # use this method to get rid of spurious data and normalize to CvP's initial value
    def index_CvP(self, base_peak_function, reference = [46.57933694, 52.57407389]):
        
        if self.base is None:
            self.base_index(base_peak_function)
        self.end_index(base_peak_function)
        #self.base.to_csv('~/tmp/base.csv')
        #self.end.to_csv('~/tmp/end.csv')
        self.base = 100 * self.base / reference[0]
        self.end = 100 * self.end / reference[1]
        
        # drop last row, unless the power prices stop at the end of the month
        # this ignores the trading calendar! end of month need not be a trading day.
        # override with cmdl-option --eom_override
        if not self.eom_override:
            if not pd.Series(self.F1BQ.df.index).dt.is_month_end.values[-1]:
                self.Power_Prices = self.Power_Prices[:-1]
                self.base = self.base[:-1]
                self.end = self.end[:-1]

    def end_index(self, base_peak_function):
        
        if self.base is None:
            self.base_index(base_peak_function)
        df = self.base.copy()
        
        # add stuff
        df = pd.merge_asof(df, 10 * self.KWK.df['Kat B'], left_index = True, right_index = True)
        df = pd.merge_asof(df, 10 * self.EEG.df, left_index = True, right_index = True)
        df = pd.merge_asof(df, 10 * self.AblaV.df, left_index = True, right_index = True)
        df = pd.merge_asof(df, 10 * self.Offshore.df['Kat B'], left_index = True, right_index = True)
        df = pd.merge_asof(df, self.Stromsteuer.df['Stromsteuer'], left_index = True, right_index = True)
        df = pd.merge_asof(df, 10 * self.Konzessionsabgabe.df, left_index = True, right_index = True)
        df = pd.merge_asof(df, 10 * self.P19.df['Kat B'], left_index = True, right_index = True)
        
        df.columns = ['VIK Base', 'KWK', 'EEG', 'AblaV', 'Offshore', 'Stromsteuer', 'Konzession', 'Paragraph 19(2)']
        
        self._end = df
        
        self.end = df.sum(axis = 1)
        self.end.name = 'VIK End'

    # export base_index
    def export(self, filename):
        # export to csv and xlsx
        self.base_index.to_csv(filename)
        pass
    
    def plot(self, filename = ''):
        # plot the index in corporate colours
        # plot base/end index and rolling 12-month mean
        # add labels to defined dates
        # 
        pass
    
    def describe(self, filename = ''):
        # pump out a short markdown file with descriptive statistics
        # like we usually publish with the indices
        # add the plot and the explanatory stuff.
        # such that only the prose-paragraph has to be typed
        pass
    

class Calendar_Years(object):
    ''' class for importing/analysig calendar year data for Andreas' publications'''
    
    def __init__(self, reload = False, verbose = True, **kwargs):
        
        self.F1BY = eex.Contract(kwargs['basename'], 'F1BY', import_specs = eex.Phelix(kwargs['last_year'], profile = 'B', maturity = 'Y'))
        self.F1PY = eex.Contract(kwargs['basename'], 'F1PY', import_specs = eex.Phelix(kwargs['last_year'], profile = 'P', maturity = 'Y'))
        
        if reload:
            self.F1BY.import_xls(range = range(2002, kwargs['last_year']), verbose = verbose)
            self.F1PY.import_xls(range = range(2002, kwargs['last_year']), verbose = verbose)
            self.F1BY.export_csv(kwargs['basename'] + '/' + 'F1BY.csv')
            self.F1PY.export_csv(kwargs['basename'] + '/' + 'F1PY.csv')
        else:
            self.F1BY.read_csv(kwargs['basename'] + '/' + 'F1BY.csv')
            self.F1PY.read_csv(kwargs['basename'] + '/' + 'F1PY.csv')

        # dictionary for relative years
        self.front_year = {}

    def plot_last_year(self):
        pass

    def get_front_year(self, lag = 1):
        L = []
        for C in [self.F1BY, self.F1PY]:
            df =  C.df
            df['Maturity'] = pd.to_datetime(df['Maturity'].astype(str), format = '%Y-%m')
            df2 = df[df.index.year == df['Maturity'].dt.year - lag]
            df2 = df2.drop('Maturity', axis = 1)
            L.append(df2)
        self.front_year[lag] = L
    
    def monthly_avg(self):
        df = self.F1BY.df.copy().dropna(axis = 0)
        
        df = df.set_index([df.index, df['Maturity']])
        df = df.drop('Maturity', axis = 1).unstack(level = 1).resample('1M').mean()
        
        return df.iloc[-12:-1].dropna(axis = 1)
    
    # Heatmap for calendar prices as they should appear in our circulars.
    # ToDo: find a suitable colormap, vmin/vmax and center, e.g. avg. price of last year, past 250 days...
    def heatmap(self, filename, center = None):
        
        df = self.monthly_avg()
        df = df.round(2)
        df.index = df.index.strftime('%b %Y')
        df.columns = df.columns.droplevel(0)
        df.columns = [col[:4] for col in df.columns] # only use year
        ax = plt.axes()
        ax = sns.heatmap(df, ax = ax, annot = True, yticklabels = df.index, xticklabels = df.columns, fmt = '.2f', 
                         center = center)
        plt.yticks(rotation=0)
        ax.xaxis.tick_top() # x axis on top
        ax.xaxis.set_label_position('top')
        ax.tick_params(axis = 'y', left = False)
        plt.xlabel('Jahreskontrakt', labelpad = 10)
        plt.ylabel('Handelsmonat', labelpad = 10)
        plt.savefig(filename)
        

def eex_import_xls(filename, product, default_col_names, cols = ['Maturity', 'Settlement Price'], header = 2, index_col = 0, informat = ''):
    
    df = pd.read_excel(filename, sheet_name = product, header = header, index_col = 0)
    df = df[cols]
    df[cols[0]] = pd.to_datetime(df[cols[0]].astype(str), format = informat).dt.strftime('%Y-%m')
    df.index = df.index.rename(default_col_names['Index'])
    df.rename(columns = {cols[0]: default_col_names['Maturity'], cols[1]: default_col_names['Price']}, inplace = True)
    return df

def get_eex_prices(year = '2020'):
    
    default_col_names = {'Index': 'Trading Day',
                         'Maturity': 'Maturity',
                         'Price': 'Settlement Price'}
    
    if int(year) < 2002:
        return None
    elif int(year) <= 2014:
        contract = ('F1BQ', 'F1PQ')
        header = 1
        filename = f'energy_phelix_power_futures_historie_{year}.xls'
        cols = ['Delivery\nPeriod', 'Settlement\nPrice']
        informat = '%b-%Y'
    # elif 2005 < int(year) <= 2014:
    #     contract = ('F0BQ', 'F0PQ')
    #     header = 1
    #     filename = f'energy_german_power_futures_historie_{year}.xls'
    #     cols = ['Delivery\nPeriod', 'Settlement\nPrice']
    elif 2014 < int(year) <= 2018:
        contract = ('F1BQ', 'F1PQ')
        header = 2
        filename = f'PhelixPowerFuturesHistory_{year}.xls'
        cols = ['Delivery Period', 'Settlement\nPrice']
        informat = '%Y.%m'
    else:
        contract = ('DEBQ', 'DEPQ')
        header = 2
        filename = f'PowerFutureHistory_Phelix-DE_{year}.xlsx'
        cols = ['Maturity', 'Settlement Price']
        informat = '%Y-%m'
    # import base and peak

    df = eex_import_xls(filename, contract[0], default_col_names, cols = cols, header = header, informat = informat)
    df2 = eex_import_xls(filename, contract[1], default_col_names, cols = cols, header = header, informat = informat)
    # merge (seems not very elegant)
    prices = df.set_index([df.index, df['Maturity']]).drop('Maturity', axis = 1).merge(df2.set_index([df2.index, df2['Maturity']]).drop('Maturity', axis = 1), left_index = True, right_index = True, suffixes = ('_base', '_peak'))
    prices = prices.reset_index(level = 1)
    # get next four quarters and average
    P = prices.groupby('Maturity').resample('1M').mean().reset_index(level = 0)
    # get the four next quarters (NB: prices are not listed if product not tradable anymore)
    P2 = P.sort_values(by = ['Trading Day', 'Maturity'])
    P2 = P2.groupby(P2.index).head(4)
    return P2

def base_peak(hours):
    ''' Das ist diese komische Base-Peak-Aufteilung, die undokumentiert ist.'''
    
    peak = 310.72 * m.exp(-0.0006*hours)
    return (100 - peak, peak)
    

def eex_aggregate_price(df, Hour_dict, prefix = 'P_'):
    result = df.copy()
    for k, v in Hour_dict.items():
        result[prefix + str(k)] = (v[0] * df['Settlement Price_base'] + v[1] * df['Settlement Price_peak'])/100
        
    
    return result
    

if __name__ == '__main__':
    config = cp.ConfigParser()
    config.sections()
    config.read('config.ini')
    
    EEX = {'host': config['sFTP']['server'], 'user': config['sFTP']['username'], 'passwd': config['sFTP']['password']}

    files = [{'r_path': config['Paths']['r_path'],
              'r_filename': config['Paths']['r_filename'],
              'l_path': config['Paths']['l_path']}]


    pm.eex.import_from_sftp(EEX, files)
    
    VIK = VIndex('Abgaben_Umlagen.xlsx', basename = './')
    
    VIK.index_CvP(VIK.base_peak_function)
    df = pd.concat([VIK.base, VIK.end], axis = 1)
    df.plot()
    
    # Calendars  
    Cal = Calendar_Years(basename = './', last_year = '2020', reload = True)
    
    
