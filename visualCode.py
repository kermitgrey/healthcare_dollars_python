import pandas as pd
import numpy as np
from pandas import Series,DataFrame
import matplotlib.pyplot as plt
import matplotlib
import pickle
import time
import string

datadir = 'OpenPay/dataOut/'

# load payments data file

dfpivotpay = pd.read_pickle(datadir+'pivotpays-allyears.plk') 

# load dictionary of docid->full name
doc_dict = np.load(datadir+'doctor_dict.npy', allow_pickle=True).item()

# load dictionary of payorid->payor name
payor_dict = np.load(datadir+'payors_dict.npy', allow_pickle=True).item()

def createSummaryBar():
    
    fig, [ax1,ax2, ax3] = plt.subplots(1,3, figsize=(20,5))
    axes = [ax1, ax2, ax3]
    # plot the data of total payments by year
    dfpivotpay[[2016,2017,2018,2019]].sum().plot(kind='bar', ax=axes[0], stacked=True, \
                                      color=['#228b22','#cd2626','#1874cd','#f87f38'])
    axes[0].grid(True, axis='y', linestyle='-', linewidth=.1, color='b')
    axes[0].set_title('Total Payments By Year', fontdict={'fontsize':12})
    axes[0].set_ylabel('$ in 1B')
    axes[0].set_xlabel('')
    
    # plot the data of # doctors paid by yar
    counts = list()
    years = [2016,2017,2018,2019]
    for year in years :
        dftemp=dfpivotpay.groupby('docid')[year].sum()
        counts.append(dftemp[dftemp>0].count())
    
    sertemp = Series(counts, index=years)
    
    sertemp.plot(kind='bar', ax=axes[1], stacked=True, \
                color=['#228b22','#cd2626','#1874cd','#f87f38'])
    axes[1].grid(True, axis='y', linestyle='-', linewidth=.1, color='b')
    axes[1].set_title('# Doctors Paid By Year', fontdict={'fontsize':12})
    axes[1].set_ylabel('Count')
    axes[1].set_xlabel('')
    
    # plot the data of # companies making payments by year
    counts = list()
    years = [2016,2017,2018,2019]
    for year in years :
        dftemp = dfpivotpay.groupby('payorid')[year].sum()
        counts.append(dftemp[dftemp>0].count())

    sertemp = Series(counts, index=years)

    sertemp.plot(kind='bar', ax=axes[2], stacked=True, color=['#228b22','#cd2626','#1874cd','#f87f38'])
    axes[2].grid(True, axis='y', linestyle='-', linewidth=.1, color='b')
    axes[2].set_title('# Companies Making Payments', fontdict={'fontsize':12})
    axes[2].set_ylabel('Count')
    axes[2].set_xlabel('')
    
    return axes

def createDoctorCounts():
    '''
    Create chart breaking down counts for each year by range
    of aggregate values; (0,1k], (1k,10k], etc.
    '''
    def autolabel(totals, percnts, x_ticklist):
        '''
        Attach a text label at top of bar displaying percent of total.
        '''
        for bintot, pct, xtick in zip(totals, percnts, x_ticklist):
            axes.annotate('{:,.1f}%'.format(pct),
                        xy=(xtick-.4, bintot),
                        xytext=(18,10), 
                        textcoords="offset points",
                        ha='center', va='top')
            
    fig, [ax1,ax2,ax3,ax4] = plt.subplots(1,4, figsize=(20,5))
    axes = [ax1,ax2,ax3,ax4]
    
    # create bin ranges and labels
    binRange = [0, 1000, 10000,100000,1000000, 60000000]
    binLabels = ['(0,1k]', '(1k,10k]', '(10k,100k]', '(100k,1M]', '(1M,60M]']

    barcolors = {2016: '#228b22',
                 2017: '#cd2626',
                 2018: '#1874cd',
                 2019: '#f87f38'}
    
    # loop thru all years and plot the data
    for year, axes in zip([2016,2017,2018,2019], axes):     

        # create temp df of data for specified year
        dftemp=dfpivotpay.groupby('docid')[year].sum()
        dftemp = dftemp[dftemp>0]

        # use pd.cut to bin physician pays and get a value_count
        allBin = pd.cut(dftemp, binRange, labels=binLabels)
        tempBins = pd.value_counts(allBin)
        totals = tempBins[tempBins>0].sum()
        totpct = [x/totals*100 for x in tempBins[tempBins>0]]

        # import table and create table of data using tempBins to display on chart
        from pandas.plotting import table
        table(axes, tempBins[tempBins>0].sort_index(), loc='upper right', colWidths=[0.2])

        # plot the data
        axes.bar(np.arange(len(tempBins[tempBins>0])),tempBins[tempBins>0], color=barcolors[year])
        
        # alter ylabels based on year
        if year == 2016:      
            axes.set_ylabel('Count')
        else:
            axes.set_yticklabels([])
            axes.set_ylabel('')
        
        axes.set_xticklabels(binLabels, rotation=60)
        axes.set_xticks([x for x in range(5)])
        axes.set_xlabel('Pay Bin')
        axes.set_title('Physician Counts - '+str(year))
        
        x_ticklist = axes.get_xticks()

        autolabel(tempBins[tempBins>0], totpct, x_ticklist)
        
    return axes

def createDoctorDollars():
    '''
    Create chart breaking down payment counts for each year by range
    of aggregate values; (0,1k], (1k,10k], etc.
    '''
    
    fig, [ax1,ax2,ax3,ax4] = plt.subplots(1,4, figsize=(20,5))
    axes = [ax1,ax2,ax3,ax4]
    
    # create bin ranges and labels
    binRange = [0, 1000, 10000,60000000]
    binLabels = ['(0,1k]', '(1k,10k]', '(10k,60M]']

    barcolors = {2016: '#228b22',
                 2017: '#cd2626',
                 2018: '#1874cd',
                 2019: '#f87f38'}
    
    # loop thru all years and plot the data
    for year, axes in zip([2016,2017,2018,2019], axes):
        
        # create temp df of data for specified year
        dftemp=dfpivotpay.groupby('docid')[year].sum()
        dftemp = dftemp[dftemp>0]
        
        # use pd.cut to bin physician pays and get a value_count
        allBin = pd.cut(dftemp, binRange, labels=binLabels)
        tempBins = dftemp.groupby(allBin).sum()
        tempBins = tempBins[tempBins>0]

        # plot the data
        axes.bar(np.arange(len(tempBins)),tempBins, color=barcolors[year])      
        axes.set_xticklabels(binLabels, rotation=60)
        axes.set_xticks([x for x in range(3)])
        axes.set_xlabel('Pay Bin')
        axes.set_ylabel('$')
        axes.set_title('Total Dollars - '+str(year))
        
    return axes


def createDoctorBar(topN=10):
    '''
    Create bar chart of total payments by payor for a given slice
    of the data.
    '''
    # set figsize based on value of slice.
    if topN == 10:
        rowval = 5
    elif topN < 20:
        rowval = 7
    elif topN < 30:
        rowval = 9
    else:
        rowval = 12
        
    fig, axes = plt.subplots(1,1, figsize=(8,rowval))
    
    # gen payment totals by doctor, sort, then take a slice of the data
    dftemp = dfpivotpay.groupby('docid').sum().sort_values(by='total', ascending=True)

    dftemp = dftemp[[2016,2017,2018,2019]][-topN:]
    
    tempindx = [doc_dict[x] for x in dftemp.index]
    
    dftemp.index = tempindx
    
    # plot the data
    dftemp[[2016,2017,2018,2019]].plot(kind='barh', ax=axes, stacked=True, \
                             color=['#228b22','#cd2626','#1874cd','#f87f38'])
    axes.grid(True, axis='x', linestyle='-', linewidth=.1, color='b')
    axes.set_title('Total Payments By Doctor - Top '+str(topN), fontdict={'fontsize':12})
    axes.set_xlabel('$ in 100M')
    axes.set_ylabel('')
        
    return axes

def createStateBar(topN=10):
    '''
    Create bar chart of total payments by year and state for
    a given number of states based on value of topN.
    '''
    rownum = 8
    
    if topN <10:
        rownum = 8
    elif topN < 20:
        rownum = 10
    elif topN < 40:
        rownum = 12
    else:
        rownum = 14
        
    fig, axes = plt.subplots(1,1, figsize=(rownum,5))

    # gen total payments by state and sort.
    dftemp = dfpivotpay.groupby('state')[[2016,2017,2018,2019,'total']].sum().sort_values(by='total', ascending=False)

    # plot the data
    dftemp[[2016,2017,2018,2019]][0:topN].plot(kind='bar', ax=axes, stacked=True, \
                                      color=['#228b22','#cd2626','#1874cd','#f87f38'])
    axes.grid(True, axis='y', linestyle='-', linewidth=.1, color='b')
    axes.set_title('Total Payments By State - Top '+str(topN), fontdict={'fontsize':12})
    axes.set_ylabel('$ in 1B')
    axes.set_xlabel('')
    # change fontsize of x-tick labels
    x = plt.gca().xaxis
    for item in x.get_ticklabels():
        item.set_rotation(0)
        
    return axes

def createSpecialtyBar(topN=10):
    '''
    Create bar chart of total payments by year and doctor specialty
    '''
    # gen figsize value based on slice value
    if topN == 10:
        rowval = 5
    elif topN < 20:
        rowval = 7
    elif topN < 30:
        rowval = 9
    else:
        rowval = 12
    
    fig, axes = plt.subplots(1,1, figsize=(15,rowval))
    
    def autolabel(totals, percnts, y_ticklist):
        '''
        Attach a text label at top of bar displaying percent of total.
        '''
        for cattot, pct, ytick in zip(totals, percnts, y_ticklist):
            axes.annotate('{:,.1f}%'.format(pct),
                        xy=(cattot, ytick+.15),
                        xytext=(18,0), 
                        textcoords="offset points",
                        ha='center', va='top')

    # gen the pay data
    dftemp = dfpivotpay.groupby('specialty').sum().sort_values(by='total', ascending=True)
    
    sumtotals = dftemp['total'].sum()  # gen total of all specialties
    
    dftemp = dftemp[-topN:]    # extract only top N specialties
    totals = dftemp['total'].values  # get total values for the top N.

    
    percnts = [x/sumtotals*100 for x in totals]  # create % of total for each specialty in Top N.
    
    # plot the data
    dftemp[[2016,2017,2018,2019]].plot(kind='barh', ax=axes, stacked=True, \
                             color=['#228b22','#cd2626','#1874cd','#f87f38'])
    axes.grid(True, axis='x', linestyle='-', linewidth=.1)
    axes.set_title('Total Payments By Specialty', fontdict={'fontsize':12})
    axes.set_xlabel('$ in 1B')
    axes.set_ylabel('')
    axes.set_yticks(np.arange(len(totals)))
    axes.set_yticklabels(dftemp.index)
    
    y_ticklist = axes.get_yticks()
    
    autolabel(totals, percnts, y_ticklist)
        
    return axes

def createCategoryBar():
    '''
    Create bar chart of total payments by year and category.
    '''
    
    def autolabel(totals, percnts, y_ticklist):
        '''
        Attach a text label at top of bar displaying percent of total.
        '''
        for cattot, pct, ytick in zip(totals, percnts, y_ticklist):
            axes.annotate('{:,.1f}%'.format(pct),
                        xy=(cattot, ytick+.15),
                        xytext=(18,0), 
                        textcoords="offset points",
                        ha='center', va='top')

    
    fig, axes = plt.subplots(1,1, figsize=(12,6))

    # gen the pay data
    dftemp = dfpivotpay.groupby('paytype').sum().sort_values(by='total', ascending=True)
    
    totals = dftemp['total'].values
    sumtotals = dftemp['total'].sum()
    
    percnts = [x/sumtotals*100 for x in totals]  # create % of total for each category
    
    # plot the data
    dftemp[[2016,2017,2018,2019]].plot(kind='barh', ax=axes, stacked=True, \
                             color=['#228b22','#cd2626','#1874cd','#f87f38'])
    axes.grid(True, axis='x', linestyle='-', linewidth=.1)
    axes.set_title('Total Payments By Category', fontdict={'fontsize':12})
    axes.set_xlabel('$ in 1B')
    axes.set_ylabel('')
    axes.set_yticks(np.arange(len(totals)))
    axes.set_yticklabels(dftemp.index)
    
    y_ticklist = axes.get_yticks()
    
    autolabel(totals, percnts, y_ticklist)
 
    return axes

def createPayorCounts():
    '''
    Create chart breaking down counts for each year by range
    of aggregate values; (0,100k], (100k,1M], etc.
    '''
    def autolabel(totals, percnts, x_ticklist):
        '''
        Attach a text label at top of bar displaying percent of total.
        '''
        for bintot, pct, xtick in zip(totals, percnts, x_ticklist):
            axes.annotate('{:,.1f}%'.format(pct),
                        xy=(xtick-.4, bintot),
                        xytext=(18,10), 
                        textcoords="offset points",
                        ha='center', va='top')

    fig, [ax1,ax2,ax3,ax4] = plt.subplots(1,4, figsize=(20,5))
    axes = [ax1,ax2,ax3,ax4]

    # create bin ranges and labels
    binRange = [0, 100000, 1000000, 20000000, 50000000, 75000000, 100000000]
    binLabels = ['(0k,100k]', '(100k,1M]', '(1M,20M]',\
                     '(20M,50M]', '(50M,75M]', '(75M,100M]']

    barcolors = {2016: '#228b22',
                 2017: '#cd2626',
                 2018: '#1874cd',
                 2019: '#f87f38'}

    # loop thru all years and plot the data
    for year, axes in zip([2016,2017,2018,2019], axes):

        # create temp df of data for specified year
        dftemp=dfpivotpay.groupby('payorid')[year].sum()
        dftemp = dftemp[dftemp>0]

        # use pd.cut to bin company pays and get a value_count
        allBin = pd.cut(dftemp, binRange, labels=binLabels)
        tempBins = pd.value_counts(allBin)
        totals = tempBins[tempBins>0].sum()
        totpct = [x/totals*100 for x in tempBins[tempBins>0]]

        # import table and create table of data using tempBins to display on chart
        from pandas.plotting import table
        table(axes, tempBins[tempBins>0].sort_index(), loc='upper right', colWidths=[0.2])

        # plot the data
        axes.bar(np.arange(len(tempBins[tempBins>0])),tempBins[tempBins>0], color=barcolors[year])
        axes.set_xticklabels(binLabels, rotation=60)
        axes.set_xticks([x for x in range(len(tempBins[tempBins>0]))])
        axes.set_xlabel('Pay Bin')
        axes.set_ylabel('Count')
        axes.set_title('Company Counts - '+str(year))

        x_ticklist = axes.get_xticks()

        autolabel(tempBins[tempBins>0], totpct, x_ticklist)
        
    return axes



def createPayorDollars():
    '''
    Create chart breaking down payment counts for each year by range
    of aggregate values; (0,1k], (1k,10k], etc.
    '''
    
    fig, [ax1,ax2,ax3,ax4] = plt.subplots(1,4, figsize=(20,5))
    axes = [ax1,ax2,ax3,ax4]
    
    # create bin ranges and labels
    binRange = [0, 100000, 1000000, 100000000]
    binLabels = ['(0k,100k]', '(100k,1M]', '(1M,100M]']

    barcolors = {2016: '#228b22',
                 2017: '#cd2626',
                 2018: '#1874cd',
                 2019: '#f87f38'}
    
    # loop thru all years and plot the data
    for year, axes in zip([2016,2017,2018,2019], axes):
        
        # create temp df of data for specified year
        dftemp=dfpivotpay.groupby('payorid')[year].sum()
        dftemp = dftemp[dftemp>0]
        
        # use pd.cut to bin physician pays and get a value_count
        allBin = pd.cut(dftemp, binRange, labels=binLabels)
        tempBins = dftemp.groupby(allBin).sum()
        tempBins = tempBins[tempBins>0]
        
        # plot the data
        axes.bar(np.arange(len(tempBins[tempBins>0])),tempBins[tempBins>0], color=barcolors[year])
        axes.set_xticklabels(binLabels, rotation=60)
        axes.set_xticks([x for x in range(len(tempBins[tempBins>0]))])
        axes.set_xlabel('Pay Bin')
        axes.set_ylabel('$ Amount')
        axes.set_title('Total Dollars - '+str(year))
        
    return axes

def createPayorBar(topN=10):
    '''
    Create bar chart of total payments by payor for a given slice
    of the data.
    '''
    # set figsize based on value of slice.
    if topN == 10:
        rowval = 5
    elif topN < 20:
        rowval = 7
    elif topN < 30:
        rowval = 9
    else:
        rowval = 12
        
    fig, axes = plt.subplots(1,1, figsize=(8,rowval))
    
    # gen payment totals by payor, sort, then take a slice of the data
    dftemp = dfpivotpay.groupby('payorid').sum().sort_values(by='total', ascending=True)

    dftemp = dftemp[[2016,2017,2018,2019]][-topN:]
    
    tempindx = [payor_dict[x] for x in dftemp.index]
    
    dftemp.index = tempindx
    
    # plot the data
    dftemp[[2016,2017,2018,2019]].plot(kind='barh', ax=axes, stacked=True, \
                             color=['#228b22','#cd2626','#1874cd','#f87f38'])
    axes.grid(True, axis='x', linestyle='-', linewidth=.1, color='b')
    axes.set_title('Total Payments By Company - Top '+str(topN), fontdict={'fontsize':12})
    axes.set_xlabel('$ in 100M')
    axes.set_ylabel('')
        
    return axes
