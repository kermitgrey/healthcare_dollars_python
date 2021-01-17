import pandas as pd
import numpy as np
from pandas import Series,DataFrame
import pickle
import time
import string
import os

# create a dictionary used to convert
# the payment record pay type id to 
# a new category label.
newpayid_dct = {0: 'Honoraria',
                 1: 'Consulting',
                 2: 'Food/Bev',
                 3: 'Travel/Lodge',
                 4: 'Education',
                 5: 'Gift',
                 6: 'Ownership/Invest',
                 7: 'Services',
                 8: 'Royalty',
                 9: 'Entertainment',
                 10: 'Faculty, non-accredited',
                 11: 'Grant',
                 12: 'Charity',
                 13: 'Faculty, accredited'}

def generatePivot():
    '''
    Fcn loads the payment files for each year previously created
    in the generatePayments process, combines them into one df
    and then pivots the df create a new df.  The new df contains 
    physician information, payor name, and pay type category name in lieu
    of the associated id for each of those vars.  Finally, the
    pivoted df is written to file.
    '''
   
    # create a list of the payment files in the directory 'data/'
    paymentfiles = list()  
    dirpath = 'OpenPay/dataOut/'      # dir where files exist.

    for file in os.listdir(dirpath):
        if file.startswith('payments') & file.endswith('.csv'):
            paymentfiles.append(dirpath+file)
 

    # loop thru the files list and build one df containing all
    # the records
    for indx, infile in enumerate(paymentfiles):
        if indx==0:
            df = pd.read_csv(infile, low_memory=False)
        else:
            dftemp = pd.read_csv(infile, low_memory=False)
            df = df.append(dftemp, ignore_index=True)

        # empty the temp df
        dftemp = DataFrame()

    # PIVOT the df 
    dfPivot = df.pivot_table(index = ['docid','payorid', 'paytypeid'], \
                         columns = df['progyr'], \
                         values='amount', aggfunc=np.sum )

    # fill any NaN values with 0
    dfPivot.fillna(0, inplace=True)

    # Create a summary column
    dfPivot['total'] = dfPivot.sum(axis=1)

    # Reset the index names 
    dfPivot.index.names = ['docid', 'payorid', 'paycategory']
    dfPivot.columns.names = ['']

    # reset the df index
    dfPivot.reset_index(inplace=True)

    # set columns to string datatype
    dfPivot['docid'] = dfPivot['docid'].astype(str)
    dfPivot['payorid'] = dfPivot['payorid'].astype(str)
    dfPivot['paycategory'] = dfPivot['paycategory'].astype(str)

    # load doctors and payors files created previously for use below.
    payors_dct = np.load(dirpath+'payors_dict.npy', allow_pickle=True).item() # load payors dict
    dfdocs = pd.read_pickle(dirpath+'doctors.plk') # load doctors df

    # insert payor-name column into df using payors dict
    dfPivot.insert(2,'payor', dfPivot['payorid'].map(lambda x: payors_dct[x]))

    # insert pay-category column into df using newpayid dict
    dfPivot.insert(4, 'paytype', dfPivot['paycategory'].map(lambda x: newpayid_dct[int(x)]))

    # merge pivot + doctors to get further doctor info
    dfPivot = pd.merge(dfPivot, dfdocs[['docid','lastname','fullname','state','specialty',\
                        'subspecialty','fullspecialty']], left_on='docid', right_on='docid')
  
    # pickle df
    dfPivot.to_pickle(dirpath+'pivotpays-allyears.plk')
    
generatePivot()
