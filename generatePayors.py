import pandas as pd
import numpy as np
from pandas import Series,DataFrame
import pickle
import time
import string
import os

def generatePayors():
    '''
    Fcn loads all the payor files written out in the 
    generate payments process and creates one df of
    all the records while eliminating duplicates.
    It also creates a dict of the resulting df.  Both
    are written out to file.
    '''
    # create a list of the payor files in the directory 'data/'
    payorfiles = list()  
    dirpath = 'OpenPay/dataOut/'      # dir where files exist.

    for file in os.listdir(dirpath):
        if file.startswith('payors') & file.endswith('.csv'):
            payorfiles.append(dirpath+file)

    # loop thru the files list and build one df containing all
    # the records of the payors

    for indx, infile in enumerate(payorfiles):
        if indx==0:
            dfpayors = pd.read_csv(infile, low_memory=False)
        else:
            dftemp = pd.read_csv(infile, low_memory=False)
            dfpayors = dfpayors.append(dftemp, ignore_index=True)

        #empty out the temp df
        dftemp = DataFrame()

    # assign index name to df
    dfpayors.index.names = ['payorid']

    # reset the index to payorid
    dfpayors = dfpayors.set_index('payorid')

    # remove duplicate payors based on index values (payorid).
    # Retain only the 1st duplicate record
    dfpayors = dfpayors[~dfpayors.index.duplicated(keep='first')]

    # apply capwords to payorname and add it to new column
    dfpayors.insert(1,'newname', dfpayors['payorname'].map(lambda x: string.capwords(x)))

    # create series using the reformatted name
    payors = dfpayors['newname']

    # create a dict of payors
    payors_dct = dict()
    for indx in payors.index:
        payors_dct[str(indx)] =  payors.loc[indx]
        
    # write out file
    payors.to_csv(dirpath+'allpayors.csv')
    np.save(dirpath+'payors_dict.npy', payors_dct)
    
generatePayors()
