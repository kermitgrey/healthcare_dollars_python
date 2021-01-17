import pandas as pd
import numpy as np
from pandas import Series,DataFrame
import pickle
import time
import string
import dask
import dask.dataframe as dd

# create dictionary of nature_of_payment->paytype id
# this dict is used to store a paytypeid instead of the
# var label when creating the summary payment record.
payid_dct = {'Honoraria': 0,
             'Consulting Fee': 1,
             'Food and Beverage': 2,
             'Travel and Lodging': 3,
             'Education': 4,
             'Gift': 5,
             'Current or prospective ownership or investment interest': 6,
             'Compensation for services other than consulting, including serving as faculty or as a speaker at a venue other than a continuing education program': 7,
             'Royalty or License': 8,
             'Entertainment': 9,
             'Compensation for serving as faculty or as a speaker for a non-accredited and noncertified continuing education program': 10,
             'Grant': 11,
             'Charitable Contribution': 12,
             'Compensation for serving as faculty or as a speaker for an accredited or certified continuing education program': 13}

def convertIds(var):
    '''
    Use fcn during exec of read_csv.  
    Receive a var (payor id or doc id) and convert to string.
    '''
    
    if var == '':
        return str(0)
    else:
        return str(var)

def convertChar(var):
    '''
    Use fcn during exec of read_csv.
    Receive a var and convert to lower case; if null assign
    default value of NaN
    '''
    
    if var == '':
        return np.NaN
    else:
        return var.lower()

def generatePayments():
    '''
    This fcn generates a file of the payment records by
    year and writes them out to file.  It also generates
    a df of unique payorname-payorid and write them out
    as well.  Both of the files are use later by a separate
    process.
    '''
    dirpath = 'OpenPay/'
    
    # create a list of the payment files that need to be processed.
    dirList = [dirpath+'PGYR16_P063020/OP_DTL_GNRL_PGYR2016_P06302020.csv',
               dirpath+'PGYR17_P063020/OP_DTL_GNRL_PGYR2017_P06302020.csv',
               dirpath+'PGYR18_P063020/OP_DTL_GNRL_PGYR2018_P06302020.csv',
               dirpath+'PGYR19_P063020/OP_DTL_GNRL_PGYR2019_P06302020.csv']

    years = ['2016', '2017','2018','2019']

    # load doctor id list from file.  This file is created by the generateDoctors process.
    doclist = np.load(dirpath+'dataOut/doctorlist.npy') 

    # set column information to load
    colNames = ['covered','docid','payorid','payorname','amount','paytype','relatedprod',\
               'prodname1', 'prodname2','prodname3', 'prodname4', 'prodtype5', 'progyr']
    selectCols = [1,5,26,27,30,34,47,51,56,61,66,71,73]

    # loop thru file-list and generate dfs for each set of payments by year
    for year, infile in zip(years, dirList):

        df = dd.read_csv(infile,
                        low_memory=False,
                        usecols=selectCols,
                        names=colNames, 
                        converters={'docid': convertIds,
                                    'payorid': convertIds,
                                    'prodname1': convertChar,
                                    'prodname2': convertChar,
                                    'prodname3': convertChar,
                                    'prodname4': convertChar,
                                    'prodname5': convertChar})

        # generate df using dask definition
        df = df[df.covered=='Covered Recipient Physician'].compute()
        
        # cast field as float.
        df['amount'] = df['amount'].astype(float)

        # create subset of df restricting payments only to doctors residing in USA
        # which are the ones present in this doclist
        df = df[df.docid.isin(doclist)]

        # create df of all unique payorid-payorname records
        # and write to file.  This file is later used in the process
        # generatePayors
        payorid = []
        payorname = []

        for rec in df[['payorid','payorname']].drop_duplicates().values:
            payorid.append(rec[0])
            payorname.append(rec[1])

        DataFrame({'payorid': payorid,
                  'payorname': payorname},
                 index=np.arange(len(payorid))).to_csv(dirpath+'dataOut/payors'+year+'.csv', index=False)

        # create new df column and insert the paytype-id for each record.
        # this converts the label to a number in the payment record.
        df.insert(3, 'paytypeid', df['paytype'].map(lambda x: payid_dct[x]))

        # drop unneeded columns
        df.drop(['covered','payorname','paytype'], axis=1, inplace=True)

        # write df data to file
        df.to_csv(dirpath+'dataOut/payments'+year+'.csv')
        
        # empty out the df
        df = DataFrame()
    
generatePayments()

