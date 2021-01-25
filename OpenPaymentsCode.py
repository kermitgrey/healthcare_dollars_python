import pandas as pd
import numpy as np
from pandas import Series,DataFrame
import pickle
import time
import string
import os.path
import dask
import dask.dataframe as dd


# create dictionary of state codes=>statename

states = {'OH': 'Ohio', 'KY': 'Kentucky', 'NV': 'Nevada', 
      'WY': 'Wyoming', 'AL': 'Alabama', 'MD': 'Maryland', 
      'AK': 'Alaska', 'UT': 'Utah', 'OR': 'Oregon', 'MT': 'Montana', 
      'IL': 'Illinois', 'TN': 'Tennessee', 'DC': 'District of Columbia', 
      'VT': 'Vermont', 'ID': 'Idaho', 'AR': 'Arkansas', 'ME': 'Maine', 
      'WA': 'Washington', 'HI': 'Hawaii', 'WI': 'Wisconsin', 'MI': 'Michigan', 
      'IN': 'Indiana', 'NJ': 'New Jersey', 'AZ': 'Arizona', 
      'MS': 'Mississippi', 'PR': 'Puerto Rico', 'NC': 'North Carolina', 
      'TX': 'Texas', 'SD': 'South Dakota', 
      'IA': 'Iowa', 'MO': 'Missouri', 'CT': 'Connecticut', 'WV': 'West Virginia', 
      'SC': 'South Carolina', 'LA': 'Louisiana', 'KS': 'Kansas', 'NY': 'New York', 
      'NE': 'Nebraska', 'OK': 'Oklahoma', 'FL': 'Florida', 'CA': 'California', 
      'CO': 'Colorado', 'PA': 'Pennsylvania', 'DE': 'Delaware', 'NM': 'New Mexico',
      'RI': 'Rhode Island', 'MN': 'Minnesota', 
      'NH': 'New Hampshire', 'MA': 'Massachusetts', 'GA': 'Georgia', 
      'ND': 'North Dakota', 'VA': 'Virginia'}


def generateDoctors():
    '''
    Load cms physician supplemental file to build all
    physician information including name, address, and
    specialty for all USA domiciled individuals.
    Utilize the nucc_taxonomy file to obtain the practice
    specialty/subspecialty for each doctor.  Additionally,
    prepare a list of all the physician identification number
    (docid) for use in downstream processing.
    Write all information out to file.
    '''
    dirpath = 'OpenPay/'

    # read new file
    dfdocs = pd.read_csv(dirpath+'OP_PH_PRFL_SPLMTL_P06302020.csv',
                        low_memory=False)

    # change case of column names
    dfdocs.columns = [x.lower() for x in dfdocs.columns]

    # extract only doctors with a US state-code residence.
    dfdocs = dfdocs[dfdocs.physician_profile_state.isin(list(states.keys()))]

    # convert physician_profile_id to string
    dfdocs['physician_profile_id'] = dfdocs['physician_profile_id'].astype(str)

    # Generate the Doctors first, middle, and last names by choosing from the primary name column
    # or the alternate name column if primary name column is blank.
     # First Name
    yesfirst = dfdocs[~dfdocs.physician_profile_first_name.isna()].index # rec index with a primary firstname
    nofirst = dfdocs[dfdocs.physician_profile_first_name.isna()].index # rec index with no primary firstname
    # rec index w/ no first name but has alternate first name
    altfirst = dfdocs.loc[nofirst,'physician_profile_alternate_first_name'][~dfdocs.physician_profile_alternate_first_name.isna()].index

    # populate new column with the first-name
    dfdocs.loc[yesfirst,'firstname'] = dfdocs.loc[yesfirst,'physician_profile_first_name'] # use standard
    dfdocs.loc[altfirst,'firstname'] = dfdocs.loc[altfirst,'physician_profile_alternate_first_name'] # use alternate

    # assign label to those with no name, else apply capwords to name
    dfdocs['firstname'] = dfdocs['firstname'].map(lambda x: 'No First-Name' if pd.isna(x) else \
                                                  (string.capwords(x, sep='-') if '-' in x else string.capwords(x)))

     # Middle Name
    yesmiddle = dfdocs[~dfdocs.physician_profile_middle_name.isna()].index # rec index with a primary middle name
    nomiddle = dfdocs[dfdocs.physician_profile_middle_name.isna()].index # rec index with no primary middle name
    # rec index w/ no primary middle name but has alternate middle name
    altmiddle = dfdocs.loc[nomiddle,'physician_profile_alternate_first_name'][~dfdocs.physician_profile_alternate_middle_name.isna()].index

    # populate new column with the middle name
    dfdocs.loc[yesmiddle,'middlename'] = dfdocs.loc[yesmiddle,'physician_profile_middle_name'] # use standard
    dfdocs.loc[altmiddle,'middlename'] = dfdocs.loc[altmiddle,'physician_profile_alternate_middle_name'] # use alternate

    # assign NaN to those with no middle name, else apply capwords to name
    dfdocs['middlename'] = dfdocs['middlename'].map(lambda x: np.NaN if pd.isna(x) else \
                                                    (string.capwords(x, sep='-') if '-' in x else string.capwords(x)))

     # Last Name
    yeslast = dfdocs[~dfdocs.physician_profile_last_name.isna()].index # rec index with a primary last name
    nolast = dfdocs[dfdocs.physician_profile_last_name.isna()].index # rec index with no primary last name
    # rec index w/ no primary last name but has an alternate last name
    altlast = dfdocs.loc[nolast,'physician_profile_alternate_last_name'][~dfdocs.physician_profile_alternate_last_name.isna()].index

    # populate new column with the last name
    dfdocs.loc[yeslast,'lastname'] = dfdocs.loc[yeslast,'physician_profile_last_name'] # use standard
    dfdocs.loc[altlast,'lastname'] = dfdocs.loc[altlast,'physician_profile_alternate_last_name'] # use alternate

    # assign label to those with no last name, else apply capwords to name
    dfdocs['lastname'] = dfdocs['lastname'].map(lambda x: 'No Last-Name' if pd.isna(x) else \
                                                (string.capwords(x, sep='-') if '-' in x else string.capwords(x)))

    # create full name for doctor record; only display middlename if it exists.
    dfdocs['fullname'] = dfdocs['lastname']+', '+dfdocs['firstname']+dfdocs['middlename'].map(lambda x: '' if pd.isna(x) else ' '+x)

    # create single taxonomy code for each doctor; if no taxonomy code exists in column 1, columns 2-5 are set to NaN.
    # Taxonomy code represents the specialty/subspecialty of a physician
    yesTaxon = dfdocs[~dfdocs.physician_profile_ops_taxonomy_1.isna()].index # recs index with a taxonomy code
    noTaxon = dfdocs[dfdocs.physician_profile_ops_taxonomy_1.isna()].index  # recs index with no taxonomy code

    # populate new taxonomy column for each
    dfdocs.loc[yesTaxon, 'taxonomy'] = dfdocs.loc[yesTaxon, 'physician_profile_ops_taxonomy_1']
    dfdocs.loc[noTaxon, 'taxonomy'] = 'none-provided'

    # update city name for each doctor using capwords
    dfdocs['city'] = dfdocs['physician_profile_city'].map(lambda x: string.capwords(x, sep='-') if '-' in x else string.capwords(x))

    # load taxonomy file
    dftaxon = pd.read_csv(dirpath+'nucc_taxonomy_201.csv')

    # merge taxonomy + dfdocs to get doctor specialization info    
    dfdocs = pd.merge(dfdocs, dftaxon[['Code', 'Classification', 'Specialization']],\
                         left_on='taxonomy', right_on='Code', how='left')

    # create a column consisting of the combination of Class & Specialization
    dfdocs['fullspecialty'] = dfdocs['Classification']+dfdocs['Specialization'].\
                                    map(lambda x: '' if pd.isna(x) else ' > '+x)

    # write out original file
    dfdocs.to_pickle(dirpath+'dataOut/original_doctors.plk')

    # create subset of original file and rename columns
    dfdocs = dfdocs[['physician_profile_id', 'firstname','middlename','lastname','fullname',\
                    'city','physician_profile_state','physician_profile_zipcode',\
                    'Classification', 'Specialization', 'fullspecialty']]

    dfdocs.rename(columns={'physician_profile_id': 'docid',
                          'physician_profile_state': 'state',
                           'physician_profile_zipcode': 'zipcode',
                          'Classification': 'specialty',
                          'Specialization': 'subspecialty'},\
                 inplace=True)

    # save all docids to a list
    np.save(dirpath+'dataOut/doctorlist', list(dfdocs.docid))

    # create dict of docid->fullname
    doc_dict = dict()
    for rec in dfdocs[['docid','fullname']].values:
        doc_dict[rec[0]] = rec[1]

    # write out dict to file
    np.save(dirpath+'dataOut/doctor_dict.npy', doc_dict)

    # write full doctor record out to file.
    dfdocs.to_pickle(dirpath+'dataOut/doctors.plk')
        
        
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
    
