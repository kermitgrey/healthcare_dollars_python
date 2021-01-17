import pandas as pd
import numpy as np
from pandas import Series,DataFrame
import pickle
import time
import string
import os.path

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
        
        
generateDoctors()
