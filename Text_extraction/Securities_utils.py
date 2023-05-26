"""
Filename: text_extraction.py
Purpose:       This file implements the text extraction process of the pipeline.
Functionality: This file provides functions for text extraction, including
               tasks like reading input files, preprocessing text, applying
               extraction techniques, and generating extracted text output.
Usage:         Import and invoke the relevant functions from this file in the desired
               module or script within the pipeline.
Dependencies: Install the required package dependencies by running
              "pip3 install -r requirements.txt" to ensure proper functionality.
              The requirements.txt file contains the necessary dependencies.
"""


import os
import pandas as pd
import numpy as np
import os
import regex as re
import time
from collections import Counter
#TODO: extraction part to this
#OUTPUT: List of extracted_text

"""
First Attempt
 
  0       26 | 26 | 26

 
-1     2430 | 2430 | same
 1      869 |  902 | same
-2      169 | 143  | same
-3       16 | 169  | same 
-20     176 | 16   | same

Second attempt:
(1214, 16) for total of (1214, 3) files , 622 year-ids, and 307 unique IDs done for Securities Done
-1     2430
 1      902
-3      169
-2      143
 0       26
-20      16

"""

def securities_extractor(subsection,Additional_class_flag = False):
    # First, we only extract the information we can get from the stated values
    # has issues with additional sometimes being captialized,
    # maybe use regex to fix it or change all to lower case (Done)
    #IDEA for standar
    dates_tracker = {}

    count = 0
    Trading = re.search(r"(?<=[Tt]+rading symbol.*:).*(?=\n)", subsection)
    if Trading:
        Trading = Trading.group()

    else:
        count += 1
        Trading = np.nan

    Title = re.search(r"(?<=[Ee]+xact title and class of securities outstanding.*:).*(?=\n)", subsection)
    if Title:
        Title = re.sub(r'\W+', '', Title.group())
    else:
        count += 1
        Title = np.nan

    cusp = re.search(r"(?<=(CUSIP|cusip).*:).*(?=\n)", subsection)
    if cusp:
        cusp = re.sub(r'\W+', '', cusp.group())
    else:
        count += 1
        cusp = np.nan
    par_value = re.search(r"(?<=[Pr]+ar *or *stated *value.*:).*(?=\n)", subsection)
    if par_value:
        par_value = par_value.group()
    else:
        count += 1
        par_value = np.nan

    #(?=(as *of *date)|('\n'))
    authorized_shares = re.search(r"(?<=[Tt]+otal *shares *authorized.*:).*(?='\n')", subsection.lower())
    print(authorized_shares)
    if authorized_shares:
        authorized_shares = authorized_shares.group()
        cut_off = re.search("as *of *",authorized_shares)
        if cut_off:
            authorized_shares = authorized_shares[:cut_off.start()]
        dates_tracker[0] = True
    else:
        count += 1
        dates_tracker[0] = False
        authorized_shares = np.nan


    outstanding_shares = re.search(r"(?<=[Tt]+otal *shares *outstanding *:).*(?=\n)", subsection)
    if outstanding_shares:
        outstanding_shares = outstanding_shares.group()
        cut_off = re.search("as *of *", outstanding_shares)
        if cut_off:
            outstanding_shares = outstanding_shares[:cut_off.start()]
        dates_tracker[1] = True
    else:
        count += 1
        dates_tracker[1] = False
        outstanding_shares = np.nan

    shares_in_public = re.search(r"(?<=[Nn]+umber of shares in the Public Float.*:).*(?=\n)", subsection)
    if shares_in_public:
        shares_in_public = shares_in_public.group()
        cut_off = re.search("as *of *", shares_in_public)
        if cut_off:
            shares_in_public = shares_in_public[:cut_off.start()]
        dates_tracker[2] = True
    else:
        count += 1
        dates_tracker[2] = False
        shares_in_public = np.nan

    shareholders_record = re.search(r"(?<=[Tt]+otal *number *of *shareholders *of *record.*:).*(?=\n)",
                                    subsection)
    if shareholders_record:
        shareholders_record = shareholders_record.group()
        cut_off = re.search("as *of *", shareholders_record)
        if cut_off:
            shareholders_record = shareholders_record[:cut_off.start()]
        dates_tracker[3] = True

    else:
        count += 1
        dates_tracker[3] = False
        shareholders_record = np.nan

    #if more than half are nans, then return error
    try:
        assert count < 4
    except:
        return -20

    dates = re.findall("(?<=[Aa]s *of *date *:).*(?=\n)", subsection)
    dates_orig = dates.copy()

    if len(list(set(dates_tracker.values()))) == 1 and dates[0] == False:
        results = re.findall('(?<=\n)[0-9a-zA-z]*(?=as *of *)',subsection)
        authorized_shares   = results[0]
        outstanding_shares  = results[1]
        shares_in_public    = results[2]
        shareholders_record = results[3]

    #if more than half are nans, then return error
    try:
        assert count < 4
    except:
        return -20
    #if we have less than four values and it is not additional class
    if len(dates) != len(dates_tracker.keys()) and Additional_class_flag == False:

        for index,exists in enumerate(dates_tracker.keys()):
            #if the relevent information for the date does not exist, add "None" until information is filled
            if dates_tracker[exists] == False and len(dates) < len(dates_tracker.keys()):
                dates.append(np.nan)
        print(subsection)
        print(dates,dates_tracker)
        #Some of the really unstructured ones do not mention
        #Additional information correctly.
    try:
        returned_values = [Additional_class_flag, Trading, Title, cusp, par_value,
     authorized_shares, dates[0], outstanding_shares, dates[1],
     shares_in_public, dates[2], shareholders_record,
     dates[3]]
        if len(dates_orig) != len(dates_tracker.keys()) or np.nan in dates:
            #return error code -2 for metadata to inform that we have some
            #good information
            #this one has a higher degree of accuracy in my opinion
            return [returned_values,-2]
    except:
        #I am not sure why these ones are not being read correctly,
        #Definetely worth investigating
        while True:
            try:
                returned_values = returned_values = [Additional_class_flag, Trading, Title, cusp, par_value,
     authorized_shares, dates[0], outstanding_shares, dates[1],
     shares_in_public, dates[2], shareholders_record,
     dates[3]]
                return [returned_values,-3]
            except:
                dates.append(np.nan)

    return [returned_values,1]


def get_securities(text, row):
    text = text.strip()
    # Create the basic dataframes to return
    Securities = pd.DataFrame(
        columns=['Other Flag', 'Trading symbol', 'Exact title and class of securities outstanding', 'CUSIP',
                 'Par/stated value', 'Total shares authorized', 'Authorized Date', 'Total shares oustanding',
                 'Outstanding Date', 'Number of shares in the Public Float', ' Public float Date',
                 'Total number of shareholders of Record', 'Shareholders Date'] + list(row.columns))
    Transfer_agent = pd.DataFrame(columns=['Agent Name', 'Phone', 'Email', 'Address'] + list(row.columns))

    # if we have the section
    if '2) Security Information' in text and '3) Issuance History' in text:
        section = text[text.index('2) Security Information'):text.index('3) Issuance History')]
    else:
        # if we cannot find the information p
        print(
            f"Get Securities:-1 because 2) Security Information {'2) Security Information' in text} 3) Issuance History {'3) Issuance History' in text}")
        return -1

    Transfer_agent = re.search('(?<=[Tt]+ransfer).*(?=\n)',section)
    if Transfer_agent:
        Transfer_agent = Transfer_agent.start()


    end_index = re.search('(?<=[Aa]+dditional class).*(?=\n)',section)
    if end_index:
        start_index = 0
        #If transfer agent is at the top of the securities section, remove it
        if Transfer_agent < end_index.start():
            start_index = Transfer_agent
        subsection = section[start_index:end_index.start()]
    else:
        subsection = section

    results = securities_extractor(subsection,False)
    error_code = None

    if isinstance(results, int):
        return results
    if isinstance(results,list):
        error_code = results[1]
        results = results[0]


    # add the relevent informatio here
    Securities.loc[Securities.shape[0]] = results + list(row.reset_index().loc[0])[1:]
    if error_code:
        return [Securities,error_code]
    else:
        return Securities


def clean_output(text_arr):
    print("Before",text_arr)
    for i,val in enumerate(text_arr):
        text_arr[i] = re.sub("|_","",val)

    text_arr_orig = []

    for i,val in enumerate(text_arr):
        if val != "":
            text_arr_orig.append(val)
    print("After",text_arr_orig)
    return text_arr_orig
def fill_max(arr,max_val):

    i = 0
    while len(arr) < max_val:
        if len(arr) != max_val:
            arr.append(np.nan)
    return arr


def get_additional_securities(text,row):

    text = text.strip()
    # Create the basic dataframes to return
    Securities = pd.DataFrame(
        columns=['Other Flag', 'Trading symbol', 'Exact title and class of securities outstanding', 'CUSIP',
                 'Par/stated value', 'Total shares authorized', 'Authorized Date', 'Total shares oustanding',
                 'Outstanding Date', 'Number of shares in the Public Float', ' Public float Date',
                 'Total number of shareholders of Record', 'Shareholders Date'] + list(row.columns))

    second_section = re.search("2 *\) *[Ss]+ecurity", text)
    third_section  = re.search("3 *\) *[Ii]+ssuance",text)

    start_index_section = re.search('(?<=[Aa]+dditional class).*(?=\n)', text)
    end_index_section = re.search('(?<=[Tt]+ransfer *[Aa]gent).*(?=\n)', text)
    if not end_index_section:
        end_index_section = third_section

    # if we have the section
    if start_index_section and end_index_section and second_section and third_section:
        section = text[start_index_section.start():end_index_section.end()]
    else:
        # if we cannot find the information p
        print(
            f"Get Additional Securities:-1 because Additional one: {start_index_section} -- {end_index_section} and {second_section} and {third_section}")
        return -1
    #if the section length is very small and there is no "None"
    if len(section) < 40 and 'none' in section.lower():
        #None seem to go in here
        return -2
    Trading = re.findall(r"(?<=[Tt]+rading *[Ss]+ymbol *:).*(?=\n)", section)
    Trading = clean_output(Trading)
    Title = re.findall(r"(?<=[Ee]+xact *title *and *class *of *securities outstanding *:).*(?=\n)", section)
    Title = clean_output(Title)
    cusp = re.findall(r"(?<=CUSIP *:).*(?=\n)", section)
    cusp = clean_output(cusp)
    par_value = re.findall(r"(?<=[Pp]+ar *or *stated value *:).*(?=\n)", section)
    par_value = clean_output(par_value)

    #if all are empty
    if len(Trading) == 0 and len(Title) == 0 and len(cusp) == 0 and len(par_value) == 0:
        # this shows there was no possible result
        return -3

    maximum_entries = max([len(Trading),len(Title),len(cusp), len(par_value)])
    Trading = fill_max(Trading,maximum_entries)
    Title   = fill_max(Title,maximum_entries)
    cusp    = fill_max(cusp,maximum_entries)
    par_value = fill_max(par_value,maximum_entries)

    authorized_shares  = re.findall(r"(?<=[Tt]+otal *shares *authorized *:).*(?=\n)", section)
    authorized_shares  = clean_output(authorized_shares)
    authorized_shares  = fill_max(authorized_shares,maximum_entries)
    print(authorized_shares)
    authorized_shares  = [(re.sub('[Aa]+s *of.*','',share)if share is not np.nan else share) for share in authorized_shares]

    outstanding_shares = re.findall(r"(?<=[Tt]+otal *shares *outstanding *:).*(?=\n)", section)
    #outstanding_shares = [re.sub(" ")]
    outstanding_shares = clean_output(outstanding_shares)
    outstanding_shares = fill_max(outstanding_shares,maximum_entries)
    outstanding_shares  = [(re.sub('[Aa]+s *of.*','',share)if share is not np.nan else share) for share in outstanding_shares]

    dates = re.findall(r"(?<=as *of *date *:).*(?=\n)", section)
    dates              = clean_output(dates)
    dates              =fill_max(dates,maximum_entries*2)
    #if len(authorized_shares)  != len(dates):
     #   return -4
    #return 4
    error_flag = False
    for i in range(len(Trading)):

        results =   [True, Trading[i], Title[i], cusp[i], par_value[i],
                                                   authorized_shares[i], dates[2 * i], outstanding_shares[i],
                                                   dates[i * 2 + 1], "None", "None", "None", "None"]
        Securities.loc[Securities.shape[0]] = results + list(row.reset_index().loc[0])[1:]
        #except:
           # error_flag = True
    #this error code tell us there was some issue with the final output of the code
    if error_flag: return [Securities,-5]

    return Securities

