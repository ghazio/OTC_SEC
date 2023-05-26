import os
import pandas as pd
import numpy as np
import os
import regex as re
import time
from collections import Counter
import sys
print(os.getcwd())

#These are utility functions for each class of data we are extracting
from Securities_utils import get_securities,get_additional_securities
from TPA_utils import get_third_party
from Transfer_agents_utils import get_Transfer_agents
from Financial_Statements_utils import get_Financial_Statements
from Insider_utils import get_insiders
DATA = os.getcwd() + "/Webscrapping/converted_text"
OUTPUT = os.getcwd()+"/../Data"




# This seems hard to extract from because the table is not being read properly.
# Instead, I would try to use different version


"""
Input: Filename
Output: information that was extracted
Error return codes: 
        1 -- File is pink disclosure guideline
        2 -- File did not have the relevent information about the Year 
"""


def get_info(file, row):
    # open the file
    file = open(os.getcwd() + f'/../Webscrapping/converted_text/{file}', "r")
    text_orig = file.read()
    text = text_orig.strip()
    # search for year Reporting
    year_reporting = re.search("(?<=Ending *:).*(?=\n)", text_orig)

    # if the string is empty or the function returned None type object, try again
    if year_reporting is None:
        year_reporting = re.search("(?<=Ended *:).*(?=\n)", text_orig)

    if year_reporting is not None:
        year_reporting = year_reporting.group()

        #if for some reason, it is longer than 20, do this:
        if len(year_reporting) > 20:
            year_reporting = year_reporting[0:21]

    if year_reporting is None:
        year_reporting = -1

    elif len(year_reporting) < 5:
        year_reporting = -1

    third_parties  = get_third_party(text_orig, row)
    trading_agents = get_Transfer_agents(text_orig,row)
    #First attempt (1358, 16), for (479, 2) ID - Submission Year, done for Securities Done
    securities     = get_securities(text_orig, row)
    additional_securities = get_additional_securities(text_orig,row)


    return [third_parties, insiders, securities,trading_agents, year_reporting,additional_securities]





if __name__ == "__main__":
    print("this is me,", os.getcwd(),os.listdir(os.getcwd()+"/../Webscrapping"))
    #Get the metadata
    metadata = pd.read_csv(os.getcwd()+"/../Webscrapping/metadata_textfiles.csv")
    print(metadata.columns)

    #rearranging the metadata
    del metadata["Reporting Year"]
    del metadata["Financial Statements Done"]
    del metadata["Trading Agents Done"]

    metadata["Transfer Agents Done"]      = 0
    metadata["Financial Statements Done"] = 0
    metadata["Additional Securities Done"] = 0
    metadata["Reporting Year"]            = 0

    print(metadata.columns)

    metadata['file'] = metadata["text_file"]

    # Metadata column for the Reporting year, dummy value is the year with the most revolutions in human history
    metadata_cols = list(metadata[["ID", "Submission Year", "Submission Year File Number","file","Link"]].columns)

    # Dataframe for the files that are causing an error
    Extraction_erring = pd.DataFrame(metadata[["ID", "Submission Year", "Submission Year File Number"]],
                                     columns=metadata_cols)
    Extraction_erring["Reason"] = "No Error"

    # Dataframe for the third_party_advisors
    third_party_advisors = pd.DataFrame(
        columns=(['Role', 'Name', 'Firm', 'Address_1', 'Address_2', 'Phone', 'Email'] + metadata_cols))
    insiders = pd.DataFrame(columns=['Name Of Officer/Director',
                                     'Affiliation with Company(eg, Officer Title/Director/Owner of more than 5 percent)',
                                     'Residential Address', 'Number of shares owned', 'Share type',
                                     'Ownership Percentage of Class Outstanding', 'Note'] + metadata_cols)
    Securities = pd.DataFrame(
        columns=['Other Flag', 'Trading symbol', 'Exact title and class of securities outstanding', 'CUSIP',
                 'Par/stated value', 'Total shares authorized', 'Authorized Date', 'Total shares oustanding',
                 'Outstanding Date', 'Number of shares in the Public Float', ' Public float Date',
                 'Total number of shareholders of Record', 'Shareholders Date'] + metadata_cols)
    Additional_Securities = pd.DataFrame(
        columns=['Other Flag', 'Trading symbol', 'Exact title and class of securities outstanding', 'CUSIP',
                 'Par/stated value', 'Total shares authorized', 'Authorized Date', 'Total shares oustanding',
                 'Outstanding Date', 'Number of shares in the Public Float', ' Public float Date',
                 'Total number of shareholders of Record', 'Shareholders Date'] + metadata_cols)
    Transfer_agents = pd.DataFrame(columns=['Agent Name', 'Phone', 'Email', 'Address'] + metadata_cols)
    # Sort the metadata and reindex it
    metadata = metadata.sort_values(["ID", "Submission Year", "Submission Year File Number"]).reset_index(drop=True)

    for i, row in metadata.iterrows():

        # if we did not have a file skip it,
        if row['text_file'] is np.nan:
            print(f"Skipping {row['text_file']} because nan")
            continue

        #if row['text_file'] != 'AAPT_2021_Annual_2.txt': continue
        #this is fixing an error in the Company Names
        metadata.at[i,"Company Name"] = re.search(r"(?<=\s\s\s\s).*(?=\nName *:)", row["Company Name"]).group()
        #assert isinstance(metadata.at[i, "Company Name"], str) == False
        try:
            #passes all assertions so far
            assert isinstance(metadata.at[i,"Company Name"] , str) and len(metadata.at[i,"Company Name"]) > 0
        except:
            print(isinstance(metadata.at[i,"Company Name"] , str),metadata.at[i,"Company Name"])
            exit()

        # Extract the information as relevent
        print(i,row['text_file'] )

        # Extract the information
        information = get_info(row['text_file'],
                               metadata.iloc[[i]][["ID", "Submission Year", "Submission Year File Number","file","Link"]])

        # if reported year was not extracted properly,
        if information[4] == -2:
            metadata.at[i, "Reporting Year/Period"] = np.nan
        else:
            metadata.at[i, "Reporting Year/Period"] = information[4]
        print("Reporting year",metadata.at[i,"Reporting Year/Period"])
        # if the Securities information has been extracted properly
            # add info to metadata and
        if isinstance(information[2], pd.DataFrame):
                Securities = pd.concat([Securities, information[2]], ignore_index=True)
                metadata.at[i, "Securities Done"] = 1
        #No information extracted, probably -1
        elif isinstance(information[2], int):
                metadata.at[i, "Securities Done"] = information[2]

        elif isinstance(information[2], list):
                if information[2][1] != 0: metadata.at[i,"Securities Done"] = information[2][1]
                Securities = pd.concat([Securities, information[2][0]], ignore_index=True)
        #These are additional securities information
        if isinstance(information[5], pd.DataFrame):
                Additional_Securities = pd.concat([Additional_Securities, information[5]], ignore_index=True)
                metadata.at[i, "Additional Securities Done"] = 1
        #if just an error code was returned,
        elif isinstance(information[5],int):
                metadata.at[i, "Additional Securities Done"] = information[5]

        #else-if the information was returned as a list
        elif isinstance(information[5], list):
                if information[5][1] != 0: metadata.at[i,"Additional Securities Done"] = information[5][1]
                Securities = pd.concat([Securities, information[5][0]], ignore_index=True)

        # if Transfer agent information was extracted properly
        if isinstance(information[3], pd.DataFrame) == True:
            Transfer_agents = pd.concat([Transfer_agents, information[3]], ignore_index=True)
            metadata.at[i, "Transfer Agents Done"] = 1
        #else record the error code
        elif isinstance(information[3],int) == True:
            metadata.at[i, "Transfer Agents Done"] = information[3]


        #if list, it is error -3 or -4
        if isinstance(information[0],list):
            metadata.at[i, "Third Party Providers Done"] = information[0][0]
            third_party_advisors = pd.concat([third_party_advisors, information[0][1]], ignore_index=True)

        #record the error if we returned an error code
        elif isinstance(information[0],int):
            metadata.at[i, "Third Party Providers Done"] = information[0]
        #elif we got correct return
        elif isinstance(information[0], pd.DataFrame) == True:
            metadata.at[i, "Third Party Providers Done"] = 1
            third_party_advisors = pd.concat([third_party_advisors, information[0]], ignore_index=True)
    print(f"{len(metadata['Reporting Year/Period'])} {len(metadata[metadata['Reporting Year/Period'] != -1]['Reporting Year/Period'])}")

    print(f"{len(metadata['Reporting Year/Period'].unique())} {len(metadata[metadata['Reporting Year/Period'] != -1]['Reporting Year/Period'].unique())}")
    print(f"{Transfer_agents.shape} for {Transfer_agents[['ID','Submission Year','Submission Year File Number']].drop_duplicates().shape[0]} files,"
          f" {Transfer_agents[['ID','Submission Year']].drop_duplicates().shape[0]} ID-Year pairs, {Transfer_agents[['ID']].drop_duplicates().shape[0]} ids,done for {metadata['Transfer Agents Done'].value_counts()}")
    print(f"{Securities.shape} for total of {Securities[['ID', 'Submission Year', 'Submission Year File Number']].drop_duplicates().shape} files",
        f", {Securities[['ID', 'Submission Year']].drop_duplicates().shape[0]} year-ids, and {Securities[['ID']].drop_duplicates().shape[0]} unique IDs done for {metadata['Securities Done'].value_counts()}")
    print(
        f"{Additional_Securities.shape} for total of {Additional_Securities[['ID', 'Submission Year', 'Submission Year File Number']].drop_duplicates().shape} files",
        f", {Additional_Securities[['ID', 'Submission Year']].drop_duplicates().shape[0]} year-ids, and {Additional_Securities[['ID']].drop_duplicates().shape[0]} unique IDs done for {metadata['Additional Securities Done'].value_counts()}")

    print(f"{third_party_advisors.shape} for total of {third_party_advisors[['ID','Submission Year','Submission Year File Number']].drop_duplicates().shape} files",
          f", {third_party_advisors[['ID','Submission Year']].drop_duplicates().shape[0]} year-ids, and {third_party_advisors[['ID']].drop_duplicates().shape[0]} done for {metadata['Third Party Providers Done'].value_counts()}")

    Transfer_agents.to_csv("Transfer_agents_2nd.csv",index=False)
    Securities.to_csv("Securities_2nd.csv",index=False)
    Additional_Securities.to_csv("Additional_Securities_2nd.csv",index=False)
    third_party_advisors.to_csv("TPA_2nd.csv",index=False)
    metadata.to_csv("metadata_extracted_2nd.csv",index=False)
