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




def get_Transfer_agents(text,row):
    #TODO?: Convert this into a separate function to separate out the errors caused by
    # get information for transfer agent.

    Transfer_agent = pd.DataFrame(columns=['Agent Name', 'Phone', 'Email', 'Address'] + list(row.columns))

    # if we have the section
    if '2) Security Information' in text and '3) Issuance History' in text:
        section = text[text.index('2) Security Information'):text.index('3) Issuance History')]
    else:
        # if we cannot find the information p
        print(
            f"Get Securities:-1 because 2) Security Information {'2) Security Information' in text} 3) Issuance History {'3) Issuance History' in text}")
        return -1
    subsection = section[section.index('Transfer Agent'):]
    name = re.search(r'(?<=Name *:).*(?=\n)', subsection)
    if name:
        name = name.group()

    else:
        name = ""

    firm = re.search(r'(?<=Firm *:).*(?=\n)', subsection)
    #Check if the firm name exists
    assert firm is None

    if firm and name == "":
        name = firm.group()


    phone = re.search(r'(?<=Phone *:).*(?=\n)', subsection)
    if phone:
        phone = phone.group()
    else:
        phone = ""

    email = re.search(r'(?<=Email *:).*(?=\n)', subsection)
    if email:
        email = email.group()
    else:
        email = ""

    address = re.search(r'(?<=Address *:).*(?=\n)', subsection)
    if address:
        address = address.group()
    else:
        address = ""
    #strip the information
    strip_table = {}
    for i in [" ","|","_","-"]:
        strip_table[ord(i)] = None
    #if all of them exist and are not
    if name != "" and phone != ""  and email != ""  and address != "" :
        results = [name, phone, email, address]
    else:
        #return error code -2 to show no valid selection
        return -2

    #replace t
    count_empty = 0
    for index,element in enumerate(results):
        if element != "":
            place_holder = element.translate(strip_table)
            #if address and place holder is not empty, do not update
            if index != 3 and index != 0 and place_holder != "":
                results[index] = place_holder

        if results[index] == "":
            count_empty += 1
    #if all are empty strings, return -3
    if count_empty == 4:
        return -3
    else:
        Transfer_agent.loc[0] = results + list(row.reset_index().loc[0])[1:]
        return Transfer_agent

