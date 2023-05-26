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



def third_party_extractor(Role, subsection):
    Names = re.search(r"(?<=Name *:).*(?=\n)", subsection)

    if Names == None:
        Names = ""
    else:
        Names = Names.group()

    Firms = re.search(r"(?<=Firm *:).*(?=\n)", subsection)
    if Firms == None:
        Firms = ""
    else:
        Firms = Firms.group()

    # if both firms do not exist, then quit to the next role.
    #TODO: Wondering whether I can remove this and still try to collect other information

    if Firms == "" and Names == "":
        return -1

    #address 1
    ad_1 = re.search(r"(?<=Address 1 *:).*(?=\n)", subsection)
    if ad_1 == None:
        ad_1 = ""
    else:
        ad_1 = ad_1.group()

    #address 2
    ad_2 = re.search(r"(?<=Address 2 *:).*(?=\n)", subsection)
    if ad_2 == None:
        ad_2 = ""
    elif ad_2.group().replace(" ", "") == "":
        ad_2 = ""
    else:
        ad_2 = ad_2.group()
    #Phone
    Phone = re.search(r"(?<=Phone *:).*(?=\n)", subsection)
    if Phone == None:
        Phone = ""
    else:
        Phone = Phone.group()
    #email
    email = re.search(r"(?<=Email *:).*(?=\n)", subsection)
    if email == None:
        email = ""
    else:
        email = email.group()


    strip_table = {}
    #Table for translating the strings
    for i in [" ","|","_","-"]:
        strip_table[ord(i)] = None

    results = [Role, Names, Firms, ad_1, ad_2, Phone, email]
    count_empty = 0
    for index,element in enumerate(results):
        if element != "" and index > 0:
            try:
                place_holder = element.translate(strip_table)
            except:
                print(results)
                place_holder = element.translate(strip_table)
            if (index == 0 or  index > 4 ) and place_holder != "":
                results[index] = place_holder
        if results[index] == "" and index > 0:
            count_empty += 1
    #if all are empty
    if count_empty == 6:
        return -3
    #if all are not empty, return the row
    else:
       # print(count_empty,results)
        return results


def get_third_party(text, row):
    third_parties = pd.DataFrame(
        columns=['Role', 'Name', 'Firm', 'Address_1', 'Address_2', 'Phone', 'Email'] + list(row.columns))

    Roles = ["Securities Counsel", "Accountant or Auditor", "Investor Relations"]
    Roles_final = []
    key = re.search(r"(?<=\n *)[0-9]*(?= *\) *Third Party Providers)",text)
    #if no such key exists,
    if key is None:
        return -1

    try:
        key = int(key.group())
    except:
        print(f"\n \n key {key.group()}\n \n ")
    # figure out the section of the code
    if 'Third Party Providers' in text and str(key+1) + ")" in text:
        total_section = text[text.index('Third Party Providers'):text.index(str(key+1) +")")]
    else:
        print(
            f"Returning -1 in third party getter. Third Party Providers {(str(key+1) +')') in text} Issuer Certification {'Issuer Certification' in text}")
        return -2

    #If we have a distinct cut-off point available
    if "Other Service Providers" in total_section:
        section = total_section[:total_section.index("Other Service Providers")]
    #Otherwise,
    else:
        section =  total_section

    # Find all the possibele roles in this place
    Roles_final = re.finditer(r"(?<=((providers:)|(Email:)).*\n*).*(?=\n*Name:.*(?!Email:))",section)
    Roles = []
    indices = []

    #collect the relevent information
    for role in Roles_final:
        Roles.append(role.group())
        indices.append([role.start(),role.end()])

   # if len(Roles) >= 3:
    #    print(indices)
     #   print(Roles)

    i = 0

    # Get the number of accurances of 'Name' in the section
    while i < len(Roles):
        # if roles are in the text
        if Roles[i] in section:
            start_index = indices[i][0]
            if i != len(Roles) - 1:
                end_index   = indices[i+1][0]
            else:
                end_index   = len(section)
            subsection = section[start_index:end_index]
            extracted_text = third_party_extractor(Roles[i], subsection)
            if extracted_text == -1:
                print(Roles[i],"Is return nan for both names and firms")
                i += 1
                continue
            elif extracted_text == -2 or extracted_text == -3:
                print(Roles[i],"is nan or empty")
                i += 1
                continue
            third_parties.loc[third_parties.shape[0]] = extracted_text + list(
                row.reset_index().loc[0])[1:]
        else:
            print(Roles[i], "not found in document")
        i += 1

    #TODO: If the Roles exist, but it is different than Name:, etc pattern,
    #TODO: Check and develop function for that here


    #if there was data but it was not collected properly for one reason or another,
   # if third_parties.empty == True:
    #    return -2
    if "Other Service Providers" not in total_section:
        return third_parties

    # for other service providers
    subsection = total_section[total_section.index("Other Service Providers"):]

    # if there is information in this section
    Names = re.finditer(r"(?<=Name *:).*(?=\n)",subsection)
    Firms = re.finditer(r"(?<=Firm *:).*(?=\n)",subsection)
    services = re.finditer(r"(?<=Nature *of *Services *:).*(?=\n)", subsection)

    # If there is nothing available, we just return the third parties we have from previous part of the code
    if Names is None and Firms is None and services is None:
        print("In first if in TPP",Names,Firms,services)
        return third_parties

    #if nature of services is not given, but we have information for other 2 is present
    if services is None and Names is not None and Firms is not None:
        #checking if one of them is greater than the other: Seems to work for all of them, good news
        assert len([*Names]) == len([*Firms])
        #unpack the list
        Names_text = [i.group() for i in Names]
        Names_indices = [i.start() for i in Names]
        firm_text = [i.group() for i in Firms]
        firm_indices = [i.start() for i in Firms]
        length = len([*Names])

        for i in range(length):
            role = "OSP " + str(i)
            #if name is first, firm second
            if Names_indices[i] <= firm_indices[i]:
                start_index = Names_indices[i]
            else:
                start_index = firm_indices[i]

            #if it is not the last one
            if i != length - 1:
                #same logic as above
                if Names_indices[i+1] <= firm_indices[i+1]:
                    end_index = Names_indices[i + 1].start()
                else:
                    end_index = firm_indices[i+1].start()
            else:
                end_index = len(subsection)

            subsection = section[start_index:end_index]
            extracted_text = third_party_extractor(Roles[i], subsection)
            #if both names and firms are not mentioned, skip
            if extracted_text == -1:
                print(Roles[i],"Is return nan for both names and firms")
                i += 1
                continue
            elif extracted_text == -2 or extracted_text == -3:
                print(Roles[i],"is empty")
                i += 1
                continue
            third_parties.loc[third_parties.shape[0]] = extracted_text + list(
                row.reset_index().loc[0])[1:]

    #else if the company has services description and firms description but no Names description
    elif services is not None and Names is not None and Firms is None:
        assert len([*Names]) == len([*services])
        Names_text = [i.group() for i in Names]
        Names_indices = [i.start() for i in Names]
        Services = [i.group() for i in services]
        length = len([*Names])

        for i in range(length):
            role = Services[i]
            # if name is first
            start_index = Names_indices[i]
            if i != len(Roles) - 1:
                end_index = Names_indices[i + 1].start()

            else:
                end_index = len(subsection)

            subsection = section[start_index:end_index]
            extracted_text = third_party_extractor(Roles[i], subsection)
            #if both names and firms are not mentioned, skip
            if extracted_text == -1:
                print(Roles[i],"Is return nan for both names and firms")
                i += 1
                continue
            elif extracted_text == -2 or extracted_text == -3:
                print(Roles[i],"is empty")
                i += 1
                continue
            third_parties.loc[third_parties.shape[0]] = extracted_text + list(
                row.reset_index().loc[0])[1:]

    #else if the company has services description and firms description but no Names description
    elif services is not None and Names is None and Firms is not None:
        assert len([*Firms]) == len([*services])
        Firms_text = [i.group() for i in Firms]
        Firms_indices = [i.start() for i in Firms]
        Services = [i.group() for i in services]
        length = len([*Firms])

        for i in range(length):
            role = Services[i]
            start_index = Firms_indices[i]
            if i != len(Roles) - 1:
                end_index = Firms_indices[i + 1].start()
            else:
                end_index = len(subsection)

            subsection = section[start_index:end_index]
            extracted_text = third_party_extractor(Roles[i], subsection)
            # if both names and firms are not mentioned, skip
            if extracted_text == -1:
                print(Roles[i], "Is return nan for both names and firms")
                i += 1
                continue
            elif extracted_text == -2 or extracted_text == -3:
                print(Roles[i], "is empty")
                i += 1
                continue
            third_parties.loc[third_parties.shape[0]] = extracted_text + list(
                row.reset_index().loc[0])[1:]

    #if document has all three services
    elif services is not None and Names is not None and Firms is not None:
        #try to catch the relevent(I am try-catching this because it does not seem like there are a lot of files in
        # category)
        try:
            assert len([*Firms]) == len([*services]) and len([*Names]) == len([*services])
            Names_text = [i.group() for i in Names]
            Names_indices = [i.start() for i in Names]
            firm_text = [i.group() for i in Firms]
            firm_indices = [i.start() for i in Firms]
            Services = [i.group() for i in services]
            length = len([*Names])

            for i in range(length):
                role = Services[i]
                # if name is first, firm second
                if Names_indices[i] <= firm_indices[i]:
                    start_index = Names_indices[i]
                else:
                    start_index = firm_indices[i]

                # if it is not the last one
                if i != length - 1:
                    # same logic as above
                    if Names_indices[i + 1] <= firm_indices[i + 1]:
                        end_index = Names_indices[i + 1].start()
                    else:
                        end_index = firm_indices[i + 1].start()
                else:
                    end_index = len(subsection)

                subsection = section[start_index:end_index]
                extracted_text = third_party_extractor(Roles[i], subsection)
                # if both names and firms are not mentioned, skip
                if extracted_text == -1:
                    print(Roles[i], "Is return nan for both names and firms")
                    i += 1
                    continue
                elif extracted_text == -2 or extracted_text == -3:
                    print(Roles[i], "is empty")
                    i += 1
                    continue
                third_parties.loc[third_parties.shape[0]] = extracted_text + list(
                    row.reset_index().loc[0])[1:]
        except:
            return [-5,third_parties]

    if third_parties.empty == True:
        return -3

    return third_parties
