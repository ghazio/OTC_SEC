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



def get_insiders(text, row):
    text = text.strip()
    insiders = pd.DataFrame(columns=['Name Of Officer/Director',
                                     'Affiliation with Company(eg, Officer Title/Director/Owner of more than 5 percent)',
                                     'Residential Address', 'Number of shares owned', 'Share type',
                                     'Ownership Percentage of Class Outstanding', 'Note'])

    if '5%' in text and '8)' in text:
        section = text[text.index('7'):text.index('8)')]
        section = text[text.index('5%'):]
    else:
        print(f"Returning -1 in third party getter. 7) {'7)' in text} 8) {'8)' in text}")
        return -1
    Insiders = re.findall(r"\n*(?=\n)", section)
    # print("HERE:",Insiders)

    return Insiders
