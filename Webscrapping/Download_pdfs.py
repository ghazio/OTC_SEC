import pandas as pd
import numpy as np
#import scrappy as sc
import selenium
import urllib
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait as wait
from selenium.webdriver.chrome.options import Options
from fake_useragent import UserAgent
import pyautogui, os, pdb, time, re
import tabula as tb

to_extract = "Anuual Report"
BASE = os.getcwd()
DATA = os.getcwd() + '/downloaded_pdfs/'
print(DATA)
#If data does not exist, create a directory folder for storing the data
if os.path.exists(DATA) == False:
    os.makedirs(DATA)

#else, delete all the files that are already existing
#else:
print(os.listdir(DATA))

"""
This is the part of the pipeline that takes in a list of promoted stocks on the OTC market website that have opted to perform financial disclosures, and it webscraps the OTC-Markets website for the following data:

    Financial statements:
        -Third party providers(8?)
        -Company insiders table(7)
        -Security Information (2)
"""
#//*[@id="root"]/div/div[2]/div[3]/div[2]/div/div/div[5]/div/div[2]/div/div[2]/div[3]/div[1]/table






if __name__ == '__main__':

    #read in the Promoted Stocks file
    #All_reports = pd.read_csv(DATA+'metadata.csv',low_memory=False)
    #read in the Promoted Stocks file
    promoted_stocks = pd.read_csv('promoted_stocks.csv',low_memory=False,header=None)

    #575 Companies
    #name_ids = pd.DataFrame(promoted_stocks[[2,4]])
    #name_ids = name_ids.set_axis(['ID', 'Name'], axis='columns')
    All_reports = pd.read_csv(BASE+'/metadata.csv')#DataFrame(columns = ['ID','Name','Report','Date','Year', 'Version','Link'])
    #Novalues = pd.DataFrame(columns = ['ID','Name','Reason'])
    All_reports = All_reports.loc[:, ~All_reports.columns.str.contains('^Unnamed')]

    print("Starting to download")
    options = Options()
        
        #options.add_argument('--headless')
    ua = UserAgent()
    userAgent = ua.random
    options.add_argument(f'user-agent={userAgent}')
    options.add_experimental_option('prefs',
        {"download.default_directory": "/Users/ghazi12/Documents/Work/SEC_folder/Webscrapping/downloaded_pdfs",
            "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "plugins.always_open_pdf_externally": True
                                    })
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()),options=options)
    for index,row in All_reports.iterrows():
        #open the driver
        file = str(row[0]) + '_' + str(row[-3]) +'_Annual.pdf'
        #if we have not already downloaded the file,
        if row[-2] == 1 and os.path.isfile(DATA+file) == False:
            
            print(f"Downloading {file} {index}")
            """
            response = urllib.request.urlopen(row[-1])
            file = open(DATA+file, 'wb')
            file.write(response.read())
            file.close()
            """
            
            driver.get(row[-1])
            time.sleep(5)
            pyautogui.press('enter')
            time.sleep(5)
            os.rename(DATA+'content.pdf',DATA+file)
    driver.quit()
            
    #delete any files
    #
     #
     #that
    #for file in os.listdir:
     #   if 'content' in file:
      #      os.remove(file)
    #make the url coder
    
    
    
    
	
	
