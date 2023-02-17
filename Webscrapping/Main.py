import pandas as pd
import numpy as np
#import scrappy as sc
import selenium
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import pyautogui, os, pdb, time, re
import tabula as tb


#TODO:

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
    promoted_stocks = pd.read_csv('promoted_stocks.csv',low_memory=False,header=None)
    
    
    
    #575 Companies
    name_ids = pd.DataFrame(promoted_stocks[[2,4]])
    name_ids = name_ids.set_axis(['ID', 'Name'], axis='columns')
    All_reports = pd.DataFrame(columns = ['ID','Name','Report','Date','Year','Link'])
    options = Options()
    
    options.add_experimental_option('prefs', {"download.default_directory": "/Users/ghazi12/Documents/Work/SEC_folder/Webscrapping/downloaded_pdfs","download.prompt_for_download": False, "download.directory_upgrade": True,"plugins.always_open_pdf_externally": True
            })

    #open the driver
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()),options=options)
    #for each of the company in the data,
    Annual_reports_links = []
    
    for i, ID in enumerate(name_ids['ID']):
        ID = 'GEGP'
        print(f"Getting Links for {ID}")
        #open the website
        driver.get("https://www.otcmarkets.com/stock/"+ID+"/disclosure")
        driver.implicitly_wait(20)
        #Load the table completely
        #get all the rows
        pdb.set_trace()
        rows_of_disclosure = driver.find_elements(By.XPATH, "//table/tbody/tr/td[2]/span/span/a")
        years_of_disclosure = driver.find_elements(By.XPATH, "//table/tbody/tr/td[1]/span/span")[0:len(rows_of_disclosure)]
        #get all the table
        print("got all the elements, starting to loop")
        #Annual_report_links = [if "report" in row.text : row.get_attribute('href') for row in rows_of_disclosure]
        for j,row in enumerate(rows_of_disclosure):
            print(f"In row {j}")
            if "Report" in row.text:
                link = (row.get_attribute('href'))
                date = (years_of_disclosure[j].text)
                year = date[-4:]
                #need to add buttons to make the whole page visible
                All_reports.loc[len(All_reports.index)] = ([ID,name_ids['Name'][i],row.text,date,year,link])
        print(f"Done with {ID}")

    pdb.set_trace()
    for i,report in enumerate(Annual_reports_links):
        print(i)
        driver.get(report)
        time.sleep(3)
        pyautogui.press('enter')
        file = '/Users/ghazi12/Documents/Work/SEC_folder/Webscrapping/downloaded_pdfs/content.pdf'
        print(df)
        pdb.set_trace()
    driver.quit()

    
    #make the url coder
    
    
    
    
	
	
