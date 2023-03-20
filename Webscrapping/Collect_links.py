import pandas as pd
import numpy as np
import multiprocessing as mp
import selenium,time
from selenium import webdriver
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from itertools import repeat
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait as wait
from selenium.webdriver.chrome.options import Options
from fake_useragent import UserAgent
from random import randint
import pyautogui, os, pdb, time, re
import tabula as tb

to_extract = "Annual Report"
BASE = os.getcwd()
DATA = os.getcwd() + '/downloaded_pdfs'
print(DATA)
#If data does not exist, create a directory folder for storing the data
if os.path.exists(DATA) == False:
    os.makedirs(DATA)

def get_links(ID,i,name_ids):
    All_reports = pd.DataFrame(columns = ['ID','Name','Report','Date','Year', 'Version','Link'])
    Novalues = pd.DataFrame(columns = ['ID','Name','Reason'])
    options = Options()
    options.add_argument('--headless')
    time.sleep(randint(10,20))
    ua = UserAgent()
    userAgent = ua.random
    options.add_argument(f'user-agent={userAgent}')
    #open the driver
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()),options=options)
    #ID = 'GEGP'
    print(f"Getting Links for {ID} index {i}")
    #open the website
    driver.get("https://www.otcmarkets.com/stock/"+ID+"/disclosure")
  
    driver.implicitly_wait(randint(1,10))
    ac = ActionChains(driver)
    last_row = driver.find_elements(By.XPATH,"//table[1]/tbody/tr[10]/td[2]/span/span")
    
    while 1:
        try:
            axd = driver.find_elements(By.XPATH,"//*[contains(text(),\'More')]")
            #if len(axd) == 4 :
             #   ac.move_to_element(axd[0]).click().perform()
              #  time.sleep(5)
            for element in axd:
                #print(element.text,len(element.text))
                if len(element.text) == 4:
                    ac.move_to_element(element).click().perform()
                    time.sleep(randint(10,20))
                    continue
            #if no 'more' element was found, all tables have been expanded to the max but there was some other element with 'more' in its text
            break
                
        except:
            print("Expanded all for {ID}")
            break

    rows_of_disclosure = driver.find_elements(By.XPATH, "//table/tbody/tr/td[2]/span/span/a")
    print("rows",len(rows_of_disclosure))
    years_of_disclosure = driver.find_elements(By.XPATH, "//table/tbody/tr/td[1]/span/span")[0:len(rows_of_disclosure)]
   
    #if we do not have any suitable rows,
    if len(rows_of_disclosure) == 0:
        
        driver.get_screenshot_as_file(BASE + ID + "/screenshot.png")
        print(f"Screenshoted the {ID}")
        Novalues.loc[len(Novalues.index)] = [ID,name_ids['Name'][i],'Null']
        driver.quit()
   
        return None,Novalues
        
    #get all the table
    print("got all the elements, starting to loop")

    prev_year = 0
    year_count = 1

    for j,row in enumerate(rows_of_disclosure):
        #print(f"In row {j}")
        if to_extract in row.text:
            #print(f"In row {j}")
            link = (row.get_attribute('href'))
            date = (years_of_disclosure[j].text)
            year = date[-4:]
            
            #If it is annual report from the same year as the last one, we increment the submission counter by 1 to account for resubmitting the year(So 1st on our list would be the latest version of the submission)
            if (prev_year == year):
                year_count += 1
            else:
                year_count = 1
            #need to add buttons to make the whole page visible
            All_reports.loc[len(All_reports.index)] = [ID,name_ids['Name'][i],row.text,date,year,year_count,link]
            prev_year = year
    driver.quit()
    print(f"Done with {ID}, got {All_reports.shape} links")
    return All_reports,None




"""
This is the part of the pipeline that takes in a list of promoted stocks on the OTC market website that have opted to perform financial disclosures, and it webscraps the OTC-Markets website for the following data:

    Financial statements:
        -Third party providers(8?)
        -Company insiders table(7)
        -Security Information (2)
"""
if __name__ == '__main__':

    #read in the Promoted Stocks file
    promoted_stocks = pd.read_csv('promoted_stocks.csv',low_memory=False,header=None)
    
    
    #575 Companies
    name_ids = pd.DataFrame(promoted_stocks[[2,4]])
    name_ids = name_ids.set_axis(['ID', 'Name'], axis='columns')
    All_reports = pd.DataFrame(columns = ['ID','Name','Report','Date','Year', 'Version','Link'])
    Novalues = pd.DataFrame(columns = ['ID','Name','Reason'])
    #get_links('GEGP',1,name_ids)
    pool = mp.Pool(mp.cpu_count()-1)
    #for each of the company in the data,
    results = pool.starmap(get_links,zip(name_ids['ID'],range(len(name_ids)),repeat(name_ids)))
    const1, const2 = zip(*results)
    All_reports = pd.concat(const1)
    Novalues    = pd.concat(const2)
    #Save the results
    All_reports.to_csv(DATA+'/metadata_final.csv')
    Novalues.to_csv(DATA+'/Dropped_companies_final.csv')
    ##Go through each row, and add
   
