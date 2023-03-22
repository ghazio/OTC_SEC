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
import os, pdb, time, re
import tabula as tb

to_extract = "Annual Report"
BASE = os.getcwd()
DATA = os.getcwd() + '/downloaded_pdfs'
print(DATA)
#If data does not exist, create a directory folder for storing the data
if os.path.exists(DATA) == False:
    os.makedirs(DATA)

def get_links(ID,i,name_ids):
    print(f"Starting {i} {ID}")
    All_reports = pd.DataFrame(columns = ['ID','Name','Report','Date','Year', 'Version','Link'])
    Novalues = pd.DataFrame(columns = ['ID','Name','Reason'])
    options = Options()
    options.add_argument('--start-maximized')
    #options.add_argument('--start-fullscreen')
    #if ID != 'CDSG':
    options.add_argument('--headless')
    time.sleep(randint(1,1.5 * mp.cpu_count()-1))
    ua = UserAgent()
    userAgent = ua.random
    options.add_argument(f'user-agent={userAgent}')
    #open the driver
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()),options=options)
    #ID = 'GEGP'
    #print(f"Getting Links for {ID} index {i}")
    #open the website
    try:
        driver.get("https://www.otcmarkets.com/stock/"+ID+"/disclosure")
    except:
        driver.implicitly_wait(100)
        print(f"{ID} Did not load properly")
        driver.get("https://www.otcmarkets.com/stock/"+ID+"/disclosure")
    driver.implicitly_wait(100)
    #pdb.set_trace()
    #check for access denied error
    #try:
     #   gives_erro
    #If page does not exist
    
    try:
        page_exists = driver.find_element(By.XPATH,"//*[contains(text(),\'Page Not Found')]")
        print(page_exists.text)
        driver.get_screenshot_as_file(BASE +"/"+ ID + "screenshot.png")
        print(f"Screenshoted the {ID}, 404 error")
        #add the information to Novalues table
        Novalues.loc[len(Novalues.index)] = [ID,name_ids['Name'][i],'404']
        driver.quit()
        return None,Novalues
    except:
        #print("Page exists")
        pass
    
    #if the data was not loaded properly, try reloading the page(it happens for some pages)
    while True:
        try:
            unavailable = driver.find_element(By.XPATH,"//*[contains(text(),\'Unavailable')]").text
            print(f"{unavailable} for {ID}, reloading")
            driver.get("https://www.otcmarkets.com/stock/"+ID+"/disclosure")
            driver.implicitly_wait(100)
        except:
            #print("Data loaded properly")
            break

    #Check if the main table has no submissions to OTC makets
    try:
        no_submissions = driver.find_element(By.XPATH,"//*[contains(text(),\'The company has not provided financial reports or other disclosures to OTC Markets Group')]")
        #pdb.set_trace()
        driver.get_screenshot_as_file(BASE +"/"+ ID + "screenshot.png")
        print(f"Screenshoted the {ID}, No submissions")
        #add the information to Novalues table
        Novalues.loc[len(Novalues.index)] = [ID,name_ids['Name'][i],'NoSubmissions']
        driver.quit()
        return None,Novalues
    except:
        pass
        #print(f"{ID} submitted to the OTC markets")
    
    #Find the row that is displaying the number of rows
    totals = [element.text.split() for element in driver.find_elements(By.XPATH,"//*[(contains(text(),\'Displaying'))]")]
           #(driver.find_elements(By.XPATH,"//*[(contains(text(),\'Disclosure & News'))]"))
       #driver.find_elements(By.XPATH,"//*[(contains(text(),\'Displaying'))]"))
    #pdb.set_trace()
    #if
    exists = False
    for i,sentence in enumerate(totals):
        if 'Disclosure' in sentence:
            exists = True
            index  = i
    
    max = int(totals[index][3])
    cur = int(totals[index][1])
    to_click = round((max-cur)/10)
    """
    for i in range(to_click):
        axd = driver.find_elements(By.XPATH,"//*[contains(text(),\'More')]")
        for element in axd:
            if len(element.text) == 4:
                ac.move_to_element(element).click().perform()
                time.sleep(randint(10,15))
                break"""

    ac = ActionChains(driver)
    number_clicks = 0
    while cur < max or number_clicks > 10 * to_click:
        number_clicks += 1
        axd = driver.find_elements(By.XPATH,"//*[contains(text(),\'More')]")
        for element in axd:
            if len(element.text) == 4:
                ac.move_to_element(element).click().perform()
                time.sleep(randint(20,40))
                break
        #cur += 10
        totals_new = [element.text.split() for element in (driver.find_elements(By.XPATH,"//*[contains(text(),\'Displaying')]"))]
        exists = False
        for i,sentence in enumerate(totals):
            if 'Disclosure' in sentence and 'News' in sentence:
                exists = True
                index  = i
        cur = int(totals_new[index][1])
    if number_clicks > 10 * to_click: print(f"{ID} could not load all {number_clicks} the possible clicks {to_click}")

                
    rows_of_disclosure = driver.find_elements(By.XPATH, "//table/tbody/tr/td[2]/span/span/a")
    years_of_disclosure = driver.find_elements(By.XPATH, "//table/tbody/tr/td[1]/span/span")[0:len(rows_of_disclosure)]

        
    prev_year = 0
    year_count = 1
    no_val = True
    for j,row in enumerate(rows_of_disclosure):
        #print(f"In row {j}")
        if to_extract in row.text:
            no_val = False
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

    if no_val == True:
        driver.get_screenshot_as_file(BASE +"/"+ ID + "screenshot.png")
        print(f"Screenshoted the {ID}, no {to_extract} found")
        Novalues.loc[len(Novalues.index)] = [ID,name_ids['Name'][i],f'No {to_extract} files found']
        driver.quit()
        return None,Novalues
    
    driver.quit()
    print(f"Done with {ID}, got {All_reports.shape} links {cur} out of {max}")
    return All_reports,None




"""
This is the part of the pipeline that takes in a list of promoted stocks on the OTC market website that have opted to perform financial disclosures, and it webscraps the OTC-Markets website for the following data:

    Financial statements:
        -Third party providers(8?)
        -Company insiders table(7)
        -Security Information (2)
    No submission : PYYX
    No page: MXMG
    
"""
import random
if __name__ == '__main__':

    #read in the Promoted Stocks file
    promoted_stocks = pd.read_csv('promoted_stocks.csv',low_memory=False,header=None)
    
    
    #575 Companies
    name_ids = pd.DataFrame(promoted_stocks[[2,4]])
    name_ids = name_ids.set_axis(['ID', 'Name'], axis='columns')
    All_reports = pd.DataFrame(columns = ['ID','Name','Report','Date','Year', 'Version','Link'])
    Novalues = pd.DataFrame(columns = ['ID','Name','Reason'])
    #get_links('GEGP',1,name_ids)
    #print(mp.cpu_count()-1)
    pool = mp.Pool(mp.cpu_count()-1)
    #for i,name in enumerate(['GEGP','PYXX','MXMG']):
     #   print(f"Starting {name} {i}")
      #  a,b = get_links(name,i,name_ids)
       # All_reports.append(a)
        #Novalues.append(b)
    
    #for each of the company in the data,
    #random.shuffle(name_ids['ID'])
    results = pool.starmap(get_links,zip(name_ids['ID'],range(len(name_ids)),repeat(name_ids)))
    const1, const2 = zip(*results)
    All_reports = pd.concat(const1)
    Novalues    = pd.concat(const2)
    
    #Save the results
    All_reports.to_csv(DATA+'/metadata_final.csv')
    Novalues.to_csv(DATA+'/Dropped_companies_final.csv')

