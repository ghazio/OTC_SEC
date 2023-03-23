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
import os, pdb, time, re, math
import tabula as tb

to_extract = "Annual Report"
BASE = os.getcwd()
DATA = os.getcwd()
#If data does not exist, create a directory folder for storing the data
if os.path.exists(DATA) == False:
    os.makedirs(DATA)

def get_links(ID,i,name_ids):
    print(f"Starting {i} {ID}")
    All_reports = pd.DataFrame(columns = ['ID','Name','Report','Date','Year', 'Version','Link'])
    Novalues = pd.DataFrame(columns = ['ID','Name','Reason'])
    options = Options()
    options.add_argument('--start-maximized')


    #if ID != 'CDSG':
    options.add_argument('--headless')
    time.sleep(randint(0,10))
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
        time.sleep(30)
        print(f"{ID} Did not load properly")
        driver.get("https://www.otcmarkets.com/stock/"+ID+"/disclosure")
    #s = datetime.now()
    time.sleep(randint(30,50))
    wait_count = 0
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
    while True:
        try:
            if wait_count > 50:
                print(f"Screenshoted the {ID}, Access_Denied")
                #add the information to Novalues table
                Novalues.loc[len(Novalues.index)] = [ID,name_ids['Name'][i],'Access_denied']
                driver.quit()
                return None,Novalues
            unavailable = driver.find_element(By.XPATH,"//*[contains(text(),\'Access Denied')]")
            print(f"Access denied for {ID}, reloading")
            driver.get("https://www.otcmarkets.com/stock/"+ID+"/disclosure")
            wait_count += 1
            driver.implicitly_wait(10)
        except:
            
            break
    #check for access denied error
    #try:
     #   gives_erro
    #If page does not exist
        #if the data was not loaded properly, try reloading the page(it happens for some pages)
    while True:
        try:
            unavailable = driver.find_element(By.XPATH,"//*[contains(text(),\'unavailable')]").text
            print(f"{unavailable} for {ID}, reloading")
            driver.get("https://www.otcmarkets.com/stock/"+ID+"/disclosure")
            driver.implicitly_wait(10)
        except:
            #print(f"{ID} Data loaded properly")
            break
    
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

    #Check if the main table has no submissions to OTC makets
    try:
        no_submissions = driver.find_element(By.XPATH,"//*[contains(text(),\'The company has not provided financial reports or other disclosures to OTC Markets Group')]")
        driver.get_screenshot_as_file(BASE +"/"+ ID + "screenshot.png")
        print(f"Screenshoted the {ID}, No submissions")
        #add the information to Novalues table
        Novalues.loc[len(Novalues.index)] = [ID,name_ids['Name'][i],'NoSubmissions']
        driver.quit()
        return None,Novalues
    except:
        pass
        #print(f"{ID} submitted to the OTC markets")
    time.sleep(4)
    #Find the row that is displaying the number of rows
    totals = [element.text.split() for element in driver.find_elements(By.XPATH,"//*[(contains(text(),\'Displaying'))]")]

    #if it contains Disclosure
    exists = False
    for i,sentence in enumerate(totals):
        if 'Disclosure' in sentence:
            exists = True
            index  = i
            break
    #totals[index][3]
        #print(ID,"Faulting with totals",totals)
        #exit()
    try:
        totals[index][3]
    except:
        print(f"Screenshoted the {ID}, Indexing_error")
        #add the information to Novalues table
        Novalues.loc[len(Novalues.index)] = [ID,name_ids['Name'][i],'Indexing_error']
        driver.quit()
        return None,Novalues
    max = int(totals[index][3])
    cur = int(totals[index][1])
    to_click = math.ceil((max-cur)/10)
    """
    for i in range(to_click):
        axd = driver.find_elements(By.XPATH,"//*[contains(text(),\'More')]")
        for element in axd:
            if len(element.text) == 4:
                ac.move_to_element(element).click().perform()
                time.sleep(randint(10,15))
                break"""

    number_clicks = 0
    

    #for click_attempt in range(100 * to_click):
    while cur < max:
        ac = ActionChains(driver)
        number_clicks += 1
        if number_clicks > 100*10:
            print(f"Breaking at {number_clicks} for {ID}")
            break
        #pdb.set_trace()
        #if click_attempt % 25 == 0 and click_attempt >0:
         #   driver.get("https://www.otcmarkets.com/stock/"+ID+"/disclosure")
          #  time.sleep(20)
        axd = driver.find_elements(By.XPATH,"//*[contains(text(),\'More')]")
        for element in axd:
            if len(element.text) == 4:
                #ac.move_to_element(element).perform()
                driver.execute_script("arguments[0].scrollIntoView(true);", element);
                time.sleep(1)
                try:
                    ac.move_to_element(element).move_by_offset(1,10).click().perform()
                except:
                    print(f"{ID} could not click on {element.text}")
                time.sleep(5)
                break
        #cur += 10
        totals_new = [element.text.split() for element in (driver.find_elements(By.XPATH,"//*[contains(text(),\'Displaying')]"))]
        exists = False
        for i,sentence in enumerate(totals):
            if 'Disclosure' in sentence and 'News' in sentence:
                exists = True
                index  = i
                break
        cur = int(totals_new[index][1])
        del ac
    if number_clicks > 100*10: print(f"{ID} could not load all {number_clicks}")

    time.sleep(2)

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
    if cur < max and number_clicks > 100*10:
        driver.get_screenshot_as_file(BASE +"/"+ ID + "screenshot.png")
        print(f"Screenshoted the {ID}, could not find max rows found")
        Novalues.loc[len(Novalues.index)] = [ID,name_ids['Name'][i],f'Not completely loaded']
    
    driver.quit()
    print(f"Done with {ID}, got {All_reports.shape} links {cur} out of {max}")
    return All_reports,Novalues

def get_links_run(ID,i,name_ids):
    try:
        return get_links(ID,i,name_ids)
    except:
        print(f"{ID} failed for some reason.")
        Novalues = pd.DataFrame(columns = ['ID','Name','Reason'])
        Novalues.loc[len(Novalues.index)] = [ID,name_ids['Name'][i],f'Unknown error']
        return None,Novalues


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
from datetime import datetime
if __name__ == '__main__':
    s = datetime.now()
    #read in the Promoted Stocks file
    promoted_stocks = pd.read_csv('promoted_stocks.csv',low_memory=False,header=None)
    
    
    #575 Companies
    name_ids = pd.DataFrame(promoted_stocks[[2,4]])
    name_ids = name_ids.set_axis(['ID', 'Name'], axis='columns')
    All_reports = pd.DataFrame(columns = ['ID','Name','Report','Date','Year', 'Version','Link'])
    Novalues = pd.DataFrame(columns = ['ID','Name','Reason'])
    #get_links('GRCO',1,name_ids)
    pool = mp.Pool(mp.cpu_count())
    results = pool.starmap(get_links_run,zip(name_ids['ID'],range(len(name_ids)),repeat(name_ids)))
    print(datetime.now()-s)
    const1, const2 = zip(*results)
    All_reports = pd.concat(const1)
    Novalues    = pd.concat(const2)
    
    #Save the results
    All_reports.to_csv(DATA+'/metadata.csv')
    Novalues.to_csv(DATA+'/Dropped_companies.csv')

