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
import pyautogui, os, pdb, time, re, shutil
from tqdm import tqdm
import multiprocessing as mp

BASE = os.getcwd()
DATA = os.getcwd() + '/downloaded_pdfs_all_versions/'
#if os.path.exists(DATA+"bad_files.csv") == True:
 #   os.remove(DATA+"bad_files.csv")
print(DATA)
#If data does not exist, create a directory folder for storing the data
if os.path.exists(DATA) == False:
    os.makedirs(DATA)

#else, delete all the files that are already existing
#else:

"""
This is the part of the pipeline that takes in a list of promoted stocks on the OTC market website that have opted to perform financial disclosures, and it webscraps the OTC-Markets website for the following data:

    Financial statements:
        -Third party providers(8?)
        -Company insiders table(7)
        -Security Information (2)
"""
#//*[@id="root"]/div/div[2]/div[3]/div[2]/div/div/div[5]/div/div[2]/div/div[2]/div[3]/div[1]/table


def download_file(index,row):
    row = row.drop(["reason"])

    #options.add_argument(f'--headless')
    dir =  str(row[0]) + '_' + str(row[-3]) +f'_Annual_{str(row[-2])}'
    #open the driver
    file = str(row[0]) + '_' + str(row[-3]) +f'_Annual_{str(row[-2])}.pdf'
    #if the file was already downloaded
    if file in os.listdir(DATA):
        return 0
    if os.path.exists(DATA+dir) == False:
        os.makedirs(DATA+dir)
    #if a file already exists in the folder, delete it
    if os.path.isfile(DATA+dir+'/content.pdf'):
        return 0
    options = Options()
    ua = UserAgent()
    userAgent = ua.random
    options.add_argument(f'user-agent={userAgent}')
    options.add_experimental_option('prefs',
        {"download.default_directory": DATA+dir,
            "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "plugins.always_open_pdf_externally": True
                                    })
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()),options=options)
    
    try:
        driver.get(row[-1])
        i = 0
        while True:
            #if downloaded already, break
            if os.path.exists(DATA + dir +'/content.pdf') == True:
                break
            #else keep waiting for incremental times
            else:
                time.sleep(i)
                i = i+1
                if i > 10:
                    return 1
    except:
        print(f"{file} is not being loaded")
        driver.quit()
        return 1
    driver.quit()
    return 0


def convert_file(index,row):

    
    if row['reason'] == 1: return 0
    row = row.drop(["reason"])
    file = str(row[0]) + '_' + str(row[-3]) +f'_Annual_{str(row[-2])}.pdf'
    dir =  str(row[0]) + '_' + str(row[-3]) +f'_Annual_{str(row[-2])}'
    #if we have already
   # if os.file.exists(DATA + dir +'/content.pdf') == False:
    #    return 1
    if os.path.isfile(DATA+file) == True:
        return 0
    #os.rename(DATA + dir +'/content.pdf',DATA+file)
    try:
        os.rename(DATA + dir +'/content.pdf',DATA+file)
        shutil.rmtree(DATA + dir)
        return 0
    except:
        print(f"{file} is not being renamed properly")
        return 1
        
def delete_excess(index,row):

    
    if row['reason'] == 1: return 0
    row = row.drop(["reason"])
    file = str(row[0]) + '_' + str(row[-3]) +f'_Annual_{str(row[-2])}.pdf'
    dir =  str(row[0]) + '_' + str(row[-3]) +f'_Annual_{str(row[-2])}'

    if (os.path.isfile(DATA+file) == True and os.path.exists(DATA+dir == True)):
        try:
            shutil.rmtree(DATA + dir)
        except:
            pass
        return 0
    return 1
        



if __name__ == '__main__':
    All_reports = pd.read_csv(BASE+'/metadata.csv')#DataFrame(columns = ['ID','Name','Report','Date','Year', 'Version','Link'])
    All_reports['reason'] = 0
    #All_reports = pd.read_csv(DATA+"bad_files.csv",low_memory=False)

    bad_files = pd.DataFrame(columns = All_reports.columns)
    pool = mp.Pool(mp.cpu_count()-1)
    inputs = [(i,row) for i,row in All_reports.iterrows()]
    
    #first download each file
    results1 = pool.starmap(download_file,inputs)
    for i,result in enumerate(results1):
        if result == 1:
            row = All_reports.loc[i]
            row.loc['reason'] = 1
            bad_files.loc[len(bad_files.index)] = row
            All_reports.loc[i]['reason'] = 1

    #Then, change the location of the files if we need to
    inputs = [(i,row) for i,row in All_reports.iterrows()]
    results2 = pool.starmap(convert_file,inputs)
    for i,result in enumerate(results2):
        if result == 1:
            row = All_reports.loc[i]
            row.loc['reason'] = 2
            bad_files.loc[len(bad_files.index)] = row
            
    #then delete any excess folders
    results3 = pool.starmap(delete_excess,inputs)
    All_reports['pdf_file'] = ''
    print(len(os.listdir(DATA)))
    for index,row in inputs:
        row = row.drop(['reason'])
        file = str(row[0]) + '_' + str(row[-3]) +f'_Annual_{str(row[-2])}.pdf'
        if file in os.listdir(DATA):
            All_reports.at[index,'pdf_file'] = file
        else:
            print("Here for ",index)
            All_reports.at[index,'pdf_file'] = "Nan"
            bad_files.loc[len(bad_files),index] = row.drop['pdf_file']
    print(All_reports['reason'])
    All_reports = All_reports.drop('reason',axis = 1)
    All_reports.to_csv(DATA+'../metadata_files.csv',index=False)
    #save the bad files
    bad_files.to_csv(DATA+"bad_files.csv",index=False)

    
    
"""


if __name__ == '__main__':

    #read in the Promoted Stocks file
    #All_reports = pd.read_csv(DATA+'metadata.csv',low_memory=False)
    #575 Companies
    #name_ids = pd.DataFrame(promoted_stocks[[2,4]])
    #name_ids = name_ids.set_axis(['ID', 'Name'], axis='columns')
    All_reports = pd.read_csv(BASE+'/metadata.csv')#DataFrame(columns = ['ID','Name','Report','Date','Year', 'Version','Link'])
    All_reports['reason'] = 0
    bad_files = pd.DataFrame(columns = All_reports.columns)
    print("Starting to download")
    options = Options()
    #options.add_argument('--headless')
    ua = UserAgent()
    userAgent = ua.random
    options.add_argument(f'user-agent={userAgent}')
    print(DATA)
    options.add_experimental_option('prefs',
        {"download.default_directory": DATA,
            "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "plugins.always_open_pdf_externally": True
                                    })
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()),options=options)
    pool = mp.Pool(mp.cpu_count()-1)
    results1 = pool.starmap(download_file,zip(range(All_reports.shape[0]),All_reports))
    for i,result in enumerate(results1):
        if result == 1:
            row = All_reports.loc[i]
            row['reason'] = 1
            bad_files.loc[len(bad_files.index)] = row

    results2 = pool.starmap(rename_file,zip(range(All_reports.shape[0]),All_reports))
    for i,result in enumerate(results1):
        if result == 1:
            row = All_reports.loc[i]
            row['reason'] = 2
            bad_files.loc[len(bad_files.index)] = row
    skipped = 0
    pbar = tqdm(All_reports.iterrows(),total=All_reports.shape[0])
    #for each of the file, create a temporary directory and direct the downoload there
    for index,row in pbar :
        dir =  str(row[0]) + '_' + str(row[-3]) +f'_Annual_{str(row[-2])}'
        #make the temporary directory
        if os.path.exists(DATA+dir) == False:
            os.makedirs(DATA+dir)
        if os.path.isfile(DATA+dir+'/content.pdf'):
            os.remove(DATA+dir+'/content.pdf')
        #open the driver
        file = str(row[0]) + '_' + str(row[-3]) +f'_Annual_{str(row[-2])}.pdf'
        pbar.set_description(f"Downloading {file}")
        #if we have not already downloaded the file,
        if os.path.isfile(DATA+file) == False:
        
            try:
                driver.get(row[-1])
                time.sleep(2)
            except:
                print(f"{file} is not being loaded")
                bad_files.loc[bad_files.shape[0]] = row
                continue
            try:
                os.rename(DATA+'content.pdf',DATA+file)
            except:
                print(f"{file} is not being renamed")
                bad_files.loc[bad_files.shape[0]] = row
                continue
    driver.quit()
    bad_files.to_csv(DATA+"bad_files.csv",index=False)
    """

    
    
    
    
	
	
