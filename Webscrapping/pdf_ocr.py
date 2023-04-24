import pytesseract
from PIL import Image
import os
import sys
from tqdm import tqdm
import multiprocessing as mp
#from itertool
import pdb
from pdf2image import convert_from_path
if os.path.exists(os.getcwd()+"/converted_text/") == False:
    os.makedirs(os.getcwd()+"/converted_text/")
bad_files = []

def convert(i,file):
    temp_folder = os.getcwd()+f"/temp_download/{file[:-4]}"
    
    input_file  = f"{os.getcwd()}/downloaded_pdfs_all_versions/{file}"
    output_file = f"{os.getcwd()}/converted_text/{file[:-3]}txt"
    #if the output file already exists, we do not do anything about it.
    if os.path.exists(output_file) == True:
        return 1
    #print(f"{file} {i}th file in the order")
    if os.path.exists(temp_folder) == False:
        os.makedirs(temp_folder)
    try:
        pages = convert_from_path(input_file, 350,output_folder = temp_folder)

        # Perform OCR using Pytesseract
        text = ''
        for i, file in enumerate(sorted(os.listdir(temp_folder))):
            with Image.open(f'{temp_folder}/{file}') as img:
                text += pytesseract.image_to_string(img)
    
        # Save the OCR text to a file
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(text)

        # Clean up temporary images
        for file in os.listdir(temp_folder):
            os.remove(f'{temp_folder}/{file}')
        os.rmdir(temp_folder)
    except:
        return -1
    return 0

if __name__ == '__main__':

    #read in all the files
    files       = os.listdir(f"{os.getcwd()}/downloaded_pdfs_all_versions/")
    bad_files   = []
    items = [(i,file) for i,file in enumerate(files)]
    Pool = mp.Pool(mp.cpu_count()-1)
    returns = Pool.starmap(convert,items)
    Pool.close()
    for i,ret_val in enumerate(returns):
        if ret_val == -1:
            bad_files.append(i)
    print(bad_files)
        

        


