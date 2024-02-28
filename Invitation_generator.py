from numpy import rec
from tabula import read_pdf
from tabulate import tabulate
import csv
import pdfplumber
import re
import mysql.connector as mysql
import regex
from datetime import date
## from multiprocessing import context
import os, sys
from docxtpl import DocxTemplate
import logging
import json


## Below variables is entered manually
measurement_data = '2024-01-13 d. 10:00 val.'
sudarymo_vieta = '  Vilnius'

## Data input file:
filepath = r"C:\Users\Shark\Desktop\Gretimybiu_pazyma_165544438456234.pdf"
def configure_logging():
    logger = logging.getLogger('errorlog')
    hdlr = logging.FileHandler('errors.log')
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.INFO)

    return logger

## Getting measured plot m_kad - Cadastral number, m_unique - Unique number
def get_measured_plot_data():
    with pdfplumber.open(filepath) as pdf:
        first_page = pdf.pages[0]
        
        # PDF converted to text
        a = first_page.extract_text()

        # Unique and Cadastral number of the measured plot
        x = re.findall(r"kurio unikalus Nr.:\s\w\w\w\w-\w\w\w\w-\w\w\w\w", a)
        m_unique = re.findall(r"\w\w\w\w-\w\w\w\w-\w\w\w\w",str(x))

        x = re.findall(r"kadastro Nr.: \w\w\w\w/\w\w\w\w:[00-99]*", a)
        m_kad = re.findall(r"\w\w\w\w/\w\w\w\w:[00-99]*", str(x))

        ## Unique number formating to match database format
        m_unique = m_unique[0]
        m_unique = m_unique.replace("-", "")   
        m_kad = m_kad[0]
        pdf.close()

    return m_kad, m_unique

## Exctract table from pdf file
def get_table_data():
    ## Reads table from pdf file
    tables = read_pdf(filepath, pages="all") 
    print(tabulate(tables[0]))

    ## Export table individually as CSV
    tables[0].to_csv("data.csv", encoding="utf-8") 

    ## Opening table data from CSV
    file = open("data.csv", encoding="utf-8")
    csvreader = csv.reader(file)
    header = next(csvreader)

    ## Table data in list format
    table_data_list = []
    for row in csvreader:
        table_data_list.append(row)
    file.close()
    
    return table_data_list

## Connecting to mysql sklypailt database
class Conn:
    def __init__(self):
        self.conn = mysql.connect(
            host = "localhost",
            user = 'LTsklypai',
            passwd = 'LTsklypai1',
            database = "ltsklypai"
        )
        self.cursor = self.conn.cursor()

## Choosing correct database table
def choose_database_table():
    with open("range.json", encoding = 'utf-8') as f:
        range = json.load(f)

    location_id = int(m_kad[0:4])

    for key, value in range.items():
        if location_id in value:
            database_table = key
    return location_id, database_table

## Getting measured plot address from databse
def get_measured_plot_address():
    ## Defining the Query
    conn = Conn()
    query = f"SELECT * FROM {database_table} WHERE UNIKAL_ID = {m_unique}"
    conn.cursor.execute(query)

    ## Fetching all records from the 'cursor' object
    records = conn.cursor.fetchall()

    mat_adress = ""
    try:
        for record in records:
            mat_adress = list(record)[16]
    except:
        mat_adress = "Address not found in database"
    
    return mat_adress

## Getting measured plot neighbours data: unique/cadastral numbers, neighbour_identity with address in list
def get_neighbours_data():
    neighbour_unikal = []
    neighbours_kad = []
    neighbour_identity_list = []

    for x in table_data_list:
        neighbour_unikal.append(x[2].replace("-", ""))
        try:
            neighbours_kad.append(x[3].replace("* ","")) 
        except:
            print('There is no * symbol in pdf table')
            neighbours_kad.append(x[3])
            
        # Formating neighbour identity with addressess in list (fifth column)
        fifth_column_row = x[4].replace('\n'," ")
        fifth_column_row = fifth_column_row.replace('UŽDAROJI AKCINĖ BENDROVĖ','UAB')
        fifth_column_row = fifth_column_row.replace('"','')
        neighbour_identity_list.append(fifth_column_row)
    
    return neighbour_unikal, neighbours_kad, neighbour_identity_list

## Getting neighbours addresses
def get_neighbours_address():
    neighbours_address_ = []
    k_sist_= []
    for x in range(len(table_data_list)):
        query = f"SELECT * FROM {database_table} WHERE UNIKAL_ID = {neighbour_unikal[x]}"
        conn = Conn()
        conn.cursor.execute(query)
        records = conn.cursor.fetchall()
        if records == []:
            neighbours_address_.append('Do not found')
        for record in records:
            gret_adress = list(record)[16]
            neighbours_address_.append(gret_adress)
            ## Getting K_SIST from database
            k_sist = list(record)[10]
            if k_sist != "KS94":
                k_sist = 2
            else: 
                k_sist = 1
            k_sist_.append(k_sist)
            
    return neighbours_address_, k_sist_ 

## Getting MATA_TIP from database
def get_mata_tip():
    mata_tip = []
    mata_tip_ = []
    for neighbour_unikal_ in neighbour_unikal:
        query = f"SELECT MATA_TIP FROM {database_table} WHERE UNIKAL_ID = {neighbour_unikal_}"
        conn = Conn()
        conn.cursor.execute(query)
        records = conn.cursor.fetchall()
        for record in records:
            record = list(record)[0]
            mata_tip_.append(record)
    for num1, num2 in zip(mata_tip_, k_sist_):
        mata_tip.append(num1 * num2) 
 
    return mata_tip

## Function to delete list dublicate values by list index 
def delete_multiple_element(list_object, indices):
    indices = sorted(indices, reverse=True)
    for idx in indices:
        if idx < len(list_object):
            list_object.pop(idx)
            
## Getting measured plot neighbours identity and deleting dublicates
def get_neighbour_identity():
    dublicate_index = [] 
    dublicate_index_list = []
    owner_number = []
    neighbour_identity = []

    for s in neighbour_identity_list:
        people = regex.findall(r'\p{Lu}*\s\p{Lu}*, gim. \w*-\w*-\w*|\p{Lu}*\s\p{Lu}*\s\p{Lu}*, gim. \w*-\w*-\w*',s)
        company = regex.findall(r'(.*?), a.k',s)
        if len(people) != 0 and "LIETUVOS RESPUBLIKA" in people[0]:
            people = []
        if len(company) != 0 and "LIETUVOS RESPUBLIKA" in company[0]:
            company = []

        ## Search dublicates and delete
        owners = [] 
        for i in people:
            if i[0] == ' ':
                owners.append(i[1:])
            else: 
                owners.append(i)
        for i in range(len(owners)):
            for j in range(i + 1, len(owners)):
                if owners[i] == owners[j]:
                    dublicate_index.append(j)

        delete_multiple_element(owners,dublicate_index)

        dublicate_index_list.append(dublicate_index)
        owner_number.append(len(owners))
        dublicate_index = []

        ## The owner of the adjacent plot is obtained
        for name_surname in owners:
            name_surname = re.sub(r", gim. \d\d\d\d-\d\d-\d\d","",name_surname)
            neighbour_identity.append(name_surname)

        for title in company:
            neighbour_identity.append(title)

    return neighbour_identity, dublicate_index_list, owner_number

## Exctracting neighbours plot addresses
def get_neighbours_plot_address():
    neighbour_identity_ = []

    for x in neighbour_identity_list:
        
        x = re.sub(r", gim. \w\w\w\w-\w\w-\w\w,",";",x)
        x = re.sub(r",\s*UAB, a.k.\s*\w*, ",";",x)
        x = re.sub(r", a.k.\s*\w*, ",";",x)
        x = re.sub(r"\n"," ",x) 
        
        neighbour_identity_.append(x)

    neighbours_address_list = []

    for x in range(0,len(neighbour_identity_)):
        if "LIETUVOS RESPUBLIKA" in neighbour_identity_[x]:
            #logger.info("LIETUVOS RESPUBLIKA in neighbour_identity")
            ik = re.split(r"LIETUVOS RESPUBLIKA...",neighbour_identity_[x])
            ik.pop()
            neighbour_identity_[x] = ' '.join(ik)
            for i in neighbour_identity:
                if i in neighbour_identity_[x]:
                    for i in neighbour_identity:
                        neighbour_identity_[x] = neighbour_identity_[x].replace(f'{i}','')
            
                    res_adresiukas = re.split(r";", neighbour_identity_[x])

                    ## Deletes empty [''] items from the list
                    res_adresiukas = list(filter(lambda a: a != '', res_adresiukas))

                    if dublicate_index_list[x] != []:
                        delete_multiple_element(res_adresiukas,dublicate_index_list[x])

                    for i in res_adresiukas:
                        if i[0] == ' ':
                            neighbours_address_list.append(i[1:])
                        else: 
                            neighbours_address_list.append(i)
        else:
            for i in neighbour_identity:
                neighbour_identity_[x] = neighbour_identity_[x].replace(f'{i}','')
        
            res_adresiukas = re.split(r";", neighbour_identity_[x])

            ## Deletes empty [''] items from the list
            res_adresiukas = list(filter(lambda a: a != '', res_adresiukas))

            if dublicate_index_list[x] != []:
                delete_multiple_element(res_adresiukas,dublicate_index_list[x])

            for i in res_adresiukas:
                if i[0] == ' ':
                    neighbours_address_list.append(i[1:])
                else: 
                    neighbours_address_list.append(i)
    return neighbours_address_list

## Collecting people birhtday date and companies a.k number 
def get_owner_id():
    neighbour_identity_gimdata = []

    for i in neighbour_identity_list:
        people_birthday = regex.findall(r"gim.\s\w*-\w*-\w*,",i)
        company_id = regex.findall(r"(.*?),",i)
        company_id_ = regex.findall(r"(.*?),",i)

        if 'LIETUVOS RESPUBLIKA' not in company_id[0]:
            if len(company_id) >= 2:
                for i in range(0,len(company_id)):
                    if 'LIETUVOS RESPUBLIKA' in company_id[i]:
                        for n in range(4):
                            company_id_.pop(i)

            for x in company_id_:
                if 'a.k.' in x:
                    neighbour_identity_gimdata.append(x)
            
        if people_birthday == []:
            pass
            #logger.info("There is no data about people birthday date")
        for x in people_birthday:
            x = x.replace(",","")
            neighbour_identity_gimdata.append(x)
    
    return neighbour_identity_gimdata

## Adjust MATA_TIP, neighbours cadastral number, neighbours plot addressess by number of owners
def adjust_data():
    mata_tip = sum([[s] * n for s, n in zip(get_mata_tip(), owner_number)], [])
    adjusted_neighbours_kad = sum([[s] * n for s, n in zip(neighbours_kad, owner_number)], [])
    adjusted_address = sum([[s] * n for s, n in zip(neighbours_address_, owner_number)], [])
    
    return mata_tip, adjusted_neighbours_kad, adjusted_address

## Deleting owners duplicates
def delete_owner_duplicates():
    to_whom = [] + neighbour_identity
    to_whom = list(dict.fromkeys(to_whom))
    return to_whom

## Combining the obtained data in dict
def merge_data_to_dict():
    letters = {'name': '', 'gim_data': '', 'kad_nr': [],'siuntimui':'', 'neighbours_address_list': []}
    letters_ = []
    for i in range(len(to_whom)):
        if mata_tip[i] > 0:
            letters['name'] = to_whom[i]
            for x in range(len(neighbour_identity)):
                if to_whom[i] == neighbour_identity[x]:
                    letters['gim_data'] = neighbour_identity_gimdata[x]
                    letters["kad_nr"].append(adjusted_neighbours_kad[x])
                    letters['siuntimui'] = (neighbours_address_list[x])
                    letters['neighbours_address_list'].append(adjusted_address[x])
            letters_.append(letters)
            letters = {'name': '', 'gim_data': '', 'kad_nr': [],'siuntimui':'', 'neighbours_address_list': []}
        else:
            print("Letters are not generated for these individuals: ")
            print(neighbour_identity[i], "Cadastral number: ",adjusted_neighbours_kad[i])

    return letters_

## The number from which the documents will be numbered is obtained
def get_document_nr():
    with open("MB_NR.txt", "r") as f:
        f = open("MB_NR.txt", "r")
        mb_nr = int(f.read())
    return mb_nr

## Letters are generated
def generate_letters():
    os.chdir(sys.path[0])
    doc = DocxTemplate('Template.docx')
    today = date.today()
    mb_nr = get_document_nr()
    path = "C:\\Users\\Shark\\Desktop\\Laisku_siuntimas\\Github\\LT-Cadastre-Invitation-Generator\\LT-CadastreInvitationGenerator\\Letters\\"
    if letters_ != []:
        for i in range(0,len(letters_)):
            print("Creating letter for:\n ",letters_[i]['name'])
            kad_nr = ''
            address_final = ''
            if len(letters_[i]['kad_nr']) > 1: 
                for n in range(0,len(letters_[i]['kad_nr'])):
                    if n != len(letters_[i]['kad_nr'])-1:
                        kad_nr += letters_[i]['kad_nr'][n] + " ir "
                        address_final += letters_[i]['neighbours_address_list'][n] + " ir "
                    else:
                        kad_nr += letters_[i]['kad_nr'][n]
                        address_final += letters_[i]['neighbours_address_list'][n]
                        a = letters_[i]['name']
                        b = letters_[i]['siuntimui']
                mb_nr += 1
                context = {'name': f'{a}', 'matavimu_data':f'{measurement_data}','siuntimui': f'{b}', 'MKAD':f'{m_kad}', 'gret_mat':f'{kad_nr}', \
                    'mat_adress':f'{mat_adress}', 'date':f'{today}', 'neighbours_address_list': f'{address_final}', 'sudarymo_vieta': f'{sudarymo_vieta}',\
                        'MB_NR': f'{mb_nr}'}
                doc.render(context)
                doc.save(f'{a}'+f'{i}'+'_Rendered.docx')
                print(kad_nr)
                print(address_final,"\n")
                doc.save(path + f'{a}'+f'{i}'+'_Rendered.docx')
            else:
                mb_nr += 1
                kad_nr += letters_[i]['kad_nr'][0]
                address_final += letters_[i]['neighbours_address_list'][0]
                a = letters_[i]['name']
                b = letters_[i]['siuntimui']
                context = {'name': f'{a}', 'matavimu_data':f'{measurement_data}','siuntimui': f'{b}', 'MKAD':f'{m_kad}', 'gret_mat':f'{kad_nr}', \
                    'mat_adress':f'{mat_adress}', 'date':f'{today}', 'neighbours_address_list': f'{address_final}', 'sudarymo_vieta': f'{sudarymo_vieta}' ,\
                        'MB_NR': f'{mb_nr}'}
                doc.render(context)
                doc.save(path + f'{a}'+f'{i}'+'_Rendered.docx')
                
        print("Total letters created: ",len(letters_))
    else:
        print("Data could not be retrieved")
        logger.info("Data could not be retrieved")
    return mb_nr

## The last document number is saved to txt file for future use 
def save_document_nr():
    with open("MB_NR.txt", "w") as f:
        f.write(f'{mb_nr}')
        

m_kad, m_unique = get_measured_plot_data()

print("\n","Measured plot Unique/Cadastral number:","\n") 
print(" Unique number: ",m_unique)
print(" Cadastral number:",m_kad)

table_data_list = get_table_data()
location_id, database_table = choose_database_table()
print("\n Measured location ID:",location_id,"Table used: ",database_table)

mat_adress = get_measured_plot_address()
print("\n Measured plot address: ",mat_adress)

neighbour_unikal, neighbours_kad, neighbour_identity_list = get_neighbours_data()
print("\n Neighbours Cadastral number: ",neighbours_kad)
print("\n Neighbours Unique numbers: ",neighbour_unikal)

neighbours_address_, k_sist_ = get_neighbours_address()
print("\n Neighbours addresses: ",neighbours_address_)
print(k_sist_)

mata_tip = get_mata_tip()
print("\n MATA_TIP data: ",mata_tip)
print('---------------------------------------------------------------------')

neighbour_identity, dublicate_index_list, owner_number = get_neighbour_identity()
print("\n Neighbour identity: ",neighbour_identity)

neighbours_address_list = get_neighbours_plot_address()
print("\n Neighbours plot address list: ",neighbours_address_list)

print("\n Number of owners of the adjacent plot: ",owner_number)

mata_tip, adjusted_neighbours_kad,adjusted_address  = adjust_data()
neighbour_identity_gimdata = get_owner_id()

print("\n Adjusted MATA_TIP by number of owners: ",mata_tip)
print("\n Adjusted neighbours cadastral number by number of owners: ",adjusted_neighbours_kad)
print("\n Adjusted neighbours plot addressess by number of owners: ",adjusted_address)
print("\n Owners birhtday data and companies a.k number:",neighbour_identity_gimdata)
print("----------------------------------------------------------------------------")

to_whom = delete_owner_duplicates()
print("\n Generating emails for these individuals: ", to_whom)

letters_ = merge_data_to_dict()
print("\n Created dict:\n",letters_) 

print("--------------------------------------------------------")
mb_nr = get_document_nr()
mb_nr = generate_letters()
save_document_nr()

if __name__ == '__main__':
    configure_logging()
    logger = logging.getLogger(__name__)