from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import time
import sqlite3
import json
import re
import os
from selenium.common.exceptions import TimeoutException
import json
driver = webdriver.Chrome()

def get_credentials(key):
    with open("credentials.json", 'r') as json_file:
        data = json.load(json_file)
        return find_key_value(data, key)

def find_key_value(data, key):
    if isinstance(data, dict):
        for k, v in data.items():
            if k == key:
                return v
            elif isinstance(v, (dict, list)):
                result = find_key_value(v, key)
                if result is not None:
                    return result
    elif isinstance(data, list):
        for item in data:
            result = find_key_value(item, key)
            if result is not None:
                return result

    return None


def scrape_data(leads_url,iteration):
    try:
        if iteration==0:
        
            driver.get("https://app.apollo.io/#/login?redirectTo=https%3A%2F%2Fapp.apollo.io%2F%23%2F")

            wait = WebDriverWait(driver, 15)
            wait.until(EC.visibility_of_element_located((By.ID, "o1-input")))
            username_element  = driver.find_element(By.ID, "o1-input")
            password_element  = driver.find_element(By.ID, "current-password")
            username_element.send_keys(get_credentials("email"))
            password_element.send_keys(get_credentials("password"))
            password_element.send_keys(Keys.ENTER)

            wait.until(EC.url_changes("https://app.apollo.io/#/login?redirectTo=https%3A%2F%2Fapp.apollo.io%2F%23%2F"))
        
            driver.get(leads_url)
        else:
            driver.refresh()
        
        wait = WebDriverWait(driver, 15)
        wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "tr.zp_cWbgJ")))
    except:
        driver.refresh()

    data_rows = driver.find_elements(By.CSS_SELECTOR, "tr.zp_cWbgJ")

    output_data = []

    for row in data_rows:
        try:
            email_button = row.find_element(By.CSS_SELECTOR, ".zp-button.zp_zUY3r.zp_jSaSY.zp_MCSwB.zp_IYteB")
            email_button.click()
            zp_Y6y8d_elems = row.find_elements(By.CSS_SELECTOR, "span.zp_Y6y8d")
            title = zp_Y6y8d_elems[0].text
            location = zp_Y6y8d_elems[1].text
            employees = zp_Y6y8d_elems[2].text
            try:
                industry = row.find_element(By.CSS_SELECTOR, "div.zp_PHqgZ.zp_TNdhR").text
            except:
                industry =""
            try:
                keywords_container = row.find_element(By.CSS_SELECTOR, "div.zp_HlgrG.zp_y8Gpn")
                input_string = keywords_container.get_attribute('innerHTML')
                stripped_string = re.sub('<[^>]*>', '', input_string)
                unescaped_string = re.sub('&amp;', '&', stripped_string)
                keywords = unescaped_string.split(', ')
            except:
                keywords = []

            full_name = row.find_element(By.CSS_SELECTOR, "div.zp_xVJ20 > a").text
            first_name =full_name.split()[0]
            try: 
                last_name = full_name.split()[1]
            except:
                last_name =""
            plink = row.find_element(By.CSS_SELECTOR, "div.zp_xVJ20 > a").get_attribute("href")
            company_name = row.find_element(By.CSS_SELECTOR, "a.zp_WM8e5.zp_kTaD7").text



            company_website = ''
            company_linkedin = ''

            social_links_container = row.find_elements(By.CSS_SELECTOR, "div.zp_I1ps2")[1]
            social_links = social_links_container.find_elements(By.CSS_SELECTOR, "a.zp_OotKe")


            for link in social_links:
                icon = link.find_element(By.CSS_SELECTOR, "i.zp-icon")

                if 'apollo-icon-link' in icon.get_attribute("class").split():
                    company_website = link.get_attribute("href")


                elif 'apollo-icon-linkedin' in icon.get_attribute("class"):
                    company_linkedin = link.get_attribute("href")

            linkedin = ''

            linkedin_container = row.find_elements(By.CSS_SELECTOR, "div.zp_I1ps2")[0]
            social_links = linkedin_container.find_elements(By.CSS_SELECTOR, "a.zp_OotKe")

            for link in social_links:
                icon = link.find_element(By.CSS_SELECTOR, "i.zp-icon")
                if 'apollo-icon-linkedin' in icon.get_attribute("class"):
                    linkedin = link.get_attribute("href")

            wait = WebDriverWait(row, 10)
            try:
                wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, ".zp_OotKe.zp_Iu6Pf")))
                email_element = row.find_element(By.CSS_SELECTOR, ".zp_OotKe.zp_Iu6Pf")
                email = email_element.text

            except TimeoutException:
                break


            lead_data = {
                "Full_Name": full_name,
                "First_Name": first_name,
                "Last_Name": last_name,
                "plink": plink,
                "Title": title,
                "Email": email,
                "Company_Name": company_name,
                "Website": company_website,
                "Location": location,
                "Employees": employees,
                "Industry": industry,
                "Keywords": keywords,
                "Linkedin": linkedin,
                "CompanyLinkedin": company_linkedin
            }

            output_data.append(lead_data)

        except Exception as e:
            print(f"Error: {e}")
            pass

    return output_data

def add_lead(Email, Full_Name, First_Name, Last_Name, plink, Title,  Company_Name, Website, Location, Employees, Industry, Keywords, Linkedin, CompanyLinkedin):
    conn = sqlite3.connect('leads.db')
    c = conn.cursor()
    c.execute("INSERT INTO leads (Email, Full_Name, First_Name, Last_Name, plink, Title, Company_Name, Website, Location, Employees, Industry, Keywords, Linkedin, CompanyLinkedin) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
              (Email, Full_Name, First_Name, Last_Name, plink, Title, Company_Name, Website, Location, Employees, Industry, Keywords, Linkedin, CompanyLinkedin))
    conn.commit()
    conn.close()

def save_to_sqlite(output_data, sqlite_file):
    conn = sqlite3.connect(sqlite_file)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS leads
                 (Email TEXT PRIMARY KEY, Full_Name TEXT, First_Name TEXT, Last_Name TEXT, plink TEXT, Title TEXT, Company_Name TEXT, Website TEXT, Location TEXT, Employees TEXT, Industry TEXT, Keywords TEXT, Linkedin TEXT, CompanyLinkedin TEXT)''')
    conn.commit()
    conn.close()

    for lead_data in output_data:
        try:
            add_lead(
                lead_data["Email"],
                lead_data["Full_Name"],
                lead_data["First_Name"],
                lead_data["Last_Name"],
                lead_data["plink"],
                lead_data["Title"],
                lead_data["Company_Name"],
                lead_data["Website"],
                lead_data["Location"],
                lead_data["Employees"],
                lead_data["Industry"],
                ', '.join(lead_data["Keywords"]),
                lead_data["Linkedin"],
                lead_data["CompanyLinkedin"]
            )
        except Exception as e:
            print(f"Error inserting lead into SQLite: {e}")
            continue

def sqlite_to_bigquery(sqlite_db_file):

    # BigQuery settings
    bigquery_project_id = "sturdy-analyzer-381820"
    # bigquery_project_id = "sturdy-analyzer"
    bigquery_dataset_id = "scraped_data_raw"
    bigquery_table_id = "apollo_program_scraping_raw"
    bigquery_temp_table_id = "apollo_program_scraping_temp"
    # BigQuery settings


    # Path to the service account key file
    key_path = "sturdy-analyzer-81f33f4f5c8f.json"

    # Connect to the SQLite database
    sqlite_conn = sqlite3.connect(sqlite_db_file)

    # Read data from SQLite database
    data_df = pd.read_sql_query('SELECT * FROM leads;', sqlite_conn)

    # Close the SQLite connection
    sqlite_conn.close()

    # Set up BigQuery client
    credentials = service_account.Credentials.from_service_account_file(key_path)
    bigquery_client = bigquery.Client(credentials=credentials, project=bigquery_project_id)

    # Upload data to BigQuery
    data_df.to_gbq(
        f"{bigquery_dataset_id}.{bigquery_table_id}",
        project_id=bigquery_project_id,
        credentials=credentials,
        if_exists="replace"
    )

if __name__=="__main__":
    
    sqlite_file='leads.db'
    
    leads_url="https://app.apollo.io/#/people?finderViewId=642fa780a7f45f00dcd8ad81&organizationNumEmployeesRanges[]=11%2C20&organizationNumEmployeesRanges[]=21%2C50&organizationNumEmployeesRanges[]=51%2C100&organizationNumEmployeesRanges[]=101%2C200&organizationNumEmployeesRanges[]=10001&organizationNumEmployeesRanges[]=5001%2C10000&organizationNumEmployeesRanges[]=2001%2C5000&organizationNumEmployeesRanges[]=1001%2C2000&organizationNumEmployeesRanges[]=501%2C1000&organizationNumEmployeesRanges[]=201%2C500&personLocations[]=United%20States&personSeniorities[]=manager&personSeniorities[]=director&personSeniorities[]=head&personSeniorities[]=vp&personSeniorities[]=owner&personSeniorities[]=founder&personSeniorities[]=c_suite&personSeniorities[]=partner&organizationIndustryTagIds[]=5567e0bf7369641d115f0200&organizationIndustryTagIds[]=5567cd4e7369644cf93b0000&organizationIndustryTagIds[]=5567cdf27369644cfd800000&organizationIndustryTagIds[]=5567e0dd73696416d3c20100&contactEmailStatus[]=verified&prospectedByCurrentTeam[]=no&page=1&viewMode=table"
    
    iterations=9999 #defines how many times the script will run

    time_between_iterations=300 #time in seconds
    
    for i in range(iterations):
        try:
            leads = scrape_data(leads_url, i) #gets 25 records and save into a list of dictionaries

            save_to_sqlite(leads, sqlite_file) #Adds the 25 records to sqlite db
            
            sqlite_to_bigquery(sqlite_file) #Adds the 25 records to bigquery using sqlite file

            time.sleep(time_between_iterations) #defines the time between iterations. Set it to 300 for 5 minutes
        except:
            pass
    
    driver.quit()
    


