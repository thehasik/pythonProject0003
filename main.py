import pandas as pd
from bs4 import BeautifulSoup
import requests
import datetime
import sqlalchemy
import sqlite3
import urllib

SEARCH_PHRASE = urllib.parse.quote_plus(".net")
print(SEARCH_PHRASE)
FILE_NAME = "dotnet"
DATABASE_LOCATION = f"sqlite:///scraped_data_{FILE_NAME}.sqlite"
URL = f"https://www.jobs.cz/prace/?q%5B%5D={SEARCH_PHRASE}&page="


if __name__ == "__main__":
    for page in range(1, 100):
        html_text = requests.get(URL + str(page)).text
        # print(html_text)

        soup = BeautifulSoup(html_text, 'lxml')
        content = soup.find('div', {"class": "content"})
        jobs = content.find_all('div', {"class": "standalone search-list__item"})

        job_titles = []
        company_names = []
        company_locations = []
        salaries = []
        timestamps = []
        job_ids = []

        for job in jobs:
            job_title = job.find('h3', {"class": "search-list__main-info__title"})
            if job_title is None:
                continue
            else:
                job_title = job.find('h3', {"class": "search-list__main-info__title"}).text.strip()
                company_name = job.find_all('span', {"class": 'search-list__secondary-info--label'})[0].text.strip()
                company_location = job.find_all('span', {"class": 'search-list__secondary-info--label'})[1].get_text().strip()
                salary = job.find('span', {"class": 'search-list__tags__label search-list__tags__salary--label'})
                if salary is not None:
                    salary = job.find('span', {"class": 'search-list__tags__label search-list__tags__salary--label'}).get_text().strip().replace("\n", " ").replace(" ", "")
                else:
                    salary = "Not specified"
                published = job.find('span', {"class": "label-added label-added--newest"})
                if published is None:
                    published = "Date not specified"
                    timestamp = "Who knows"
                else:
                    published = job.find('span', {"class": "label-added label-added--newest"}).get('data-label-added-valid-from')
                    timestamp = published[:10] + " " + published[11:-6]
                    timestamp = datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
                job_id = str(timestamp) + job_title

                # print(f"Job Position: {job_title}")
                # print(f"Company: {company_name}")
                # print(f"Location: {company_location}")
                # print(f"Salary: {salary}")
                # print(f"Published: {timestamp}")
                # print("\n")

                job_titles.append(job_title)
                company_names.append(company_name)
                company_locations.append(company_location)
                salaries.append(salary)
                timestamps.append(timestamp)
                job_ids.append(job_id)

        # Preparing dict:
        job_dict = {
            "Position": job_titles,
            "Company": company_names,
            "Location": company_locations,
            "Salary": salaries,
            "Timestamp": timestamps,
            "ID": job_ids
        }

        print(job_dict)

        job_df = pd.DataFrame(job_dict, columns=["Position", "Company", "Location", "Salary", "Timestamp", "ID"])

        # Load
        engine = sqlalchemy.create_engine(DATABASE_LOCATION)
        conn = sqlite3.connect(f'scraped_data_{FILE_NAME}.sqlite')
        cursor = conn.cursor()

        sql_query = f"""
            CREATE TABLE IF NOT EXISTS scraped_data_{FILE_NAME}(
                Position VARCHAR(200),
                Company VARCHAR(200),
                Location VARCHAR(200),
                Salary VARCHAR(200),
                Timestamp VARCHAR(200),
                ID VARCHAR(200),
                CONSTRAINT primary_key_constraint PRIMARY KEY (ID)
            )
        """

        cursor.execute(sql_query)
        print("Database opened successfully")

        try:
            job_df.to_sql(f"scraped_data_{FILE_NAME}", engine, index=False, if_exists='append')
        except:
            print("Data already exists in the database")

        conn.close()
        print("Database closed successfully")