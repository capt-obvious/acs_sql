from typing import List, Optional
from mysql.connector import errorcode
from sqlalchemy import create_engine
from requests import Session
from requests.exceptions import RequestException
from bs4 import BeautifulSoup
import pandas as pd
import io
import config

import logging

logging.basicConfig(
    filename=config.log_file, level=logging.DEBUG, format="%(asctime)s %(message)s"
)

dataUrl = config.dataUrl
dataExt = config.dataExt
dataUserName = config.dataUserName
dataPassword = config.dataPassword


sql = config.sql

# function to access https directory and return filelist
def listHref(
    userName: str, password: str, url: str, ext: Optional[str] = None
) -> List[str]:
    with Session() as session:
        session.auth = (userName, password)
        page = session.get(url, verify=False).text
    soup = BeautifulSoup(page, "lxml")

    if ext == None:
        return [
            url + "/" + node.get("href")
            for node in soup.find_all("a")
            if node.get("href")
        ]
    else:
        return [
            url + "/" + node.get("href")
            for node in soup.find_all("a")
            if node.get("href").endswith(ext)
        ]


# connect to directory of export data, generate filelist
try:
    importFiles = [f for f in listHref(dataUserName, dataPassword, dataUrl, None)]
    logging.info("Connected to " + dataUrl)
    print("Connected to " + dataUrl)
except RequestException as err:
    logging.warning(err)
    print(err)

all_folders = list()

for folder in importFiles[1:]:
    print(folder)
    try:
        importFolderss = [f for f in listHref(dataUserName, dataPassword, folder, None)]
        logging.info("Connected to " + folder)
        print("Connected to " + folder)
    except RequestException as err:
        logging.warning(err)
        print(err)
    for folder in importFolderss[1:]:
        all_folders.append(folder)


# concat all files per folder and generate sql table
for folder in all_folders:

    print("Current folder: " + folder)

    try:
        importFiles = [f for f in listHref(dataUserName, dataPassword, folder, dataExt)]
        logging.info("Connected to " + dataUrl)
        print("Connected to " + dataUrl)
    except RequestException as err:
        logging.warning(err)
        print(err)

    outputDF = pd.DataFrame()
    with Session() as session:
        session.auth = (dataUserName, dataPassword)
        for f in importFiles:
            # terrible hack to exclude non-batch output files
            if f.find("library_input") == -1:
                response = session.get(f, verify=False)
                outputDF = outputDF.append(pd.read_csv(io.BytesIO(response.content)))

    try:
        engine = create_engine(
            "mysql+mysqlconnector://"
            + sql["User"]
            + ":"
            + sql["Pass"]
            + "@"
            + sql["Address"]
            + ":"
            + sql["Port"]
            + "/"
            + sql["DB"],
            echo=False,
        )

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logging.warning(errorcode.ER_ACCESS_DENIED_ERROR)
            print("Wrong user name or password.")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            logging.warning(errorcode.ER_BAD_DB_ERROR)
            print("Database does not exist.")
        else:
            logging.warning(err)
            print("Error: ", err)
    else:
        logging.info("Connected to " + sql["DB"])
        print("Connected to " + sql["DB"])

    if outputDF.empty == False:
        outputDF.to_sql(
            name=folder[47:], con=engine, if_exists="replace", chunksize=1000
        )
