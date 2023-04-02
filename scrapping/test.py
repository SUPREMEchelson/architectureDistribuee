from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service

import time
import os
from bs4 import BeautifulSoup
import pandas as pd
from selenium import webdriver
import re 
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
options = Options()
options.binary_location = r"C:\Program Files\Mozilla Firefox\firefox.exe"
capabilities = DesiredCapabilities.FIREFOX
capabilities['browserName'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:94.0) Gecko/20100101 Firefox/94.0'
driver = webdriver.Firefox(service=Service(executable_path=r'C:\Python\Python311\Scripts\geckodriver.exe'), options=options, desired_capabilities=capabilities)
#driver = webdriver.Firefox(executable_path=r'C:\Python\Python311\Scripts\geckodriver.exe', options=options)
driver = webdriver.Firefox(service=Service(executable_path=r'C:\Python\Python311\Scripts\geckodriver.exe'), options=options)


def get_page(count=1):

    pages = []

    for pages_nb in range(1, count+1):
        pages_url = f"https://www.logic-immo.com/appartement-paris/location-appartement-paris-75-100_1-{pages_nb}.html"
        driver.get(pages_url)
        time.sleep(100)
        pages.append(driver.page_source.encode("utf-8"))
    return pages

def save_pages(pages):
    os.makedirs("data", exist_ok = True)
    for page_nb, page  in enumerate(pages):
        with open(f"data/page_{page_nb}.html", "wb") as f_out:
            f_out.write(page)

def parse_pages():
    pages_paths = os.listdir("data")
    results = pd.DataFrame()

    for page_path in pages_paths:
            with open("data/" + page_path, "rb") as f_in:
                 page = f_in.read().decode("utf-8")
                 result = parse_page(page)
                 results = results.append(result)
    return results

def parse_page(page):
    result = pd.DataFrame()
    soup = BeautifulSoup(page, "html.parser")
    areas= soup.find_all(attrs={"class": "announceDtlInfosArea"})

    result["loyer (€)"] = [
        clean_price(tag) for tag in soup.find_all(attrs={"class": "announceDtlPrice"})
    ]
    result["type"] = [
        clean_type(tag) for tag in soup.find_all(attrs={"class": "announceDtlInfosPropertyType"})
    ]
    result["surface (m²)"] = [
        clean_surface(tag)
        for tag in soup.find_all(attrs={"class": "announceDtlInfos announceDtlInfosArea"})
    ]
    result["nb_pieces"] = [
        clean_rooms(tag)
        for tag in soup.find_all(attrs={"class": "announceDtlInfos announceDtlInfosNbRooms"})
    ]
    result["emplacement"] = [
        clean_postal_code(tag) for tag in soup.find_all(attrs={"class": "announcePropertyLocation"})
    ]
    result["description"] = [
        tag.text.strip() for tag in soup.find_all(attrs={"class": "announceDtlDescription"})
    ]
    return result


def clean_price(tag):
    text = tag.text.strip()
    price = int(text.replace("€", "").replace(" ", ""))
    return price


def clean_type(tag):
    text = tag.text.strip()
    return text.replace("Location ", "")


def clean_surface(tag):
    text = tag.text.strip()
    return int(text.replace("m²", ""))


def clean_rooms(tag):
    text = tag.text.strip()
    rooms = int(text.replace("p.", "").replace(" ", ""))
    return rooms


def clean_postal_code(tag):
    text = tag.text.strip()
    match = re.match(".*\(([0-9]+)\).*", text)
    return match.groups()[0]







def main():
    pages = get_page(count=20)
    #save_pages(pages)
    results =  parse_pages()
    print(results)
    results.to_csv("donneesImmo.csv")

if __name__ == "__main__":
    main()

