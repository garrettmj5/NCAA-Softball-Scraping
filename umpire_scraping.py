import pandas as pd
import requests
from requests.exceptions import ConnectionError, Timeout, RequestException
from urllib3.exceptions import ProtocolError  # Only keep ProtocolError
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from tqdm import tqdm  # Import tqdm for progress bar
import re

# Update main_url to be a list of URLs
main_urls = [
    "https://texaslonghorns.com/sports/softball/schedule/2024",
    "https://latechsports.com/sports/softball/schedule/2024",
    "https://nmstatesports.com/sports/softball/schedule/2024",
    "https://utepminers.com/sports/softball/schedule/2024",
    "https://tsusports.com/sports/softball/schedule/2024",
    "https://ulmwarhawks.com/sports/softball/schedule/2024",
    "https://tarletonsports.com/sports/softball/schedule/2024",
    "https://acusports.com/sports/softball/schedule/2024",
    "https://goislanders.com/sports/softball/schedule/2024",
    "https://hcuhuskies.com/sports/softball/schedule/2024",
    "https://lamarcardinals.com/sports/softball/schedule/2024",
    "https://mcneesesports.com/sports/softball/schedule/2024",
    "https://geauxcolonels.com/sports/softball/schedule/2024",
    "https://nsudemons.com/sports/softball/schedule/2024",
    "https://lionsports.net/sports/softball/schedule/2024",
    "https://uiwcardinals.com/sports/softball/schedule/2024"
]

# Create an empty set to get rid of duplicate links
all_links = set()

# Iterate through each main URL
for main_url in tqdm(main_urls, desc="Processing main URLs", unit="URL"):
    print(f"Processing URL: {main_url}")  # This will print the current URL being processed

    results = requests.get(main_url)
    doc = BeautifulSoup(results.text, 'html.parser')
    box_score_links = doc.find_all('a', href=True, string='Box Score')

    # Print the number of box score links found for this URL
    print(f"Found {len(box_score_links)} box score links for {main_url}")

    # Iterate through the box score links in each main URL
    for link in box_score_links:
        box_score_url = link.get('href')
        full_url = urljoin(main_url, box_score_url)

        # Print the full URL of each link being processed
        print(f"Adding link: {full_url}")
        # Add URLs to the set
        all_links.add(full_url)

# After processing all URLs, print out the list of links to check if they were added correctly
print(f"Total box score links: {len(all_links)}")
print("Links: ")
for link in all_links:
    print(link)

# Safe request function to handle potential errors (using a try block)
def safe_request(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for 4xx/5xx HTTP errors
        return response
    except (ConnectionError, Timeout, ProtocolError) as e:
        print(f"Connection error for URL {url}: {e}")
    except RequestException as e:
        print(f"Request error for URL {url}: {e}")
    return None  # Return None if there's an error

def get_home_plate_umpire(box_score_url):
    response = requests.get(box_score_url)
    soup = BeautifulSoup(response.text, 'html.parser')
    #Create blank list
    my_umps = []
    # Find the Umpire Tags
    tags = soup.find_all('dl')
    # Iterate through the tags
    for dl in tags:
        dt_tags = dl.find_all('dt')
        dd_tags = dl.find_all('dd')
        for dt in dt_tags:
            if 'Umpires' in dt.get_text(strip=True):
                for dd in dd_tags:
                    text = dd.get_text(strip=True)
                    if text.startswith('Home Plate:'):
                        home_plate_umpire = text.split('First:')[0].replace('Home Plate:', '').strip()
                        my_umps.append(home_plate_umpire)
    return my_umps

def get_date(box_score_url):
    response = requests.get(box_score_url)
    soup = BeautifulSoup(response.text, 'html.parser')
    # Create a blank list
    my_dates = []
    dl_tags = soup.find_all('dl')
    if len(dl_tags) >= 2:
        second_dl = dl_tags[1]
        dd_tags = second_dl.find_all('dd')
        if dd_tags:
            date = dd_tags[0].get_text(strip=True)
            my_dates.append(date)

    return my_dates

# Create a list to store all the data
full_data = []

# Iterate over each link to get the date and umpire with tqdm progress bar
for url in tqdm(all_links, desc="Processing box score links", unit="link"):
    dates = get_date(url)[0] if get_date(url) else None
    umpire_name = get_home_plate_umpire(url)[0] if get_home_plate_umpire(url) else None
    # 3D list with dates, umpire, and url
    temp = [url, dates, umpire_name]
    full_data.append(temp)

# Create DataFrame
df = pd.DataFrame(full_data, columns=["URL", "Date", "Home Plate Umpire"])

# Save DataFrame to CSV
df.to_csv("final_umpdata.csv", index=False)
print("Data saved to 'final_umpdata.csv'")

# Print the collected data
#print(f"Collected Data: {len(full_data)} records")
#for record in full_data:
   # print(record)