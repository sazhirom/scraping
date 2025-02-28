---
<a id="scraping-section"></a>
## ~~~ 1. Data wrangling ~~~
--- 

Data is collected sequentially from three websites and saved in CSV format.  

As an example, here is a script for data collection from a website with infinite scrolling.  

Comments and explanations are provided in the code.  


<details>
  <summary><strong>🖼️ Website image</strong></summary>

  ![Внешний вид сайта](https://raw.githubusercontent.com/sazhirom/images/main/%D0%BC%D0%B0%D1%80%D0%BA%D0%B5%D1%82.PNG)
</details>

<details>
  <summary><strong>📜 Scraping market</strong></summary>

```python
import pandas as pd
import undetected_chromedriver as uc
from bs4 import BeautifulSoup
from selenium.webdriver.chrome.service import Service
import re
import time
from selenium.webdriver.common.action_chains import ActionChains
import gc
from datetime import datetime
from random import uniform

timestamp = datetime.now().strftime('%d-%m-%Y')

def initialize_driver():
    ''' Function to launch Chrome with all possible options to reduce load:
      - headless mode, minimal resolution, disabling image loading.
      - Page loading mode set to "eager" - based on tests, it loads better this way because scrolling happens during loading.'''
    options = uc.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--incognito")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=800x600")
    options.add_argument("--disable-extensions")
    options.page_load_strategy = 'eager'
    options.experimental_options["prefs"] = {"profile.default_content_setting_values": {"images": 2}}
    
    service = Service(executable_path="chromedriver.exe")
    driver = uc.Chrome(service=service, options=options)
    driver.set_page_load_timeout(30)
    return driver


def extract_items(data):
    ''' Function to scrape item data in the format: name, price.'''
    soup = BeautifulSoup(data, 'html.parser')
    items_list = soup.find_all('div', class_=re.compile(r'item-info-block'))
    cleaned_list = [item.get_text().strip() for item in items_list if item.get_text().strip()]
    
    batch_data = []
    for item in cleaned_list:
        pattern = r'.+?\$(\d+\.\d+)\s-?\d+%\s(.+)'
        match = re.search(pattern, item)
        if match:
            batch_data.append([match.group(2).replace(',', ''), match.group(1).replace(',', '')])
    return batch_data


def scroll_and_extract(driver, duration, output_file):
    ''' Creates a file with BOM separately - several errors occurred before, and this helped.
    Since item names contain Chinese characters and many special symbols, we save the data in UTF-16.
    Data is loaded and cleared in batches of 2200 - this number was chosen based on practical experience, balancing memory usage and I/O operations.
    Several error-handling conditions are also included to restart the driver when necessary.
    The key to stable operation is periodic cache clearing.'''

    start_time = time.time()
    batch_data = []
    global timestamp

    with open(f'{output_file}_{timestamp}.csv', 'wb') as file:
        file.write(b'\xff\xfe')
        pd.DataFrame(columns=["Item", "market_price", "Date"]).to_csv(file, index = False, encoding = 'utf-16')

    while True:
        try:
            data = driver.page_source
            batch_data.extend(extract_items(data))
            
            ActionChains(driver).scroll_by_amount(0, 600).perform()
            time.sleep(uniform(3.44, 5.17))

            if len(batch_data) >= 2200:
                df = pd.DataFrame(batch_data, columns = ['Item','market_price'])
                df = df.drop_duplicates()
                df.to_csv(f'{output_file}_{timestamp}.csv', mode = 'a', index=False, header = False, encoding = 'utf-16')
                batch_data.clear()
                df = None
                driver.execute_cdp_cmd("Network.clearBrowserCache", {})
                gc.collect()

            if time.time() - start_time > duration:
                print("Time is up.")
                break
        except Exception as e:
            print(f"Scrolling error: {e}")
            driver.quit()
            time.sleep(22)
            driver = initialize_driver()
            time.sleep(22)


    if batch_data:
        df = pd.DataFrame(batch_data, columns = ['Item','market_price'])
        df = df.drop_duplicates()
        df.to_csv(f'{output_file}_{timestamp}.csv', mode = 'a', index=False, header = False, encoding = 'utf-16')



def cleaning(file_name):
    global timestamp
    """ Final rounding, removing spaces, and checking for duplicates. """
    df = pd.read_csv(file_name, encoding='utf-16')
    df['market_price'] = df['market_price'].astype(float).round(4)
    df['Item'] = df['Item'].str.strip()
    df['Date'] = timestamp
    df = df.sort_values(by='Item').drop_duplicates(subset=['Item']).reset_index(drop=True)
    df.to_csv(file_name, index=False, encoding='utf-16')
    print(f"Data saved to {file_name}")


def main():
    timestamp = datetime.now().strftime('%d-%m-%Y')
    output_file = 'market'
    duration = 1322
    
    driver = initialize_driver()
    print('Driver launched.')
    
    driver.get('https://market.csgo.com/en/?priceMin=4')
    time.sleep(5)  
    
    scroll_and_extract(driver,duration = duration, output_file = output_file)
    print('Scrolling completed.')
    
    driver.quit()
    print('Driver closed.')
    
    cleaning(f'{output_file}_{timestamp}.csv')
    print('Cleaning completed.')


if __name__ == "__main__":
    main()

```

</details>
<br></br>

---
<a id="wrangling-section"></a>

## ~~~ 2. Data wrangling ~~~
--- 


### Merge  

We perform an outer merge on three CSV files, round numerical values, convert currencies to USD, remove duplicates and errors, and identify the most profitable deals for sales from China to Russia and Russia to China.
<details>
  <summary><strong>📜 Merge </strong></summary>

```python
import pandas as pd
import numpy as np
from datetime import datetime

timestamp = datetime.now().strftime('%d-%m-%Y')

path_c5 = f'c5game_{timestamp}.csv'
path_market = f'market_{timestamp}.csv'
path_buff = f'buff_{timestamp}.csv'
path_buff_buyorders = f'buff_buyorders_{timestamp}.csv'

# There is no automatic exchange rate update, but the CNY/USD rate has remained stable for many years.
cny_usd = 0.14
profit_coef = 0.9025

df_c5 = pd.read_csv(path_c5, encoding='utf-16')
df_c5.drop_duplicates()
df_c5.rename(columns={'c5_item': 'Item'}, inplace=True)
# Created DataFrame df_c5 with columns 'Item', 'c5_price'

df_buff_buyorders = pd.read_csv(path_buff_buyorders, encoding='utf-16')
df_buff_buyorders.rename(columns={'buff_item': 'Item', 'buff_price': 'price'}, inplace=True)
# Created DataFrame df_buff_buyorders with columns 'Item', 'buyorders_price'

df_market = pd.read_csv(path_market, encoding='utf-16')
# Created DataFrame df_market with columns 'Item', 'market_price'

df_buff = pd.read_csv(path_buff, encoding='utf-16')
df_buff.rename(columns={'buff_item': 'Item'}, inplace=True)
# Created DataFrame df_buff with columns 'Item', 'buff_price'

df_final = pd.merge(df_buff, df_market, on='Item', how='outer')
df_final = pd.merge(df_final, df_c5, on='Item', how='outer')
# Outer merge

df_final['c5_price'] = df_final['c5_price'].astype(float).fillna(0)
df_final['buff_price'] = df_final['buff_price'].astype(float).fillna(0)
df_final['market_price'] = df_final['market_price'].astype(float).fillna(0)

df_final['buff_price'] = df_final['buff_price'].astype(float).apply(lambda x: x * cny_usd)
df_final['c5_price'] = df_final['c5_price'].astype(float).apply(lambda x: x * cny_usd)
# Rounding, cleaning, and converting CNY to USD

'''Comparing sale prices from China to Market, selecting the best deals on c5game and buff163'''
df_direct = df_final.copy(deep=True)

df_direct['market_price'] = df_direct['market_price'].astype(float).apply(lambda x: x * profit_coef)
df_direct['market_c5'] = df_direct['market_price'] / df_direct['c5_price']
df_direct['market_buff'] = df_direct['market_price'] / df_direct['buff_price']
df_direct.replace([np.inf, -np.inf], 0, inplace=True)

df_direct['best_price'] = np.maximum(df_direct['market_buff'], df_direct['market_c5'])
df_direct['label'] = np.where(df_direct['market_buff'] > df_direct['market_c5'], 'buff', 'c5')
df_direct = df_direct.sort_values(by='best_price', ascending=False)

# Rounding values and cleaning data
columns_to_round = ['c5_price', 'market_price', 'buff_price', 'market_c5', 'market_buff', 'best_price']
df_direct[columns_to_round] = df_direct[columns_to_round].round(4)
df_direct['Item'] = df_direct['Item'].str.strip()
df_direct.drop_duplicates(subset=['Item'], inplace=True)
df_direct.reset_index(drop=True, inplace=True)

df_direct.drop(columns=['Date', 'Date_x', 'Date_y'], inplace=True)
df_direct['Date'] = timestamp

df_direct.drop_duplicates(inplace=True)

df_direct.to_csv(f'direct_{timestamp}.csv', index=False, encoding='utf-16')

'''Comparing sale prices from Market to China, selecting the best deals based on buy orders on Buff'''
df_reverse = pd.merge(df_market, df_buff_buyorders, on='Item', how='outer')
df_reverse = df_reverse.fillna(0)
df_reverse['buyorders_price'] = df_reverse['buyorders_price'].astype(float).apply(lambda x: x * cny_usd)
df_reverse['coef'] = df_reverse['buyorders_price'] / df_reverse['market_price']
df_reverse.replace(np.inf, 0, inplace=True)
df_reverse.sort_values(by='coef', ascending=False, inplace=True)

df_reverse.drop(columns=['Date_x', 'Date_y'], inplace=True)
df_reverse['Date'] = timestamp
df_reverse.drop_duplicates(inplace=True)

df_reverse.to_csv(f'reverse_{timestamp}.csv', index=False, encoding='utf-16')

```
</details>  
<br>

### Generating Links for Recommendation Validation
120 items with potentially the best profit are checked based on their sales history over the past month. To do this, a link must be generated to navigate to the item's page on the market-csgo website.  

According to the site's data structure, a reference table was created to construct the link based on the item name.  

An interesting detail—special characters like '™' and '★'.  
For category generation and correct comparison with the reference table, these characters are removed using a dedicated function.  
However, when generating links, they are replaced with specific placeholders and then converted back, resulting in links like:  
AK-47/StatTrak™%20AK-47  
Shadow%20Daggers/★%20Shadow%20Daggers  

<details>
  <summary><strong>📜 Url_creator </strong></summary>
  
```python
import pandas as pd
import re
from urllib.parse import quote
from datetime import datetime

timestamp = datetime.now().strftime('%d-%m-%Y')
path = f'direct_{timestamp}.csv'

df = pd.read_csv(path,encoding = 'utf-16',on_bad_lines='skip')

def categorization(item):
    category_mapping = {
        "Knife": [
            "Bayonet", "Bowie Knife", "Butterfly Knife", "Classic Knife", "Falchion Knife", "Flip Knife", "Gut Knife", 
            "Huntsman Knife", "Karambit", "M9 Bayonet", "Navaja Knife", "Nomad Knife", "Paracord Knife", "Skeleton Knife", 
            "Stiletto Knife", "Survival Knife", "Talon Knife", "Ursus Knife", "Kukri Knife", "Shadow Daggers",
            "Butterfly Knife | Fade"
        ],
        "Agent": [
            "Agent", "Master Agent", "Exceptional Agent", "Superior Agent", "Sir Bloody", "Lt. Commander", "Vypa Sista",
            "Buckshot", "Special Agent", "Chem-Haz Capitaine", "Dragomir", "Cmdr. Davida", "Slingshot", "Chef d'Escadron",
            "Safecracker", "1st Lieutenant", "Number K", "Enforcer", "Markus Delrow", "Maximus", "Ricksaw", "Goggles",
            "Crasswater The Forgotten", "Trapper Aggressor", "Lieutenant Rex Krikey", "Col. Mangos Dabisi",
            "B Squadron Officer", "Trapper", "Cmdr. Frank 'Wet Sox' Baroud", "John 'Van Healen' Kask", "Little Kev",
            "D Squadron Officer", "Bio-Haz Specialist", "Chem-Haz Specialist", "'Medium Rare' Crasswater",
            "Operator", "The Elite Mr. Muhlik", "Street Soldier", "Getaway Sally", "Bloody Darryl The Strapped",
            "3rd Commando Company", "Cmdr. Mae 'Dead Cold' Jamison", "Sous-Lieutenant Medic", "Michael Syfers",
            "Jungle Rebel", "Elite Trapper Solman", "Sergeant Bombson", "Officer Jacques Beltram", "Arno The Overgrown",
            "'The Doctor' Romanov", "Osiris", "'Two Times' McCoy", "Blackwolf", "Aspirant",
            "Lieutenant 'Tree Hugger' Farlow", "Ground Rebel", "Primeiro Tenente", "Prof. Shahmat", 
            "Rezan The Ready", "Rezan the Redshirt", "Seal Team 6 Soldier"
        ],
        "Pistol": ["USP-S", "Glock-18", "P2000", "Desert Eagle", "Five-SeveN", "CZ75-Auto", "P250", "Dual Berettas", "Tec-9", "R8 Revolver"],
        "Rifle": ["AK-47", "M4A4", "M4A1-S", "FAMAS", "Galil AR", "SG 553", "AUG"],
        "Sniper Rifle": ["AWP", "SSG 08", "SCAR-20", "G3SG1"],
        "SMG": ["MP7", "P90", "UMP-45", "MAC-10", "MP9", "PP-Bizon", "MP5-SD"],
        "Shotgun": ["Nova", "XM1014", "MAG-7", "Sawed-Off"],
        "Machine Gun": ["M249", "Negev"],
        "Gloves": [
            "Gloves", "Hand Wraps", "Driver Gloves", "Moto Gloves", "Specialist Gloves", "Sport Gloves",
            "★ Hand Wraps | Spruce DDPAT (Field-Tested)"
        ],
        "Container": [
            "Case", "Capsule", "Container", 
            "Music Kit | Feed Me, High Noon",
            "StatTrak™ Initiators Music Kit Box", 
            "StatTrak™ NIGHTMODE Music Kit Box", 
            "StatTrak™ Masterminds 2 Music Kit Box", 
            "StatTrak™ Masterminds Music Kit Box", 
            "StatTrak™ Tacticians Music Kit Box",
            "Berlin 2019 Vertigo Souvenir Package", 
            "Rio 2022 Vertigo Souvenir Package",
            "Stockholm 2021 Vertigo Souvenir Package",
            "ESL One Cologne 2014 Challengers",
            "ESL One Cologne 2014 Legends",
            "Katowice 2019 Legends (Holo/Foil)",
            "Katowice 2019 Minor Challengers (Holo/Foil)",
            "Gift Package"
        ],
        "Music Kit": ["Music Kit"],
        "Sticker": ["Sticker"],
        "Charm": ["Charm"],
        "Graffiti": ["Graffiti"],
        "Patch": ["Patch"],
        "Pass": ["Pass"],
        "Equipment": ["Zeus"],
        "Collectible": ["Pin"],
        "Key": ["Key", "eSports Key"] 
    }

    for key, values in category_mapping.items():
        for value in values:
            if value.lower() in item.lower():
                return key
    return "Unknown"

def subcategory(item):
    subcategory_mapping = {
        "Pistol": ["USP-S", "Glock-18", "P2000", "Desert Eagle", "Five-SeveN", "CZ75-Auto", "P250", "Dual Berettas", "Tec-9", "R8 Revolver"],
        "Rifle": ["AK-47", "M4A4", "M4A1-S", "FAMAS", "Galil AR", "SG 553", "AUG"],
        "Sniper Rifle": ["AWP", "SSG 08", "SCAR-20", "G3SG1"],
        "SMG": ["MP7", "P90", "UMP-45", "MAC-10", "MP9", "PP-Bizon", "MP5-SD"],
        "Shotgun": ["Nova", "XM1014", "MAG-7", "Sawed-Off"],
        "Machine Gun": ["M249", "Negev"]}
    for key, values in subcategory_mapping.items():
        for value in values:
            if value in item:
                return value
    return None

def normalization(item):
    item = re.sub('★','',item)
    item = re.sub('StatTrak™','',item)
    return item
    
df['Item_cleaned'] = df['Item'].apply(lambda x: normalization(x))
df['Category'] = df['Item_cleaned'].apply(lambda x: categorization(x))
df['Subcategory'] = df['Item_cleaned'].apply(lambda x: subcategory(x))

base = 'https://market.csgo.com/en/'
def custom_quote(item):
    
    item = item.replace('™', 'PLACEHOLDER_TM').replace('★', 'PLACEHOLDER_STAR')
    
    encoded = quote(item)
   
    return encoded.replace('PLACEHOLDER_TM', '™').replace('PLACEHOLDER_STAR', '★')

df['url'] = df.apply(lambda row: f"{base}{row['Category']+'/'}"f"{row['Subcategory']+'/' if pd.notna(row['Subcategory']) else ''}"f"{custom_quote(row['Item'])}", axis=1)
df.to_csv(path, encoding = 'utf-16', index = False)
```
</details>  
<br>

### Checking 120 Items via Generated Links  

At this stage, our task is to extract data from 120 pages to build a sales history graph for each item, retrieve the current buy order price, and refine the selling price.  

For further processing, the average transaction price over the last 4 days is calculated, along with the number of completed transactions.  

<details>
  <summary><strong>🖼️ Страница предмета</strong></summary>

  ![Внешний вид сайта](https://raw.githubusercontent.com/sazhirom/images/main/item%20page.PNG)
</details>
<details>
  <summary><strong>📜 history check </strong></summary>

```python
import pandas as pd
import undetected_chromedriver as uc
from bs4 import BeautifulSoup
from selenium.webdriver.chrome.service import Service
import re
import time
from datetime import datetime
from random import uniform

timestamp = datetime.now().strftime('%d-%m-%Y')

def get_medium_price(history):
    now = datetime.now()
    list1=[]
    for record in history:
        item1 = [item.strip() for item in record.split(',')]
        item1.pop(2)
        item1.pop(0)
        item1[1] = item1[1].replace('. Price.','')
        list1.append(item1)
        date_format = "%b %d"
    
    print(list1)
    filtered_list = [
        item for item in list1
        if (now - datetime.strptime(item[0], date_format).replace(year=2024)).days < 4
    ]
    print(filtered_list)    
    sum1=0
    if len(filtered_list)<=2:
        history_price = 'меньше двух сделок за 4 дня'
    else:
        for price2 in filtered_list:
            sum1 += float(price2[1])
            history_price = sum1/len(filtered_list)
    print(history_price)
    return history_price

def frequency_calc(history):
    now = datetime.now()
    list1=[]
    for record in history:
        item1 = [item.strip() for item in record.split(',')]
        item1.pop(2)
        item1.pop(0)
        item1[1] = item1[1].replace('. Price.','')
        list1.append(item1)
        date_format = "%b %d"
    filtered_list = [
        item for item in list1
        if (now - datetime.strptime(item[0], date_format).replace(year=2024)).days < 4
    ]

    if len(filtered_list)<=2:
        frequency = 'low'
    elif len(filtered_list)<=5:
        frequency = 'medium'
    elif len(filtered_list)<=9:
        frequency = 'high'
    else:
        frequency = 'very high'
    return frequency


def initialize_driver():

    options = uc.ChromeOptions()
    options.add_argument("--headless=new")  
    options.add_argument("--disable-gpu")  
    my_user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
    options.add_argument("--incognito") 
    options.add_argument(f"--user-agent={my_user_agent}")
    options.add_argument('--disable-extensions')
    options.add_argument("--disable-plugins-discovery")
    options.add_argument("--no-sandbox")  
    options.add_argument("--disable-dev-shm-usage")  
    options.add_argument("--window-size=800x600")  
    options.add_argument("--disable-extensions")  
    options.add_argument("--disable-software-rasterizer") 
    
    chrome_prefs = {
        "profile.default_content_setting_values": {
            "images": 2,
        }
        }
    
    options.page_load_strategy = 'eager'
    options.experimental_options["prefs"] = chrome_prefs

    service = Service(executable_path="chromedriver.exe")
    driver = uc.Chrome(service=service, options=options)
    driver.set_page_load_timeout(60) 
    return driver

path = f'direct_{timestamp}.csv'
number = 100
k=0
df = pd.read_csv(path, encoding = 'utf-16')
driver = initialize_driver()


bad_url=[]
history_prices = []
actual_prices = []
request_prices = []
frequency_list = []
soup = None
duration = 2000

for url in df['url']:
    start = time.time()
    attempts = 0
    while attempts<=2:
        try:
            driver.get(url)
            time.sleep(uniform(5.45,7.11))
            data = driver.page_source
            soup = BeautifulSoup(data,'html.parser')
            prices = soup.find_all('div', class_='price')
            prices1 = [price.get_text() for price in prices]
            if prices!=[]:
                break
        except Exception as e:
            driver.quit()
            time.sleep(10)
            driver = initialize_driver()
            time.sleep(10)
            print(f'возниклда ошибка {e}')
            attempts+=1

    if any('₽' in price for price in prices1):
        history_prices.append('ошибочный url')
        request_prices.append('ошибочный url')
        actual_prices.append('ошибочный url')
        frequency_list.append('ошибочный url')
        k+=1
        continue


    if prices1[3].startswith(' '):
        actual_prices.append('нет цены на покупку')
        request_prices.append(prices1[3])
    else:
        prices1[-1] = prices1[-1].replace('≤ ', '').replace('$', '').replace(' ','')
        request_prices.append(prices1[-1])

        pattern = r'\$(\d+(\.\d+)?)'
        match = re.search( pattern, prices1[3])
        if match:
            actual_prices.append(match.group(1))
            print(match.group(1))
        else:
            actual_prices.append('цена отсутствует')
            print('цена отсутсвует')


    history_charts = soup.select('path.highcharts-point.highcharts-color-0')
    history = [chart.get('aria-label') for chart in history_charts]
    history_prices.append(get_medium_price(history))
    frequency_list.append(frequency_calc(history))

   
    print(f'страница номер {k} обработана')
    print(len(actual_prices))
    print(len(request_prices))
    print(len(history_prices))
    k+=1

    if k%10==0:
        time.sleep(5)
        print('перегрузка')
        driver.execute_cdp_cmd("Network.clearBrowserCache", {})
        time.sleep(5)

    if k>number:
        print('конец')
        driver.quit()
        print(len(actual_prices))
        print(len(request_prices))
        print(len(history_prices))
        break
        
    if (time.time() - start) > duration:
        driver.quit()
        break

df['Medium Prices'] = None
df['Actual Prices'] = None
df['Request price'] = None
df['Frequency'] = None

df.loc[0:number, 'Medium Price'] = history_prices
df.loc[0:number, 'Actual Prices'] = actual_prices
df.loc[0:number, 'Request price'] = request_prices
df.loc[0:number, 'Frequency'] = frequency_list
print('данные загружены')

if bad_url!=[]:
    df_badurl = pd.DataFrame(bad_url)
    df_badurl.to_csv('badurl.csv')

df.to_csv(path, encoding='utf-16',index = False)
print('файл сохранен')
```
</details>  
<br>

### Ranking  
Based on the collected data and an arbitrary empirical formula, deals are ranked by their attractiveness.  

Additionally, all deals are checked for profitability to catch any errors in the algorithms or scraping process.  

<details>
  <summary><strong>📜 Ranging </strong></summary>

```python
import pandas as pd
from datetime import datetime

timestamp = datetime.now().strftime('%d-%m-%Y')

path = f'direct_{timestamp}.csv'

df = pd.read_csv(path, encoding = 'utf-16')
columns_con = ['Request price','buff_price','c5_price','best_price', 'Actual Prices', 'market_price', 'Medium Price']
df[columns_con] = df[columns_con].apply(pd.to_numeric, errors = 'coerce')

def calculate_rating_coef(row):
    valid_min = min([value for value in [row['buff_price'], row['c5_price']] if value > 0], default=1)
    
    if row['Frequency'] == 'low' and row['Request price'] < valid_min:
        return 0
    elif row['Frequency'] == 'medium':
        return 1.2 * ((row['Medium Price'] / valid_min)**3) * (valid_min**0.16)
    elif row['Frequency'] == 'high':
        return 1.4 * ((row['Medium Price'] / valid_min)**3) * (valid_min**0.16)
    elif row['Frequency'] == 'very high':
        return 1.6 * ((row['Medium Price'] / valid_min)**3) * (valid_min**0.16)
    else:
        return 1 
    
def check_profit(row):
    valid_min = min([value for value in [row['buff_price'], row['c5_price']] if value > 0], default=1)
    if row['Medium Price'] * 0.9 < valid_min*1.03:
        return 0
    else:
        return 1
 

df['Rating'] = df.apply(calculate_rating_coef, axis=1)
df['Check'] = df.apply(check_profit, axis = 1)  
df['Rating'] = df['Rating'] * df['Check']

df = df.sort_values(by='Rating', ascending=False)
print(df.head())
df.to_csv(path, encoding= 'utf-16', index=False)
```
</details>  
<br>

### Cleaning Outdated Files  
Storage capacity is limited, so we implement a cleanup process to remove files older than 15 days.  


<details>
  <summary><strong>📜 Folder cleaning</strong></summary>

```python
import os
from datetime import datetime, timedelta


delete_day = (datetime.now() - timedelta(days = 16)).strftime('%d-%m-%Y')

names = ['market_','direct_','reverse_','c5_','buff_','buff_buyorders_']

found = False

for filename in os.listdir('/home/ec2-user/'):
    for name in names:
        if filename == f'{name}{delete_day}.csv':
            os.remove(f'/home/ec2-user/{filename}')
            print(f'удален файл {filename}')
            found = True

if not found:
    print("удалять нечего")
```
</details>
<br></br>  

---
<a id="SQL-section"></a>
## ~~~ 3. SQL ~~~
--- 
<a id="SQL"></a>

### Database Creation  

We use PgAdmin for data management and visualization. The connection is established via SSH to the public IP of an EC2 instance, acting as a bastion server.  

<details>
  <summary><strong>🖼️ SSH connection </strong></summary>

  ![Connection](https://raw.githubusercontent.com/sazhirom/images/main/ssh%20SQL%20bastion%20server.PNG)
  ![SSH tunnel](https://raw.githubusercontent.com/sazhirom/images/main/sql%20SSH%20tunnel.PNG)
</details>

For scraped data create 4 table, data + item is a unique key

<details>
  <summary><strong>🖼️ PGAdmin</strong></summary>

  ![Внешний вид сайта](https://raw.githubusercontent.com/sazhiromru/images/main/sql.PNG)
</details>  
<br>

### Data loading  

<details>
  <summary><strong>📜 SQL load</strong></summary>

```python
from datetime import datetime
import os
import pandas as pd
import psycopg2

timestamp = datetime.now().strftime('%d-%m-%Y')

conn = psycopg2.connect(
    host=
    database=
    user=
    password=
)
cursor = conn.cursor()

try:
    for filename in os.listdir(os.path.dirname(os.path.abspath(__file__))):
        if filename == f'market_{timestamp}.csv':
            df = pd.read_csv(filename, encoding='utf-16')
            df['Date'] = pd.to_datetime(df['Date'], format='%d-%m-%Y').dt.strftime('%Y-%m-%d')
            for index, row in df.iterrows():
                cursor.execute(
                    """INSERT INTO public.market (item, price, date, id) 
                    VALUES (%s, %s, %s, %s)
                    on conflict(id)
                    do update set
                    item = excluded.item,
                    price = excluded.price,
                    date = excluded.date""",
                    (row['Item'], row['market_price'], row['Date'], row['Item'] + ' ' + str(row['Date']))
                )
    conn.commit()
    print('market has been loaded')
except Exception as e:
    print(f'возникла ошибка {e}')
    conn.rollback()

try:
    for filename in os.listdir(os.path.dirname(os.path.abspath(__file__))):
        if filename == f'buff_{timestamp}.csv':
            df = pd.read_csv(filename, encoding='utf-16')
            df['Date'] = pd.to_datetime(df['Date'], format='%d-%m-%Y').dt.strftime('%Y-%m-%d')
            for index, row in df.iterrows():
                cursor.execute(
                    """INSERT INTO public.buff_sell (item, price, date, id) 
                    VALUES (%s, %s, %s, %s)
                    on conflict(id)
                    do update set
                    item = excluded.item,
                    price = excluded.price,
                    date = excluded.date""",
                    (row['Item'], row['buff_price'], row['Date'], row['Item'] + ' ' + str(row['Date']))
                )
    conn.commit()
    print('buff has been loaded')
except Exception as e:
    print(f'возникла ошибка {e}')
    conn.rollback()

try:
    for filename in os.listdir(os.path.dirname(os.path.abspath(__file__))):
        if filename == f'buff_buyorders_{timestamp}.csv':
            df = pd.read_csv(filename, encoding='utf-16')
            df['Date'] = pd.to_datetime(df['Date'], format='%d-%m-%Y').dt.strftime('%Y-%m-%d')
            for index, row in df.iterrows():
                cursor.execute(
                    """INSERT INTO public.buff_buyorders (item, price, date, id) 
                    VALUES (%s, %s, %s, %s)
                    on conflict(id)
                    do update set
                    item = excluded.item,
                    price = excluded.price,
                    date = excluded.date""",
                    (row['Item'], row['buyorders_price'], row['Date'], row['Item'] + ' ' + str(row['Date']))
                )
    conn.commit()
    print('buff buy has been loaded')
except Exception as e:
    print(f'возникла ошибка {e}')
    conn.rollback()

try:
    for filename in os.listdir(os.path.dirname(os.path.abspath(__file__))):
        if filename == f'c5game_{timestamp}.csv':
            df = pd.read_csv(filename, encoding='utf-16')
            df['Date'] = pd.to_datetime(df['Date'], format='%d-%m-%Y').dt.strftime('%Y-%m-%d')
            for index, row in df.iterrows():
                cursor.execute(
                    """INSERT INTO public.c5 (item, price, date, id) 
                    VALUES (%s, %s, %s, %s)
                    on conflict(id)
                    do update set
                    item = excluded.item,
                    price = excluded.price,
                    date = excluded.date""",
                    (row['Item'], row['c5_price'], row['Date'], row['Item'] + ' ' + str(row['Date']))
                )
    conn.commit()
    print('c5 has been loaded')
except Exception as e:
    print(f'возникла ошибка {e}')
    conn.rollback()

try:
    for filename in os.listdir(os.path.dirname(os.path.abspath(__file__))):
        if filename == f'direct_{timestamp}.csv':
            df = pd.read_csv(filename, encoding='utf-16')
            df['Date'] = pd.to_datetime(df['Date'], format='%d-%m-%Y').dt.strftime('%Y-%m-%d')
            for index, row in df.iterrows():
                cursor.execute(
                    """INSERT INTO public.direct (item, buff_price, market_price, c5_price, market_c5, market_buff, best_price, label, date, category, frequency, medium_price, rating, id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    on conflict(id)
                    do update set
                    item = excluded.item,
                    buff_price = excluded.buff_price,
                    market_price = excluded.market_price,
                    c5_price = excluded.c5_price,
                    market_c5 = excluded.market_c5,
                    market_buff = excluded.market_buff,
                    best_price = excluded.best_price,
                    label = excluded.label,
                    date = excluded.date,
                    category = excluded.category,
                    frequency = excluded.frequency,
                    medium_price = excluded.medium_price,
                    rating = excluded.rating""",
                    (row['Item'], row['buff_price'], row['market_price'], row['c5_price'], row['market_c5'], row['market_buff'], row['best_price'], row['label'], row['Date'], row['Category'], row['Frequency'], row['Medium Price'], row['Rating'], row['Item'] + ' ' + str(row['Date']))
                )
    conn.commit()
    print('direct has been loaded')
except Exception as e:
    print(f'возникла ошибка {e}')
    conn.rollback()

try:
    for filename in os.listdir(os.path.dirname(os.path.abspath(__file__))):
        if filename == f'reverse_{timestamp}.csv':
            df = pd.read_csv(filename, encoding='utf-16')
            df['Date'] = pd.to_datetime(df['Date'], format='%d-%m-%Y').dt.strftime('%Y-%m-%d')
            for index, row in df.iterrows():
                cursor.execute(
                    """INSERT INTO public.reverse (item, market_price, buff_price, coef, date, id) 
                    VALUES (%s, %s, %s, %s, %s, %s)
                    on conflict(id)
                    do update set
                    item = excluded.item,
                    market_price = excluded.market_price,
                    buff_price = excluded.buff_price,
                    coef = excluded.coef,
                    date = excluded.date""",
                    (row['Item'], row['market_price'], row['buyorders_price'], row['coef'], row['Date'], row['Item'] + ' ' + str(row['Date']))
                )
    conn.commit()
    print('reverse has been loaded')
except Exception as e:
    print(f'возникла ошибка {e}')
    conn.rollback()

cursor.close()
conn.close()
```
</details>
<br>

### Cleaning outdated data

After every iteration check data and delete old files

<details>
  <summary><strong>📜 SQL cleaning py</strong></summary>

```python
import psycopg2

conn = psycopg2.connect( 
    host=
    database=
    user=
    password=
)
    
cursor = conn.cursor()

try:
    cursor.execute("""
    delete from market where date < now() - interval'61 DAYS';
    delete from c5 where date < now() - interval'61 DAYS';
    delete from buff_buyorders where date < now() - interval'61 DAYS';
    delete from buff_sell where date < now() - interval'61 DAYS';
    delete from direct where date < now() - interval'61 DAYS';
    delete from reverse where date < now() - interval'61 DAYS';             
    """)
    print('данные очищены')
except Exception as e:
    print(f'возникла ошибка {e}')
    conn.rollback()
finally:
    cursor.close()
    conn.close()
```
</details>  
<br></br>  

---
<a id="aws-section"></a>
## ~~~ 4. AWS ~~~
---  


### VPC

When creating the database, be sure to enable DNS resolution; otherwise, you won't be able to connect via the internal S3 endpoint. Initially, I wasn’t aware of this. The endpoint format follows com.ap-southeast..., meaning DNS is required for proper routing.
<details>
  <summary><strong>🖼️ VPC</strong></summary>

  ![VPC](https://raw.githubusercontent.com/sazhirom/images/main/VPC%200.PNG)
  ![VPC](https://raw.githubusercontent.com/sazhirom/images/main/VPC%20settings.PNG)
  ![VPC](https://raw.githubusercontent.com/sazhirom/images/main/VPC%20creation.PNG)
  ![VPC](https://raw.githubusercontent.com/sazhirom/images/main/VPC%20scheme.PNG)
  ![VPC](https://raw.githubusercontent.com/sazhirom/images/main/DNS%20resolution.PNG)
</details>  
<br>

### S3 Connection

To upload files, I used scp via Amazon CLI, but for downloading and viewing script results, I used S3. To achieve this, we create an access point and a route that ensures traffic from EC2 to S3 flows through the internal endpoint.
<details>
  <summary><strong>🖼️ S3</strong></summary>

  ![S3](https://raw.githubusercontent.com/sazhirom/images/main/s3%20endpoint%20creation.PNG)
  ![S3](https://raw.githubusercontent.com/sazhirom/images/main/s3%20endpoint.PNG)
  ![S3](https://raw.githubusercontent.com/sazhirom/images/main/s3%20-%20endpoint%20internal%20route.PNG)
  ![S3](https://raw.githubusercontent.com/sazhirom/images/main/ec2%20s3%20lists.PNG)
  ![S3](https://raw.githubusercontent.com/sazhirom/images/main/s3%20final.PNG)
</details>    
<br>

### Creating RDS and EC2  
Standard free-tier settings are sufficient in most cases. For RDS, we disable auto-scaling; for EC2, we disable enhanced monitoring. We also link RDS with EC2 during creation for convenience.  
Configure security groups to allow unrestricted access to EC2 from any IP.  
Configure security groups to allow SQL database access via EC2.  

<details>
  <summary><strong>🖼️ RDS and EC2 </strong></summary>

  ![RDS и EC2 ](https://raw.githubusercontent.com/sazhirom/images/main/RDS%20autoscaling%20off.PNG)
  ![RDS и EC2 ](https://raw.githubusercontent.com/sazhirom/images/main/aws%20rds%20connect%20to%20ec2.PNG)
  ![RDS и EC2 ](https://raw.githubusercontent.com/sazhirom/images/main/ec2%20security.PNG)
</details>  
<br>

### IAM  
Create an IAM role for EC2 with S3 access and management permissions, allowing the use of console commands.  
Create a role for a user to configure AWS CLI.  

<details>
  <summary><strong>🖼️ IAM </strong></summary>

  ![EC2 role ](https://raw.githubusercontent.com/sazhirom/images/main/IAM%20EC2%20role%20for%20rds.PNG)
  ![user role](https://raw.githubusercontent.com/sazhirom/images/main/IAM%20user%20access%20creation.PNG)
</details>  
<br>

### AWS CLI Setup & Connecting to EC2 via Console with a PEM Key  
Install Amazon CLI on Windows.  
Generate access credentials for the previously created IAM user in Amazon CLI.  
Download the CSV file containing login credentials for the IAM role.  
Configure AWS CLI access using the credentials from the CSV file.  
Attempt to SSH into EC2 using the PEM key via the command line but encounter a permission error.  
Restrict permissions for the PEM key file and disable inheritance in Windows.  
Successfully log in to the EC2 instance.  

<details>
  <summary><strong>🖼️ AWS CLI setting </strong></summary>

  ![AWS CLI](https://raw.githubusercontent.com/sazhirom/images/main/AWS%20CLI%20access%20key.PNG)
  ![AWS CLI](https://raw.githubusercontent.com/sazhirom/images/main/aws%20cli%20key%20csv.PNG)
  ![AWS CLI](https://raw.githubusercontent.com/sazhirom/images/main/aws%20cli%20configure.PNG)
  ![AWS CLI](https://raw.githubusercontent.com/sazhirom/images/main/aws%20pem%20cli%20key%20heritage.PNG)
  ![AWS CLI](https://raw.githubusercontent.com/sazhirom/images/main/pem%20heritage%20disabled.PNG)
  ![AWS CLI](https://raw.githubusercontent.com/sazhirom/images/main/aws%20cli%20pem%20solved.PNG)
</details>  
<br>

  

### CloudWatch  
Monitor CPU usage while testing scripts. If CPU remains at 100% for more than 10 minutes, EC2 stops functioning properly. Optimize scripts based on CPU performance.  
Set up an alert for EC2 connection loss and configure automatic server reboot if the connection is lost for 15 minutes.  
Create an alert for the SQL database when storage usage reaches a critical level.  
Subscribe to all notifications via SNS to stay updated on system events.  

<details>
  <summary><strong>🖼️ Cloudwatch </strong></summary>

  ![Cloudwatch](https://raw.githubusercontent.com/sazhirom/images/main/cloudwatch%20CPU%20utilization.PNG)
  ![Cloudwatch](https://raw.githubusercontent.com/sazhirom/images/main/AWS%20cloud%20system%20failure%20alert.PNG)
  ![Cloudwatch](https://raw.githubusercontent.com/sazhirom/images/main/cloudwatch%20alarms.PNG)
  ![Cloudwatch](https://raw.githubusercontent.com/sazhirom/images/main/email%20alert%20cloudwatch.PNG)
  ![Cloudwatch](https://raw.githubusercontent.com/sazhirom/images/main/subscription%20cloudwatch%20confirmed.PNG)
  
</details>   
<br></br>

---
<a id="docker-section"></a>
## ~~~ 5. Docker ~~~
---
Running Google Chrome on EC2  
Container Setup & Testing:  

Create a Docker container for initial component selection and testing.  
Test it locally on PC, then push it to Docker Hub.  
Deploy and run the container on EC2.  
Optimizing File Structure:  
Use VSCode Docker extension to streamline development and deployment.  
Library Integration:  
Copy necessary libraries from similar GitHub projects to ensure compatibility and reduce setup time.  

<details>
  <summary><strong>🖼️ Docker</strong></summary>

  ![Docker](https://raw.githubusercontent.com/sazhirom/images/main/docker%20load.PNG)
  ![Docker](https://raw.githubusercontent.com/sazhirom/images/main/docker%20pull.PNG)
  ![Docker](https://raw.githubusercontent.com/sazhirom/images/main/Docker%20install.PNG)
  ![Docker](https://raw.githubusercontent.com/sazhirom/images/main/docker%20run.PNG)
</details>

<details>
  <summary><strong>📜 Docker</strong></summary>

```python
# For more information, please refer to https://aka.ms/vscode-docker-python
FROM python:3-slim

#библиотеки
RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    curl \ 
    fonts-liberation \
    libnss3 \
    libxss1 \
    libasound2 \
    libatk-bridge2.0-0 \
    libgtk-3-0 \
    libdrm2 \
    libgbm1 \
    python3-distutils 

#хром
RUN wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \
    dpkg -i google-chrome-stable_current_amd64.deb || apt-get -f install -y && \
    rm google-chrome-stable_current_amd64.deb

RUN CHROME_DRIVER_VERSION=$(curl -sS chromedriver.storage.googleapis.com/LATEST_RELEASE) && \
    wget -q "https://chromedriver.storage.googleapis.com/${CHROME_DRIVER_VERSION}/chromedriver_linux64.zip" && \
    unzip chromedriver_linux64.zip -d /usr/local/bin/ && \
    rm chromedriver_linux64.zip

ENV DISPLAY=:99

COPY requirements.txt .
RUN python -m pip install -r requirements.txt

WORKDIR /app
COPY . /app


RUN adduser -u 5678 --disabled-password --gecos "" appuser && chown -R appuser /app
USER appuser

CMD ["python3", "/app/c5game/c5game/spiders/c5game.py"]
```
</details>
<br></br>


---
<a id="bash-section"></a>
## ~~~ 6. BASH ~~~ 
---


Setting Up EC2 for Automated Data Collection & Dashboard Display  
Install all components used in the Docker container.  
Install Nano for easier text editing.  
Install and enable Cron via systemctl to automate script execution.  
Create a shell script (.sh) to sequentially run all required Python scripts.  
Automate execution with Cron:  
Night: Data collection runs.  
Daytime: Dashboard is updated and displayed.  

<details>
  <summary><strong>🖼️ Bash</strong></summary>

  ![Docker](https://raw.githubusercontent.com/sazhirom/images/main/environment%20creation.PNG)

</details>

<details>
  <summary><strong>📜 SH скрипт</strong></summary>

```bash
#!/bin/bash

# list of py files to execute
scripts=("market2.py" "buff_sell.py" "c5game.py" "buff_buyorders.py" "merge.py" "url_creator.py" "market_detailed.py" "ranging.py" "sql_load.py" "sql_cleaning.py" "folder_cleaning.py")

# number of attempts
max_attempts=2

# script
for script in "${scripts[@]}"
do
    echo "-------------------------"
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Запускаем $script..."
    attempt=1
    while [ $attempt -le $max_attempts ]
    do
        echo "$(date '+%Y-%m-%d %H:%M:%S') - Попытка $attempt..."
        python3 "$script"
        if [ $? -eq 0 ]; then
            echo "$(date '+%Y-%m-%d %H:%M:%S') - $script выполнен успешно."
            break
        else
            echo "$(date '+%Y-%m-%d %H:%M:%S') - Ошибка при выполнении $script. Попытка $attempt завершилась неудачей."
            ((attempt++))
            if [ $attempt -le $max_attempts ]; then
                echo "$(date '+%Y-%m-%d %H:%M:%S') - Повторная попытка запуска $script..."
            else
                echo "$(date '+%Y-%m-%d %H:%M:%S') - Превышено количество попыток для $script. Переходим к следующему скрипту."
            fi
        fi
    done
    sleep 5
done

echo "$(date '+%Y-%m-%d %H:%M:%S') - Все скрипты обработаны."
```
</details>


<details>
  <summary><strong>🖼️ Cron</strong></summary>

  ![Cron](https://raw.githubusercontent.com/sazhirom/images/main/crontab.PNG)

</details>
<br>

---
<a id="metabase-section"></a>
## ~~~ 7. Metabase ~~~
---
Install Metabase on the server.  
Create necessary SQL queries to display key metrics.  
Example: A SQL query using window functions to calculate daily KPI, based on purchasing items worth $500 per day.  
Schedule Metabase container start/stop using Cron to manage dashboard availability.  

<details>
  <summary><strong>🖼️ Bash</strong></summary>

  ![Docker](https://raw.githubusercontent.com/sazhirom/images/main/metabase%20started.PNG)

</details>  

<details>
  <summary><strong>📜 SQL запрос</strong></summary>
  
```sql

SELECT
 max(cumulative_profit)/500, date
FROM
  (
    WITH sales_batch AS (
      SELECT
        date,
        rating,
        market_price,
        best_price,
        market_price / best_price AS purchase_price,
        market_price - market_price / best_price as profit
      FROM
        direct
     
WHERE
        best_price != 0 and market_price < 50
       
   AND rating > 1
    ),
    cumulative_sales AS (
      SELECT
        date,
        rating,
        market_price,
        best_price,
        SUM(purchase_price) OVER (
          PARTITION BY date
         
ORDER BY
            rating
        ) AS cumulative_purchase,
        sum(profit) over (
          partition by date
          order by
            rating
        ) as cumulative_profit
      FROM
        sales_batch
    )
    SELECT
      date,
      rating,
      market_price,
      best_price,
      cumulative_purchase,
      cumulative_profit
    FROM
      cumulative_sales
    WHERE
      cumulative_purchase < 500
  ) AS "source"
  group by date
LIMIT
  1048575

```
</details> 
