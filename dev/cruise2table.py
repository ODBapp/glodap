import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import re

# Base URL for relative paths
base_url = "https://www.ncei.noaa.gov/access/ocean-carbon-acidification-data-system/oceans/"
metadata_base_url = "https://www.ncei.noaa.gov"

# URL of the GLODAPv2.2023 cruise table
url = "https://www.ncei.noaa.gov/access/ocean-carbon-acidification-data-system/oceans/GLODAPv2_2023/cruise_table_v2023.html"

# Fetch the HTML
response = requests.get(url)
soup = BeautifulSoup(response.content, 'html.parser')

# Locate the table
table = soup.find('table', id='CruiseTable')

# Extract headers
headers = [header.get_text(strip=True) for header in table.find_all('th')]

# Extract rows
data_rows = []
for row in table.find('tbody').find_all('tr'):
    cols = row.find_all('td')
    row_data = []

    for idx, col in enumerate(cols):
        if idx == 1:  # EXPOCODE/Map column
            link = col.find('a')
            expocode = link.get_text(strip=True) if link else col.get_text(strip=True)
            map_href = link['href'].replace('../', base_url) if link else np.nan
            row_data.extend([expocode, map_href])
        elif headers[idx] in ['Cruise Data Referencesa', 'Data Files', 'QC Details & Adjustments']:
            link = col.find('a')
            href = link['href'].replace('../', base_url) if link else np.nan
            row_data.append(href)
        elif headers[idx] == 'Metadata Report':
            link = col.find('a')
            href = metadata_base_url + link['href'] if link else np.nan
            row_data.append(href)
        else:
            row_data.append(col.get_text(strip=True))

    data_rows.append(row_data)

# Adjust headers to match the new structure
new_headers = headers[:1] + ['expocode', 'map'] + headers[2:]

# Create DataFrame safely
df_cruise = pd.DataFrame(data_rows, columns=new_headers[:len(data_rows[0])])

# Rename columns for clarity and consistency
df_cruise.rename(columns={
    'Region': 'region',
    'Alias': 'alias',
    'Ship': 'ship',
    'Chief Scientist': 'chief_scientist',
    'Carbon PI': 'carbon_PI',
    'Hydrography (T, S) PI': 'hydrography_PI',
    'Oxygen PI': 'oxygen_PI',
    'Nutrients PI': 'nutrients_PI',
    'CFC (CFC-11, CFC-12, CFC-113, CCl4, SF6) PI': 'cfc_PI',
    'Organics (DOC, TDN, POC, PON) PI': 'organics_PI',
    'Isotopes (C14, C13, H3, He3, He, Neon,18O, Ba) PI': 'isotopes_PI',
    'Other PI': 'other_pi',
    'Measurements in Dataset': 'measurements',
    'Cruise Data Referencesa': 'cruise_references',
    'Data Files': 'data_files',
    'Metadata Report': 'metadata_report',
    'QC Details & Adjustments': 'qc_details_adjustments'
}, inplace=True)

# Hard-coded corrections for problematic dates
hard_coded_dates = {
    '06AQ19950707': ('1995-07-07', '1995-09-20'),
    '18DL20030913': ('2003-09-13', '2003-10-14'),
    '18DL20031015': ('2003-10-15', '2003-11-25'),
    '18HS19900906': ('1990-09-06', '1990-09-19'),
    '06MM20060523': ('2006-05-23', '2006-06-28'),
    '316N19871123': ('1987-12-18', '1989-04-19'),
    '316N19961102': ('1996-11-02', '1996-12-05'),
    '31WT19841001': ('1984-10-01', '1984-10-22'),
    '33AT20120419': ('2012-04-19', '2012-05-01'),
    '45CE20090206': ('2009-02-06', '2009-02-14'),
    '45CE20100209': ('2010-02-09', '2010-02-16'),
    '90MS19811009': ('1981-10-09', '1981-11-25'),
    '316N19950829': ('1995-08-29', '1996-10-16'),
    '318M19771204': ('1977-12-04', '1978-04-24'),
    '35MF19850224': ('1985-02-24', '1987-02-20'),
    '41SS19940301': ('1994-03-01', '1995-05-03'),
    '18EN19850212': ('1981-02-11', '1981-02-11'),
    '18PZ19860711': ('1982-07-10', '1982-07-10'),
    '318M19730822': ('1973-08-22', '1974-06-09'),
    '320619960830': ('1996-08-30', '1996-09-24'),
    '320619970113': ('1997-01-13', '1997-02-11'),
    '320619970404': ('1997-04-04', '1997-05-12'),
    '33RR19971020': ('1997-10-20', '1997-11-24'),
    '49UF20090610': ('2009-06-10', '2009-08-12'),
    '49UP20131128': ('2013-11-28', '2013-12-23'),
# newly added    
    '74JC19990315': ('1999-03-15', '1999-04-23'),
    'OMEX1NS': ('1993-04', '1995-11'),
    'ZZIC2005SWYD': ('2005', '2009'),
#    '06AQ19921203': ('1992-12-03', '1993-01-22'),
#    '06MT19921227': ('1992-12-27', '1993-01-31'),
    '49HH19910813': ('1991-08-13,1991-09-17', '1991-09-01,1991-10-02'),
    '49NZ20030803': ('2003-08-03,2003-09-09', '2003-09-05,2003-10-16'),     
}

def parse_dates(date_str, expocode):
    if expocode in hard_coded_dates:
        return hard_coded_dates[expocode]

    periods = [p.strip() for p in date_str.split(';')]
    start_dates, end_dates = [], []

    for period in periods:
        try:
            if period.count('-') > 1:
                period = period.replace('-', '/', 1)

            start, end = period.split('-')

            if re.search(r'\d{4}', start):
                start_date = pd.to_datetime(start.strip(), errors='coerce')
            else:
                end_year_match = re.search(r'\d{4}', end)
                end_year = end_year_match.group()
                start_date = pd.to_datetime(f"{start.strip()}/{end_year}", errors='coerce')

            end_date = pd.to_datetime(end.strip(), errors='coerce')

            if pd.notnull(start_date) and pd.notnull(end_date):
                start_dates.append(str(start_date.date()))
                end_dates.append(str(end_date.date()))
        except Exception:
            continue

    return ",".join(start_dates), ",".join(end_dates)

df_cruise[['start_date', 'end_date']] = df_cruise.apply(lambda row: parse_dates(row['Dates'], row['expocode']), axis=1, result_type='expand')

# Drop the original Dates column
df_cruise.drop(columns=['Dates'], inplace=True)

# Replace empty strings with NaN
df_cruise.replace('', np.nan, inplace=True)

# Save to CSV
df_cruise.to_csv('./data_src/glodapv2_2023_cruise_metadata_tmp.csv', index=False)
# still need some manual cleaning, to fix dates, map link, legs, and cruise data references...

