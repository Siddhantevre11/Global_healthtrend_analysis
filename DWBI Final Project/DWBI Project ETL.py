import pandas as pd
import sqlite3

# Connecting to SQLite database
conn = sqlite3.connect('life_expectancy.db')

# Creating a cursor object using the cursor method
cursor = conn.cursor()

# SQL commands to create tables
create_country_dim = """
CREATE TABLE IF NOT EXISTS Country_Dimension (
    Country_ID INTEGER PRIMARY KEY,
    Country TEXT NOT NULL,
    Region TEXT NOT NULL,
    Economy_status_Developed INTEGER NOT NULL,
    Economy_status_Developing INTEGER NOT NULL
);
"""

create_year_dim = """
CREATE TABLE IF NOT EXISTS Year_Dimension (
    Year_ID INTEGER PRIMARY KEY,
    Year INTEGER NOT NULL
);
"""

create_fact_life_expectancy = """
CREATE TABLE IF NOT EXISTS Fact_Life_Expectancy (
    Fact_ID INTEGER PRIMARY KEY, 
    Country_ID INTEGER,
    Year_ID INTEGER,
    Life_expectancy REAL,
    Adult_mortality REAL,
    Under_five_deaths REAL,
    Infant_deaths REAL,
    Alcohol_consumption REAL,
    GDP_per_capita INTEGER,
    Population_mln REAL,
    Thinness_ten_nineteen_years REAL,
    Thinness_five_nine_Years REAL,
    BMI REAL,
    Hepatitis_B INTEGER,
    Polio INTEGER,
    Diphtheria INTEGER,
    Incidents_HIV REAL,
    Measles INTEGER,
    Schooling REAL,

    FOREIGN KEY (Country_ID) REFERENCES Country_Dimension(Country_ID),
    FOREIGN KEY (Year_ID) REFERENCES Year_Dimension(Year_ID)
);
"""

# Executing the SQL commands
cursor.execute(create_country_dim)
cursor.execute(create_year_dim)
cursor.execute(create_fact_life_expectancy)

# Loading the transformed dataset
file_path = '/Users/vikramjeet/Downloads/DWBI Final Project/Life-Expectancy-Data-Updated.csv'
data = pd.read_csv(file_path)

# Printing all column names
print(data.columns.tolist())

# Creating mappings for Country, Year
country_mapping = {country: idx for idx, country in enumerate(data['Country'].unique(), 1)}
year_mapping = {year: idx for idx, year in enumerate(data['Year'].unique(), 1)}

# Applying mappings to the data
data['Country_ID'] = data['Country'].map(country_mapping)
data['Year_ID'] = data['Year'].map(year_mapping)

# Creating a DataFrame for the country dimension
country_df = data[['Country', 'Region', 'Economy_status_Developed', 'Economy_status_Developing']].drop_duplicates()
country_df['Country_ID'] = country_df['Country'].map(country_mapping)

# Inserting data into Country_Dimension
country_df.to_sql('Country_Dimension', conn, if_exists='append', index=False)

# Inserting data into Year_Dimension
year_df = pd.DataFrame(list(year_mapping.items()), columns=['Year', 'Year_ID'])
year_df.to_sql('Year_Dimension', conn, if_exists='append', index=False)

# Preparing the data for Fact_Life_Expectancy
fact_columns = [
    'Country_ID', 'Year_ID', 'Life_expectancy', 'Adult_mortality',
    'Infant_deaths', 'Alcohol_consumption', 'Hepatitis_B', 'Measles',
    'BMI', 'Under_five_deaths', 'Polio', 'Diphtheria',
    'Incidents_HIV', 'GDP_per_capita', 'Population_mln', 'Thinness_ten_nineteen_years', 'Thinness_five_nine_years',
    'Schooling']

fact_data = data[fact_columns]

# Inserting data into Fact_Life_Expectancy
fact_data.to_sql('Fact_Life_Expectancy', conn, if_exists='append', index=False)

# Exporting DataFrame to CSV
output_file_path = '/Users/vikramjeet/downloads/DWBI Final Project/dwbi project ETL dataset updated.csv'
data.to_csv(output_file_path, index=False)

# Committing the changes and closing the connection
conn.commit()
conn.close()

print("Data loading and exporting completed successfully.")
