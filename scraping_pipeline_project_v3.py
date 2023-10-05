#!/usr/bin/env python
# coding: utf-8

# In[1]:


from io import StringIO
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
import sqlalchemy
import psycopg2
import requests
import os
import cufflinks as cf # library that binds plotly directly to pandas dfs, allowing visualisations to be created much easier
import pandas as pd
import numpy as np


# In[2]:


# 1. Establishing a connection to PostgreSQL database and creating a cursor object to interact with the database

conn = psycopg2.connect(
    host="data-sandbox.c1tykfvfhpit.eu-west-2.rds.amazonaws.com",
    database="pagila",
    user="de8_viag13",
    password="EKvkz34,",
    port="5432",
    options="-c search_path=student"
    )

cur = conn.cursor()

conn.set_session(autocommit=True)


# In[3]:


# 2. Creating table in database - if not already created

cur.execute("""
    CREATE TABLE IF NOT EXISTS footballer_stats_va (
        player VARCHAR,
        nation VARCHAR,
        position VARCHAR,
        team VARCHAR,
        league VARCHAR, 
        age INT,
        matches_played INT,
        minutes_played INT,
        goals INT,
        assists INT,
        goals_and_assists INT, 
        non_pk_goals INT,
        pk_goals INT,
        pks_taken INT,
        expected_goals FLOAT,
        non_pk_expected_goals FLOAT,
        expected_assisted_goals FLOAT,
        non_PK_expected_goals_and_assisted_goals FLOAT,
        progressive_carries INT,
        progressive_passes INT,
        progressive_passes_received INT,
        goals_per_90 FLOAT,
        assists_per_90 FLOAT, 
        goals_and_assists_per_90 FLOAT,
        non_pk_goals_per_90 FLOAT,
        non_PK_goals_and_assists_per_90 FLOAT,
        expected_goals_per_90 FLOAT,
        expected_assisted_goals_per_90 FLOAT,
        expected_goals_and_assisted_goals_per_90 FLOAT,
        non_pk_expected_goals_per_90 FLOAT,
        non_pk_expected_goals_and_assisted_goals_per_90 FLOAT,
        unique_identifier VARCHAR
    );"""                              
)


# In[4]:


flag_file = "data_loaded.flag"

if not os.path.exists(flag_file): # Below will be skipped after initial loading of data

# 3. Create - and concatenate - pandas dataframes based on scraped football stats data

    player_stats_22 = "https://fbref.com/en/comps/Big5/2022-2023/stats/players/2022-2023-Big-5-European-Leagues-Stats"
    player_data_22 = requests.get(player_stats_22)
    stats_22 = pd.read_html(player_data_22.text, match="Player Standard Stats")[0] # using string matching to only select (the first) HTML table containing Player Standard Stats
    stats_22.columns = stats_22.columns.droplevel()
    stats_22['UID'] = stats_22['Player'] + '_22/23'

    player_stats_21 = "https://fbref.com/en/comps/Big5/2021-2022/stats/players/2021-2022-Big-5-European-Leagues-Stats"
    player_data_21 = requests.get(player_stats_21)
    stats_21 = pd.read_html(player_data_21.text, match="Player Standard Stats")[0]
    stats_21.columns = stats_21.columns.droplevel()
    stats_21['UID'] = stats_21['Player'] + '_21/22'

    player_stats_master = pd.concat([stats_21, stats_22], ignore_index=True)

# 4. Clean concatenated dataframe

# a. fill in values for players with NaN nation entries

    condition_one = (player_stats_master['Player'] == 'Wahid Faghir') & (player_stats_master['Nation'].isna())
    condition_two = (player_stats_master['Player'] == "Mamadou N'Diaye") & (player_stats_master['Nation'].isna())
    condition_three = (player_stats_master['Player'] == 'Blanco') & (player_stats_master['Nation'].isna())
    condition_four = (player_stats_master['Player'] == 'Hugo Guillamón') & (player_stats_master['Nation'].isna())
    condition_five = (player_stats_master['Player'] == 'Gabriel Strefezza') & (player_stats_master['Nation'].isna())

    player_stats_master.loc[condition_one, 'Nation'] = 'DEN'
    player_stats_master.loc[condition_two, 'Nation'] = 'SEN'
    player_stats_master.loc[condition_three, 'Nation'] = 'ESP'
    player_stats_master.loc[condition_four, 'Nation'] = 'ESP'
    player_stats_master.loc[condition_five, 'Nation'] = 'BRA'

# b. delete excess rows containing no data and also excess columns

    player_stats_master.drop(player_stats_master[player_stats_master['MP'] == 'MP'].index, inplace=True)
    player_stats_master.drop(columns=['Rk', 'Born', 'Starts', '90s', 'CrdY', 'CrdR', 'Matches'], inplace=True)

# c. clean nation, comp, and age columns 

    player_stats_master['Nation'] = player_stats_master['Nation'].apply(lambda x: x.split()[1] if len(x) == 2 else x[0])
    player_stats_master['Comp'] = player_stats_master['Comp'].apply(lambda x: ' '.join(x.split()[1:]))
    player_stats_master['Age'] = player_stats_master['Age'].apply(lambda x: x.split('-')[0])

# 5. Copy dataframe into SQL table

    output = StringIO() # provides an in-memory file-like object
    player_stats_master.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0) # resetting the pointer to the beginning of the file-like object, so that the cursor can start copying from there
    cur.copy_from(output, 'footballer_stats_va', null="")

# 6. Create an empty 'flag' file to signal that initial data loading has been completed

    with open(flag_file, 'w') as f:
        f.write('')


# In[5]:


# 7. Creating a secondary dataframe that captures scraped data from ongoing season - this acts as a staging layer where continually updated data can begin to be processed. As a first step, similar
#    data cleaning is carried out as before, i.e., fill in NaN values, deleting excess rows, and columns, as well as refining formatting in other columns.

player_stats_23 = "https://fbref.com/en/comps/Big5/stats/players/Big-5-European-Leagues-Stats"
player_data_23 = requests.get(player_stats_23)
stats_23 = pd.read_html(player_data_23.text, match="Player Standard Stats")[0]
stats_23.columns = stats_23.columns.droplevel()
stats_23['unique_identifier'] = stats_23['Player'] + '_23/24'

stats_23.fillna(value={'Nation': 'ESP'}, inplace=True)

stats_23.drop(stats_23[stats_23['MP'] == 'MP'].index, inplace=True)
stats_23.drop(columns=['Rk', 'Born', 'Starts', '90s', 'CrdY', 'CrdR', 'Matches'], inplace=True)

stats_23['Nation'] = stats_23['Nation'].apply(lambda x: x.split()[1] if len(x) == 2 else x[0])
stats_23['Comp'] = stats_23['Comp'].apply(lambda x: ' '.join(x.split()[1:]))
stats_23['Age'] = stats_23['Age'].apply(lambda x: x.split('-')[0])


# In[6]:


# 8. Final step in data cleaning - ensuring staging layer dataframe columns are consistent with SQL table columns

new_names = ['player', 'nation', 'position', 'team', 'league', 'age', 'matches_played', 'minutes_played', 'goals', 'assists', 'goals_and_assists', 'non_pk_goals', 'pk_goals', 'pks_taken', 'expected_goals', 'non_pk_expected_goals', 'expected_assisted_goals', 'non_pk_expected_goals_and_assisted_goals', 'progressive_carries', 'progressive_passes', 'progressive_passes_received', 'goals_per_90', 'assists_per_90', 'goals_and_assists_per_90', 'non_pk_goals_per_90', 'non_pk_goals_and_assists_per_90', 'expected_goals_per_90', 'expected_assisted_goals_per_90', 'expected_goals_and_assisted_goals_per_90', 'non_pk_expected_goals_per_90', 'non_pk_expected_goals_and_assisted_goals_per_90', 'unique_identifier']

for col, new_name in zip(range(33), new_names):
    stats_23.columns.values[col] = new_name


# In[7]:


# 9. Comparison of existing data in SQL table with scraped data to ensure only new changes - the delta of the data - are captured

connection_string = "postgresql+psycopg2://de8_viag13:EKvkz34,@data-sandbox.c1tykfvfhpit.eu-west-2.rds.amazonaws.com:5432/pagila"
engine = sqlalchemy.create_engine(connection_string)
metadata = sqlalchemy.MetaData(bind=engine)
master_data = pd.read_sql('SELECT * FROM footballer_stats_va', engine) # load the SQL table into a dataframe for easier comparison with staging layer dataframe
master_data_table = sqlalchemy.Table('footballer_stats_va', metadata, autoload=True) 

# There are two scenarios that need to be accounted for in terms of updates to our dataset - i) new player data, and ii) updated data for an existing player

new_rows = stats_23[~stats_23['unique_identifier'].isin(master_data['unique_identifier'])] # scenario one is captured here by comparing UIDs between master table and staging layer data
new_rows.to_sql('footballer_stats_va', engine, if_exists='append', index=False)

stats_23 = stats_23.set_index('unique_identifier') # scenario two is captured here by indicating which rows across the two datasets with a common UID have at least one column with a different value
master_data = master_data.set_index('unique_identifier')

merged = stats_23.merge(master_data, left_index=True, right_index=True, how='right', suffixes=('_stats', '_master')) # merge two datasets but include suffixes to specify source
columns = [col.replace('_stats', '') for col in merged.filter(like='_stats').columns] 
mask = np.any([merged[col + '_stats'] != merged[col + '_master'] for col in columns], axis=0)
diff_rows = merged[mask]

for UID, row in diff_rows.iterrows():
    updates = row.filter(like='_stats').to_dict()
    updates_clean = {key.replace('_stats', ''): value for key, value in updates.items()}
    master_data_table.update().where(master_data_table.c.unique_identifier == UID).values(**updates_clean)

cur.close()
conn.close()


# In[8]:


# 10. Filter for best forwards with SQL 

best_forwards = pd.read_sql('''
    SELECT 
        player, 
        AVG(assists_per_90) p90_assists, 
        AVG(non_pk_goals_per_90) p90_non_pk_goals
    FROM 
        footballer_stats_va
    WHERE
        matches_played > 10 AND
        minutes_played > 900 AND
        goals_and_assists > 10 
    GROUP BY 
        player 
    ORDER BY 
        (AVG(assists_per_90) + AVG(non_pk_goals_per_90)) DESC
    LIMIT
        250
        ;'''
        , engine)


# In[9]:


# 11. Filter for best creative players with SQL 

best_creatives = pd.read_sql('''
    SELECT 
        player, 
        AVG(expected_assisted_goals_per_90) p90_xAG, 
        SUM(progressive_passes) progressive_passes
    FROM 
        footballer_stats_va
    WHERE
        matches_played > 10 AND
        minutes_played > 900 
    GROUP BY 
        player 
    ORDER BY 
        (AVG(expected_assisted_goals_per_90) + SUM(progressive_passes)) DESC
    LIMIT
        250
        ;'''
        , engine)


# In[10]:


best_creatives.dropna(inplace=True)


# In[11]:


# 12. Visualisation of the best forwards 

init_notebook_mode(connected=True)
cf.go_offline()

best_forwards.iplot(kind='scatter',x='p90_non_pk_goals',y='p90_assists',
           mode='markers',text='player',size=10,
          xTitle='Non-penalty goals per 90',yTitle='Assists per 90', title='Best forwards across top 5 leagues in past three seasons') 


# In[12]:


# 13. Visualisation of the best creative players

init_notebook_mode(connected=True)
cf.go_offline()

best_creatives.iplot(kind='scatter',x='p90_xag',y='progressive_passes',
           mode='markers',text='player',size=10,
          xTitle='Expected assisted goals per 90',yTitle='Progressive passes', title='Most creative players across top 5 leagues in past three seasons') 

