# tablemaster
A Python package makes it easy to manage tables anywhere.

# Install
```
pip install tablemaster
```

# Preparation
### To use the function related to mysql, need to put a file named cfg.yaml in the same path, which is like:
```
 db_name_example:
   name: db_name_example
   user: user_name_example
   password: pw_example
   host: host_example
   database: db_example
```

### To use the function related to google sheet, need to put a file named client_secret.json in the same path, here is the guide:
1. Go to the Google Developers Console at https://console.developers.google.com/
2. Create a new project by clicking on the drop-down menu at the top of the page and selecting "New Project".
3. Enter a name for your project and click on the "Create" button.
4. Select your project from the drop-down menu at the top of the page and click on the "Dashboard" button.
5. Click on the "Enable APIs and Services" button.
6. Search for "Google Sheets API" and click on the "Enable" button.
7. Click on the "Create Credentials" button.
8. Select "OAuth client ID" as the type of credentials to create.
9. Choose "Desktop App" as the application type and enter a name for your OAuth client ID.
10. Click on the "Create" button.
11. Click on the "Download" button next to your new OAuth client ID.
12. Rename the downloaded file to "client_secret.json" and save it to the directory where your script is located.

# Examples

## import
```
import tablemaster as tm
```

## Query from mysql
```
sql_query = 'SELECT * FROM table_name LIMIT 20'
df = tm.query(sql_query, tm.cfg.db_name)
df
```

## Change column name
```
sql_query = ('ALTER TABLE table_name RENAME COLUMN column1 TO column2')
tm.opt(sql_query, tm.cfg.db_name)
```

## create a table in mysql and upload data from dataframe df
```
tb = tm.Manage_table('table_name_2', tm.cfg.db1)
tb.upload_data(df)
```

## delete a table in mysql
```
tb = tm.Manage_table('table_name_2', tm.cfg.db1)
tb.delete_table()
```

## read table from google sheet
```
google_sheet = ('GoogleSheet Table Name', 'GoogleSheet Sheet Name')
df = tm.gs_read_data(google_sheet)
df
```

## write data df to google sheet
```
google_sheet = ('GoogleSheet Table Name', 'GoogleSheet Sheet Name')
tm.gs_write_data(google_sheet, df)
```