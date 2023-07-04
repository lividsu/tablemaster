# tablemaster
A Python package makes it easy to manage tables anywhere.

# Install
```
pip install -U tablemaster
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

### To use the function related to google sheet, here is the guide:
https://docs.gspread.org/en/latest/oauth2.html

# Examples

## import
```
import tablemaster as tm
```

## query from mysql
```
sql_query = 'SELECT * FROM table_name LIMIT 20'
df = tm.query(sql_query, tm.cfg.db_name)
df
```

## import one file from local
```
df = tm.read("*Part of File Name*")
df
```

## batch import and merge
```
df = tm.batch_read("*Part of File Name*")
df
```

## batch import without merging
```
df = tm.read_dfs("*Part of File Name*")
df
```

## change column name
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

## delete rows in mysql with condition
```
tb = tm.Manage_table('table_name_2', tm.cfg.db1)
tb.par_del("order_date > '2023-01-01' ")
```

## read table from google sheet
```
google_sheet = ('GoogleSheet Table Name', 'GoogleSheet Sheet Name')
df = tm.gs_read_df(google_sheet)
df
```

## write data df to google sheet
```
google_sheet = ('GoogleSheet Table Name', 'GoogleSheet Sheet Name')
tm.gs_write_df(google_sheet, df)
```
