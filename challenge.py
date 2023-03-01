#Import modules
import pandas as pd
import sqlite3
import datetime
import numpy as np

#Create usefull functions
def trim_all_columns(df):
    """
    Trim whitespace from ends of each value across all series in dataframe
    """
    trim_strings = lambda x: x.strip() if isinstance(x, str) else x
    return df.applymap(trim_strings)
def column_to_upper_case(column_name):
    """
    Convert strings to upper case for column in dataframe
    """
    upper_case_strings = column_name.str.upper()
    return upper_case_strings
def remove_space_from_column(column_name):
    """
    Removes white space for column in dataframe
    """
    remove_space_in_strings = column_name.str.replace(' ', '')
    return remove_space_in_strings
def column_to_lower_case(column_name):
    """
    Convert strings to upper case for column in dataframe
    """
    lower_case_strings = column_name.str.lower()
    return lower_case_strings


#Read in the csv, clean the data and return the data in dataframes
def read_csv_and_create_dfs(csv_path):
    sales_df = pd.read_csv(csv_path)
    sales_df_cleaned = trim_all_columns(sales_df)
    sales_df_cleaned['PaymentDate'] = pd.to_datetime(sales_df_cleaned['PaymentDate'], dayfirst=True)
    sales_df_cleaned['DeliveryCountry']= column_to_upper_case(sales_df_cleaned['DeliveryCountry'])
    sales_df_cleaned['DeliveryCountry'] = np.where((sales_df_cleaned['DeliveryCountry']  == 'UK') | (sales_df_cleaned['DeliveryCountry'] == 'UNITED KINGDOM'), 'UNITED KINGDOM', 'INTERNATIONAL')
    sales_df_cleaned['DeliveryPostcode'] = remove_space_from_column(sales_df_cleaned['DeliveryPostcode'])
    sales_df_cleaned['DeliveryPostcode'] = column_to_upper_case(sales_df_cleaned['DeliveryPostcode'])
    sales_df_cleaned['ClientName'] = column_to_lower_case(sales_df_cleaned['ClientName'])
    sales_df_cleaned['DeliveryAddress'] = column_to_lower_case(sales_df_cleaned['DeliveryAddress'])
    sales_df_cleaned['DeliveryCity'] = column_to_lower_case(sales_df_cleaned['DeliveryCity'])
    sales_df_cleaned['DeliveryContactNumber'] = remove_space_from_column(sales_df_cleaned['DeliveryContactNumber'])
    sales_df_cleaned = sales_df_cleaned .assign(TransactionId = sales_df_cleaned.OrderNumber)
    sales_df_cleaned[['PO', 'OrderNumber']] = sales_df_cleaned['OrderNumber'].str.split('-',expand=True)
    
    #Create Product, Customer and Transaction dataframesproduct_dim_df = pd.DataFrame().assign(ProductName=sales_df_cleaned['ProductName'], ProductType=sales_df_cleaned['ProductType'], UnitPrice=sales_df_cleaned['UnitPrice'])
    product_dim_df = sales_df_cleaned[['ProductName', 'ProductType', 'UnitPrice', 'OrderNumber']]
    product_dim_df = product_dim_df.groupby(['ProductName', 'ProductType', 'UnitPrice']).count()
    cust_dim_df = sales_df_cleaned[['PO', 'ClientName', 'DeliveryAddress', 'DeliveryCity', 'DeliveryPostcode', 'DeliveryContactNumber', 'OrderNumber']]
    cust_dim_df = cust_dim_df.groupby(['PO', 'ClientName', 'DeliveryAddress', 'DeliveryCity', 'DeliveryPostcode', 'DeliveryContactNumber']).count()
    transaction_df = sales_df_cleaned[['PO', 'TransactionId', 'OrderNumber', 'PaymentType', 'PaymentBillingCode', 'PaymentDate', 'ProductName', 'ProductQuantity', 'TotalPrice', 'Currency']]

    return product_dim_df, cust_dim_df, transaction_df

cleaned_dfs = read_csv_and_create_dfs('sales.csv')





#Create a connection with the SQLite db, create tables, insert or update data    
def create_db_connection_and_tables(db_path, df_for_products, df_for_customers, df_for_transactions):
    try:
        #Create Connection, if name already exists it will connect, if not, it will create it
        sqliteConnection = sqlite3.connect(db_path)
        print("Database created and Successfully Connected to SQLite")
        
        #Create tables if they don't exist
        sql_create_product_dim = '''CREATE TABLE IF NOT EXISTS ProductsDim (
                                              ProductName TEXT PRIMARY KEY,
                                              ProductType TEXT NOT NULL,
                                              UnitPrice REAL NOT NULL);'''
        sql_create_customers_dim = '''CREATE TABLE IF NOT EXISTS CustomersDim (
                                            PO TEXT PRIMARY KEY,
                                            ClientName TEXT NOT NULL,
                                            DeliveryAddress TEXT NOT NULL,
                                            DeliveryCity TEXT NOT NULL,
                                            DeliveryPostcode TEXT NOT NULL,
                                            DeliveryCountry TEXT NOT NULL,
                                            DeliveryContactNumber TEXT NULL)'''
        sql_create_transaction_fact = '''CREATE TABLE IF NOT EXISTS TransactionsFact (
                                                TranscationId TEXT PRIMARY KEY,
                                                OrderNumber TEXT NOT NULL,
                                                ClientName TEXT NOT NULL,
                                                ProductQuantity INTEGER NOT NULL,
                                                TotalPrice REAL NOT NULL,
                                                Currency TEXT NOT NULL,
                                                PaymentType TEXT NOT NULL,
                                                PaymentBillingCode TEXT NOT NULL,
                                                PaymentDate DATE NOT NULL,
                                                FOREIGN KEY(PO) REFERENCES CustomersDim(PO),
                                                FOREIGN KEY(ProductName) REFERENCES ProductsDim(ProductName));'''
        
        #Curser allows execution of SQLite commands and queries from python
        cursor = sqliteConnection.cursor()
        #Execute carries out the SQL query, commit
        cursor.execute(sql_create_product_dim)
        sqliteConnection.commit()
        print("ProductDim table created")
        cursor.execute(sql_create_customers_dim)
        sqliteConnection.commit()
        print("CustomersDim table created")
        cursor.execute(sql_create_transaction_fact)
        sqliteConnection.commit()
        print("TransactionsFact table created")
        
        
        
        #Insert DataFrames into SQL Tables
        df_for_products.to_sql("ProductsDim", sqliteConnection, if_exists="replace")
        print("ProductsDim table updated")
        df_for_customers.to_sql("CustomersDim", sqliteConnection, if_exists="replace")
        print("CustomersDim table updated")
        df_for_transactions.to_sql("TransactionsFact", sqliteConnection, if_exists="replace")
        print("TransactionsFact table updated")
        
        
        #These dataframes and print statements are for checking purposes only and can be removed once finalised
        ProductTable = pd.read_sql('SELECT * FROM ProductsDim', sqliteConnection)
        print(ProductTable)
        CustomerTable = pd.read_sql('SELECT * FROM CustomersDim', sqliteConnection)
        print(CustomerTable)
        TransactionsTable = pd.read_sql('SELECT * FROM TransactionsFact', sqliteConnection)
        print(CustomerTable)

        
        cursor.close()
        return sqliteConnection

    except sqlite3.Error as error:
        print("Error while connecting to sqlite or creating tables", error)
    finally:
        if sqliteConnection:
            sqliteConnection.close()
            print("The SQLite connection is closed")

create_db_connection_and_tables('SQLite_Python.db', cleaned_dfs[0], cleaned_dfs[1], cleaned_dfs[2])

