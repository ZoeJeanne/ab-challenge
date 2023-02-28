import pandas as pd
import sqlite3
import datetime

#Read in the csv, clean the data and return the data in a dataframe
def read_csv_and_clean(csv_path):
    sales_df = pd.read_csv(csv_path)
    sales_df['PaymentDate'] = pd.to_datetime(sales_df['PaymentDate'], dayfirst=True)
    
    #Create Product, Custoemr and Transaction dataframes
    
    print(sales_df)
    #return sales_df

#read_csv_and_clean('sales.csv')



#Create a connection with the SQLite db, create tables     
def create_db_connection_and_tables(db_path):
    try:
        #Create Connection, if name already exists it will connect, if not, it will create it
        sqliteConnection = sqlite3.connect(db_path)
        print("Database created and Successfully Connected to SQLite")
        
        #Create tables if they don't exist
        sql_create_product_dim = '''CREATE TABLE IF NOT EXISTS ProductsDim (
                                              ProductID INTEGER PRIMARY KEY,
                                              ProductName TEXT NOT NULL,
                                              ProductType TEXT NOT NULL,
                                              UnitPrice REAL NOT NULL);'''
        sql_create_customers_dim = '''CREATE TABLE IF NOT EXISTS CustomersDim (
                                            ClientName TEXT PRIMARY KEY,
                                            DeliveryAddress TEXT NOT NULL,
                                            DeliveryCity TEXT NOT NULL,
                                            DeliveryPostcode TEXT NOT NULL,
                                            DeliveryCountry TEXT NOT NULL,
                                            DeliveryContactNumber TEXT NULL)'''
        sql_create_transaction_fact = '''CREATE TABLE IF NOT EXISTS TransactionsFact (
                                                OrderNumber TEXT PRIMARY KEY,
                                                ClientName TEXT NOT NULL,
                                                ProductID INTEGER NOT NULL,
                                                ProductQuantity INTEGER NOT NULL,
                                                TotalPrice REAL NOT NULL,
                                                Currency TEXT NOT NULL,
                                                PaymentType TEXT NOT NULL,
                                                PaymentBillingCode TEXT NOT NULL,
                                                PaymentDate DATE NOT NULL,
                                                FOREIGN KEY(ProductID) REFERENCES ProductsDim(ProductID));'''
        
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
        cursor.close()
        
        
        # do this next to get df into table
        df.to_sql(table_name,conn,if_exists='replace',index=False)
        conn.commit()
        conn.close()

        return sqliteConnection

    except sqlite3.Error as error:
        print("Error while connecting to sqlite or creating tables", error)
    #finally:
    #    if sqliteConnection:
    #        sqliteConnection.close()
    #        print("The SQLite connection is closed")

create_db_connection_and_tables('SQLite_Python.db')

    
    #def create_dimension_tables(self):
        # Create product dimension table
        #conn = self.create_connection()
        #c = conn.cursor()
        #c.execute('''CREATE TABLE IF NOT EXISTS Product (
          #          ProductID INTEGER PRIMARY KEY,
         #           ProductName TEXT NOT NULL,
        #            ProductType TEXT NOT NULL,
       #             UnitPrice REAL NOT NULL
      #              )''')
     #   conn.commit()
    #    conn.close()
    
    #def update_dimension_tables(self, sales_df):
        # Update product dimension table
        #conn = self.create_connection()
        #c = conn.cursor()
        #for index, row in sales_df.iterrows():
        #    c.execute('''INSERT OR IGNORE INTO Product (
       #                 ProductName, ProductType, UnitPrice)
      #                  VALUES (?, ?, ?)''', (row['ProductName'], row['ProductType'], row['UnitPrice']))
     #   conn.commit()
    #    conn.close()
    
    #def create_fact_table(self, sales_df):
        # Create fact table
        #conn = self.create_connection()
        #c = conn.cursor()
        #c.execute('''CREATE TABLE IF NOT EXISTS Customer (
          #          OrderNumber TEXT PRIMARY KEY,
         #           ClientName TEXT NOT NULL,
        #            ProductID INTEGER NOT NULL,
                    #ProductQuantity INTEGER NOT NULL,
                   # TotalPrice REAL NOT NULL,
                  #  Currency TEXT NOT NULL,
                 #   DeliveryAddress TEXT NOT NULL,
                #    DeliveryCity TEXT NOT NULL,
               #     DeliveryPostcode TEXT NOT NULL,
              #      DeliveryCountry TEXT NOT NULL,
             #       DeliveryContactNumber TEXT NOT NULL,
            #        PaymentType TEXT NOT NULL,
           #         PaymentBillingCode TEXT NOT NULL,
          #          PaymentDate DATE NOT NULL,
         #           FOREIGN KEY(ProductID) REFERENCES Product(ProductID)
        #            )''')
        
        # Update customer fact table
        #for index, row in sales_df.iterrows():
            #c.execute('''INSERT OR IGNORE INTO Customer (
                 #       OrderNumber, ClientName, ProductID, ProductQuantity,
                #        TotalPrice, Currency, DeliveryAddress, DeliveryCity,
               #         DeliveryPostcode, DeliveryCountry, DeliveryContactNumber,
              #          PaymentType, PaymentBillingCode, PaymentDate)
             #           VALUES (?, ?, (SELECT ProductID FROM Product WHERE ProductName=?), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', 
            #          (row['OrderNumber'], row['ClientName'], row['ProductName'], row['ProductQuantity'], 
           #            row['TotalPrice'], row['Currency'], row['DeliveryAddress'], row['DeliveryCity'],
          #             row['DeliveryPostcode'], 'UK' if row['DeliveryCountry'] == 'United Kingdom' else row['DeliveryCountry'],
         #              row['DeliveryContactNumber'], row['PaymentType'], row['PaymentBillingCode'], row['PaymentDate']))
        #conn.commit()
        #conn.close()
    
    #def run(self):
        # Read csv file
       # sales_df = self.read_csv()
        
        # Create and update dimension tables
        #self.create_dimension_tables()
        #self.update_dimension_tables(sales_df)
        
        # Create and update fact table
        #self.create_fact_table(sales_df)
        
