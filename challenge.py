import pandas as pd
import sqlite3

class SalesData:
    
    def __init__(self, csv_path, db_path):
        self.csv_path = csv_path
        self.db_path = db_path
        
    def read_csv(self):
        # Read csv file and convert PaymentDate column to date type
        sales_df = pd.read_csv(self.csv_path)
        sales_df['PaymentDate'] = pd.to_datetime(sales_df['PaymentDate']).dt.date
        return sales_df
    
    def create_connection(self):
        # Create a connection to the SQLite database
        conn = sqlite3.connect(self.db_path)
        return conn
    
    def create_dimension_tables(self):
        # Create product dimension table
        conn = self.create_connection()
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS Product (
                    ProductID INTEGER PRIMARY KEY,
                    ProductName TEXT NOT NULL,
                    ProductType TEXT NOT NULL,
                    UnitPrice REAL NOT NULL
                    )''')
        conn.commit()
        conn.close()
    
    def update_dimension_tables(self, sales_df):
        # Update product dimension table
        conn = self.create_connection()
        c = conn.cursor()
        for index, row in sales_df.iterrows():
            c.execute('''INSERT OR IGNORE INTO Product (
                        ProductName, ProductType, UnitPrice)
                        VALUES (?, ?, ?)''', (row['ProductName'], row['ProductType'], row['UnitPrice']))
        conn.commit()
        conn.close()
    
    def create_fact_table(self, sales_df):
        # Create fact table
        conn = self.create_connection()
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS Customer (
                    OrderNumber TEXT PRIMARY KEY,
                    ClientName TEXT NOT NULL,
                    ProductID INTEGER NOT NULL,
                    ProductQuantity INTEGER NOT NULL,
                    TotalPrice REAL NOT NULL,
                    Currency TEXT NOT NULL,
                    DeliveryAddress TEXT NOT NULL,
                    DeliveryCity TEXT NOT NULL,
                    DeliveryPostcode TEXT NOT NULL,
                    DeliveryCountry TEXT NOT NULL,
                    DeliveryContactNumber TEXT NOT NULL,
                    PaymentType TEXT NOT NULL,
                    PaymentBillingCode TEXT NOT NULL,
                    PaymentDate DATE NOT NULL,
                    FOREIGN KEY(ProductID) REFERENCES Product(ProductID)
                    )''')
        
        # Update customer fact table
        for index, row in sales_df.iterrows():
            c.execute('''INSERT OR IGNORE INTO Customer (
                        OrderNumber, ClientName, ProductID, ProductQuantity,
                        TotalPrice, Currency, DeliveryAddress, DeliveryCity,
                        DeliveryPostcode, DeliveryCountry, DeliveryContactNumber,
                        PaymentType, PaymentBillingCode, PaymentDate)
                        VALUES (?, ?, (SELECT ProductID FROM Product WHERE ProductName=?), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', 
                      (row['OrderNumber'], row['ClientName'], row['ProductName'], row['ProductQuantity'], 
                       row['TotalPrice'], row['Currency'], row['DeliveryAddress'], row['DeliveryCity'],
                       row['DeliveryPostcode'], 'UK' if row['DeliveryCountry'] == 'United Kingdom' else row['DeliveryCountry'],
                       row['DeliveryContactNumber'], row['PaymentType'], row['PaymentBillingCode'], row['PaymentDate']))
        conn.commit()
        conn.close()
    
    def run(self):
        # Read csv file
        sales_df = self.read_csv()
        
        # Create and update dimension tables
        self.create_dimension_tables()
        self.update_dimension_tables(sales_df)
        
        # Create and update fact table
        self.create_fact_table(sales_df)
