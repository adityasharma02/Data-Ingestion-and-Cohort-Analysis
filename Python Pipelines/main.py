import pandas as pd
from sqlalchemy import create_engine
import logging

# Configure logging
logging.basicConfig(filename='pipeline.log', level=logging.INFO)


# Function to load data into MySQL database
def load_data_to_database(users_df, orders_df, products_df, db_url):
    try:
        # Create a MySQL engine using sqlalchemy
        engine = create_engine(db_url)

        # Write dataframes to MySQL tables
        users_df.to_sql('users', con=engine, index=False, if_exists='replace')
        orders_df.to_sql('orders', con=engine, index=False, if_exists='replace')
        products_df.to_sql('products', con=engine, index=False, if_exists='replace')

        logging.info("Data loaded into the database successfully.")

    except Exception as e:
        logging.error(f"Error loading data into the database: {str(e)}")


# Function to handle missing values and duplicates
def preprocess_data(df):
    # Handling missing values
    df.fillna(0, inplace=True)  # Replace NaN values with 0

    # Handling duplicates
    df.drop_duplicates(inplace=True)


# Read CSV files into dataframes
users_df = pd.read_csv('C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Users.csv')
orders_df = pd.read_csv('C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Orders.csv')
products_df = pd.read_csv('C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Products.csv')

# Preprocess data
preprocess_data(users_df)
preprocess_data(orders_df)
preprocess_data(products_df)

# Load data into the database
db_url = 'mysql://root:Ads%4054321@localhost/ecommerce2'      # url to our database with credentials
load_data_to_database(users_df, orders_df, products_df,db_url)
