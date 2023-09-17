import psycopg2
import pandas as pd


def main():
    host = "postgres"
    database = "postgres"
    user = "postgres"
    pas = "postgres"
    conn = psycopg2.connect(host=host, database=database, user=user, password=pas)
    dfaccounts=pd.read_csv("data/accounts.csv")
    dfproducts=pd.read_csv("data/products.csv")
    dftransactions=pd.read_csv("data/transactions.csv")
    # print(dfproducts["product_id"])
    # print(dftransactions["transaction_id"].iloc[0])

 
    create_table_accounts="CREATE TABLE accounts(customer_id PRIMARY KEY AUTO_INCREMENT, first_name VARCHAR(20), last_name VARCHAR(20), address_1 VARCHAR(20), address_2 VARCHAR(20), city VARCHAR(20), state VARCHAR(20), zip_code INT, join_date INT)"
    create_table_products="CREATE TABLE products(product_id PRIMARY KEY AUTO_INCREMENT, product_code int, product_description VARCHAR(100))"
    create_table_transactions="CREATE TABLE transactions(transaction_id int PRIMARY KEY AUTO_INCREMENT, transaction_date DATE, product_id INT, product_code INT, product_description VARCHAR(100), quantity INT, account_id INT, FOREIGN KEY (account_id) REFERENCES accounts (customer_id), FOREIGN KEY (product_id) REFERENCES products (product_id))"
    insert_data_accounts="INSERT INTO accounts (customer_id, first_name, last_name, address_1, address_2, city, state, zip_code, join_date) VALUES('"+dfaccounts[:,0]+"','"+dfaccounts.iloc[:,1]+"','"+dfaccounts.iloc[:,2]+"','"+dfaccounts.iloc[:,3]+"','"+dfaccounts.iloc[:4]+"','"+dfaccounts.iloc[:,2]+"','"+dfaccounts.iloc[:,5]+"','"+dfaccounts.iloc[:,6]+"')"
    insert_data_products="INSERT INTO products(product_id, product_code, product_description) VALUES('"+dfaccounts['product_id']+"','"+dfproducts.iloc[:,1]+"','"+dfproducts.iloc[:,2]+"')"
    insert_data_transactions="INSERT INTO transactions(transaction_id, transaction_date, product_id, product_code, product_description, quantity, account_id) VALUES('"+dftransactions["transaction_id"].iloc[0]+"'"+dftransactions["transaction_date"].iloc[0]+"'"+dftransactions["product_id"].iloc[0]+"'"+dftransactions["product_code"].iloc[0]+"'"+dftransactions["product_description"].iloc[0]+"'"+dftransactions["quantity"].iloc[0]+"'"+dftransactions["account_id"].iloc[0]+"')"
    insert_data_accounts = "INSERT INTO accounts(customer_id,first_name, last_name, address_1, address_2, city, state, zip_code, join_date) VALUES('"+dfaccounts["customer_id"].iloc[0]+"'"+dfaccounts["first_name"].iloc[0]+"'"+dfaccounts["last_name"].iloc[0]+"'"+dfaccounts["address_1"].iloc[0]+"'"+dfaccounts["address_2"].iloc[0]+"'"+dfaccounts["city"].iloc[0]+"'"+dfaccounts["state"].iloc[0]+"'"+dfaccounts["zip_code"].iloc[0]+"'"+dfaccounts["join_date"].iloc[0]+"')"
    try:
        cur=conn.cursor()
        cur.execute(create_table_products)
        cur.execute(insert_data_products)
        cur.execute(create_table_accounts)
        cur.execute(insert_data_accounts)
        cur.execute(create_table_transactions)
        cur.execute(insert_data_transactions)
        cur.close()
    except (Exception, psycopg2.DatabaseError) as Error:
        print(Error)

    

if __name__ == "__main__":
    main()
   