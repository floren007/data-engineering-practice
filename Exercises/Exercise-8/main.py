import duckdb


def main():
#     1. create a DuckDB Table including DDL and correct data types that will hold the data in this CSV file.
#  - inspect data types and make DDL that makes sense. Don't just `String` everything.

#   2. Read the provided `CSV` file into the table you created.

    #with the method duckdb.read_csv() automatically covert into right types
    
    df = duckdb.read_csv('data/Electric_Vehicle_Population_Data.csv')
    print(df.dtypes)

    # 3. Calculate the following analytics.
    # - Count the number of electric cars per city.
    duckdb.sql('SELECT City,COUNT(*) FROM df GROUP BY City').show()
    # -Find the top 3 most popular electric vehicles.
    # just sql knowledge
    duckdb.sql("SELECT model FROM df GROUP BY model ORDER BY count(*) DESC LIMIT 3").show()
    # Find the most popular electric vehicle in each postal code.
    #duckdb.sql("SELECT 'Postal Code', MAX(model) as popular FROM df GROUP BY 'Postal Code'").show()
    # sql
    duckdb.execute("""WITH RankedNames AS (
        SELECT
            'Postal Code',
            Model,
            COUNT(*) AS popularity,
            RANK() OVER (PARTITION BY 'Postal Code' ORDER BY COUNT(*) DESC) AS popularity_rank
        FROM
            df
        GROUP BY
            'Postal Code',
            Model
    )
    SELECT
        'Postal Code',
        Model AS popular_name
    FROM
        RankedNames
    WHERE
        popularity_rank = 1""").fetchall()

    pass


if __name__ == "__main__":
    main()
