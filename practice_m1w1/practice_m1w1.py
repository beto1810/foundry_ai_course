import pandas as pd
import duckdb
import logging
import time
import requests
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_data():
    """ Extract data from the API and save it as a Parquet file """
    evolution_chain = {}

    url = "https://pokeapi.co/api/v2/evolution-chain/?offset=0&limit=10"  # Start URL
    while url:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            logging.info(f"Fetched {len(data['results'])} Pokémon evolution chains from {url}")
            for chains in data['results']:
                evolution_chain_url = chains['url']
                evolution_chain_response = requests.get(evolution_chain_url, timeout=10).json() 
                evolution_chain_data = evolution_chain_response['chain'] 
                evolution_chain_id = evolution_chain_response['id']
                evolution_chain_pokemon = evolution_chain_data['species']['name']
                evolution_chain_next_evolve = evolution_chain_data['evolves_to'][0]['species']['name'] if evolution_chain_data['evolves_to'] else None
                evolution_chain[evolution_chain_id] = {
                    "pokemon": evolution_chain_pokemon,
                    "next_evolve": evolution_chain_next_evolve
                }
                logging.info(f"Saved details for {evolution_chain_pokemon}")
            # Check if there's a next page
            url = data['next']
        else:
            logging.error(f"Failed to fetch data from {url}, status code: {response.status_code}")
            break
        # Sleep to avoid hitting the API too hard
        time.sleep(0.2)


    # Create a DataFrame from the dictionary
    df = pd.DataFrame(list(evolution_chain.values()))
    # Save as Parquet
    parquet_filename = "evolution_chain_data.parquet"
    df.to_parquet(parquet_filename, engine="pyarrow")
    logging.info(f"Saved evolution chain data to {parquet_filename}")
    return parquet_filename

def load_data():
    """ Load data into DuckDB and create a table """
    con = duckdb.connect("pokemon.db")
    # Path to Parquet file
    parquet_file = "evolution_chain_data.parquet"

    # Create or replace the table
    con.execute("DROP TABLE IF EXISTS evolution_chain")  
    con.execute("""
        CREATE OR REPLACE TABLE evolution_chain AS 
        SELECT 
            row_number() OVER () AS id,  -- Auto-increment `id`
            *  -- Select all other columns from the Parquet file
        FROM read_parquet(?)
    """, [parquet_file])

    logging.info("Loaded evolution chains into DuckDB")

    con.close()
    logging.info("Created table 'evolution_chain'")

def transform_data():

    con = duckdb.connect("pokemon.db")
    
    con.execute(""" CREATE OR REPLACE TABLE evolution_chain_summary AS 
                    SELECT 
                        COUNT(*) AS number_chain,
                        SUM(CASE WHEN next_evolve IS NOT NULL THEN 1 ELSE 0 END) AS evolve_count,
                        SUM(CASE WHEN next_evolve IS NULL THEN 1 ELSE 0 END) AS no_evolve_count
                    FROM evolution_chain
                """)
    summary = con.execute("SELECT * FROM evolution_chain_summary").fetchdf()
    logging.info("Transformed data and created summary table")
    con.close()

def main():
    extract_data()
    load_data()
    transform_data()

if __name__ == "__main__":
    main()
