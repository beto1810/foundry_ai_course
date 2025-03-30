import requests
import pandas as pd
import duckdb 
import logging
import pyarrow as pa
import pyarrow.parquet as pq 
import time  

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_data():
    """ Extract data from the API and save it as a Parquet file """
    all_pokemon = {}

    url = "https://pokeapi.co/api/v2/pokemon"  # Start URL
    while url:

        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            logging.info(f"Fetched {len(data['results'])} Pokémon from {url}")
            for pokemon in data['results']:
                name = pokemon['name']
                pokemon_url = pokemon['url']
                pokemon_id = pokemon_url.split("/")[-2]
                pokemon_detail = requests.get(pokemon_url, timeout=10).json()
                all_pokemon[pokemon_id] = {
                    "name": name,
                    "height": pokemon_detail["height"],
                    "weight": pokemon_detail["weight"],
                    "types": [t['type']['name'] for t in pokemon_detail['types']],
                    "abilities": [a['ability']['name'] for a in pokemon_detail['abilities']]
                }
                logging.info(f"Saved details for {name}")
            # Check if there's a next page
            url = data['next']
        else:
            logging.error(f"Failed to fetch data from {url}, status code: {response.status_code}")
            break
        # Sleep to avoid hitting the API too hard
        time.sleep(0.6) 
    
    # Create a DataFrame from the dictionary

    df = pd.DataFrame(list(all_pokemon.values()))

    # Save as Parquet
    parquet_filename = "pokemon_data.parquet"
    df.to_parquet(parquet_filename, engine="pyarrow")

    logging.info(f"Saved Pokémon data to {parquet_filename}")

    return parquet_filename


def load_data():
    """ Load data into DuckDB and create a table """
    con = duckdb.connect("pokemon.db")
    # Path to Parquet file
    parquet_file = "pokemon_data.parquet"

    # Create or replace the table
    con.execute("DROP TABLE IF EXISTS pokedex")  
    con.execute("""
        CREATE OR REPLACE TABLE pokedex AS 
        SELECT 
            row_number() OVER () AS id,  -- Auto-increment `id`
            *  -- Select all other columns from the Parquet file
        FROM read_parquet(?)
    """, [parquet_file])

    print("Loaded data into DuckDB and created a table.🥇")

    con.close()


def transform_pokemon_data():
    """ Transform the data and create a summary table """
    con = duckdb.connect("pokemon.db")
    con.execute("""
        CREATE OR REPLACE TABLE pokemon_stats AS 
        SELECT COUNT(ID) AS total_pokemon, 
            MIN(ID) AS first_id_pokemon, 
            MAX(ID) AS last_id_pokemon,
            AVG(height) AS avg_height,
            AVG(weight) AS avg_weight,
            COUNT(DISTINCT types) AS unique_types
        FROM pokedex
    """)

    result_1 = con.execute("SELECT * FROM pokedex").fetchdf()

    result = con.execute("SELECT * FROM pokemon_stats").fetchdf()

    con.close()
    logging.info("Created summary table 'pokemon_stats'")
    logging.info(f"Stats: {result.to_string()}")

    

def main():
    extract_data()
    load_data()
    transform_pokemon_data()

if __name__ == "__main__":
    main()

