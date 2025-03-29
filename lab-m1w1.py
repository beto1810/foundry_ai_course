import requests
import pandas as pd
import duckdb 

def extract_data():
    """ Extract data from the API and save it as a Parquet file """
    all_pokemon = []

    url = "https://pokeapi.co/api/v2/pokemon"  # Start URL
    while url:

        response = requests.get(url, timeout=10)
        data = response.json()
        pokemon_list = [pokemon["name"] for pokemon in data["results"]]
        all_pokemon.extend(pokemon_list)

        print(f"Extracted {len(all_pokemon)} Pokémon names from {url}.���")
        # Update the URL for the next page
        url = data["next"] if "next" in data else None
        if not data["next"]:
            break
        # Extract details for each Pokémon
        

    # Loop through the Pokémon names and fetch their details
    df = pd.DataFrame(all_pokemon)

    # Save as Parquet
    parquet_filename = "pokemon_data.parquet"
    df.to_parquet(parquet_filename, engine="pyarrow")

    print(f"Saved {len(df)} Pokémon to {parquet_filename}")


    return all_pokemon


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
            MAX(ID) AS last_id_pokemon
        FROM pokedex
    """)

    result_1 = con.execute("SELECT * FROM pokedex").fetchdf()

    result = con.execute("SELECT * FROM pokemon_stats").fetchdf()

    con.close()
    print(result_1)
    print(result)
    print("Transformed Pokémon data and created summary table.📊")


    

def main():
    extract_data()
    load_data()
    transform_pokemon_data()

if __name__ == "__main__":
    main()

