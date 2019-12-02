import sqlite3
import pandas as pd

conn = sqlite3.connect('/Users/laurenpettit/pokemon.db')
c = conn.cursor()

def execute_query(query_str):
    conn = sqlite3.connect('/Users/laurenpettit/pokemon.db')
    df = pd.read_sql_query(query_str, conn)
    conn.close()
    return df

def get_avg_weight_by_type():
    # What is the average weight of the pokemon by Pokemon type?
    # Assumption: If a pokemon has more than 1 type, use it it each pokemon type
    # calculation
    query = """
    SELECT
    	dpt.type_name,
    	AVG(pokemon_weight) AS average_weight
    FROM dim_pokemon p
    JOIN pokemon_to_pokemon_type_mapping ptm
        ON p.pokemon_id = ptm.pokemon_id
    JOIN dim_pokemon_types dpt
        ON ptm.type_id = dpt.type_id
    WHERE p.pokemon_id <= 15
    GROUP BY dpt.type_name
    """

    return execute_query(query)

def get_highest_accuracy_by_type():
    # List the highest accuracy move by Pokemon type
    # Notes: From API results I found that mulitple moves can have 100% accuracy.
    # Assumption: Aggregate all moves with that have the max accuracy for the pokemon type
    query = """
    --List the highest accuracy move by Pokemon type
    WITH max_accuracy_by_type AS (
    SELECT
    	pokemon_type_name,
    	MAX(accuracy) AS max_accuracy
    FROM dim_moves
    WHERE move_id IN (SELECT DISTINCT move_id FROM pokemon_to_moves_mapping WHERE pokemon_id <= 15)
    AND accuracy IS NOT NULL
    GROUP BY 1
    )
    SELECT
    	m.pokemon_type_name,
    	GROUP_CONCAT(m.move_name) AS move_names
    FROM dim_moves m
    JOIN max_accuracy_by_type mat
    	ON m.pokemon_type_name = mat.pokemon_type_name
    	AND m.accuracy = mat.max_accuracy
    WHERE move_id IN (SELECT DISTINCT move_id FROM pokemon_to_moves_mapping WHERE pokemon_id <= 15)
    AND accuracy IS NOT NULL
    GROUP BY 1
    """
    return execute_query(query)

def get_num_moves_by_pokemon():
    # Count the number of moves by Pokemon and order from greatest to least
    query = """
    SELECT
    	p.pokemon_name,
    	COUNT(move_id) AS num_moves
    FROM dim_pokemon p
    JOIN pokemon_to_moves_mapping ptm
    	ON p.pokemon_id = ptm.pokemon_id
	WHERE ptm.pokemon_id <= 15
    GROUP BY 1
    ORDER BY 2 DESC
    """
    return execute_query(query)

print("Question 1: What is the average weight of the pokemon by Pokemon type?")
q1_results = get_avg_weight_by_type()
print(q1_results.to_string(index=False))

print("Question 2: List the highest accuracy move by Pokemon type")
q2_results = get_highest_accuracy_by_type()
print(q2_results.to_string(index=False))

print("Question 3: Count the number of moves by Pokemon and order from greatest to least")
q3_results = get_num_moves_by_pokemon()
print(q3_results.to_string(index=False))

conn.close()
