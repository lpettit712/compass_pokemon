import requests
import json
import pandas as pd
import sqlite3

def load_to_df_from_api(url, result_limit, return_cols, df):
    while url is not None:
        resp = requests.get(url, params={"limit": result_limit})
        if resp.status_code != 200:
            raise ApiError('GET /tasks/ {}'.format(resp.status_code))
        for result in resp.json()['results']:
            id_resp = requests.get(result['url'])
            if id_resp.json()['id'] not in df.index:
                df.loc[id_resp.json()['id']] = [
                    id_resp.json()[col] for col in return_cols
                ]
        url = resp.json()['next']


def get_mapping_table_data(base_url, pokemon_id, mapping_type, mapping_type_object_url):
    mapping_dict = {}
    map_id = int(mapping_type_object_url.replace(base_url,'').replace('/', ''))
    mapping_dict = {
            'pokemon_id': pokemon_id,
            '{type}_id'.format(type=mapping_type): map_id
    }
    return mapping_dict

conn = sqlite3.connect('/Users/laurenpettit/pokemon.db')
c = conn.cursor()

print("Loading dim_moves")
dim_pokemon_moves_df = pd.DataFrame(columns=['move_id', 'move_name', 'accuracy', 'type']).set_index('move_id')
load_to_df_from_api("https://pokeapi.co/api/v2/move", 200, ['name', 'accuracy', 'type'], dim_pokemon_moves_df)
dim_pokemon_moves_df['pokemon_type_name'] = dim_pokemon_moves_df.apply(lambda row: row.type['name'], axis=1)
dim_pokemon_moves_df[['move_name', 'accuracy', 'pokemon_type_name']].to_sql('dim_moves', conn, if_exists='replace', index=True, index_label='move_id')


print("Loading dim_pokemon_types")
dim_pokemon_types_df = pd.DataFrame(columns=['type_id', 'type_name']).set_index('type_id')
load_to_df_from_api("https://pokeapi.co/api/v2/type", 200, ['name'], dim_pokemon_types_df)
dim_pokemon_types_df.to_sql('dim_pokemon_types', conn, if_exists='replace', index=True, index_label='type_id')


print("Loading dim_pokemon")
dim_pokemon_df = pd.DataFrame(columns=['pokemon_id', 'pokemon_name', 'pokemon_weight', 'moves', 'types']).set_index('pokemon_id')
load_to_df_from_api("https://pokeapi.co/api/v2/pokemon", 200, ['name', 'weight', 'moves', 'types'], dim_pokemon_df)
dim_pokemon_df[['pokemon_name', 'pokemon_weight']].to_sql('dim_pokemon', conn, if_exists='replace', index=True, index_label='pokemon_id')

print("Loading pokemon_to_moves_mapping and pokemon_to_pokemon_type_mapping")
moves_mapping_df = pd.DataFrame(columns=['pokemon_id', 'move_id'])
move_base_url = "https://pokeapi.co/api/v2/move"

types_mapping_df = pd.DataFrame(columns=['pokemon_id', 'type_id'])
type_base_url = "https://pokeapi.co/api/v2/type"

for index, row in dim_pokemon_df.iterrows():
    for result in row['moves']:
        row_data = get_mapping_table_data(move_base_url, index, 'move', result['move']['url'])
        moves_mapping_df = moves_mapping_df.append(
            row_data,
            ignore_index=True
        )
    for result in row['types']:
        row_data = get_mapping_table_data(type_base_url, index, 'type', result['type']['url'])
        types_mapping_df = types_mapping_df.append(
            row_data,
            ignore_index=True
        )
# Drop duplicates, if any exist
moves_mapping_df.drop_duplicates(keep=False,inplace=True)
types_mapping_df.drop_duplicates(keep=False,inplace=True)

# Load to SQL Tables
moves_mapping_df.to_sql('pokemon_to_moves_mapping', conn, if_exists='replace', index=False)
types_mapping_df.to_sql('pokemon_to_pokemon_type_mapping', conn, if_exists='replace', index=False)

conn.close()
