import csv
import logging
import datetime
import os

import sqlalchemy as sa
import pandas as pd
import numpy as np
from tqdm import tqdm

def load_imdb_dump(file_path, nrows=None):
    '''Load Data from *.tsv files to Pandas Dataframes
    
    file_path: str path to the root directory of the IMDb dump
    nrows: int how many rows should be loaded
    
    return: dictonary of type {str: pandas dataframe}
    '''
    
    def conv(x):
        try:
            return bool(x)
        except:
            logging.warning(f"value {x} could not be parset to boolean and will be replaced with NaN")
            return np.nan

    general_parameters = {
        'sep': '\t',
        'nrows': nrows,
        'na_values': r"\N",
        # 'low_memory': False,
        # 'engine': 'python',
    }
        
    tables = {}

    logging.info("Read title.akas")
    tables['title.akas'] = pd.read_csv(
            f"{file_path}/title.akas.tsv",
            converters={'isOriginalTitle': conv},
            **general_parameters,
    )

    logging.info("Read title.basics")
    tables['title.basics'] = pd.read_csv(
            f"{file_path}/title.basics.tsv",
            dtype={'runtimeMinutes': 'Int64',
                   'startYear': 'Int64',
                   'endYear': 'Int64'
                   },
            converters={'isAdult': conv},
            # there are cells in the csv which beginns with a " but dont close them!!
            # therefore we have to ignore quotations. wtf IMDb?
            quoting=csv.QUOTE_NONE, 
            **general_parameters,
    )

    logging.info("Read title.crew")
    tables['title.crew'] = pd.read_csv(
            f"{file_path}/title.crew.tsv",
            **general_parameters
    )

    # logging.info("Read title.episode")
    # tables['title.episode'] = pd.read_csv(
    #         f"{file_path}/title.episode.tsv",
    #         dtype={'seasonNumber': 'Int64', 'episodeNumber': 'Int64'},
    #         **general_parameters,
    # )

    logging.info("Read title.principals")
    tables['title.principals'] = pd.read_csv(
            f"{file_path}/title.principals.tsv",
            **general_parameters
    )

    logging.info("Read title.ratings")
    tables['title.ratings'] = pd.read_csv(
            f"{file_path}/title.ratings.tsv",
            **general_parameters
    )

    # logging.info("Read name.basics")
    # tables['name.basics'] = pd.read_csv(
    #     f"{file_path}/name.basics.tsv",
    #     dtype={'birthYear': 'Int64', 'deathYear': 'Int64'},
    #     **general_parameters
    # )
    
    return tables

def load_tables_from_feather(files_path):
    tables = {}
    
    for file_name in ['name.basics', 
                      'title.akas', 
                      'title.basics', 
                      'title.crew', 
                      'title.episode', 
                      'title.principals', 
                      'title.ratings'
                      ]:
        logging.info(f'read {file_name} from feather')
        tables[file_name] = pd.read_feather(f'{files_path}/{file_name}.ftr')
    return tables

def define_sql_tables(metadata_obj):
    index_str_length = 12
    str_length = 200
    title_length = 500
    
    name_basics = sa.Table(
        'name_basics',
        metadata_obj,
        sa.Column('nconst', sa.String(index_str_length), primary_key=True),
        sa.Column('primaryName', sa.String(str_length)),
        sa.Column('birthYear', sa.Integer()),
        sa.Column('deathYear', sa.Integer()),
        mysql_engine='InnoDB',
        mysql_charset='utf8mb4',
    )
    
    title_basics = sa.Table(
        'title_basics',
        metadata_obj,
        sa.Column('tconst', sa.String(index_str_length), primary_key=True),
        sa.Column('titleType', sa.String(str_length)),
        sa.Column('primaryTitle', sa.Text()),
        sa.Column('originalTitle', sa.Text()),
        sa.Column('isAdult', sa.Boolean()),
        sa.Column('startYear', sa.Integer()),
        sa.Column('endYear', sa.Integer()),
        sa.Column('runtimeMinutes', sa.Integer()),
        mysql_engine='InnoDB',
        mysql_charset='utf8mb4',
    )
    
    name_primaryProfessions = sa.Table(
        'name_primary_professions',
        metadata_obj,
        sa.Column('nconst', sa.String(index_str_length), sa.ForeignKey("name_basics.nconst"), primary_key=True),
        sa.Column('profession', sa.String(str_length), primary_key=True),
        mysql_engine='InnoDB',
        mysql_charset='utf8mb4',
    )
    
    name_knownForTitles = sa.Table(
        'name_known_for_titles',
        metadata_obj,
        sa.Column('nconst', sa.String(index_str_length), sa.ForeignKey("name_basics.nconst"), primary_key=True),
        sa.Column('tconst', sa.String(index_str_length), sa.ForeignKey("title_basics.tconst"), primary_key=True),
        mysql_engine='InnoDB',
        mysql_charset='utf8mb4',
    )
    
    title_writers = sa.Table(
        'title_writers',
        metadata_obj,
        sa.Column('tconst', sa.String(index_str_length), sa.ForeignKey("title_basics.tconst"), primary_key=True),
        sa.Column('nconst', sa.String(index_str_length), sa.ForeignKey("name_basics.nconst"), primary_key=True),
        mysql_engine='InnoDB',
        mysql_charset='utf8mb4',
    )
    
    title_directors = sa.Table(
        'title_directors',
        metadata_obj,
        sa.Column('tconst', sa.String(index_str_length), sa.ForeignKey("title_basics.tconst"), primary_key=True),
        sa.Column('nconst', sa.String(index_str_length), sa.ForeignKey("name_basics.nconst"), primary_key=True),
        mysql_engine='InnoDB',
        mysql_charset='utf8mb4',
    )
    
    title_genres = sa.Table(
        'title_genres',
        metadata_obj,
        sa.Column('tconst', sa.String(index_str_length), sa.ForeignKey("title_basics.tconst"), primary_key=True),
        sa.Column('genre', sa.String(str_length), primary_key=True),
        mysql_engine='InnoDB',
        mysql_charset='utf8mb4',
    )
    
    title_principals = sa.Table(
        'title_principals',
        metadata_obj,
        sa.Column('tconst', sa.String(index_str_length), sa.ForeignKey("title_basics.tconst"), primary_key=True),
        sa.Column('ordering', sa.Integer(), primary_key=True),
        sa.Column('nconst', sa.String(index_str_length), sa.ForeignKey("name_basics.nconst")), #, primary_key=True),
        sa.Column('category', sa.String(str_length)),
        sa.Column('job', sa.Text()),
        sa.Column('characters', sa.Text()),
        mysql_engine='InnoDB',
        mysql_charset='utf8mb4',
    )
    
    title_ratings = sa.Table(
        'title_ratings',
        metadata_obj,
        sa.Column('tconst', sa.String(index_str_length), sa.ForeignKey("title_basics.tconst"), primary_key=True),
        sa.Column('averageRating', sa.Float()),
        sa.Column('numVotes', sa.Integer()),
        mysql_engine='InnoDB',
        mysql_charset='utf8mb4',
    )
    
    title_akas = sa.Table(
        'title_akas',
        metadata_obj,
        sa.Column('tconst', sa.String(index_str_length), sa.ForeignKey("title_basics.tconst"), primary_key=True),
        sa.Column('ordering', sa.Integer(), primary_key=True),
        sa.Column('title', sa.Text()),
        sa.Column('region', sa.String(str_length)),
        sa.Column('language', sa.String(str_length)),
        sa.Column('isOriginalTitle', sa.Boolean()),
        mysql_engine='InnoDB',
        mysql_charset='utf8mb4',
    )
    
    title_episode = sa.Table(
        'title_episode',
        metadata_obj,
        sa.Column('tconst', sa.String(index_str_length), sa.ForeignKey("title_basics.tconst"), primary_key=True),
        sa.Column('parentTconst', sa.String(index_str_length), sa.ForeignKey("title_basics.tconst"), primary_key=True),
        sa.Column('seasonNumber', sa.Integer()),
        sa.Column('episodeNumber', sa.Integer()),
        mysql_engine='InnoDB',
        mysql_charset='utf8mb4',
    )
    
def fill_database(tables, db_engine):
    
    # logging.info("Write title_basics")
    # title_basics = dataframes['title.basics'][['tconst', 'titleType', 'primaryTitle', 'originalTitle', 'isAdult', 'startYear', 'endYear', 'runtimeMinutes']]
    # title_basics.to_sql('title_basics', con=db_engine, index=False, if_exists='append')
    
    logging.info("Write title_principals")
    title_principals = dataframes['title.principals'][['tconst', 'ordering', 'nconst', 'category', 'job', 'characters']]
    title_principals.to_sql('title_principals', con=db_engine, index=False, if_exists='append')
    
    logging.info("Write title_ratings")
    title_ratings = dataframes['title.ratings'][['tconst', 'averageRating', 'numVotes']]
    title_ratings.to_sql('title_ratings', con=db_engine, index=False, if_exists='append')
    
    logging.info("Write title_akas")
    title_akas = dataframes['title.akas'][['titleId', 'ordering', 'title', 'region', 'language', 'isOriginalTitle']]
    title_akas.rename(columns={'titleId': 'tconst'}, inplace=True)
    title_akas.to_sql('title_akas', con=db_engine, index=False, if_exists='append')
    
    logging.info("Write title_episode")
    title_episode = dataframes['title.episode'][['tconst', 'parentTconst', 'seasonNumber', 'episodeNumber']]
    title_episode.to_sql('title_episode', con=db_engine, index=False, if_exists='append')
    
    logging.info("Write title_directors")
    title_directors = dataframes['title.crew'][['tconst', 'directors']]
    title_directors = title_directors.assign(directors=title_directors['directors'].str.split(',')).explode('directors')
    title_directors.rename(columns={'directors': 'nconst'}, inplace=True)
    title_directors.to_sql('title_directors', con=db_engine, index=False, if_exists='append')
    
    logging.info("Write title_writers")
    title_writers = dataframes['title.crew'][['tconst', 'writers']]
    title_writers = title_writers.assign(writers=title_writers['writers'].str.split(',')).explode('writers')
    title_writers.rename(columns={'writers': 'nconst'}, inplace=True)
    title_writers.to_sql('title_writers', con=db_engine, index=False, if_exists='append')
    
    logging.info("Write name_primaryProfession")
    name_primaryProfessions = dataframes['name.basics'][['nconst', 'primaryProfession']]
    name_primaryProfessions = name_primaryProfessions.assign(primaryProfession=name_primaryProfessions['primaryProfession'].str.split(',')).explode('primaryProfession')
    name_primaryProfessions.rename(columns={'primaryProfession': 'profession'}, inplace=True)
    name_primaryProfessions.to_sql('name_primaryProfession', con=db_engine, index=False, if_exists='append')
    
    logging.info("Write name_knownForTitles")
    name_knownForTitles = dataframes['name.basics'][['nconst', 'knownForTitles']]
    name_knownForTitles = name_knownForTitles.assign(knownForTitles=name_knownForTitles['knownForTitles'].str.split(',')).explode('knownForTitles')
    name_knownForTitles.rename(columns={'knownForTitles': 'tconst'}, inplace=True)
    name_knownForTitles.to_sql('name_knownForTitles', con=db_engine, index=False, if_exists='append')
    
    logging.info("Write title_genres")
    title_genres = dataframes['title.basics'][['tconst', 'genres']]
    title_genres = title_genres.assign(genres=title_genres['genres'].str.split(',')).explode('genres')
    title_genres.rename(columns={'genres': 'genre'}, inplace=True)
    title_genres.to_sql('title_genres', con=db_engine, index=False, if_exists='append')


# def list_explode(df, list_column):
#     df[list_column] = df[list_column].str.split(',')
#     return df.explode(list_column)

def csv2sql(file_path, filename, table_name, dtypes, connection,
            specific_parameters=None, 
            explode=None, 
            rename=None,
            check_foreign_keys=None):
    if specific_parameters is None:
        specific_parameters = {}
    
    general_parameters = {
        'sep': '\t',
        'nrows': None,
        'na_values': r"\N",
        'chunksize': 5000,
        'iterator':True,
        'quoting': csv.QUOTE_NONE,
        'low_memory': False,
        'on_bad_lines': 'warn',
    }
    logging.info(f"Reading {filename} into {table_name}")
    num_rows = sum(1 for line in open(f'{file_path}/{filename}'))
    with tqdm(total=num_rows) as pbar:
        df = pd.read_csv(
                f"{file_path}/{filename}",
                usecols=dtypes.keys(),
                dtype=dtypes,
                **specific_parameters,
                **general_parameters,
        )
        for chunk in df:
            if explode:
                chunk[explode] = chunk[explode].str.split(',')
                chunk = chunk.explode(explode)
                chunk = chunk.dropna(subset=[explode])
                chunk = chunk.drop_duplicates()
            if rename:
                chunk.rename(columns=rename, inplace=True)
            
            if check_foreign_keys:
                for key_column, basic_keys in check_foreign_keys.items():
                    failed_keys = filter_not_existing_keys(chunk[key_column], basic_keys)
                    chunk = chunk[~chunk[key_column].isin(failed_keys)]

            chunk.to_sql(table_name, con=connection, index=False, if_exists='append')
            pbar.update(len(chunk))


def filter_not_existing_keys(foreign_keys, basic_keys):
    failed_keys = foreign_keys[~basic_keys.isin(basic_keys)]
    if len(failed_keys) > 0:
        logging.warning(f"{len(failed_keys)} Foreign keys are not in basic keys")
        logging.warning(failed_keys)
    return failed_keys

def bool_conv(x):
    try:
        return bool(x)
    except:
        logging.warning(f"value {x} could not be parset to boolean and will be replaced with NaN")
        return np.nan

title_basics_dtypes = {
    'tconst': 'string',
    'titleType': 'category',
    'primaryTitle': 'string',
    'originalTitle': 'string',
    'isAdult': 'boolean',
    'startYear': 'Int16',
    'endYear': 'Int16',
    'runtimeMinutes': 'Int32',
}

name_basics_dtypes = {
    'nconst': 'string',
    'primaryName': 'string',
    'birthYear': 'Int16',
    'deathYear': 'Int16',
}

title_episode_dtypes = {
    'tconst': 'string',
    'parentTconst': 'string',
    'seasonNumber': 'Int32',
    'episodeNumber': 'Int32',
}

title_akas_dtypes = {
    'titleId': 'string',
    'ordering': 'Int16',
    'title': 'string',
    'region': 'string',
    'language': 'string',
    'isOriginalTitle': 'boolean',
}

title_ratings_dtypes = {
    'tconst': 'string',
    'averageRating': 'float',
    'numVotes': 'Int32',
}

title_principals_dtypes = {
    'tconst': 'string',
    'ordering': 'Int16',
    'nconst': 'string',
    'category': 'category',
    'job': 'category',
    'characters': 'string',
}

title_genres_dtypes = {
    'tconst': 'string',
    'genres': 'string',
}

title_directors_dtypes = {
    'tconst': 'string',
    'directors': 'string',
}

title_writers_dtypes = {
    'tconst': 'string',
    'writers': 'string',
}

name_knownForTitles_dtypes = {
    'nconst': 'string',
    'knownForTitles': 'string',
}

name_primaryProfessions_dtypes = {
    'nconst': 'string',
    'primaryProfession': 'string',
}


if __name__ == '__main__':

    execution_path = os.path.dirname(os.path.realpath(__file__))

    logging.basicConfig(level=logging.INFO, filename=f'{execution_path}/create_database.log', format='%(levelname)s :: %(message)s')

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter('%(levelname)s :: %(asctime)s :: %(message)s'))
    logger = logging.getLogger()
    logger.addHandler(console_handler)


    logging.info("Start")
    now = datetime.datetime.now()
    logging.info(now)
    
    # logging.info("Load Dataframes")
    # dataframes = load_imdb_dump('./tsv_dump', nrows=None)
    
    # dataframes = load_tables_from_feather(f"./feather_dump/original_tables")
    
    
    # write dataframes to feather
    # logging.info("Write tables to feather")
    # for filename, dataframe in dataframes.items():
    #     dataframe.to_feather(f"./feather_dump/original_tables/{filename}.ftr")
    
    
    metadata_obj = sa.MetaData()
    
    logging.info("Define sql tables")
    define_sql_tables(metadata_obj)
    
    sql_user = 'root'
    sql_pass = 'myrootpassword'
    sql_host = 'localhost'
    sql_db = 'mrdatabase'

    logging.info("connect to Database")
    db_connection_str = f'mysql+pymysql://{sql_user}:{sql_pass}@{sql_host}/{sql_db}?charset=utf8mb4'
    db_engine = sa.create_engine(db_connection_str)
    db_conncetion = db_engine.connect()
    result = db_conncetion.execute(sa.text( "SET foreign_key_checks = 0;"))
    sql_table_list = [
        sa.table('name_basics'),
        sa.text('name_knownForTitles'),
        sa.text('name_PrimaryProfessions'),
        sa.table('title_basics'),
        sa.text('title_akas'),
        sa.text('title_directors'),
        sa.table('title_episode'),
        sa.text('title_genres'),
        sa.text('title_principals'),
        sa.text('title_ratings'),
        sa.text('title_writers'),
    ]
    
    logging.info('Drop sql Tables')
    metadata_obj.drop_all(
        db_engine,
        # tables=sql_table_list
    )
    
    logging.info('Create sql Tables')
    metadata_obj.create_all(
        db_engine,
        # tables=sql_table_list,
    )
    
    logging.info('Fill sql Tables')

    file_path=f'{execution_path}/tsv_dump'

    csv2sql(
        file_path=file_path,
        filename='title.basics.tsv',
        table_name='title_basics',
        dtypes=title_basics_dtypes,
        connection=db_conncetion,
    )

    csv2sql(
        file_path=file_path,
        filename='name.basics.tsv',
        table_name='name_basics',
        dtypes=name_basics_dtypes,
        connection=db_conncetion,
    )



    csv2sql(
        file_path=file_path,
        filename='title.episode.tsv',
        table_name='title_episode',
        dtypes=title_episode_dtypes,
        connection=db_conncetion,
        #check_foreign_keys={'parentTconst': title_basics_keys,
         #                   'tconst': title_basics_keys},
    )

    csv2sql(
        file_path=file_path,
        filename='title.akas.tsv',
        table_name='title_akas',
        dtypes=title_akas_dtypes,
        connection=db_conncetion,
        rename= {'titleId': 'tconst'},
       # check_foreign_keys={'tconst': title_basics_keys},
    )

    csv2sql(
        file_path=file_path,
        filename='title.ratings.tsv',
        table_name='title_ratings',
        dtypes=title_ratings_dtypes,
        connection=db_conncetion,
        # check_foreign_keys={'tconst': title_basics_keys},
    )

    csv2sql(
        file_path=file_path,
        filename='title.principals.tsv',
        table_name='title_principals',
        dtypes=title_principals_dtypes,
        connection=db_conncetion,
        # check_foreign_keys={'tconst': title_basics_keys},
    )

    csv2sql(
        file_path=file_path,
        filename='title.basics.tsv',
        table_name='title_genres',
        dtypes=title_genres_dtypes,
        connection=db_conncetion,
        explode='genres',
        rename={'genres': 'genre'},
    )

    csv2sql(
        file_path=file_path,
        filename='title.crew.tsv',
        table_name='title_directors',
        dtypes=title_directors_dtypes,
        connection=db_conncetion,
        explode='directors',
        rename={'directors': 'nconst'},
       # check_foreign_keys={'nconst': name_basics_keys, 'tconst': title_basics_keys},
    )

    csv2sql(
        file_path=file_path,
        filename='title.crew.tsv',
        table_name='title_writers',
        dtypes=title_writers_dtypes,
        connection=db_conncetion,
        explode='writers',
        rename={'writers': 'nconst'},
       # check_foreign_keys={'nconst': name_basics_keys, 'tconst': title_basics_keys},
    )

    csv2sql(
        file_path=file_path,
        filename='name.basics.tsv',
        table_name='name_known_for_titles',
        dtypes=name_knownForTitles_dtypes,
        connection=db_conncetion,
        explode='knownForTitles',
        rename={'knownForTitles': 'tconst'},
        # check_foreign_keys={'tconst': title_basics_keys, 'nconst': name_basics_keys},
    )

    csv2sql(
        file_path=file_path,
        filename='name.basics.tsv',
        table_name='name_primary_professions',
        dtypes=name_primaryProfessions_dtypes,
        connection=db_conncetion,
        explode='primaryProfession',
        rename={'primaryProfession': 'profession'},
        # check_foreign_keys={'nconst': name_basics_keys},
    )

    logging.info("Done")
    end = datetime.datetime.now()
    logging.info(end)
    logging.info(end - now)

    # fill_database(dataframes, db_engine)