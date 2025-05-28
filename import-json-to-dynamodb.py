import json
import boto3
from boto3.dynamodb.conditions import Key
from datetime import datetime


MOVIE_TABLE = 'movies'
TV_SHOW_TABLE = 'tv-shows'
EPISODE_TABLE = 'episodes'

S3_BUCKET = 'video-content-bucket-1'
JSON_FILE = 'contentFeed.json'


# Set to True to update all Movie fields except dateAdded, lastWatched, and views
# Set to False to only add new movies
UPDATE_EXISTING_MOVIE_DATA = False

# Set to True to update all TV Show fields except dateAdded, lastWatched, and views
# Set to False to only add tv shows
UPDATE_EXISTING_TV_DATA = False

# Set to True to update all Episode fields except dateAdded, lastWatched, and views
# Set to False to only add episodes
UPDATE_EXISTING_EPISODE_DATA = False


s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
movie_table = dynamodb.Table(MOVIE_TABLE)
tv_show_table = dynamodb.Table(TV_SHOW_TABLE)
episode_table = dynamodb.Table(EPISODE_TABLE)


def lambda_handler(event, context):
    print("Import started.")

    bucket = S3_BUCKET
    key = JSON_FILE

    response = s3.get_object(Bucket = bucket, Key = key)
    content = response['Body']
    jsonObject = json.loads(content.read())

    create_and_update_movies(jsonObject['Movies'])
    create_and_update_tv_shows(jsonObject['TV Shows'])

    print("Import completed.")
    

def create_and_update_movies(movie_json_data):
    print("Movie import started.")

    for movie in movie_json_data:

        # Trim last three digits to only show milliseconds
        current_date_time = datetime.today().strftime('%Y-%m-%d %H:%M:%S %f')[:-3]

        if len(get_dynamo_record_by_pk('name', movie['title'], movie_table)['Items']) > 0:
            # An array of all movies with same name
            existing_movies = get_dynamo_record_by_pk('name', movie['title'], movie_table)['Items']
        else:
            existing_movies = None

        try:
            if not existing_movies:
                movie_table.put_item(Item = {
                    'name': movie['title'], 
                    'year': movie['releaseDate'], 
                    'description': movie['longDescription'],
                    'thumbnailUrl': movie['thumbnail'], 
                    'rating': movie['rating'], 
                    'cast': movie['cast'], 
                    'director': movie['director'], 
                    'genres': movie['genres'], 
                    'duration': movie['content']['duration'], 
                    'videoType': movie['content']['videos'][0]['videoType'], 
                    'videoUrl': movie['content']['videos'][0]['url'], 
                    'trailerUrl': None,
                    'dateAdded': current_date_time, 
                    'lastWatched': None,
                    'views': 0
                    })
                print(f"Added new Movie: {movie['title']}")
            elif UPDATE_EXISTING_MOVIE_DATA:
                # If movie(s) already exists should update all fields in dynamo except dateAdded, lastWatched, and views
                for existing_movie in existing_movies:
                    movie_table.put_item(Item = {
                        'name': movie['title'], 
                        'year': movie['releaseDate'], 
                        'description': movie['longDescription'],
                        'thumbnailUrl': movie['thumbnail'], 
                        'rating': movie['rating'], 
                        'cast': movie['cast'], 
                        'director': movie['director'], 
                        'genres': movie['genres'], 
                        'duration': movie['content']['duration'], 
                        'videoType': movie['content']['videos'][0]['videoType'], 
                        'videoUrl': movie['content']['videos'][0]['url'], 
                        'trailerUrl': existing_movie['trailerUrl'],
                        'dateAdded': existing_movie['dateAdded'],
                        'lastWatched': existing_movie['lastWatched'],
                        'views': existing_movie['views']
                        })
        except Exception as ex:
                print(f"Error creating/updating Movie: {movie['title']}. Exception: {ex}")
                print(ex)
                
    print("Movie import completed.")            


def create_and_update_tv_shows(tv_shows_json_data):
    print("TV Show import started.")   

    for tv_show in tv_shows_json_data:

        # Trim last three digits to only show milliseconds
        current_date_time = datetime.today().strftime('%Y-%m-%d %H:%M:%S %f')[:-3]

        if len(get_dynamo_record_by_pk('name', tv_show['title'], tv_show_table)['Items']) == 1:
            existingTvShow = get_dynamo_record_by_pk('name', tv_show['title'], tv_show_table)['Items'][0]
        else:
            existingTvShow = None

        if not existingTvShow:
            tv_show_table.put_item(Item = {
                'name': tv_show['title'], 
                'description': tv_show['shortDescription'],
                'thumbnailUrl': tv_show['thumbnail'],
                'releaseDate': tv_show['releaseDate'],
                'rating': tv_show['rating'],
                'cast': tv_show['cast'],
                'director': tv_show['director'],
                'genres': tv_show['genres'],
                'numberOfSeasons': len(tv_show['seasons']),
                'dateAdded': current_date_time, 
                'lastWatched': None,
                'views': 0
                })
            print(f"Added new TV Show: {tv_show['title']}")
        elif UPDATE_EXISTING_TV_DATA:
            # If tv show already exists should update all fields in dynamo except dateAdded, lastWatched, and views
            tv_show_table.put_item(Item = {
                'name': tv_show['title'], 
                'description': tv_show['shortDescription'],
                'thumbnailUrl': tv_show['thumbnail'],
                'releaseDate': tv_show['releaseDate'],
                'rating': tv_show['rating'],
                'cast': tv_show['cast'],
                'director': tv_show['director'],
                'genres': tv_show['genres'],
                'numberOfSeasons': len(tv_show['seasons']),
                'dateAdded': existingTvShow['dateAdded'],
                'lastWatched': existingTvShow['lastWatched'],
                'views': existingTvShow['views']
                })
        
        for season in tv_show['seasons']:
            for episode in season['episodes']:
                
                if notSpecialSeason(season['title']):
                    season_number_padded = str(f"{int(season['title']):02}")  # e.g., '1' -> '01'
                    episode_number_padded = str(f"{int(episode['episodeNumber']):02}")  # e.g., 3 -> '03'
                    season_and_episode = f"S{season_number_padded} E{episode_number_padded}"
                else:
                    episode_number_padded = str(f"{int(episode['episodeNumber']):02}")
                    season_and_episode = f"{season['title']} E{episode_number_padded}"
                
                if len(get_dynamo_record_by_pk_and_sk('tvShowName', tv_show['title'], 'seasonAndEpisode', season_and_episode, episode_table)['Items']) == 1:
                    existing_episode = get_dynamo_record_by_pk_and_sk('tvShowName', tv_show['title'], 'seasonAndEpisode', season_and_episode, episode_table)['Items'][0]
                else:
                    existing_episode = None
                
                try:
                    if not existing_episode:
                        episode_table.put_item(Item = {
                        'tvShowName': tv_show['title'], 
                        'seasonAndEpisode': season_and_episode,
                        'season': season['title'],
                        'episode': episode['episodeNumber'],
                        'name': episode['title'],
                        'description': episode['longDescription'],
                        'thumbnailUrl': episode['thumbnail'],
                        'releaseDate': episode['releaseDate'],
                        'rating': episode['rating'],
                        'cast': episode['cast'],
                        'director': episode['director'],
                        'genres': episode['genres'],
                        'videoType': episode['content']['videos'][0]['videoType'],
                        'videoUrl': episode['content']['videos'][0]['url'],
                        'duration': episode['content']['duration'],
                        'dateAdded': current_date_time, 
                        'lastWatched': None,
                        'views': 0
                        })
                    elif UPDATE_EXISTING_EPISODE_DATA:
                        # If episode already exists should update all fields in dynamo except dateAdded, lastWatched, and views
                        episode_table.put_item(Item = {
                        'tvShowName': tv_show['title'], 
                        'seasonAndEpisode': season_and_episode,
                        'season': season['title'],
                        'episode': episode['episodeNumber'],
                        'name': episode['title'],
                        'description': episode['longDescription'],
                        'thumbnailUrl': episode['thumbnail'],
                        'releaseDate': episode['releaseDate'],
                        'rating': episode['rating'],
                        'cast': episode['cast'],
                        'director': episode['director'],
                        'genres': episode['genres'],
                        'videoType': episode['content']['videos'][0]['videoType'],
                        'videoUrl': episode['content']['videos'][0]['url'],
                        'duration': episode['content']['duration'],
                        'dateAdded': existing_episode['dateAdded'], 
                        'lastWatched': existing_episode['lastWatched'],
                        'views': existing_episode['views']
                        })

                except Exception as ex:
                    print(f"Error creating/updating TV Show: {tv_show['title']}, Season/Episode: {season_and_episode}. Exception: {ex}")


    print("TV Show import completed.")       


def notSpecialSeason(season_name):
    if season_name != 'Pilot' and season_name != 'Extras' and season_name != 'Movies' and season_name != 'Mini Series':
        return True
    else:
        return False


def get_dynamo_record_by_pk(pk_name, pk_value, table):
    try:
        return table.query(KeyConditionExpression=Key(pk_name).eq(pk_value))
    except Exception as ex:
        print(f"Error retrieving records with primary key {pk_value} from table {table}. Exception: {ex}")


def get_dynamo_record_by_pk_and_sk(pk_name, pk_value, sk_name, sk_value, table):
    try:
        return table.query(KeyConditionExpression=Key(pk_name).eq(pk_value) & Key(sk_name).eq(sk_value))
    except Exception as ex:
        print(f"Error retrieving records with primary key {pk_value} and sort key {sk_value} from table {table}. Exception: {ex}")
