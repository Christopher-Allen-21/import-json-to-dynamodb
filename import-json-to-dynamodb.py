import json
import boto3
import logging
from boto3.dynamodb.conditions import Key
from datetime import datetime


MOVIE_TABLE = 'movies'
TV_SHOW_TABLE = 'tv-shows'
EPISODE_TABLE = 'episodes'

S3_BUCKET = 'video-content-bucket-1'
JSON_FILE = 'contentFeed.json'

UPDATE_ALL_DATA_EXCEPT_TIMESTAMPS = False


logging.basicConfig(level = logging.INFO)
logger = logging.getLogger()

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
movie_table = dynamodb.Table(MOVIE_TABLE)
tv_show_table = dynamodb.Table(TV_SHOW_TABLE)
episode_table = dynamodb.Table(EPISODE_TABLE)

current_date = datetime.today().strftime('%Y-%m-%d')

def lambda_handler(event, context):
    bucket = S3_BUCKET
    key = JSON_FILE

    response = s3.get_object(Bucket = bucket, Key = key)
    content = response['Body']
    jsonObject = json.loads(content.read())

    for movie in jsonObject['Movies']:

        if len(get_dynamo_record_by_pk_and_sk('name', movie['title'], 'year', movie['releaseDate'], movie_table)['Items']) == 1:
            existingMovie = get_dynamo_record_by_pk_and_sk('name', movie['title'], 'year', movie['releaseDate'], movie_table)['Items'][0]
        else:
            existingMovie = None

        if not existingMovie:
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
                'dateAdded': current_date, 
                'lastWatched': None,
                'views': 0
                })
        elif UPDATE_ALL_DATA_EXCEPT_TIMESTAMPS:
            # If movie already exists should update all fields in dynamo except dateAdded, lastWatched, and views
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
                'trailerUrl': existingMovie['trailerUrl'],
                'dateAdded': existingMovie['dateAdded'],
                'lastWatched': existingMovie['lastWatched'],
                'views': existingMovie['views']
                })

    for tv_show in jsonObject['TV Shows']:

        
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
                'dateAdded': current_date, 
                'lastWatched': None,
                'views': 0
                })
        elif UPDATE_ALL_DATA_EXCEPT_TIMESTAMPS:
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
                'dateAdded': existingTvShow['dateAdded'],
                'lastWatched': existingTvShow['lastWatched'],
                'views': existingTvShow['views']
                })
        
        for season in tv_show['seasons']:
            for episode in season['episodes']:

                seasonAndEpisode = 'S' + season['title'] + ' E' + str(episode['episodeNumber'])
                
                if len(get_dynamo_record_by_pk_and_sk('tvShowName', tv_show['title'], 'seasonAndEpisode', seasonAndEpisode, episode_table)['Items']) == 1:
                    existingEpisode = get_dynamo_record_by_pk_and_sk('tvShowName', tv_show['title'], 'seasonAndEpisode', seasonAndEpisode, episode_table)['Items'][0]
                else:
                    existingEpisode = None
                

                if not existingEpisode:
                    episode_table.put_item(Item = {
                    'tvShowName': tv_show['title'], 
                    'seasonAndEpisode': seasonAndEpisode,
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
                    'dateAdded': current_date, 
                    'lastWatched': None,
                    'views': 0
                    })
                elif UPDATE_ALL_DATA_EXCEPT_TIMESTAMPS:
                    # If episode already exists should update all fields in dynamo except dateAdded, lastWatched, and views
                    episode_table.put_item(Item = {
                    'tvShowName': tv_show['title'], 
                    'seasonAndEpisode': seasonAndEpisode,
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
                    'dateAdded': existingEpisode['dateAdded'], 
                    'lastWatched': existingEpisode['lastWatched'],
                    'views': existingEpisode['views']
                    })


def get_dynamo_record_by_pk(pk_name, pk_value, table):
    try:
        return table.query(KeyConditionExpression=Key(pk_name).eq(pk_value))
    except Exception as ex:
        logger.error(f"Error retrieving records with primary key {pk_value} from table {table}. Exception: {ex}")


def get_dynamo_record_by_pk_and_sk(pk_name, pk_value, sk_name, sk_value, table):
    try:
        return table.query(KeyConditionExpression=Key(pk_name).eq(pk_value) & Key(sk_name).eq(sk_value))
    except Exception as ex:
        logger.error(f"Error retrieving records with primary key {pk_value} and sort key {sk_value} from table {table}. Exception: {ex}")
