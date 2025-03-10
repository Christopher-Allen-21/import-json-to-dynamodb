import json
import boto3
import logging
from datetime import datetime


MOVIE_TABLE = 'movies'
TV_SHOW_TABLE = 'tv-shows'

S3_BUCKET = 'video-content-bucket-1'
JSON_FILE = 'contentFeed.json'


logging.basicConfig(level = logging.INFO)
logger = logging.getLogger()


s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(MOVIE_TABLE)
current_date = datetime.today().strftime('%Y-%m-%d')

def lambda_handler(event, context):
    bucket = S3_BUCKET
    key = JSON_FILE

    response = s3.get_object(Bucket = bucket, Key = key)
    content = response['Body']
    jsonObject = json.loads(content.read())
    logger.info(jsonObject['Movies'])

    for movie in jsonObject['Movies']:
        logger.info(movie['title'])
        table.put_item(Item = {
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
            })


# {
#     "title": "The Wolf of Wallstreet",
#     "id": "",
#     "longDescription": "New York stockbroker Jordan Belfort, who founded brokerage firm Stratton Oakmont while still in his early 20s, develops habits of wretched excess and corruption.",
#     "thumbnail": "https://video-content-bucket-1.s3.amazonaws.com/thumbNails/theWolfOfWallstreet.jpg",
#     "releaseDate": "2013",
#     "rating": "R",
#     "cast": "Leonardo DiCaprio, Jonah Hill, Margot Robbie, Matthew McConaughey",
#     "director": "Martin Scorsese",
#     "genres": [
#         "All", "Action"
#     ],
#     "content": {
#         "duration": 10740,
#         "videos": [{
#             "videoType": "mp4",
#             "url": "https://video-content-bucket-1.s3.us-east-1.amazonaws.com/movies/The.Wolf.of.Wall.Street.2013.1080p.BluRay.x264.YIFY.mp4",
#             "quality": "HD"
#         }]			
#     }
# }