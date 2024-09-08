import python_package
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, request, jsonify
from cachetools import cached, TTLCache
import logging
import requests


app = Flask("yt_channel_analyze")
cache = TTLCache(maxsize=100, ttl=3600)
executor = ThreadPoolExecutor(max_workers=10)  # Set max_workers as needed

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@cached(cache)
def get_youtube_data(url):
    """
    Retrieves data from a YouTube URL. Results are cached for subsequent requests.
    """
    return python_package.call_youtube_request(url)


def check_ad_presence(video_id):
    video_url = f"https://www.youtube.com/watch?v={video_id['contentDetails']['videoId']}"
    http_string = get_youtube_data(video_url)
    return "paidContentOverlayRenderer" in http_string


def check_if_short(video_id):
    short_url = f"https://www.youtube.com/shorts/{video_id['contentDetails']['videoId']}"
    http_string = get_youtube_data(short_url)
    num_shorts = http_string.count("shorts")
    if num_shorts > 201:
        video_id["short"] = True
    else:
        video_id["short"] = False
    return video_id


def check_author(channel_id):
    author_url = f"https://www.youtube.com/channel/{channel_id}/about"
    http_string = get_youtube_data(author_url)
    name = python_package.transform.find_between(http_string, f'{channel_id}", "name": "', '"')
    date = python_package.transform.find_between(http_string, f'joinedDateText', '","')[-11:]
    return {"channel_id": channel_id, "name": name, "date": date}


def get_video_comments(yt_conn, video_id):
    try:
        return yt_conn.get_comment_thread(video_id=video_id['contentDetails']['videoId'])
    except Exception as e:
        logger.error(f"Error getting comments for video ID {video_id}: {e}")
        return None


def get_author_data(comment_author_channel_id):
    try:
        return check_author(comment_author_channel_id)
    except Exception as e:
        logger.error(f"Error getting gender for channel ID {comment_author_channel_id}: {e}")
        return None


def channel_data(request, context=None):
    input_url = request.args.get('input')
    global executor

    if not input_url:
        return jsonify({"error": "Invalid input"}), 400

    url = input_url

    http_string = get_youtube_data(input_url)

    # Get id from http
    channel_id = python_package.find_between(http_string, '"https://www.youtube.com/channel/', '"')[0:24]

    # YT API key
    YOUTUBE_API_KEY = "AIzaSyBg8t78HFFb4lZb-KRlUUV4EL6F-6SIxEQ"
    # Alternative "AIzaSyCHjvwJ9lBTzqyp4STGEuCet489sfmJuok" @kchowaniec@gt.com
    # Alternative_2 = "AIzaSyDWueJdR8NShrVmlRhtVLU_efqlW6ZqOIY" @czarny121@gmail
    # Alternative_3 = "AIzaSyBg8t78HFFb4lZb-KRlUUV4EL6F-6SIxEQ" @konrad.chowaniec@gmail

    yt_conn = python_package.yt_conn(API_key=YOUTUBE_API_KEY)

    # YT Channel API
    channel_details = yt_conn.process_channel_id(channel_id=channel_id)

    # YT Playlist API
    data_play_list = yt_conn.get_videos_of_channel(channel_details=channel_details, max_results=50)

    # id list of uploaded videos
    video_id_list = data_play_list['items']

    # check if short
    videos_id_list = list(executor.map(check_if_short, video_id_list))

    filtered_video_id_list = [d for d in videos_id_list if d.get('short') is False]

    # id list of uploaded videos limited to 5
    filtered_list = python_package.last_videos_id(filtered_video_id_list)

    # string to be inserted into videos API call
    video_ids = ','.join([filtered['contentDetails']['videoId'] for filtered in filtered_list])

    # Videos data
    data_videos = yt_conn.get_video_details(id_list=video_ids)

    videos_views_list = []
    videos_published_list = []
    videos_title_list = []
    videos_desc_list = []
    videos_duration_list = []
    videos_kids_list = []
    videos_like_list = []
    videos_comment_list = []
    videos_link_list = []

    videos_ads_list = list(executor.map(check_ad_presence, filtered_list))

    first_ad_count = videos_ads_list[0:1].count(True)
    first_five_ad_count = videos_ads_list[0:5].count(True)
    overall_ad_count = videos_ads_list.count(True)

    for video in data_videos['items']:
        videos_views_list.append(int(video['statistics']['viewCount']))

    views_to_sub_ratio = int(python_package.transform.avg(videos_views_list)) / \
        int(channel_details['items'][0]['statistics']['subscriberCount'])

    try:
        # Select every second item starting from the second one
        selected_videos = [filtered_list[i] for i in ([1, 3, 5] if len(filtered_list) >= 5 else
                                                      [1, 3, 4] if len(filtered_list) >= 3 else
                                                      [1, 2, 3] if len(filtered_list) >= 2 else
                                                      [1, 2] if len(filtered_list) == 1 else
                                                      [1])
                           if i < len(filtered_list)]

        comments = []
        for video in selected_videos:
            vid_comments = get_video_comments(yt_conn, video)
            if vid_comments:
                comments.extend(vid_comments['items'])

        comment_authors = set()
        for comment in comments:
            author_id = comment['snippet']['topLevelComment']['snippet']['authorChannelId']['value']
            comment_authors.add(author_id)

        comment_authors_details = list(executor.map(get_author_data, comment_authors))

        # Gender processing
        gender_list = python_package.guess_gender_parallel(comment_authors_details)
        gender_list_filtered = [gender for gender in gender_list if gender != 'unknown']
        male_percentage, female_percentage = python_package.gender_summary_generator(gender_list_filtered)

        # Account creation year analysis
        account_creation_years = [author['date'][-4:] for author in comment_authors_details if
                                  author and 'date' in author]
        year_counts = {}
        for year in account_creation_years:
            year_counts[year] = year_counts.get(year, 0) + 1

        creation_year_division = dict(sorted(year_counts.items()))
        age_brackets = python_package.transform.predict_age_brackets(creation_year_division, male_percentage,
                                                                     views_to_sub_ratio)

    except Exception as e:
        logger.error(f"General error in process_comments: {e}")

    for id in filtered_list[:15]:
        videos_link_list.append(f'https://www.youtube.com/watch?v={id["contentDetails"]["videoId"]}')

    output = python_package.form_output(data_videos, videos_ads_list, videos_link_list, videos_views_list,
                                        videos_like_list, videos_comment_list,
                                        videos_published_list, videos_title_list, videos_desc_list,
                                        videos_duration_list, videos_kids_list, channel_details,
                                        first_ad_count, first_five_ad_count, overall_ad_count,
                                        male_percentage, female_percentage, creation_year_division,
                                        age_brackets, views_to_sub_ratio)

    with app.app_context():
        response = jsonify({"data": output})
        response.headers.add('Access-Control-Allow-Origin', '*')
    return response


class MockRequest:
    def __init__(self, input_url):
        self.args = {'input': input_url}


def fetch_channel_data(channel_id):
    url = f'https://europe-west1-growthunders.cloudfunctions.net/channel-details?input=https://www.youtube.com/channel/{channel_id}'
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()  # Assuming the endpoint returns JSON data
    else:
        raise Exception(f"Failed to fetch data for channel {channel_id}, status code: {response.status_code}")


if __name__ == "__main__":
    bq_client = python_package.bigqueryConnection()

    channels_to_process = bq_client.bq_query(query="""
    
            SELECT * FROM `growthunders.Youtube.initial_search_result_playerplus` 
            where channel_id not in (Select id from `growthunders.Youtube.channel_data`)
            and subs > 5000  and country like "%United S%" order by subs desc
            """)
    channels_to_process_list = [dict(row.items()) for row in channels_to_process]

    list_to_upload = []
    for channel in channels_to_process_list[100:500]:
        try:
            response = fetch_channel_data(channel['channel_id'])
            # mock_request = MockRequest(f"https://www.youtube.com/channel/{channel['channel_id']}")
            # response = channel_data(request=mock_request)
            list_to_upload.append(response.get("data"))
        except Exception as e:
            print(f"Error p-rocessing channel: {channel}")
            print(f"Error message: {e}")

    list_to_upload_channel_data = [
        {k: v for k, v in d.items() if k not in ['creation_year_division', 'age_brackets']}
        for d in list_to_upload
    ]
    list_to_upload_creation_year_division = [
        {**d['creation_year_division'], 'channelId': d['id']}
        for d in list_to_upload if 'creation_year_division' in d
    ]

    bq_client.insert_data(table_id="growthunders.Youtube.channel_data", rows_to_insert=list_to_upload_channel_data)
    bq_client.insert_data(table_id="growthunders.Youtube.channel_audience_account_create_date",
                          rows_to_insert=list_to_upload_creation_year_division)
