import googleapiclient.discovery
import pymongo
import pandas as pd
import pymysql
from datetime import datetime
import streamlit as st



def Api_connection():

    api_service_name = "youtube"
    api_version = "v3"
    
        # Get credentials and create an API client
    api_key = 'AIzaSyDW-9Yw05o-m266Obsi2GSfyGcR6sYLyhg'
    youtube = googleapiclient.discovery.build(
            api_service_name, api_version, developerKey=api_key)
    return youtube

youtube = Api_connection()


# get channels informations


def get_channels_details(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
        )
    response=request.execute()


    for i in response['items']:
         details = dict(chanenel_name=i["snippet"]["title"],
                        channel_id = i["id"],
                        subscribers = i["statistics"]["subscriberCount"],
                        videos = i["statistics"]["videoCount"],
                        Views = i["statistics"]["viewCount"],
                        Channel_Description = i["snippet"]["description"],
                        playlist = i["contentDetails"]["relatedPlaylists"]["uploads"]
                       )
    return details



# get videos id's

def get_video_Ids(channel_id):

    videos_Id = []
    request = youtube.channels().list(
            part="contentDetails",
            id=channel_id
            )
    response=request.execute()


    playlist_ids = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None


    while True:

        request_playlistId = youtube.playlistItems().list(
                             part = "snippet",
                             playlistId	 = playlist_ids,
                             maxResults = 50,
                             pageToken = next_page_token
                             )
        response = request_playlistId.execute()


        for i in range(len(response['items'])):
            videos_Id.append(response['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = response.get('nextPageToken')


        if next_page_token is None:
            break
    return videos_Id


#get video details

def get_video_Details(videos_Id):

    video_Details = []

    for video_id in videos_Id:
        request = youtube.videos().list(
                id = video_id,
                part = "snippet,ContentDetails,statistics"

            )
        response = request.execute()

        for i in response['items']:
                data = dict(channel_Name = i['snippet']['channelTitle'],
                            channel_id = i['snippet']['channelId'],
                            Video_Id = i['id'],
                            Title = i['snippet']['title'],
                            Tags =  i['snippet'].get('tags'),
                            PublishedAt = i['snippet']['publishedAt'],
                            View_Count = i['statistics'].get('viewCount'),
                            Like_Count = i['statistics']['likeCount'],
                            Dislike_Count = i['statistics'].get('dislikeCount'),
                            Favorite_Count = i['statistics']['favoriteCount'],
                            Comment_Count = i['statistics'].get('commentCount'),
                            Duration = i['contentDetails']['duration'],
                            Thumbnail = i['snippet']['thumbnails']['default']['url'],
                            Caption_Status = i['contentDetails']['caption']
                           )
                video_Details.append(data)

    return video_Details


# get comment details
def get_Comment_info(Video_ID):
    Comment_data = []
    try:
        for videoid in Video_ID:
            request = youtube.commentThreads().list(
                        videoId = videoid,
                        part = "snippet",
                        maxResults = 50
                    )
            response = request.execute()


            for item in response['items']:
                data = dict(Comment_Id = item['snippet']['topLevelComment']['id'],
                            video_Id = item['snippet']['videoId'],
                            Comment_Text = item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_Author = item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_PublishedAt = item['snippet']['topLevelComment']['snippet']['publishedAt'])
                Comment_data.append(data)

    except:
        pass
    return Comment_data


# get playlist details

def get_playlist_infos(channel_id):

    next_page_token = None
    playlist_info = []

    while True:

        request = youtube.playlists().list(     
                  channelId = channel_id,
                  part = 'snippet,contentDetails',
                  maxResults = 50,
                  pageToken = next_page_token
        )

        response = request.execute()


        for i in response['items']:
            data = dict(playlist_Id = i['id'],
                        Title = i['snippet']['title'],
                        Channel_ID = i['snippet']['channelId'],
                        Channel_Name = i['snippet']['channelTitle'],
                        PublishedAt = i['snippet']['publishedAt'],
                        video_Count = i['contentDetails']['itemCount']
                       )
            playlist_info.append(data)

        next_page_token = response.get('nextPageToken')

        if next_page_token == None:
            break
            
    return playlist_info


# upload the youtube data to mongodb


client = pymongo.MongoClient('mongodb://localhost:27017')

mydb = client["youtube_Data"]


def channel_Details(channel_id):
      channel_details = get_channels_details(channel_id)
      video_ids = get_video_Ids(channel_id)
      playlist_Details = get_playlist_infos(channel_id)
      video_Details =  get_video_Details(video_ids)
      comment_Details =   get_Comment_info(video_ids)

        
      infos = mydb["channel_Details"]
      infos.insert_one({"channel_Information":channel_details,
                        "playList_Information":playlist_Details,
                        "video_Information":video_Details,
                        "comment_Information":comment_Details
                       })
      return "uploaded"



def channels_tables():

    myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='2104030@#',database = 'youtube_data')
    cur = myconnection.cursor()

    drop = "drop table if exists channels"
    cur.execute(drop)
    myconnection.commit()

    try:
        create = ('''create table channels(chanenel_name varchar(255),
                    channel_id varchar(255) primary key,
                    subscribers bigint,
                    videos int,
                    Views bigint,
                    Channel_Description text,
                    playlist varchar(100))''')
        cur.execute(create)
        myconnection.commit()

    except:
        st.write("channels table has been already created")


    channel_info = []
    mydb = client["youtube_Data"]
    infos = mydb["channel_Details"]

    for channel_infos in infos.find({},{"_id":0,"channel_Information":1}):
        channel_info.append(channel_infos["channel_Information"])
    df = pd.DataFrame(channel_info)

    for index,row in df.iterrows():
        data = '''insert into channels(chanenel_name,
                                                channel_id,
                                                subscribers,
                                                videos,
                                                Views,
                                                Channel_Description,
                                                playlist
                                                )
                                                values(%s,%s,%s,%s,%s,%s,%s)'''
        values = (row['chanenel_name'],
                  row['channel_id'],
                  row['subscribers'],
                  row['videos'],
                  row['Views'],
                  row['Channel_Description'],
                  row['playlist'])
        try:         
            cur.execute(data,values)
            myconnection.commit()

        except:
            print("About the channel details are already writed")


def playlist_table():
    from datetime import datetime
    myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='2104030@#',database = 'youtube_data')
    cur = myconnection.cursor()


    drop = "Drop table if exists playlist"
    cur.execute(drop)
    myconnection.commit()

    try:

        create = '''create table playlist(playlist_Id varchar(100) primary key,
                            Title varchar(255),
                            Channel_ID varchar(100),
                            Channel_Name varchar(100),
                            publishedAt timestamp,
                            video_Count int
                            )'''
        cur.execute(create)
        myconnection.commit()
    except:
        st.write("The playlist table has been already created")

    playlist_info = []
    mydb = client["youtube_Data"]
    infos = mydb["channel_Details"]

    for playlist_infos in infos.find({},{"_id":0,"playList_Information":1}):
        for i in range(len(playlist_infos['playList_Information'])):
            playlist_info.append(playlist_infos['playList_Information'][i])
        playlist_df =pd.DataFrame(playlist_info)


    try:
        for index, row in playlist_df.iterrows():

            # Convert the MongoDB timestamp to the MySQL-compatible format
            published_at = datetime.strptime(row['PublishedAt'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')

            

            data = '''INSERT INTO playlist(playlist_Id,
                                   Title,
                                   Channel_ID,
                                   Channel_Name,
                                   PublishedAt,  # Correct case here
                                   video_Count)
                                   VALUES (%s, %s, %s, %s, %s, %s)'''


            values = (row['playlist_Id'],
                      row['Title'],
                      row['Channel_ID'],
                      row['Channel_Name'],
                      published_at,
                      row['video_Count'])

            cur.execute(data, values)

        myconnection.commit()
        
    except:
        st.write("These Playlist values are already inserted into the table")
    
        
           

def videos_table():

    myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='2104030@#',database = 'youtube_data')
    cur = myconnection.cursor()


    drop = "drop table if exists videos"
    cur.execute(drop)
    myconnection.commit()

    try:
        create_query = '''CREATE TABLE videos (
            channel_Name VARCHAR(255),
            channel_id VARCHAR(100),
            Video_Id VARCHAR(100) primary key,
            Title VARCHAR(255),
            Tags text,
            PublishedAt TIMESTAMP,
            View_Count bigint,
            Like_Count bigint,
            Dislike_Count bigint,
            Favorite_Count INT,
            Comment_Count INT,
            Thumbnail VARCHAR(255),
            Caption_Status VARCHAR(50)
        )'''

        cur.execute(create_query)
        myconnection.commit()

    except:
        st.write("Videos table are already created")

    client = pymongo.MongoClient('mongodb://localhost:27017')
    mydb = client["youtube_Data"]
    infos = mydb["channel_Details"]


    videolist_info = []


    for videos_info in infos.find({}, {"_id": 0, "video_Information": 1}):
        for i in range(len(videos_info['video_Information'])):
            videolist_info.append(videos_info['video_Information'][i])


    videolist_df = pd.DataFrame(videolist_info)


    for index, row in videolist_df.iterrows():

        published_at = datetime.strptime(row['PublishedAt'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
        if not isinstance(row['Tags'], list):
            tags_string = str(row['Tags'])
        else:
            tags_string = ','.join(row['Tags'])


        data = '''INSERT INTO videos 
                  (channel_Name, channel_id, Video_Id, Title,Tags, PublishedAt, View_Count,
                  Like_Count, Dislike_Count, Favorite_Count, Comment_Count, Thumbnail, Caption_Status)
                  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s)'''

        values = (row['channel_Name'], row['channel_id'], row['Video_Id'], row['Title'],
                  tags_string,published_at, row['View_Count'], row['Like_Count'],
                  row['Dislike_Count'], row['Favorite_Count'], row['Comment_Count'],
                  row['Thumbnail'], row['Caption_Status'])
        try:
            cur.execute(data, values)
            myconnection.commit()
        except:
            st.write("These Videos values are already inserted into the tables")



def comment_table():
    myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='2104030@#',database = 'youtube_data')
    cur = myconnection.cursor()

    drop = "drop table if exists comment"
    cur.execute(drop)
    myconnection.commit()

    try:
        create = ('''create table comment(Comment_Id varchar(100) primary key,
                    video_Id varchar(50),
                    Comment_Text text,
                    Comment_Author varchar(100),
                    Comment_PublishedAt timestamp
                )''')
        cur.execute(create)
        myconnection.commit()
    except:
        st.write("Comments tables are already created")

    mydb = client["youtube_Data"]
    infos = mydb["channel_Details"]

    comment_info = []

    for comments_info in infos.find({}, {"_id": 0, "comment_Information": 1}):
        for i in range(len(comments_info["comment_Information"])):
            comment_info.append(comments_info["comment_Information"][i])

    comment_df = pd.DataFrame(comment_info)

    

    for index, row in comment_df.iterrows():
        # Convert the string to the correct datetime format
        comment_published_at = datetime.strptime(row['Comment_PublishedAt'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')

        data = '''INSERT INTO comment(Comment_Id,
                                       video_Id,
                                       Comment_Text,
                                       Comment_Author,
                                       Comment_PublishedAt)
                                       VALUES (%s, %s, %s, %s, %s)'''

        values = (row['Comment_Id'],
                  row['video_Id'],
                  row['Comment_Text'],
                  row['Comment_Author'],
                  comment_published_at)  

        try:
            cur.execute(data, values)
            myconnection.commit()
        except:
            st.write("These Comments values are already inserted into the tables")

def get_all_tables():
    channels_tables()
    playlist_table()
    videos_table()
    comment_table()
    
    return "successfully"



def st_channels_tabel():
    channel_info = []
    mydb = client["youtube_Data"]
    infos = mydb["channel_Details"]

    for channel_infos in infos.find({},{"_id":0,"channel_Information":1}):
        channel_info.append(channel_infos["channel_Information"])
    df = st.dataframe(channel_info)

    return df


def st_playlist_table():

    playlist_info = []
    mydb = client["youtube_Data"]
    infos = mydb["channel_Details"]

    for playlist_infos in infos.find({},{"_id":0,"playList_Information":1}):
        for i in range(len(playlist_infos['playList_Information'])):
            playlist_info.append(playlist_infos['playList_Information'][i])
    playlist_df =st.dataframe(playlist_info)

    return playlist_df




def st_videos_table():

    mydb = client["youtube_Data"]
    infos = mydb["channel_Details"]

    videolist_info = []

    for videos_info in infos.find({}, {"_id": 0, "video_Information": 1}):
        for i in range(len(videos_info['video_Information'])):
            videolist_info.append(videos_info['video_Information'][i])

    videolist_df = st.dataframe(videolist_info)
    return videolist_df



def st_comment_table():

    mydb = client["youtube_Data"]
    infos = mydb["channel_Details"]

    comment_info = []

    for comments_info in infos.find({}, {"_id": 0, "comment_Information": 1}):
        for i in range(len(comments_info["comment_Information"])):
            comment_info.append(comments_info["comment_Information"][i])

    comment_df = st.dataframe(comment_info)

    return comment_df




# Streamlit code

st.title(':red[Youtube Data Harvesting]')

channel_id = st.text_input("Enter the channel ID")
channels = channel_id.split(',')
channels = [ch.strip() for ch in channels if ch]


if st.button("upload data to Mongo Db"):
    for channel in channels:
        chan_id = []
        mydb = client["youtube_Data"]
        infos = mydb["channel_Details"]
        for channel_data in infos.find({},{"_id":0,"channel_Information":1}):
            chan_id.append(channel_data['channel_Information']['channel_id'])
        if channel in chan_id:
            st.success("The channel Id you entered is already exists and also the channel information")
        else:
            insert = channel_Details(channel)
            st.success(insert)


if st.button("Convert to sql"):
    display = get_all_tables()
    st.success(display)


option = st.radio(
   "Select the table you want to view",
   ("channels","playlist","videos","comment")  
)

if option == "channels":
    st_channels_tabel()
elif option == "playlist":
    st_playlist_table()
elif option == "videos":
    st_videos_table()
elif option == "comment":
    st_comment_table()


myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='2104030@#',database = 'youtube_data')
cur = myconnection.cursor()


questions = st.selectbox("Select any question from below",
                         ("1.What are the names of all the videos and their corresponding channels",
                            "2.Which channels have the most number of videos, and how many videos do they have?",
                            "3.top 10 most viewed videos and their respective channels",
                            "4.comments in each video",
                            "5.videos with the highest number of likes and their channel names",
                            "6.Total number of likes and their video names",
                            "7.Total number of views and their channel names",
                            "8.Video published in the year of 2022",
                            "9.Videos with must number of comments and their channel names"

                               ))


if questions == '1.What are the names of all the videos and their corresponding channels':
    ans1 = '''select title as videos,channel_name as channelname from videos'''
    cur.execute(ans1)
    myconnection.commit()
    a1 = cur.fetchall()
    st.write(pd.DataFrame(a1,columns=["video title","channel name"]))


elif questions == '2.Which channels have the most number of videos, and how many videos do they have?':
    ans2 = '''SELECT chanenel_name AS channelname, videos AS no_videos
                FROM channels
                ORDER BY videos DESC;'''
    cur.execute(ans2)
    myconnection.commit()
    a2 = cur.fetchall()
    st.write(pd.DataFrame(a2,columns=["Channel name","No of Videos"]))
    
elif questions == '3.top 10 most viewed videos and their respective channels':
    ans3 = '''SELECT View_Count AS views, channel_Name AS channelname, Title AS video_title
                FROM videos
                WHERE View_Count IS NOT NULL
                ORDER BY View_Count DESC
                LIMIT 10;'''
    cur.execute(ans3)
    myconnection.commit()
    a3 = cur.fetchall()
    st.write(pd.DataFrame(a3,columns=["Views","channel name","Video Title"]))  

elif questions == '4.comments in each video':
    ans4 = '''SELECT Comment_Count AS comment,Title AS video_title
                FROM videos
                where Comment_Count is NOT NULL;'''
    cur.execute(ans4)
    myconnection.commit()
    a4 = cur.fetchall()
    st.write(pd.DataFrame(a4,columns=["No of Comments","Video Title"]))  


elif questions == '5.videos with the highest number of likes and their channel names':
    ans5 = '''SELECT Title AS video_title, Like_Count AS likes, channel_Name AS channel_name
                FROM videos
                WHERE Like_Count IS NOT NULL
                ORDER BY Like_Count DESC;'''
    cur.execute(ans5)
    myconnection.commit()
    a5 = cur.fetchall()
    st.write(pd.DataFrame(a5,columns=["Video Title","Like Count","Channel Name"]))  


elif questions == '6.Total number of likes and their video names':
    ans6 = '''SELECT Title AS video_name, SUM(Like_Count) AS total_likes
                FROM videos
                WHERE Like_Count IS NOT NULL
                GROUP BY video_name;'''
    cur.execute(ans6)
    myconnection.commit()
    a6 = cur.fetchall()
    st.write(pd.DataFrame(a6,columns=["Video Title","Like Counts"]))  


elif questions == '7.Total number of views and their channel names':
    ans7 = '''SELECT chanenel_name as channel_name, Views AS total_views
                FROM channels;'''
    cur.execute(ans7)
    myconnection.commit()
    a7 = cur.fetchall()
    st.write(pd.DataFrame(a7,columns=["Channel Name","Total Views"]))  



elif questions == '8.Video published in the year of 2022':
    ans8 = '''SELECT DISTINCT channel_Name,Title,PublishedAt
                FROM videos
                WHERE YEAR(PublishedAt) = 2022;'''
    cur.execute(ans8)
    myconnection.commit()
    a8 = cur.fetchall()
    st.write(pd.DataFrame(a8,columns=["Channel Name","Video Title","Published Date"]))  
  


elif questions == '9.Videos with must number of comments and their channel names':
    ans9 = '''SELECT Title,channel_Name,Comment_Count
                FROM videos
                WHERE Comment_Count is NOT NULL ORDER BY Comment_Count desc;'''
    cur.execute(ans9)
    myconnection.commit()
    a9 = cur.fetchall()
    st.write(pd.DataFrame(a9,columns=["Video Title","Channel Name","Most comments"]))  