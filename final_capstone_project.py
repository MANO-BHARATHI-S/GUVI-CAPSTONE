from googleapiclient.discovery import build
import pymongo
import mysql.connector
import pandas as pd
import streamlit as st

## API key  AIzaSyDC7otU4u6MNGZgqmEzUxoAz_8CpWWLyRs

## API Connect
def api_connect():
    api_id="AIzaSyDC7otU4u6MNGZgqmEzUxoAz_8CpWWLyRs"
    api_service_name="youtube"
    api_version="v3"
    youtube=build(api_service_name,api_version,developerKey=api_id)
    return youtube
youtube=api_connect()


#getting channel info
def get_channel_info(channel_id):
    request=youtube.channels().list(
                    part="snippet,ContentDetails,statistics",
                    id=channel_id
    )
    response=request.execute()
    for i in response["items"]:
        data=dict(channel_name=i["snippet"]["title"],
                 channel_id=i['id'],
                 subscribers_count=i["statistics"]["subscriberCount"],
                 videos_count=i["statistics"]["videoCount"],
                 views=i["statistics"]["viewCount"],
                 channel_Description=i["snippet"]["description"],
                 playlist_id=i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data



##getting video ids
def get_all_video_ids(channel_id):
    video_ids=[]
    video_id=youtube.channels().list(id=channel_id,
                                    part="contentDetails").execute()
    playlist_id=video_id["items"][0]["contentDetails"]["relatedPlaylists"]['uploads']
    next_page_token=None
    while True:
        video_playlist_details=youtube.playlistItems().list(
                                                            part="snippet",
                                                            playlistId=playlist_id, 
                                                            maxResults=50,
                                                            pageToken=next_page_token).execute()
        for i in range (len(video_playlist_details['items'])):
            video_ids.append(video_playlist_details['items'][i]['snippet']["resourceId"]['videoId'])
        next_page_token=video_playlist_details.get('nextPageToken')
        if next_page_token is None:
            break  
    return video_ids


##getting video information 
def get_video_information(video_Ids_channel):
    video_data = []
    for video_id in video_Ids_channel:
        request = youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id
        )
        response = request.execute()
        for item in response['items']:
            video_info = dict(
                channel_name=item['snippet']['channelTitle'],
                channel_id=item['snippet']['channelId'],
                video_id=item['id'],
                video_tittle=item['snippet']['title'],
                v_thumbnail=item['snippet']['thumbnails'],
                v_description=item['snippet']['description'],
                video_tags=item.get('tags'),
                v_date_of_publish=item['snippet']['publishedAt'],
                video_duration=item['contentDetails']['duration'],
                views_count=item['statistics']['viewCount'],
                comment_count= item['statistics'].get('commentCount', 0),
                like_count=item['statistics']['likeCount'],
                fav_count=item['statistics']['favoriteCount'],
                video_Definition=item['contentDetails']['definition'],
                Caption_status=item['contentDetails']['caption']
            )
            video_data.append(video_info)
    return video_data

##if any items not available use ##get function eg video_tags=item.get('tags')



##getting comments details
def get_comment_details(videos_ids):
    comment_informations=[]
    try:
        for video_id in videos_ids:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()
            for items in response["items"]:
                comment_data=dict(commenter_id=items['snippet']['topLevelComment']['id'],
                                 video_id=items['snippet']['topLevelComment']['snippet']['videoId'],
                                 comment_text=items['snippet']['topLevelComment']['snippet']['textDisplay'],
                                 comment_author=items['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                 comment_time=items['snippet']['topLevelComment']['snippet']['publishedAt'],)
                comment_informations.append(comment_data)
    except:
        pass
    return  comment_informations



def get_playlist_details(channel_id):
    next_page_token = None  # Changed variable name to follow Python conventions
    playlist_information = []

    while True:
        request = youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()

        for item in response['items']:
            playlist_data=dict(playlist_id=item['id'],
                       playlist_tittle=item['snippet']['title'],
                       channel_id=item['snippet']['channelId'],
                       Channel_name=item['snippet']['channelTitle'],
                       published_tym=item['snippet']['publishedAt'],
                       Video_count=item['contentDetails']['itemCount'])
            playlist_information.append(playlist_data)

        next_page_token = response.get('nextPageToken')  # Corrected variable name
        if next_page_token is None:
            break

    return playlist_information  



#upload to mangodb
from pymongo import MongoClient

client = MongoClient("mongodb+srv://manobharathi:manobharathi@cluster0.1qslovj.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db=client["youtube_data"]



def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    pl_details=get_playlist_details(channel_id)
    vi_ids=get_all_video_ids(channel_id)
    vi_details=get_video_information(vi_ids)
    cm_details=get_comment_details(vi_ids)
    
    collection1=db["channel_details"]
    collection1.insert_one({"channel_information":ch_details,
                            "playlist_information":pl_details,
                            "video_information":vi_details,
                            "cm_details":cm_details})
    return "upload successfull"



##channel table creation 

def channel_table():
    try:
        mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Mano@3221",
            database="youtube_data",
            port="3306"
        )
        cursor = mydb.cursor()
        drop_query = '''drop table if exists channels'''
        cursor.execute(drop_query)
        mydb.commit()

        create_query = '''
        CREATE TABLE IF NOT EXISTS channels (
            channel_name VARCHAR(100),
            channel_id VARCHAR(80) PRIMARY KEY,
            subscribers_count BIGINT,
            views BIGINT,
            videos_count INT,
            channel_Description TEXT,
            playlist_id VARCHAR(80)
        )
        '''
        cursor.execute(create_query)
        mydb.commit()
        print("Channels table created successfully")

    except mysql.connector.Error as err:
        print("Error:", err)

    finally:
        if 'mydb' in locals() or 'mydb' in globals():
            mydb.close()

    ch_list = []
    db = client["youtube_data"]
    collection01 = db["channel_details"]

    # Iterate over documents in the collection and print the channel_informations field
    for ch_data in collection01.find({}, {"_id": 0, "channel_information": 1}):
        ch_list.append(ch_data["channel_information"])
    df = pd.DataFrame(ch_list)

    # Assuming you already have your DataFrame df populated

    try:
        mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Mano@3221",
            database="youtube_data",
            port="3306"
        )
        cursor = mydb.cursor()

        for index, row in df.iterrows():
            insert_query = '''
            INSERT INTO channels(
                channel_name,
                channel_id,
                subscribers_count,
                videos_count,
                views,
                channel_Description,
                playlist_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            '''
            values = (
                row['channel_name'],
                row['channel_id'],
                row['subscribers_count'],
                row['videos_count'],
                row['views'],
                row['channel_Description'],
                row['playlist_id']
            )

            cursor.execute(insert_query, values)
            mydb.commit()

    except mysql.connector.Error as err:
        print("values already inserted")

    finally:
        if 'mydb' in locals() or 'mydb' in globals():
            mydb.close()

channel_table()



##Playlist table creation 
def playlist_table():
    try:
        mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Mano@3221",
            database="youtube_data",
            port="3306"
        )
        cursor = mydb.cursor()
        drop_query = '''drop table if exists playlists'''
        cursor.execute(drop_query)
        mydb.commit()

        create_query = '''
        CREATE TABLE IF NOT EXISTS playlists (
            playlist_id VARCHAR(100) PRIMARY KEY,
            playlist_tittle VARCHAR(80),
            channel_id VARCHAR(80),
            Channel_name VARCHAR(80),
            published_tym TIMESTAMP,
            Video_count INT)'''

        cursor.execute(create_query)
        mydb.commit()
        print("playlist table created successfully")

    except mysql.connector.Error as err:
        print("Error:", err)

    finally:
        if 'mydb' in locals() or 'mydb' in globals():
            mydb.close()



    pl_list = []
    db = client["youtube_data"]
    collection01 = db["channel_details"]

    # Iterate over documents in the collection and print the channel_informations field
    for pl_data in collection01.find({}, {"_id": 0, "playlist_information": 1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1 = pd.DataFrame(pl_list)

    df1['published_tym'] = pd.to_datetime(df1['published_tym']).dt.strftime('%Y-%m-%d %H:%M:%S')

    try:
        mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Mano@3221",
            database="youtube_data",
            port="3306"
        )
        cursor = mydb.cursor()

        for index, row in df1.iterrows():
            insert_query = '''
            INSERT IGNORE INTO playlists(
                playlist_id,
                playlist_tittle,
                channel_id,
                Channel_name,
                published_tym,
                Video_count)
            VALUES (%s, %s, %s, %s, %s, %s)'''
            values = (
                row['playlist_id'],
                row['playlist_tittle'],
                row['channel_id'],
                row['Channel_name'],
                row['published_tym'],
                row['Video_count'])

            cursor.execute(insert_query, values)
            mydb.commit()

    except mysql.connector.Error as err:
        print("MySQL Error:", err)

    finally:
        if 'mydb' in locals() or 'mydb' in globals():
            mydb.close()

            
playlist_table()



##pideo table creation

def video_table():
    try:
        mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Mano@3221",
            database="youtube_data",
            port="3306"
        )
        cursor = mydb.cursor()
        drop_query = '''DROP TABLE IF EXISTS videos'''
        cursor.execute(drop_query)
        mydb.commit()

        create_query = '''
        CREATE TABLE IF NOT EXISTS videos (
            channel_name VARCHAR(100),
            channel_id VARCHAR(80),
            video_id VARCHAR(30) PRIMARY KEY,
            video_title VARCHAR(200),
            v_thumbnail VARCHAR(200),
            v_description TEXT,
            video_tags TEXT,
            v_date_of_publish TIMESTAMP,
            video_duration VARCHAR(20),
            views_count BIGINT,
            comment_count BIGINT,
            like_count BIGINT,
            fav_count INT,
            video_Definition VARCHAR(10),
            Caption_status VARCHAR(70)
        )'''
        cursor.execute(create_query)
        mydb.commit()
        print("videos table created successfully")

    except mysql.connector.Error as err:
        print("Error:", err)

    finally:
        if 'mydb' in locals() or 'mydb' in globals():
            mydb.close()



    vi_list=[]
    db = client["youtube_data"]
    collection01 = db["channel_details"]

    # Iterate over documents in the collection and print the channel_informations field
    for vi_data in collection01.find({}, {"_id": 0, "video_information":1 }):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2 = pd.DataFrame(vi_list) 

    df2['v_date_of_publish'] = pd.to_datetime(df2['v_date_of_publish']).dt.strftime('%Y-%m-%d %H:%M:%S')

    try:
        mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Mano@3221",
            database="youtube_data",
            port="3306"
        )
        cursor = mydb.cursor()

        for index, row in df2.iterrows():
            insert_query = '''
            INSERT IGNORE INTO videos(
                channel_name,
                channel_id,
                video_id,
                video_title,
                v_thumbnail,
                v_description,
                video_tags,
                v_date_of_publish,
                video_duration,
                views_count,
                comment_count,
                like_count,
                fav_count,
                video_Definition,
                Caption_status
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''

            values = (
                row['channel_name'],
                row['channel_id'],
                row['video_id'],
                row['video_tittle'],  
                row['v_thumbnail'],
                row['v_description'],
                row['video_tags'],
                row['v_date_of_publish'],
                str(row['video_duration']),
                row['views_count'],
                row['comment_count'],
                row['like_count'],
                row['fav_count'],
                row['video_Definition'],
                row['Caption_status']
            )

            # Ensure all values are converted to appropriate data types
            values = tuple(str(val) if isinstance(val, dict) else val for val in values)

            cursor.execute(insert_query, values)
            mydb.commit()  # Moved commit statement here

    except mysql.connector.Error as err:
        print("MySQL Error:", err)

    finally:
        if 'mydb' in locals() or 'mydb' in globals():
            mydb.close()


video_table()



##comments table creation 

def comment_table():
    try:
        mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Mano@3221",
            database="youtube_data",
            port="3306"
        )
        cursor = mydb.cursor()
        drop_query = '''DROP TABLE IF EXISTS comments'''
        cursor.execute(drop_query)
        mydb.commit()

        create_query = '''
        CREATE TABLE IF NOT EXISTS comments (
            commenter_id VARCHAR(100) PRIMARY KEY,
            video_id VARCHAR(80),
            comment_text TEXT,
            comment_author VARCHAR(200),
            comment_time TIMESTAMP
        )'''
        cursor.execute(create_query)
        mydb.commit()
        print("comment table created successfully")

    except mysql.connector.Error as err:
        print("Error:", err)

    finally:
        if 'mydb' in locals() or 'mydb' in globals():
            mydb.close()




    com_list=[]
    db = client["youtube_data"]
    collection01 = db["channel_details"]

    # Iterate over documents in the collection and print the channel_informations field
    for com_data in collection01.find({}, {"_id": 0, "cm_details":1 }):
        for i in range(len(com_data["cm_details"])):
            com_list.append(com_data["cm_details"][i])
    df3=pd.DataFrame(com_list)
    
    
    
    
    df3['comment_time'] = pd.to_datetime(df3['comment_time']).dt.strftime('%Y-%m-%d %H:%M:%S')

    try:
        mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Mano@3221",
            database="youtube_data",
            port="3306"
        )
        cursor = mydb.cursor()

        for index, row in df3.iterrows():
            insert_query = '''
            INSERT IGNORE INTO comments(
                commenter_id,
                video_id,
                comment_text,
                comment_author,
                comment_time)
            VALUES (%s, %s, %s, %s, %s)'''
            values = (
                row['commenter_id'],
                row['video_id'],
                row['comment_text'],
                row['comment_author'],
                row['comment_time'])

            cursor.execute(insert_query, values)
            mydb.commit()

    except mysql.connector.Error as err:
        print("MySQL Error:", err)

    finally:
        if 'mydb' in locals() or 'mydb' in globals():
            mydb.close()
            


comment_table()  
    
    
    
def tables(channel_name):
    news = channels_table(channel_name)
    if news:
        st.write(news)
    else:
        video_table(channel_name)
        playlist_table(channel_name)
        comment_table(channel_name)
    
    return "tables created"



def show_channels_table():
    ch_list = []
    db = client["youtube_data"]
    collection01 = db["channel_details"]

    # Iterate over documents in the collection and print the channel_informations field
    for ch_data in collection01.find({}, {"_id": 0, "channel_information": 1}):
        ch_list.append(ch_data["channel_information"])
    df = st.dataframe(ch_list)
    
    return df


def show_playlists_table():
    pl_list = []
    db = client["youtube_data"]
    collection01 = db["channel_details"]

    # Iterate over documents in the collection and print the channel_informations field
    for pl_data in collection01.find({}, {"_id": 0, "playlist_information": 1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1 = st.dataframe(pl_list)
    
    return df1
    
    
def show_videos_table():
    vi_list=[]
    db = client["youtube_data"]
    collection01 = db["channel_details"]

    # Iterate over documents in the collection and print the channel_informations field
    for vi_data in collection01.find({}, {"_id": 0, "video_information":1 }):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2 = st.dataframe(vi_list)

    return df2


def show_comments_table():
    com_list=[]
    db = client["youtube_data"]
    collection01 = db["channel_details"]

    # Iterate over documents in the collection and print the channel_informations field
    for com_data in collection01.find({}, {"_id": 0, "cm_details":1 }):
        for i in range(len(com_data["cm_details"])):
            com_list.append(com_data["cm_details"][i])
    df3=st.dataframe(com_list)
    

#streamlit part
        
with st.sidebar:
    st.title(":cyan[YouTube Data Harvesting and Warehousing]")
    st.header("skill takeaway")
    st.caption("python scripting")
    st.caption("data collection")
    st.caption("MongoDB")
    st.caption("API loading")
    st.caption("Data management")
    
channel_id=st.text_input("Enter the Channel ID")

if st.button("collect and store data"):
    ch_ids=[]
    db = client["youtube_data"]
    collection01 = db["channel_details"]
    for ch_data in collection01.find({}, {"_id": 0, "channel_information": 1}):
        ch_ids.append(ch_data["channel_information"]["channel_id"])
        
    if channel_id in ch_ids:
        st.success("channel details already exists")
    else:
        insert=channel_details(channel_id)
        st.success("channel details inserted")

if st.button("Import to SQL"):
    Table=Tables()
    st.success("Table imported")
    
show_table=st.radio("SELECT TABLE", ("CHANNELS", "PLAYLISTS", "VIDEOS", "COMMENTS"))

if show_table=="CHANNELS":
    show_channels_table()
    
elif show_table=="PLAYLISTS":
    show_playlists_table()
    
elif show_table=="VIDEOS":
    show_videos_table()
    
elif show_table=="COMMENTS":
    show_comments_table()
    
    
#SQl connection


mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Mano@3221",
    database="youtube_data",
    port="3306")

cursor = mydb.cursor()

Question=st.selectbox("my question",("1. What are the names of all the videos and their corresponding channels",
                                     "2. Which channels have the most number of videos, and how many videos do they have",
                                     "3. What are the top 10 most viewed videos and their respective channels",
                                     "4. How many comments were made on each video, and what are their corresponding video names?",
                                     "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                                     "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                     "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                                     "8. What are the names of all the channels that have published videos in the year 2022?",
                                     "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                     "10. Which videos have the highest number of comments, and what are their corresponding channel names?"))

if question=="1. What are the names of all the videos and their corresponding channels":

    question01 = '''select video_title as Videos, channel_name as channels from videos'''
    cursor.execute(question01)
    #mydb.commit()
    t1 = cursor.fetchall()
    df = pd.DataFrame(t1, columns=["video name", "channel name"])
    st.write(df)
    
    
elif question=="2. Which channels have the most number of videos, and how many videos do they have":
    question02 = '''select channel_name as Channelname, videos_count as no_videos from channels order by videos_count desc'''
    cursor.execute(question02)
    t2 = cursor.fetchall()
    mydb.commit()
    df2 = pd.DataFrame(t2, columns=["channel name", "No of videos"])
    st.write(df2)
    
    
elif question == "3. What are the top 10 most viewed videos and their respective channels":
    question03 = '''SELECT views_count AS views, channel_name AS channelname, video_title AS videotitle 
                    FROM videos WHERE views_count IS NOT NULL ORDER BY views_count DESC LIMIT 10'''
    cursor.execute(question03)
    #mydb.commit()
    t3 = cursor.fetchall()
    df3 = pd.DataFrame(t3, columns=["views", "channel name", "videotitle"])
    st.write(df3)   


elif question=="4. How many comments were made on each video, and what are their corresponding video names?":
    question04='''select comment_count as no_comments,video_title as videotitle from videos where comment_count is not null'''
    cursor.execute(question04)
    #mydb.commit()
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=["no of comments", "videotitle"])
    st.write(df4)
    
    
elif question=="5. Which videos have the highest number of likes, and what are their corresponding channel names?":
    question05='''select video_title as videotitle,channel_name as channelname,like_count as likecount
                from videos where like_count is not null order by like_count desc'''
    cursor.execute(question05)
    #mydb.commit()
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["videotitle","channelname","likecount"])
    st.write(df5) 
    
    
elif question=="6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
    question06='''select like_count as likecount,video_title as videotitle from videos'''
    cursor.execute(question06)
    #mydb.commit()
    t6=cursor.fetchall()
    df6 =pd.DataFrame(t6, columns=["likecount", "videotitle"])
    st.write(df6) 
    
    
elif question=="7. What is the total number of views for each channel, and what are their corresponding channel names?":
    question07='''select channel_name as channelname ,views as totalviews from channels'''
    cursor.execute(question07)
    #mydb.commit()
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=["channel name","totalviews"])
    st.write(df7)
    
    
elif question == "8. What are the names of all the channels that have published videos in the year 2022?":
    question08 = '''SELECT 
                        video_title AS video_title, 
                        v_date_of_publish AS video_release, 
                        channel_name AS channelname 
                    FROM 
                        videos
                    WHERE 
                        EXTRACT(YEAR FROM v_date_of_publish) = 2022'''

    cursor.execute(question08)
    t8 = cursor.fetchall()
    df8 = pd.DataFrame(t8, columns=["videotitle", "published_date", "channelname"])
    st.write(df8)

    
    
    
elif question=="9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    question09='''select channel_name as channelname, AVG(video_duration) as averageduration from videos group by channel_name'''
    cursor.execute(question09)
    #mydb.commit()
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9,columns=["channelname","averageduration"])

    T9=[]
    for index,row in df9.iterrows():
        channel_title=row["channelname"]
        average_duration=row["averageduration"]
        average_duration_str=str(average_duration)
        T9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))
    df9=pd.DataFrame(T9)
    st.write(df9)
    
    

elif question=="10. Which videos have the highest number of comments, and what are their corresponding channel names?":
    question10 = '''SELECT 
                        video_title AS videotitle, 
                        channel_name AS channelname,
                        IFNULL(comment_count, 0) AS comment_count 
                    FROM 
                        videos 
                    WHERE 
                        comment_count IS NOT NULL 
                    ORDER BY 
                        comment_count DESC'''

    cursor.execute(question10)
    t10 = cursor.fetchall()
    df10 = pd.DataFrame(t10, columns=["video title", "channel name", "comment_count"])
    st.write(df10)