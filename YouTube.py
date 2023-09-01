import pandas as pd
import plotly.express as px
import streamlit as st
from streamlit_option_menu import option_menu
import mysql.connector as sql
import pymongo
from googleapiclient.discovery import build
from PIL import Image

# SETTING PAGE CONFIGURATIONS
st.set_page_config(page_title= "Youtube Data Harvesting and Warehousing | By Paila Hemalatha",
                   layout= "wide",
                   initial_sidebar_state= "expanded")


#Connection with Mongodb and creating a new database
client = pymongo.MongoClient("mongodb://localhost:27017")
db = client["Youtube_database"]

#Connection with mysql database
mydb = sql.connect(host="127.0.0.1",
                   user="root",
                   password="Mysql@2023",
                   database= "youtube",
                   port = "3306"
                  )

cursor = mydb.cursor()

# BUILDING CONNECTION WITH YOUTUBE API
api_key = 'AIzaSyAbKVvw49P8TnpEJBEf4WJpcapMioRqEYM'
# channel_ids = "UCihUiDJzjyo2ov_qGtW33lw"
api_service_name = "youtube"
api_version = "v3"

# Get credentials and create an API client
youtube = build(
    api_service_name, api_version, developerKey=api_key)

#To get the channel details
def get_channel_stats(channel_id):
    all_data = []

    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()

    # loop through items
    for item in response['items']:
        data = {'channelName': item['snippet']['title'],
                'subscribers': item['statistics']['subscriberCount'],
                'views': item['statistics']['viewCount'],
                'totalViews': item['statistics']['videoCount'],
                'playlistId': item['contentDetails']['relatedPlaylists']['uploads'],
                'Description': item['snippet']['description']

                }
        all_data.append(data)

    return all_data

#To get video IDs
def get_channel_videos(channel_id):
    video_ids = []
    # get Uploads playlist id
    res = youtube.channels().list(id=channel_id,
                                  part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None

    while True:
        request = youtube.playlistItems().list(playlistId=playlist_id,
                                           part='contentDetails',
                                           maxResults=50,
                                           pageToken=next_page_token)
        response = request.execute()

        for item in response['items']:
            video_ids.append(item['contentDetails']['videoId'])
        next_page_token = response.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids

#To get Video_details
def get_video_details(video_ids):
    video_stats = []

    for i in range(len(video_ids)):
        response = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_ids[i]).execute()
        for video in response['items']:
            video_details = dict(Channel_name=video['snippet']['channelTitle'],
                                 Channel_id=video['snippet']['channelId'],
                                 Video_id=video['id'],
                                 Title=video['snippet']['title'],
                                 Tags=str(video['snippet'].get('tags')),
                                 Thumbnail=video['snippet']['thumbnails']['default']['url'],
                                 Description=video['snippet']['description'],
                                 Published_date=video['snippet']['publishedAt'],
                                 Duration=video['contentDetails']['duration'],
                                 Views=video['statistics']['viewCount'],
                                 Likes=video['statistics'].get('likeCount'),
                                 Comments=video['statistics'].get('commentCount'),
                                 Favorite_count=video['statistics']['favoriteCount'],
                                 Definition=video['contentDetails']['definition'],
                                 Caption_status=video['contentDetails']['caption']
                                 )
            video_stats.append(video_details)
    return video_stats

#To get comment details
def get_comments_details(video_id):
    comment_data = []
    try:
        next_page_token = None
        while True:
            response = youtube.commentThreads().list(part="snippet,replies",
                                                    videoId=video_id,
                                                    maxResults=100,
                                                    pageToken=next_page_token).execute()
            for cmt in response['items']:
                data = dict(Comment_id = cmt['id'],
                            Video_id = cmt['snippet']['videoId'],
                            Comment_text = cmt['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_author = cmt['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_posted_date = cmt['snippet']['topLevelComment']['snippet']['publishedAt'],
                            Like_count = cmt['snippet']['topLevelComment']['snippet']['likeCount'],
                            Reply_count = cmt['snippet']['totalReplyCount']
                           )
                comment_data.append(data)
            next_page_token = response.get('nextPageToken')
            if next_page_token is None:
                break
    except:
        pass
    return comment_data

def all_comments(video_ids):
    comments = list()
    for video_id in video_ids:
        comments += get_comments_details(video_id)
    return comments

def channel_names():
    channels = list()
    for i in db.channels.find():
        channels.append(i['channelName'])
    return channels
# print(channel_names())

def channel_ids():
    ids = list()
    for i in db.channels.find():
        ids.append(i['Channel_id'])
    return ids

# CREATING OPTION MENU
with st.sidebar:
    selected = option_menu(None, ["Home","Extract & Transform","View"],
                           icons=["house-door-fill","tools","card-text"],
                           default_index=0,
                           orientation="vertical"
                           )

#Home Page
if selected == "Home":
    col1,col2 = st.columns(2,gap= 'medium')
    col1.markdown("## :Orange[Domain] : Social Media")
    col1.markdown("## :Orange[Technologies used] : Python,MongoDB, Youtube Data API, MySql, Streamlit")
    col1.markdown("## :Orange[Overview] : Retrieving the Youtube channels data from the Google API, storing it in a MongoDB as data lake, migrating and transforming data into a SQL database,then querying the data and displaying it in the Streamlit app.")
    col2.markdown("#   ")
    col2.markdown("#   ")
    col2.markdown("#   ")

# EXTRACT AND TRANSFORM PAGE
elif selected == "Extract & Transform":
    tab1, tab2 = st.tabs(["$\huge 📝 EXTRACT $", "$\huge🚀 TRANSFORM $"])

    # EXTRACT TAB
    with tab1:
        st.markdown("#    ")
        st.write("### Enter YouTube Channel_ID below :")
        ch_id = st.text_input(
            "Hint : Goto channel's home page > Right click > View page source > Find channel_id").split(',')

        if ch_id and st.button("Extract Data"):
            if ch_id not in channel_ids():
                ch_details = get_channel_stats(ch_id)
                st.write(f'#### Extracted data from :green["{ch_details[0]["channelName"]}"] channel')
                st.table(ch_details)
            else:
                st.info('Channel details are already available', icon="ℹ️")

        if st.button("Upload to MongoDB"):
            with st.spinner('Please Wait for it...'):
                ch_details = get_channel_stats(ch_id)
                v_ids = get_channel_videos(ch_id)
                vid_details = get_video_details(v_ids)
                comm_details = all_comments(v_ids)

                db.channels.insert_many(ch_details)
                db.videos.insert_many(vid_details)

                try:
                    db.comments_details.insert_many(comm_details)
                except:
                    st.warning('No comments for this channel')
                st.success("Data Uploaded to MongoDB successfully !!")

# TRANSFORM TAB
    with tab2:
        st.markdown("#   ")
        st.markdown("### Select a channel to begin Transformation to SQL")
        ch_names = channel_names()
        user_inp = st.selectbox("Select channel", options=ch_names)

        def insert_into_channels():
            query = """INSERT INTO channel_details VALUES(%s,%s,%s,%s,%s,%s)"""

            for i in db.channels.find({'channelName': user_inp}, {'_id': 0}):
                cursor.execute(query, tuple(i.values()))
            mydb.commit()


        def insert_into_videos():
            query1 = """INSERT INTO videos VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

            for i in db.videos.find({"Channel_name" : user_inp},{'_id' : 0}):
                cursor.execute(query1, tuple(i.values()))
            mydb.commit()


        def insert_into_comments():
            query2 = """INSERT INTO comments VALUES(%s,%s,%s,%s,%s,%s,%s)"""

            for vid in db.videos.find({"Channel_name" : user_inp},{'_id' : 0}):
                for i in db.comments_details.find({'Video_id': vid['Video_id']},{'_id' : 0}):
                    cursor.execute(query2,tuple(i.values()))
                    mydb.commit()


        if st.button("SUBMIT"):
                # try:
                    with st.spinner("Transforming MongoDB data to Sql"):
                        insert_into_channels()
                        insert_into_videos()
                        insert_into_comments()
                        st.success("Transformation to MySQL Successful !!")
                # except:
                #     st.error("Channel details already transformed !!")

# VIEW PAGE
elif selected == "View":

    st.write("## :orange[Select any question to get Insights]")
    questions = st.selectbox('Questions',
                             ['1. What are the names of all the videos and their corresponding channels?',
                              '2. Which channels have the most number of videos, and how many videos do they have?',
                              '3. What are the top 10 most viewed videos and their respective channels?',
                              '4. How many comments were made on each video, and what are their corresponding video names?',
                              '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                              '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                              '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                              '8. What are the names of all the channels that have published videos in the year 2022?',
                              '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                              '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])

    if questions == '1. What are the names of all the videos and their corresponding channels?':
        cursor.execute("""SELECT video_title AS Video_name, channel_name AS Channel_Name
                            FROM videos
                            ORDER BY channel_name""")
        df = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)
        st.write(df)

    elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
        cursor.execute("""SELECT channel_name AS Channel_Name, total_videos AS Total_Videos
                            FROM channel_details
                            ORDER BY total_videos DESC""")
        df = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)
        st.write(df)
        st.write("### :green[Number of videos in each channel :]")
        # st.bar_chart(df,x= mycursor.column_names[0],y= mycursor.column_names[1])
        fig = px.bar(df,
                     x=cursor.column_names[0],
                     y=cursor.column_names[1],
                     orientation='v',
                     color=cursor.column_names[0]
                     )
        st.plotly_chart(fig, use_container_width=True)

    elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
        cursor.execute("""SELECT channel_name AS Channel_Name, Video_title AS Video_name, viewCount AS Views 
                            FROM videos
                            ORDER BY Views DESC
                            LIMIT 10""")
        df = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)
        st.write(df)
        st.write("### :green[Top 10 most viewed videos :]")
        fig = px.bar(df,
                     x=cursor.column_names[2],
                     y=cursor.column_names[1],
                     orientation='h',
                     color=cursor.column_names[0]
                     )
        st.plotly_chart(fig, use_container_width=True)

    elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
        cursor.execute("""SELECT v.video_id AS Video_id, v.Video_title AS Video_name, v.commentCount
                            FROM videos AS v
                            LEFT JOIN (SELECT Video_id,COUNT(Comment_id) AS Total_Comments
                            FROM comments GROUP BY Video_id) AS c
                            ON v.video_id = c.video_id
                            ORDER BY c.Total_Comments DESC""")
        df = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)
        st.write(df)

    elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        cursor.execute("""SELECT channel_name AS Channel_Name,Video_Title AS Title,Likecount AS Like_Count 
                            FROM videos
                            ORDER BY Like_count DESC
                            LIMIT 10""")
        df = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)
        st.write(df)
        st.write("### :green[Top 10 most liked videos :]")
        fig = px.bar(df,
                     x=cursor.column_names[2],
                     y=cursor.column_names[1],
                     orientation='h',
                     color=cursor.column_names[0]
                     )
        st.plotly_chart(fig, use_container_width=True)

    elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        cursor.execute("""SELECT video_title AS Title, Likecount AS Like_count
                            FROM videos
                            ORDER BY Like_count DESC""")
        df = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)
        st.write(df)

    elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        cursor.execute("""SELECT channel_name AS Channel_Name, views AS Views
                            FROM channel_details
                            ORDER BY views DESC""")
        df = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)
        st.write(df)
        st.write("### :green[Channels vs Views :]")
        fig = px.bar(df,
                     x=cursor.column_names[0],
                     y=cursor.column_names[1],
                     orientation='v',
                     color=cursor.column_names[0]
                     )
        st.plotly_chart(fig, use_container_width=True)

    elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
        cursor.execute("""SELECT channel_name AS Channel_Name
                            FROM videos
                            WHERE published_at LIKE '2022%'
                            GROUP BY channel_name
                            ORDER BY channel_name""")
        df = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)
        st.write(df)

    elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        cursor.execute("""SELECT channel_name AS Channel_Name,
                            AVG(duration)/60 AS "Average_Video_Duration (mins)"
                            FROM videos
                            GROUP BY channel_name
                            ORDER BY AVG(duration)/60 DESC""")
        df = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)
        st.write(df)
        st.write("### :green[Avg video duration for channels :]")
        fig = px.bar(df,
                     x=cursor.column_names[0],
                     y=cursor.column_names[1],
                     orientation='v',
                     color=cursor.column_names[0]
                     )
        st.plotly_chart(fig, use_container_width=True)

    elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        cursor.execute("""SELECT channel_name AS Channel_Name,Video_id AS Video_ID,Commentcount AS Comments
                            FROM videos
                            ORDER BY comments DESC
                            LIMIT 10""")
        df = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)
        st.write(df)
        st.write("### :green[Videos with most comments :]")
        fig = px.bar(df,
                     x=cursor.column_names[1],
                     y=cursor.column_names[2],
                     orientation='v',
                     color=cursor.column_names[0]
                     )
        st.plotly_chart(fig, use_container_width=True)


