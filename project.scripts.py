import pandas as pd
import streamlit as st
from pymongo import MongoClient
from googleapiclient.discovery import build
from PIL import Image
import psycopg2 as pg
import datetime
import time

#======================================================= SETTING STREAMLIT PAGE CONFIGURATIONS ===============================

#st.title(":red[YOUTUBE DATA HARVESTING AND ANALYSING]")
st.write(":white[|By Vigashini]")
st.markdown(
    "<h1 style='color: red;'>YOUTUBE DATA HARVESTING AND ANALYSING</h1>", 
    unsafe_allow_html=True
)

# CREATING OPTION MENU
with st.sidebar:
    option = st.selectbox( 
    'What You Like To Do?',
    ('Home', 'Extract and Transform', 'View and Analyse'))
    st.write('You selected:')
    st.write(option)

#================================================================== CONNECTIONS =================================================================

#                                               ----------Connection with MongoDB compass------------

client = MongoClient("mongodb://localhost:27017")
db = client['Youtube']
coll = db['Channels']

#                                               ---------Connection with Postgresql DATABASE---------

Integration=pg.connect(host='localhost',port=5432,user='postgres',password='******', database='Youtube1')
connect = Integration.cursor()



#                                               --------------#Connecting to youtube api:-----------
api_key = "AIzaSyBWT06efcfqHDMAHg3QRktxFf9ntL3h5vM"
api_service_name = 'youtube'
api_version = 'v3'
youtube = build(api_service_name, api_version, developerKey=api_key)


#=========================================================== IMPORTING INTO MongoDB =================================================================


#Importing Channel Details Into MongoDB
def get_channel_details(channel_id):
        request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id
        ).execute()

        channel_info = request['items'][0]

        # converting the ISO datetime format to a Python datetime.
        published_at_str = channel_info['snippet']['publishedAt']
        published_at = datetime.datetime.fromisoformat(published_at_str)

        
        channel = dict(channel_id = channel_info['id'],
        channel_name = channel_info['snippet']['title'],
        #channel_description = channel_info['snippet']['description'],
        channel_subscriberCount = int(channel_info['statistics']['subscriberCount']),
        channel_viewCount = int(channel_info['statistics']['viewCount']),
        channel_videoCount = int(channel_info['statistics']['videoCount']),
        channel_PublishedDate = int(published_at.year),
        Playlist_id = channel_info['contentDetails']['relatedPlaylists']['uploads'])
        return channel

#Importing Video Details Into MongoDB
def requesting_playlist_items(channel_id):
    VideoIds = []
    res = youtube.channels().list(id=channel_id, part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None

    while True:
        res = youtube.playlistItems().list(part = "snippet,contentDetails", playlistId = playlist_id, maxResults = 20, pageToken=next_page_token).execute()

        for i in res["items"]:
            video_id = i["contentDetails"]["videoId"]
            VideoIds.append(video_id)
        if next_page_token is None:
            break
    return VideoIds

def iso8601_to_seconds(duration_iso8601):
    # Parse the ISO 8601 duration format and convert to seconds
    duration = duration_iso8601[2:]  # Remove the 'PT' prefix
    seconds = 0

    # Parse hours, minutes, and seconds components if present
    if 'H' in duration:
        hours, duration = duration.split('H')
        seconds += int(hours) * 3600  # Convert hours to seconds
    if 'M' in duration:
        minutes, duration = duration.split('M')
        seconds += int(minutes) * 60  # Convert minutes to seconds
    if 'S' in duration:
        seconds += int(duration[:-1])  # Remove the 'S' suffix
    #(or)
    #if 'S' in duration:
    #    seconds, duration = duration.split('S')
    #    seconds += int(seconds)'''
    return seconds

def get_video_details(v_ids):
    video_details = []

    for video_id in v_ids:
        video_request = youtube.videos().list(part="snippet,contentDetails,statistics", id=video_id).execute()

        for vd in video_request['items']: 

            #converting a duration string in ISO 8601 format(eg:PT1H30M15S) into the equivalent number of seconds.
            video_duration_iso = vd["contentDetails"]["duration"]
            video_duration_seconds = iso8601_to_seconds(video_duration_iso)

            # converting the date string in ISO 8601 Format into the normal date format
            published_at_str = vd['snippet']['publishedAt']
            video_published_at = datetime.datetime.fromisoformat(published_at_str)        

            video_details_list = dict(
                channel_id = vd["snippet"]["channelId"],
                Video_id =  vd['id'],
                Video_title = vd['snippet']['title'],
                Video_publishedAt = int(video_published_at.year),
                Video_thumbnail =  vd['snippet']['thumbnails']['default']['url'],
                Video_likes = int(vd['statistics'].get('likeCount',0)),
                Video_views = int(vd['statistics']['viewCount']),
                Video_commentCount = int(vd['statistics'].get('commentCount',0)),
                Video_duration = video_duration_seconds
            )

        video_details.append(video_details_list)
    return video_details

def get_comments(v_ids):
    comment_details = []
    try:
        for video_id in v_ids:
            comments_request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                textFormat="plainText",
                maxResults=20
            ).execute()

            for cmt in comments_request.get('items', []):
                comment_details_list = dict(
                    Video_ID=cmt['snippet']['videoId'],
                    Comment_Text=str(cmt['snippet']['topLevelComment']['snippet']['textDisplay']),
                    Author=str(cmt['snippet']['topLevelComment']['snippet']['authorDisplayName']),
                    Author_Channel = str("https://www.youtube.com/channel/" + cmt['snippet']['topLevelComment']['snippet']['authorChannelId']['value']),
                    Likes=int(cmt['snippet']['topLevelComment']['snippet'].get('likeCount', 0)),
                    Publish_Date=cmt['snippet']['topLevelComment']['snippet']['publishedAt'])
                
                comment_details.append(comment_details_list)
    except Exception as e:
        print(f"An error occurred: {str(e)}")

    return comment_details

#==========================================================  TRANSFORMING TO SQL  ====================================================================

#Listing Channel Names From MongoDB
def channel_names():
    channel_names = []
    for document in coll.find():
        if 'Channel Details' in document and 'channel_name' in document['Channel Details']:
            channel_name = document['Channel Details']['channel_name']
            channel_names.append(channel_name)
    return channel_names

#Inserting Channel Details
def insert_into_channels():
    coll = db['Channels']
    connect.execute("""create table if not exists Channels_Yt(
                    channel_id VARCHAR(300) PRIMARY KEY,
                    channel_name VARCHAR(300),
                    channel_subscriberCount BIGINT,
                    channel_viewCount BIGINT,
                    channel_videoCount BIGINT,
                    channel_PublishedDate INT,
                    Playlist_id VARCHAR (200))""")
    Integration.commit()

    query = """INSERT INTO Channels_Yt (channel_id, channel_name, channel_subscriberCount, channel_viewCount, channel_videoCount, channel_PublishedDate, Playlist_id)VALUES (%s, %s, %s, %s, %s, %s, %s)"""

        
    for i in coll.find({"Channel Details.channel_name": user_input}, {'_id': 0}):
        values = (
            i['Channel Details']['channel_id'],
            i['Channel Details']['channel_name'],
            i['Channel Details']['channel_subscriberCount'],
            i['Channel Details']['channel_viewCount'],
            i['Channel Details']['channel_videoCount'],
            i['Channel Details']['channel_PublishedDate'],
            i['Channel Details']['Playlist_id']
        )

        try:
            connect.execute(query, values)
            Integration.commit()
            st.success("✅Channel Details Transformed into SQL, Successfully!")
            st.success("✅Video Details Transformed into SQL, Successfully!")
            st.success("✅Comment Details Transformed into SQL, Successfully!")

        except pg.IntegrityError as e:
            # Handle unique key violation error here
            st.error(" 🚫THIS CHANNEL DETAILS ARE ALREADY EXIST!, KINDLY CHECK IT.")
            st.error(" 🚫THIS VIDEO DETAILS ARE ALREADY EXIST!, KINDLY CHECK IT")
            st.error(" 🚫THIS COMMENTS DETAILS ARE ALREADY EXIST!, KINDLY CHECK IT")
            st.write(":black[NOTE: You cannot add again the existing datas as it VIOLATES UNIQUE KEY CONSTRAINT]") 
            Integration.rollback()  # Rollback the transaction to keep the database in a consistent state


#Inserting Video Details
def insert_into_videos():
    try:
        if connect:
            connect.execute("""create table if not exists Videos_Yt(
                            Video_id VARCHAR(200) PRIMARY KEY,
                            channel_id VARCHAR(300) ,
                            Video_title VARCHAR(200) ,
                            Video_publishedAt INT,
                            Video_thumbnail VARCHAR(200),
                            Video_likes INT,
                            Video_views INT,
                            Video_commentCount INT,
                            Video_duration INT)""")
            Integration.commit()

            for channel_document in coll.find({}):
                channel_id_to_fetch = channel_document['Channel Details']['channel_id']

                # Fetch the MongoDB document containing video details based on the channel ID
                mongo_document = coll.find_one({'Channel Details.channel_id': channel_id_to_fetch})

                if mongo_document and 'Video Details' in mongo_document:
                    video_details_array = mongo_document['Video Details']

                    for video_detail in video_details_array:
                        video_id = video_detail.get('Video_id', '')
                        channel_id = video_detail.get('channel_id','')                          
                        video_title = video_detail.get('Video_title', '')
                        video_publishedAt = video_detail.get('Video_publishedAt', '')
                        video_thumbnail = video_detail.get('Video_thumbnail', '')
                        video_likes = video_detail.get('Video_likes', 0) 
                        video_views = video_detail.get('Video_views', 0)  
                        video_commentCount = video_detail.get('Video_commentCount', 0)  
                        video_duration = video_detail.get('Video_duration',0) 

                        query = """ 
                            INSERT INTO Videos_Yt (Channel_id, Video_id, Video_title, Video_publishedAt, Video_thumbnail, Video_likes, Video_views, Video_commentCount, Video_duration)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """

                        values = (
                            channel_id,
                            video_id,
                            video_title,
                            video_publishedAt,
                            video_thumbnail,
                            video_likes,
                            video_views,
                            video_commentCount,
                            video_duration
                        )
                    
                        try:
                            connect.execute(query, values)
                            Integration.commit()
                        except Exception as e:
                            Integration.rollback()
                            
        else:
            pass 
            #st.error(" 🚫THIS VIDEO DETAILS ARE ALREADY EXIST!, KINDLY CHECK IT") 
            #st.write(":black[NOTE: You cannot add again the existing datas]") 

    except Exception as ex:
            print(f"An error occurred: {str(ex)}")
            Integration.rollback()
            

#Inserting Comments Details
def insert_into_comments():
    try:
        connect.execute("""create table if not exists Comments(
                 Video_ID varchar(200),
                 Comment_Text varchar(20000),
                 Author TEXT,
                 Author_Channel TEXT,
                 Likes BIGINT,
                 Publish_Date TIMESTAMP WITH TIME ZONE)""")
        Integration.commit()

        for channel_document in coll.find({}):
            channel_id_to_fetch = channel_document['Channel Details']['channel_id']

             # Fetch the MongoDB document containing comments details based on the channel ID
            mongo_document = coll.find_one({'Channel Details.channel_id': channel_id_to_fetch})

            if mongo_document and 'Comment Details' in mongo_document:
                comment_details_array = mongo_document['Comment Details']

                for comment_detail in comment_details_array:
                    Video_ID = comment_detail.get('Video_ID','')
                    Comment_Text = comment_detail.get('Comment_Text', '')
                    Author = comment_detail.get('Author', '')
                    Author_Channel = comment_detail.get('Author_Channel', '')
                    Likes = comment_detail.get('Likes', 0)
                    Publish_Date = comment_detail.get('Publish_Date', '') 

                    query2 = """INSERT INTO Comments(Video_ID, Comment_Text, Author, Author_Channel, Likes, Publish_Date) VALUES (%s, %s, %s, %s, %s, %s)"""

                    values = (
                        Video_ID,
                        Comment_Text,
                        Author,
                        Author_Channel,
                        Likes,
                        Publish_Date)
                    
                    try:
                        connect.execute(query2, values)
                        Integration.commit()
                        
                    except Exception as e:
                        Integration.rollback()                

    except Exception as ex:
         print(f"An error occurred: {str(ex)}")
         Integration.rollback()
        
    finally:
         # Close the database connections when done
         if Integration:
             Integration.close()
         if connect:
             connect.close()

#============================================================  STREAMLIT DASHBOARD CREATIONS:  =======================================================================
                    
# HOME PAGE
if option == "Home":

    st.title(":red[**• Home Page**]")

    st.subheader("D O M A I N")
    st.markdown(":blue[Social Media]")
    st.subheader("T E C H N O L O G I E S - U S E D")
    st.markdown(":blue[Python, MongoDB, Youtube Data API, MySql, Streamlit]")
    st.subheader("O V E R V I E W")
    st.markdown(
        ":blue[Retrieving  the  Youtube  channels  data  from  the  Google API,  storing  it  in  a  MongoDB  as  data  lake , migrating  and  transforming  data  into  a  SQL  database,  then  querying  the  data  and  displaying  it  in  the  Streamlit  app.]")
    image = Image.open('youtube2.png')
    st.image(image, caption='YouTube', width=400)



if option == "Extract and Transform":
    tab1,tab2 = st.tabs(["$\huge EXTRACT / $", "$\huge TRANSFORM $"])

    #EXTRACT TAB
    with tab1:
        names_list = ["1. Tamil Thalaivas---UCi-2sQgbsiTlKezHuk4yTWw", "2. Endless Knowledge---UCApUMSkgDT8ayJZU8jBweYw", "3. Guvi---UCApUMSkgDT8ayJZU8jBweYw",
            "4. Finance Boosan---UCmfl6VteCu880D8Txl4vEag", "5. code io---UCGoLw0tC_QAXy0b4KCoxYZw",
            "6. Coding Anna---UCGoLw0tC_QAXy0b4KCoxYZw" , "7. Santra TechSpot---UCGoLw0tC_QAXy0b4KCoxYZw", "8. Kizen English---UC44aT4ek1daiUsw2o1XUxow", 
            "9. Think Biology Think Visiom---UCauaJTp7T22U0OJVss-5h1w", "10. Wonder Creations---UC984xzYy-NEi-dm8IEgt5Kw", "11. Napstro Fusion---UC2mbzXePbOYg2kewdzkVTjA"]
  
        # Display the list of names as a bullet list
        st.markdown("**List of Channels:**")
        for name in names_list:
            st.markdown(f" {name}")

        st.markdown("##### :blue[ENTER THE CHANNEL ID BELOW:]")
        ch_id = st.text_input(":gray[Hint : Goto Channel's Homepage > About > Share Arrow Icon > Channel Id]").split(' ')

        if ch_id and st.button("Extract Data"):

            spinner = st.spinner("Harvesting...")
            with spinner:
                time.sleep(3)  # Simulate a 3-second task
                st.write('')
            ch_details = get_channel_details(ch_id)
            vid_ids = requesting_playlist_items(ch_id)
            vid_details = get_video_details(vid_ids)
            comm_details = get_comments(vid_ids)

            st.subheader("CHANNEL DETAILS")
            st.write(ch_details)
            st.subheader("VIDEO IDS")
            st.write(vid_ids)
            st.subheader("VIDEO DETAILS")
            st.write(vid_details)
            st.subheader("COMMENT DETAILS")
            st.write(comm_details)

            info = { 'Channel Details': ch_details,
                    #'Video_ids': vid_ids,
                    'Video Details': vid_details,
                    'Comment Details': comm_details
                    }

            channel_list=[]
            for i in coll.find({}):
                channel_list.append(i['Channel Details']['channel_id'])
            if ch_details['channel_id'] in channel_list:     
                st.warning('Channel Details Already Exist, Cannot Upload Into MongoDB Again', icon="⚠️")
            else:
                coll.insert_one(info)  #collection variable - coll
                st.success('✅ Data Uploaded to MongoDB Successfully!')


    #TRANSFORM TAB
    with tab2:
        st.write("#### Select a channel to begin Transformation to SQL")
        ch_names = channel_names()
        user_input = st.selectbox("Select any",options = ch_names)

        s = st.button("Transform")
        if user_input and s:
            insert_into_channels()
            insert_into_videos()
            insert_into_comments()

#=============================================================== Queries to View in Dashboard =======================================================

if option == 'View and Analyse':
    questions = st.selectbox('CLICK THE QUESTION THAT YOU WOULD LIKE TO QUERY',
        ['1. What are the names of all the videos and their corresponding channels?',
        '2. Which channels have the most number of videos, and how many videos do they have?',
        '3. What are the top 10 most viewed videos and their respective channels?',
        '4. How many comments were made on each video, and what are their corresponding video names?',
        '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
        '6. What is the total number of views for each channel, and what are their corresponding channel names?',
        '7. What are the names of all the channels that have published videos in the year 2022?',
        '8. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
        '9. Which videos have the highest number of comments, and what are their corresponding channel names?'])

    if questions == '1. What are the names of all the videos and their corresponding channels?':
        query1 = 'select videos_yt.video_title, channels_yt.channel_name from channels_yt inner join videos_yt on channels_yt.channel_id = videos_yt.channel_id'
        connect.execute(query1)
        result1 = connect.fetchall()

        #Get column names from cursor.description    =>connect.description is an attribute of a connect(i.e. connection)object, by which we csn get column names, data types etc.
        column_names = [column[0] for column in connect.description]

        table1 = pd.DataFrame(result1, columns=column_names)
        st.table(table1)    


    if questions == '2. Which channels have the most number of videos, and how many videos do they have?':
        query2 = 'select channel_name, channel_videocount from channels_yt order by channel_videocount DESC LIMIT 1;'
        connect.execute(query2)
        result2 = connect.fetchall()
        column_names = [column[0] for column in connect.description]
        table2 = pd.DataFrame(result2, columns=column_names)
        st.table(table2)  

    if questions == '3. What are the top 10 most viewed videos and their respective channels?':
        query3 = 'select video_title , video_views as views_count, channel_name from channels_yt c join videos_yt v on c.channel_id = v.channel_id order by Views_Count desc limit 10;'
        connect.execute(query3)
        result3 = connect.fetchall()
        column_names = [column[0] for column in connect.description]
        table3 = pd.DataFrame(result3, columns=column_names)
        st.table(table3)

    if questions == '4. How many comments were made on each video, and what are their corresponding video names?':
        query4 = 'select video_title, video_commentcount as comment_counts from videos_yt;'
        connect.execute(query4)
        result4 = connect.fetchall()
        column_names = [column[0] for column in connect.description]
        table4 = pd.DataFrame(result4, columns=column_names)
        st.table(table4)

    if questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        query5 = 'select channel_name, video_title, video_likes as Maximum_likes from channels_yt c join videos_yt v  on c.channel_id = v.channel_id order by video_likes desc limit 1'
        connect.execute(query5)
        result5 = connect.fetchall()
        column_names = [column[0] for column in connect.description]
        table5 = pd.DataFrame(result5, columns=column_names)
        st.table(table5)

    if questions == '6. What is the total number of views for each channel, and what are their corresponding channel names?':
        query6 = 'select distinct(channel_name), channel_viewcount from channels_yt order by channel_viewcount desc;'
        connect.execute(query6)
        result6 = connect.fetchall()
        column_names = [column[0] for column in connect.description]
        table6 = pd.DataFrame(result6, columns=column_names)
        st.table(table6)
        st.bar_chart(table6.set_index("channel_name"))

    if questions == '7. What are the names of all the channels that have published videos in the year 2022?':
        query7 = 'select distinct(channel_name), Video_publishedAt from channels_yt c join videos_yt v on c.channel_id = v.channel_id where Video_publishedAt = 2022;'
        connect.execute(query7)
        result7 = connect.fetchall()
        column_names = [column[0] for column in connect.description]
        table7 = pd.DataFrame(result7, columns=column_names)
        st.table(table7)

    if questions == '8. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        query8 = 'select channel_name, avg(video_duration) as average_video_duration from channels_yt c join videos_yt v on c.channel_id = v.channel_id group by channel_name;'       
        connect.execute(query8)
        result8 = connect.fetchall()
        column_names = [column[0] for column in connect.description]
        table8 = pd.DataFrame(result8, columns=column_names)
        st.table(table8)
        

    if questions == '9. Which videos have the highest number of comments, and what are their corresponding channel names?':
        query9 = 'select video_title, channel_name, video_commentcount as highest_comment_count from videos_yt v join channels_yt c on v.channel_id = c.channel_id where v.video_commentcount = (select max(video_commentcount) from videos_yt);'        
        connect.execute(query9)
        result9 = connect.fetchall()
        column_names = [column[0] for column in connect.description]
        table9 = pd.DataFrame(result9, columns=column_names)
        st.table(table9)
