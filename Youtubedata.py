from googleapiclient.discovery import build
import pymongo
import mysql.connector
import pandas as pd
import streamlit as st
#Function to connect with API 
def Connect_API():
    api_ID="AIzaSyAwD089K6vIWgJdm-8ARuGGtYvJoffh8vE"

    service_name = "youtube"
    api_version = "v3"
    youtube = build(service_name,api_version,developerKey=api_ID)#build function helps to connect with API and returns a service object
    return youtube

youtube=Connect_API()

#Extracting Channel details
def Channel_details(channel_id):
    
    request = youtube.channels().list(
                part = "snippet,contentDetails,statistics",id = channel_id)
            
    response=request.execute()

    if 'items' in response:

        data = dict(
                    Channel_Name = response["items"][0]["snippet"]["title"],
                    Channel_Id = response["items"][0]["id"],
                    Subscription_Count= response["items"][0]["statistics"]["subscriberCount"],
                    Views = response["items"][0]["statistics"]["viewCount"],
                    Total_Videos = response["items"][0]["statistics"]["videoCount"],
                    Channel_Description = response["items"][0]["snippet"]["description"],
                    Playlist_Id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"],
                    )
        return data
    else:
        print("Channel not found")
        return None
    #extracting the video Ids using playlist and channel ID
def Video_IDsdetails(channel_id):
    video_ids = []#array to store the video IDs
    responsev = youtube.channels().list( part='contentDetails',id=channel_id).execute()
    playlist_id = responsev['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    while True:
        res = youtube.playlistItems().list( part = 'snippet',playlistId = playlist_id,maxResults = 50,
        pageToken = next_page_token).execute()
        
        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')#pagination
        
        if next_page_token is None:
            break
    return video_ids
#get video information
def video_details(video_ids):
        video_info = []
        for video_id in video_ids:
                request = youtube.videos().list(part="snippet,contentDetails,statistics",id= video_id)
                response = request.execute()

                for item in response["items"]:
                        data = dict(Channel_Name = item['snippet']['channelTitle'],
                                        Channel_Id = item['snippet']['channelId'],
                                        Video_Id = item['id'],
                                        Title = item['snippet']['title'],
                                        Tags = item['snippet'].get('tags'),#get fuction is used to get the values of the values, if no values it returns none.To avoid error we are using this
                                        Thumbnail = item['snippet']['thumbnails']['default']['url'],
                                        Description = item['snippet']['description'],
                                        Published_Date = item['snippet']['publishedAt'],
                                        Duration = item['contentDetails']['duration'],
                                        Views = item['statistics'].get('viewCount'),
                                        Likes = item['statistics'].get('likeCount'),
                                        Comments = item['statistics'].get('commentCount'),
                                        Favorite_Count = item['statistics']['favoriteCount'],
                                        Definition = item['contentDetails']['definition'],
                                        Caption_Status = item['contentDetails']['caption']
                                        )
                        video_info.append(data)
        return video_info
#get comment information
def comment_details(video_ids):
        Comment_Info = []
        try:
                for video_id in video_ids:

                        request = youtube.commentThreads().list(
                                part = "snippet",
                                videoId = video_id,
                                maxResults = 50
                                )
                        responseC = request.execute()
                        
                        for item in responseC["items"]:
                                comment_information = dict(
                                        Comment_Id = item["snippet"]["topLevelComment"]["id"],
                                        Video_Id = item["snippet"]["videoId"],
                                        Comment_Text = item["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                                        Comment_Author = item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                                        Comment_Published = item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])

                                Comment_Info.append(comment_information)
        except:
                pass
                
        return Comment_Info
def playlist_info(channel_id):
    playlist_details = []
    next_page_token = None
    while True:

        request = youtube.playlists().list(part="snippet,contentDetails",channelId=channel_id,maxResults=50,pageToken=next_page_token)
        response = request.execute()

        for item in response['items']: 
            data={'PlaylistId':item['id'],
                    'Title':item['snippet']['title'],
                    'ChannelId':item['snippet']['channelId'],
                    'ChannelName':item['snippet']['channelTitle'],
                    'PublishedAt':item['snippet']['publishedAt'],
                    'VideoCount':item['contentDetails']['itemCount']}
            playlist_details.append(data)
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            break
    
    return playlist_details
#connecting with Mongo
client=pymongo.MongoClient("mongodb+srv://anandhianthiran:1234@cluster0.jzlsbsg.mongodb.net/?retryWrites=true&w=majority")
db=client.Youtube_Data
def Channel_fulldetails(channel_id):
    Ch=Channel_details(channel_id)
    play=playlist_info(channel_id)
    Video=Video_IDsdetails(channel_id)
    VI=video_details(Video)
    comment=comment_details(Video)
    Collection=db.YoutubeChannel_details
    Collection.insert_one({"Channel_Information":Ch,"Playlist_Information":play,"Video_Information":VI,"Comment_Information":comment})
    return "Details Uploaded Successfully!"
#Channel table creation
def Channel_Table():
  mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="Youtube_Data"
    )
  mycursor = mydb.cursor(buffered=True)
  mycursor.execute("CREATE DATABASE if not exists Youtube_Data")
  mycursor.execute("Drop Table if exists Youtube_Data.Channels")
  mydb.commit()
  try:
      mycursor.execute("Create Table Youtube_Data.Channels(Channel_Name VARCHAR(100),Channel_ID VARCHAR(80) Primary Key,Subscription_Count BigINT,Views BigINT,Total_Videos INT,Channel_Description text,Playlist_Id VARCHAR(80))")
      mydb.commit()
  except:
      print("Table already exists")

  Channel_list=[]          #getting value from Mongo
  db=client.Youtube_Data
  Collection=db.YoutubeChannel_details
  for Channel in Collection.find({},{"_id":0,"Channel_Information":1}):
      Channel_list.append(Channel["Channel_Information"])
#Dataframe Creation
  df1=pd.DataFrame(Channel_list)
#inserting into mysql
  for index,row in df1.iterrows():
    insert_query = '''INSERT into Youtube_Data.channels(Channel_Name,
                                                      Channel_ID,
                                                      Subscription_Count,
                                                      Views,
                                                      Total_Videos,
                                                      Channel_Description,
                                                      Playlist_Id)
                                          VALUES(%s,%s,%s,%s,%s,%s,%s)'''
    values =(
            row['Channel_Name'],
            row['Channel_Id'],
            row['Subscription_Count'],
            row['Views'],
            row['Total_Videos'],
            row['Channel_Description'],
            row['Playlist_Id'])
    try:
      mycursor.execute(insert_query,values)
      mydb.commit()
    except:
       print("Channeldetails already exists")    

#Playllist Table Creation
def Playlist_Table():
    mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="Youtube_Data"
    )
    mycursor = mydb.cursor(buffered=True)
    mycursor.execute("Drop Table if exists Youtube_Data.Playlist")
    mydb.commit()
    mycursor.execute("Create Table Youtube_Data.Playlist(PlaylistId VARCHAR(100) Primary Key,Title VARCHAR(100),ChannelId VarChar(100),ChannelName Varchar(100),PublishedAt timestamp,VideoCount int)")
    mydb.commit()
    Playlist_list=[]          #getting value from Mongo
    db=client.Youtube_Data
    Collection=db.YoutubeChannel_details
    for Play in Collection.find({},{"_id":0,"Playlist_Information":1}):
        for i in range(len(Play["Playlist_Information"])):
            Playlist_list.append(Play["Playlist_Information"][i])
    #converting to dataframe
    df2=pd.DataFrame(Playlist_list)
    #Inserting into mysql
    for index,row in df2.iterrows():
        insert_query = '''INSERT into Youtube_Data.Playlist(PlaylistId,
                                                        Title,
                                                        ChannelId,
                                                        ChannelName,
                                                        PublishedAt,
                                                        VideoCount)
                                            VALUES(%s,%s,%s,%s,%s,%s)'''#make sure the labels matches with the table column names
        values =(
            row['PlaylistId'],
            row['Title'],
            row['ChannelId'],
            row['ChannelName'],
            row['PublishedAt'],
            row['VideoCount'],)#make sure the labels matches with the dataframecolumn names
        mycursor.execute(insert_query,values)
        mydb.commit()
def Video_Table():
    mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="Youtube_Data"
    )
    mycursor = mydb.cursor(buffered=True)
    mycursor.execute("Drop Table if exists Youtube_Data.Videos")
    mydb.commit()
    create_query = '''create table if not exists Videos(
                            Channel_Name varchar(150),
                            Channel_Id varchar(100),
                            Video_Id varchar(50) primary key, 
                            Title varchar(150), 
                            Tags text,
                            Thumbnail varchar(225),
                            Description text, 
                            Published_Date timestamp,
                            Duration time,
                            Views bigint, 
                            Likes bigint,
                            Comments int,
                            Favorite_Count int, 
                            Definition varchar(10), 
                            Caption_Status varchar(50) 
                            )''' 
                            
    mycursor.execute(create_query)             
    mydb.commit()
    Video_list=[]          #getting value from Mongo
    db=client.Youtube_Data
    Collection=db.YoutubeChannel_details
    for Vd in Collection.find({},{"_id":0,"Video_Information":1}):
        for i in range(len(Vd["Video_Information"])):
            Video_list.append(Vd["Video_Information"][i])
    #converting to dataframe
    df3=pd.DataFrame(Video_list)
    for index,row in df3.iterrows():
        insert_query = '''INSERT INTO Videos(Channel_Name,
                        Channel_Id,
                        Video_Id, 
                        Title, 
                        Tags,
                        Thumbnail,
                        Description, 
                        Published_Date,
                        Duration, 
                        Views, 
                        Likes,
                        Comments,
                        Favorite_Count, 
                        Definition, 
                        Caption_Status 
                        )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
        tags_str = ', '.join(map(str, row['Tags'])) if row['Tags'] is not None else ''
        values =(
                row['Channel_Name'],
                    row['Channel_Id'],
                    row['Video_Id'],
                    row['Title'],
                    tags_str,
                    row['Thumbnail'],
                    row['Description'],
                    row['Published_Date'],
                    row['Duration'],
                    row['Views'],
                    row['Likes'],
                    row['Comments'],
                    row['Favorite_Count'],
                    row['Definition'],
                    row['Caption_Status']
                )
        mycursor.execute(insert_query,values)
        mydb.commit()
def Comment_Table():
    mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="Youtube_Data"
    )
    mycursor = mydb.cursor(buffered=True)
    mycursor.execute("Drop Table if exists Youtube_Data.Comment")
    mydb.commit()
    mycursor.execute("Create Table Youtube_Data.Comment(Comment_Id VarChar(100) Primary key,Video_Id varchar(50),Comment_Text text,Comment_Author varchar(150),Comment_Published timestamp)")
    mydb.commit()
    Comment_list=[]          #getting value from Mongo
    db=client.Youtube_Data
    Collection=db.YoutubeChannel_details
    for Play in Collection.find({},{"_id":0,"Comment_Information":1}):
        for i in range(len(Play["Comment_Information"])):
            Comment_list.append(Play["Comment_Information"][i])
    #converting to dataframe
    df4=pd.DataFrame(Comment_list)
    #Inserting into mysql
    for index,row in df4.iterrows():
        insert_query = '''INSERT into Youtube_Data.Comment(Comment_Id,
                                                        Video_Id,
                                                        Comment_Text,
                                                        Comment_Author,
                                                        Comment_Published)
                                            VALUES(%s,%s,%s,%s,%s)'''#make sure the labels matches with the table column names
        values =(
            row['Comment_Id'],
            row['Video_Id'],
            row['Comment_Text'],
            row['Comment_Author'],
            row['Comment_Published'],)#make sure the labels matches with the dataframecolumn names
        mycursor.execute(insert_query,values)
        mydb.commit()
def tables():
    Channel_Table()
    Playlist_Table()
    Video_Table()
    Comment_Table()
    return "Table Created Successfully"
def show_channels_table():
    Channel_list=[]          
    db=client.Youtube_Data
    Collection=db.YoutubeChannel_details
    for Channel in Collection.find({},{"_id":0,"Channel_Information":1}):
        Channel_list.append(Channel["Channel_Information"])
    df1=st.dataframe(Channel_list)
    return df1
def show_playlists_table():
    Playlist_list=[]          
    db=client.Youtube_Data
    Collection=db.YoutubeChannel_details
    for Play in Collection.find({},{"_id":0,"Playlist_Information":1}):
        for i in range(len(Play["Playlist_Information"])):
            Playlist_list.append(Play["Playlist_Information"][i])
    df2=st.dataframe(Playlist_list)
    return df2
def show_videos_table():
    Video_list=[]          
    db=client.Youtube_Data
    Collection=db.YoutubeChannel_details
    for Vd in Collection.find({},{"_id":0,"Video_Information":1}):
        for i in range(len(Vd["Video_Information"])):
            Video_list.append(Vd["Video_Information"][i])
    df3=st.dataframe(Video_list)
    return df3
def show_comments_table():
    Comment_list=[]          
    db=client.Youtube_Data
    Collection=db.YoutubeChannel_details
    for Play in Collection.find({},{"_id":0,"Comment_Information":1}):
        for i in range(len(Play["Comment_Information"])):
            Comment_list.append(Play["Comment_Information"][i])
    df4=st.dataframe(Comment_list)
    return df4
#Streamlit
with st.sidebar:
    st.title(":blue[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
Channel_input=st.text_input("Please enter your Channel ID")
if st.button("Collect and Store Channel Data"):
    Channel_IDs=[]
    db=client.Youtube_Data
    Collection=db.YoutubeChannel_details
    for ch in Collection.find({},{"_id":0,"Channel_Information":1}):
        Channel_IDs.append(ch["Channel_Information"]["Channel_Id"])
    if Channel_input in Channel_IDs:
        st.success("Channel already exists")
    else:
        insert=Channel_fulldetails(Channel_input)  
        st.success(insert)      
if st.button("Migrate to SQL"):
    Table=tables()
    st.success(Table)
show_table=st.radio("Select the Table to be viewed",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))
if show_table=="CHANNELS":
    show_channels_table()
elif show_table=="PLAYLISTS":
    show_playlists_table()
elif show_table=="VIDEOS":
    show_videos_table()
elif show_table=="COMMENTS":
    show_comments_table()
mydb = mysql.connector.connect(
host="localhost",
user="root",
password="",
database="Youtube_Data"
)
mycursor = mydb.cursor(buffered=True)
question=st.selectbox("Select your Question",("1. What are the names of all the videos and their coressponding channels?",
                                              "2. Which channels have the most number of videos and how many videos do they have?",
                                              "3. What are the top 10 most viewed videos, and how many videos do they have?",
                                              "4. How many comments were made on each video, and what are their corresponding video names?",
                                              "5. Which videos have the higher number of likes and what are their corresponding channel names?",
                                              "6. What is the total number of likes and dislikes for each video and what are their corresponsing video names?",
                                              "7. What is the total number of views of each channel,and what are their corresponsing video names?",
                                              "8. What are the names of all the channels that have published videos in the year 2022?",
                                              "9. What is the average duration of all videos in each channel and what are their corresponding channel names?",
                                              "10. Which videos have the highest number of comments, and what are their corresponding channel names?"))

if question=="1. What are the names of all the videos and their coressponding channels?":
    query1="Select Title as VideoName,Channel_Name as ChannelName from Videos"
    mycursor.execute(query1)
    mydb.commit()
    t1=mycursor.fetchall()
    df1=pd.DataFrame(t1,columns=["VIDEO_NAME","CHANNEL_NAME"])
    st.write(df1)
elif question=="2. Which channels have the most number of videos and how many videos do they have?":
    query2="Select Channel_Name as ChannelName,Total_Videos as VideoCount from channels"
    mycursor.execute(query2)
    mydb.commit()
    t2=mycursor.fetchall()
    df2=pd.DataFrame(t2,columns=["CHANNEL_NAME","VIDEO_COUNT"])
    st.write(df2)
elif question=="3. What are the top 10 most viewed videos, and how many videos do they have?":
    query3="Select Views as ViewCount,Channel_Name as ChannelName,Title as VideoName from Videos where Views is not null order by Views desc limit 10"
    mycursor.execute(query3)
    mydb.commit()
    t3=mycursor.fetchall()
    df3=pd.DataFrame(t3,columns=["VIEWCOUNT","CHANNEL_NAME","VIDEO_NAME"])
    st.write(df3)
elif question=="4. How many comments were made on each video, and what are their corresponding video names?":
    query4="Select Comments as CommentCount,Title as VideoName from Videos where comments is not null "
    mycursor.execute(query4)
    mydb.commit()
    t4=mycursor.fetchall()
    df4=pd.DataFrame(t4,columns=["COMMENTCOUNT","VIDEO_NAME"])
    st.write(df4)
elif question=="5. Which videos have the higher number of likes and what are their corresponding channel names?":
    query5="Select Channel_Name as ChannelName,Title as VideoName,Likes as LikeCount from Videos where Likes is not null order by Likes desc "
    mycursor.execute(query5)
    mydb.commit()
    t5=mycursor.fetchall()
    df5=pd.DataFrame(t5,columns=["CHANNEL_NAME","VIDEO_NAME","LIKE_COUNT"])
    st.write(df5)
elif question=="6. What is the total number of likes and dislikes for each video and what are their corresponsing video names?":
    query6="Select Title as VideoName,Likes as LikeCount from Videos"
    mycursor.execute(query6)
    mydb.commit()
    t6=mycursor.fetchall()
    df6=pd.DataFrame(t6,columns=["VIDEO_NAME","LIKE_COUNT"])
    st.write(df6)
elif question=="7. What is the total number of views of each channel,and what are their corresponsing video names?":
    query7="Select Channel_Name as Channelname,Views as ViewCount from channels "
    mycursor.execute(query7)
    mydb.commit()
    t7=mycursor.fetchall()
    df7=pd.DataFrame(t7,columns=["CHANNELNAME","VIEWCOUNT"])
    st.write(df7)
elif question=="8. What are the names of all the channels that have published videos in the year 2022?":
    query8="Select Title as Videoname,Published_Date as Video_release,Channel_Name as Channelname from Videos where extract(year from Published_Date)=2022 "
    mycursor.execute(query8)
    mydb.commit()
    t8=mycursor.fetchall()
    df8=pd.DataFrame(t8,columns=["Videoname","Published Date","ChannelName"])
    st.write(df8)
elif question=="9. What is the average duration of all videos in each channel and what are their corresponding channel names?":
    query9="Select Channel_Name as Channelname,AVG(Duration)as Averageduration from Videos group by Channel_Name "
    mycursor.execute(query9)
    mydb.commit()
    t9=mycursor.fetchall()
    df9=pd.DataFrame(t9,columns=["Channelname","AverageDuration"])
    T9=[]
    for index,row in df9.iterrows():
        Channel_Title=row["Channelname"]
        average_duration=row["AverageDuration"]
        average_duartion_str=str(average_duration)
        T9.append(dict(Channel_Name=Channel_Title,Avgduration=average_duartion_str))
    df91=pd.DataFrame(T9)
    st.write(df91)
elif question=="10. Which videos have the highest number of comments, and what are their corresponding channel names?":
    query10="Select Channel_Name as Channelname,Title as Videoname, Comments as Commentcount from Videos where Comments is not null order by Comments desc "
    mycursor.execute(query10)
    mydb.commit()
    t10=mycursor.fetchall()
    df10=pd.DataFrame(t10,columns=["Videoname","Channelname","Comments"])
    st.write(df10)
