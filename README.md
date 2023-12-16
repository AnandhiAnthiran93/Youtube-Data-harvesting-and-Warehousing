# Youtube-Data-harvesting-and-Warehousing
### Introduction:
YouTube Data Harvesting and Warehousing is a comprehensive project designed to help the users to access and analyze data from various YouTube channels. Using SQL, MongoDB, and Streamlit, this project provides a user-friendly application for retrieving, storing, and querying YouTube channel and video data.
### Components Used:
This project relies on the following components
1.	Python
2.	YouTube Data API
3.	MongoDB
4.	MySQL
5.	Streamlit
### Installation and Setup:
The following are the prerequisite in order to run the project.
1.	Set Up Google API: The Google API is set up to obtain API Credentials to access the YouTube API.
2.	Python and its libraries Installation: Install the python programming language and its related libraries including Pandas(data processing),googleapiclient(Interacting with YouTube Data API) and Streamlit(data visualization) in your system
3.	Configure Database: Set up the MongoDB and MySQL Database for storage.
### Process Involved:
1.	Connecting with API: 
The Connect_API() functions uses Google API key to connect with YouTube Data API. The build function take API key, service name and version as parameters and returns a service object (youtube) that helps to make request to the YouTube Data API.
2.	Extracting Details form YouTube: The channel details, video details, playlist details and comment details extracted using YouTube Data API. 
Channel_details(channel_id):
This function helps to extract the Channel details including Channel name, ID, Subscription count, views, total videos, channel description and playlist ID. It takes the channel id as a parameter. It request the YouTube Data API for specific channel details using youtube.channels().list method. The request is executed and the response is stored in the variable response. The relevant details from the response JSON and stored in a dictionary. Finally, the function returns data dictionary which contains the channel details.
Video_IDsdetails(channel_id):
This function helps to extract all the Video Ids that are uploaded using Playlist and Channel Id. It takes channel_id as the parameter. The youtube.playlistItems().list function helps to extract the video Ids. The extracted video ids are stored in a list using append operation. The process of extracting video id and appending continues until there is no next page. 
Video_details(video_ids):
This function takes video ids as a parameter. Using youtube.videos().list method the video details including Channel name, ID, video Id, title, tags, thumbnail, description, published date, duration, views, likes, dislikes, comments, favourite count, definition and caption status are extracted and stored in a list using append operation.
comment_details(video_ids):
This function takes video ids as the parameter. It uses youtube.commentThreads().list method to extract the details including comment ID, Video ID, comment text, comment author and comment published.
playlist_info(channel_id):
This function takes channel id as the parameter. It uses youtube.playlists().list method to extract the details including playlist id, title, channel id, channel name, published time and video count. The details of all playlist in the channel is appended in a list. The process continues till there is no next page.
3.	Uploading to MongoDB:
The pymongo.MongoClient establishes the connection to the Mongo Server. The MongoClient takes username, password, cluster address and others as parameter. Using the connection client, a database named Youtube_data in Mongo is created. The details of the channels are inserted into the collection using insert_one() function.

4.	Table Creation:
MySQL connection is set up and a database is created.The data from the MongoDB collection are insertion in SQL by creating tables. Using Pandas library a dataframe is created. The function iterrows() iterates over the rows of the DataFrame and insert each row of the Dataframe into the table in MySQL. The functions Channel_Table(), Playlist_Table(),Video_Table(),Comment_Table() are developed to create tables in MySQL.

5.	Visualization:
The Streamlit library is imported and assigned as st. The sidebar, text box, button , radio button, and  select box are created using Streamlit.

6.	Creating SQL Query:
For each question in the select box an SQL query is written which retrieves data from the MySQL database and then converted into dataframe. And using write() function the data is visualized in a tabular format.

### Conclusion:
	Thus the implementation of YouTube data harvesting and warehousing using Python, MongoDB, SQL and streamlit offers a comprehensive solution for collecting, storing and presenting valuable insights from YouTube Channels.

