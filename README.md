# Assignment-1
Youtube data harvesting

YouTube-Data-Harvesting-and-Warehousing-using-SQL-MongoDB-and-Streamlit.

Problem Statement: The problem statement is to create a Streamlit application that allows users to access and analyze data from multiple YouTube channels.

Key technoligies and skills:
Python scripting
Data mangement using mongodb and sql
Streamlit

The application have the following features:

 Youtube Api: We use to Retrive data from youtube api for the channels and videos information.
 After retrive the data we store itin mongodb.
 The stored data in mongodb are converted to a dataframe and migrate the data to a sql
 Analyze the data and show that in a webpage using streamlit.
 Perform the sql queries and gain the performance of the channels,vidoes and statistical information about that channel


# Youtube Api:
  Using the youtube api we can get the information about the channels videos comments and like count and we can store the data in json format .

# MongoDb:
  The retrived data is stored in the mongodb in the format of json. Mongodb accepts only json format because it is easy to read and it will give a unique id for every data we stored and we doesn't need to give a unique id for it.This storage process ensures efficient data management and preservation, allowing for seamless handling of the collected data.

# Sql:
  In this application it allows the user to migrate the data from mongodb to sql. User can choose the which channel can be migrate.To a ensure comptability
with a structured format we use the pandas library in python.Following that data cleaning that information stored in seperated table like chanels,videos,       
playlist,and comments.

# Data Analysis:

   Channel Analysis: Channel analysis includes insights on playlists, videos, subscribers, views, likes, comments, and durations. Gain a deep understanding of the channel's performance and audience engagement through detailed visualizations and summaries.

   Video Analysis: Video analysis focuses on views, likes, comments, and durations, enabling both an overall channel and specific channel perspectives. Leverage visual representations and metrics to extract valuable insights from individual videos.


# Streamlit:
   Using the streamlit we can show the data in a webpage we can see the infos about the data in the web page.
