# Overview

The project focusses on movie data mining with a primary objctive of deriving interesting trends from a collection of movie ratings by users. The analysis aims to uncover at least four noteworthy patterns within the data. Few of the examples are exploring the k most popular movies for a particular age group, identifying the top k movies with the most ratings, which could be considered the most popular, but intrestingly have the lowest ratings and so on.
The datasets utilized for this project are sourced from GroupLens Research, which has gathered and provided rating datasets from the MovieLens website. Specifically, the MovieLens 1M dataset consists of 1 million ratings contributed by 6000 users for 4000 movies.  A few trends were also explored on the 25M dataset to check scalability.

The dataset serves as the foundation for extracting meaningful insights into the preferences and trends related to movie ratings. For this work, we run Spark locally on our VM as well as on cluster. Spark sessions are appropriately configured and python scripts are run for implementing our program using PySpark SQL and DataFrame operations.

# Reflection

This project of movie data mining using PySpark SQL was a valuable learning experience. The challenges encountered helped us to get a better understanding of the intricacies involved in large-scale data analysis. What worked well was the easy data availability and a clear problem statement. Accessing the MovieLens dataset from GroupLens Research was straightforward, providing a rich source for movie ratings. The project's problem statement was well-defined, outlining the objective of deriving interesting trends from the movie rating dataset. This clarity facilitated a focused approach.
Spark SQL was particularly useful for this project due to its capabilities in processing and analyzing large-scale structured data using Spark. It allowed us to express complex data manipulations and analyses using SQL-like queries. Because of our familiarity with SQL syntax it became easier to formulate queries to extract specific information from the dataset.

There was an intial learning curve on understanding and adapting to the structure of the dataset. However, it also contributed to a deeper understanding of the data. Some challenges were faced in the process of rebasing in Git during the projects's development. Addressing conflicts and ensuring a smooth rebase had an initial learning curve as well. During the Git rebase process, resolving conflicts in the version history took some time. This highlighted the importance of clear communication within the development team.
In conclusion, Spark SQL streamlined the data analysis process. The project presented a range of interesting analysis opportunities, from identifying popular movies across different dimensions to exploring trends related to ratings and seasons. It prrovided valuable insights into effective data analysis and collaborative development.

o	To run 1M data on a Spark cluster that is up and running along with a Hadoop cluster, use:

- hdfs dfs -put movies
- spark-submit --master spark://cscluster00.boisestate.edu:7077 P3_1M_DataAnalysis.py hdfs://cscluster00.boisestate.edu:9000/user/ashitachandnani/movies/ml-1m/movies.dat hdfs://cscluster00.boisestate.edu:9000/user/ashitachandnani/movies/ml-1m/ratings.dat hdfs://cscluster00.boisestate.edu:9000/user/ashitachandnani/movies/ml-1m/users.dat hdfs://cscluster00.boisestate.edu:9000/user/ashitachandnani/ml-1m-output 2> /dev/null
- hdfs dfs -get ml-1m-output

# Results

The Movie Data Mining uncovered the following interesting trends

**•	What are the k most popular movies of all time?** 

This global popularity analysis provides insights into the overall popularity of movies across all genres and years. It
identifies movies that have received the highest number of ratings and highest average ratings indicating a broad appeal among users.
Platforms can use this information to understand which movies have resonated the most with their user base. This
information about the most popular movies of all time can be used for catalog promotion.
Highlighting these movies can attract new users and retain existing ones by offering a selection of widely appreciated
content.

**•	What are the k most popular movies for a particular year?** 

The query allows for an annual analysis of the most popular movies, helping content platforms understand the trends and
preferences of users during a specific year (e.g., 2002).
Yearly user engagement metrics can help the platforms assess success of their content strategy and make informed decisions
for the future.
Users may find value in discovering or revisiting movies that were celebrated during a specific time frame.
Highlighting the top movies of a given year can enhance user satisfaction and keep the catalog diverse and engaging.

**•	What are the k most popular movies for a certain age group?**

By focusing on a specific age group (e.g., 25-34), the query aims to provide personalized recommendations tailored to the
preferences of users within that age range.
This is particularly relevant for content platforms seeking to enhance user experience and engagement.
Users are more likely to return to a platform that consistently offers content that aligns with their preferences.

**•	What are the k movies with most ratings (presumably popular) with lowest Rating?**

The combination of high rating counts and lower average ratings may indicate that certain movies are widely watched but not
universally well-received.
Understanding user engagement with such movies can be crucial for content platforms to refine their recommendation
algorithms and content curation strategies.
It present an opportunity for content platforms to analyze user feedback and identify areas for improvement.
It may also be useful in balancing content recommrndations. While some users may enjoy popular blockbusters, others may
prefer movies with niche appeal or unconventional storytelling.

**•	What are the top k movies for a specific season (1: Winter, 2: Spring, 3: Summer, 4: Fall)?**

This SQL query finds the top k movies for a specified season. Understanding seasonal viewing patterns can be used for seasonal content recommendation.

Some other trends we tried to find on 1M datasets:

**•	What is the average rating recieved for each genre?**
  
  This query calculates the average rating for each genre by aggregating the ratings of movies within each genre.
  
  This information facilitates data-driven decision-making in the entertainment industry by providing insights 
  into the popularity and user perceptions of different genres.
  
  Platforms can leverage this information to enhance content curation, user engagement, and overall strategic planning.

**•	What is the average rating given by each age group?**
  
  This query is useful for understanding the average ratings given by users in different age groups.
  
  Analyzing the average ratings based on age can provide valuable insights into the preferences and satisfaction 
  levels of users across different age demographics.

**•	What is the average age of users who rated movies in each genre?**

   This query is useful to the platform for understanding the average age of users who rate movies in different genres.
   
   It provides insights into the age demographics of the audience for each genre, which can be valuable for optimizing               
   various aspects of content strategy and user engagements.

**•	What are the top rated movies in each genre ?**

  The above query calculates the average rating, rating count, and ranking for each movie by genre. It uses the RANK() 
  window function to rank movies within each genre based on the average rating and count of ratings.
  
  This information can be useful top recommend top rated movies in each genre to users who show an interest in a 
  specific genre.

**•	Which genres are preferred by people with different occupational groups?**

   The above query provides information on the preferred genres for different occupation groups based on the average 
   ratings given by users in those groups.
   
  This information can be used to provide personalized content suggestions, user engagement and marketing strategies 
  for users in specific professions and improve overall user experience in diverse occupational groups.
  
**•	Is there a genre preference base on gender?**

This information is useful to gain insights into whether there are variations in genre preferences between different genders.
It also allows quantifying the differences in user engagement between male and female users for each genre.

Some trends on **25M dataset** include : 

**•	Finding the most common tags**

This analysis of popular tags can provide insights into user interests and trends.

**•	Finding top tags for a specific movie**

This understanding of popular tags for a movie can help platforms suggest other movies with similar themes or characteristics.

**•	Explore tagging trends over time**

This analysis helps identify the most popular tags for each year by ranking them based on the number of occurrences. This information can be useful for content platform to understand user preferences and observing how tag popularity evolves over time.
Identifying tags that are becoming more popular in recent years can be indicative of emerging trends or topics.

The correctness of the reuslts was verified by first performing data analysis on a small subset of the 1M dataset comprising of 32373 ratings, 500 tags from 5000 users on 100 movies in Excel.

After that the same analyses were done on the above smaller dataset both on our VM and cscluster and the results matched.

Finally the scripts were run on the much bigger 1M dataset on the cscluster.

•	Timing analysis on the smaller dataset (32373 ratings from 5000 users on 100 movies)

o	VM - 43sec (Spark)

o	Cscluster - 31sec (Spark)

•	Timing analysis on the much larger dataset (1 million ratings from 6000 users on 4000 movies)

o	VM - couple of hours (Spark)

o	Cscluster - 46sec (Spark)

Couldn't finish running the big dataset on VM due to a much smaller RAM on the system.

•	VM Performance:

On our VM, the timing analysis shows an increase in execution time when transitioning from the smaller dataset to the much larger dataset. This is expected as processing a larger volume of data typically requires more computational resources and time.

•	Cscluster Performance:

On the cscluster, there is only a slight increase in execution time when moving from the smaller to the larger datasets. The 25M dataset processing time was improved by using caching. This demostrates the scalability of Spark SQL and is indicative of the distributed and parallel processing capabilities of Spark SQL on a cluster.
In summary, the speed-up from going from a small to a large dataset using Spark SQL is evident, especially in a distributed environment like the cscluster. The cscluster's performance is indicative of Spark SQL's scalability, making it well-suited for big data processing where parallelization and distributed computing can significantly improve execution times.



