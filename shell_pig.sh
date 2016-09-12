hdfs dfs -put general_recommendations.txt
pig recommendations.pig


echo "recommendations based on high danceability"
hdfs dfs -cat danceability_high/part-r-00000

echo "recommendations based on low danceability"
hdfs dfs -cat danceability_low/part-r-00000

echo "recommendations based on genre - rock"
hdfs dfs -cat genre_rock/part-r-00000

echo "recommendations based on genre - jazz"
hdfs dfs -cat genre_jazz/part-r-00000

echo "recommendations based on genre - classical"
hdfs dfs -cat genre_classical/part-r-00000

echo "recommendations based on genre-pop"
hdfs dfs -cat genre_pop/part-r-00000

