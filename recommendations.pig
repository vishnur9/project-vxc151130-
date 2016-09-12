


songs= LOAD 'songs.csv' USING PigStorage(',') AS (song_id:int,song_name:chararray,album:chararray,artist:chararray,year:double,genre:chararray,danceability:double);


recommend= LOAD 'general_recommendations.txt' USING PigStorage(' ') AS (songid:int);


songs_filtered = Join songs BY song_id, recommend BY songid;
genre_pop= filter songs_filtered by genre== 'pop';
g_pop= foreach genre_pop generate songs::song_name;
gp_lim= limit g_pop 25;
store gp_lim into 'genre_pop';

genre_jazz= filter songs_filtered by genre== 'jazz';
g_jazz= foreach genre_jazz generate songs::song_name;
gj_lim= limit g_jazz 25;
store gj_lim into 'genre_jazz';

genre_rock= filter songs_filtered by genre== 'rock';
g_rock= foreach genre_rock generate songs::song_name;
gr_lim= limit g_rock 25;
store gr_lim into 'genre_rock';

genre_class= filter songs_filtered by genre== 'classical';
g_class= foreach genre_class generate songs::song_name;
gc_lim= limit g_class 25;
store gc_lim into 'genre_classical';

high_dance= filter songs_filtered by danceability>= 0.7;
h1= foreach high_dance generate songs::song_name;
h1_lim= limit h1 25;
store h1_lim into 'danceability_high';

low_dance= filter songs_filtered by songs::danceability<= 0.3;
h2= foreach low_dance generate songs::song_name;
h2_lim= limit h2 25;
store h2_lim into 'danceability_low';

quit;















 