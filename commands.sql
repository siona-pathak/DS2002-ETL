--Average User Rating per Movie
SELECT user_ratings.id as "Movie ID", ROUND(AVG(user_ratings.rating),2) as "Average Movie Ratings"
FROM user_ratings
GROUP BY user_ratings.id;

--Movies that Generated over $50M
SELECT COUNT(movie_stats.id) as "Number of Movies Grossing Over $50M in Revenue"
FROM movie_stats
WHERE movie_stats.revenue >50000000;

--Show how many user ratings under 4 stars each movie had 
SELECT ratings_summary.id as "Movie ID", movie.title as "Title", SUM(ratings_summary.range_0_1 + ratings_summary.range_1_2 + ratings_summary.range_2_3 + ratings_summary.range_3_4) as "Total Ratings Under 4 stars"
FROM movie, ratings_summary
WHERE ratings_summary.id = movie.id 
GROUP BY ratings_summary.id, movie.title
ORDER by ratings_summary.id DESC