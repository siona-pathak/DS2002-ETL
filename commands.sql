SELECT user_ratings.id as "Movie ID", ROUND(AVG(user_ratings.rating),2) as "Average Movie Ratings"
FROM user_ratings
GROUP BY user_ratings.id;

SELECT COUNT(movie_stats.id) as "Number of Movies Grossing Over $50M in Revenue"
FROM movie_stats
WHERE movie_stats.revenue >50000000;

SELECT SUM()





