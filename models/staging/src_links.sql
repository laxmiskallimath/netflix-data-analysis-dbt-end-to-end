with raw_links as (
select * from MOVIELENS.RAW.RAW_LINKS
)
 select
        movieId as movie_id,
        imdbid as imdb_id,
        tmdbid as tmdb_id
 from
       raw_links