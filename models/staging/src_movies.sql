with raw_movies as (
select * from {{source('netflix_project','r_movies')}}
)
select
     movieId as movie_id,
     title,
     genres
from raw_movies