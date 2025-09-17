with ratings_summary as (
    select
        movie_id,
        avg(rating) as average_rating,
        count(*) as total_ratings
    from {{ref('fct_ratings')}}
    group by movie_id
    having count(*) > 100 -- only movies with at least 100 ratings
)
select
     m.movie_title,
     rs.average_rating,
     rs.total_ratings
from ratings_summary rs
join {{ ref ('dim_movies')}} m on m.movie_id = rs.movie_id
order by rs.average_rating desc
limit 20;
