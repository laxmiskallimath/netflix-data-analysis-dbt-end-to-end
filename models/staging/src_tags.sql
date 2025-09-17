{{config(materialized = 'table')}}

with raw_tags as(
 select * from MOVIELENS.RAW.RAW_TAGS
)
select
      userid as user_id,
      movieid as movie_id,
      tag,
      TO_TIMESTAMP_NTZ(timestamp) as tag_timestamp
from
     raw_tags