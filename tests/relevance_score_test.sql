--select
--   movie_id,
--   tag_id,
--   relevance_score
--from
--   {{ref('fct_genome_score')}}
--where relevance_score <= 3

{{ no_nulls_in_columns(ref('fct_genome_score'))}}