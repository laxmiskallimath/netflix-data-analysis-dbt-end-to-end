with raw_genome_tags as (
     select * from MOVIELENS.RAW.RAW_GENOME_TAGS
)
  SELECT
       tagId as tag_id,
       tag
  FROM
      raw_genome_tags