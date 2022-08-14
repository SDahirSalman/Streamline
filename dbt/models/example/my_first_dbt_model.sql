{{ config(materialized='table') }}

select director, count(director) as occurence 
from {{ source('movielist', 'netflix_titles') }}
where director is not null 
GROUP BY director 
ORDER BY occurence desc 


