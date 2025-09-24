with
    grouped as (
        select county, precinct, count(*) as num_results
        from {{ ref("stg_voterfile__pres24_vote_totals") }}
        group by 1, 2
    )
select *
from grouped
where num_results != 2
