with
    margins as (
        select
            county,
            precinct,
            case
                when sum(total_votes) = 0
                then null
                else
                    sum(
                        case
                            when choice_party = "DEM"
                            then total_votes
                            when choice_party = "REP"
                            then -1.0 * total_votes
                            else 0
                        end
                    )
                    / sum(total_votes)
            end as dem_margin
        from {{ ref("stg_voterfile__pres24_vote_totals") }}
        group by county, precinct
    )
select *
from margins
where dem_margin is not null
