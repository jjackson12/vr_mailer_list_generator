select *
from {{ ref("stg_voterfile__pres24_vote_totals") }}
where choice_party not in ('DEM', 'REP') or choice_party is null
