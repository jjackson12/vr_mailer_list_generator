with 

source as (

    select * from {{ source('voterfile', 'pres24_vote_totals') }}

),

renamed as (

    select
        county,
        precinct,
        cast(`total votes` as int) as total_votes,
        choice,
        `choice party` as choice_party

    from source

)

select * from renamed
