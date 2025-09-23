with 

source as (

    select * from {{ source('voterfile', 'vf_nc_partial') }}

),

renamed as (

    select
        county_id,
        county_desc,
        voter_reg_num,
        ncid,
        last_name,
        first_name,
        middle_name,
        name_suffix_lbl,
        status_cd,
        voter_status_desc,
        reason_cd,
        voter_status_reason_desc,
        res_street_address,
        res_city_desc,
        state_cd,
        zip_code,
        mail_addr1,
        mail_addr2,
        mail_addr3,
        mail_addr4,
        mail_city,
        mail_state,
        mail_zipcode,
        full_phone_number,
        confidential_ind,
        registr_dt,
        race_code,
        ethnic_code,
        party_cd,
        gender_code,
        birth_year,
        age_at_year_end,
        birth_state,
        drivers_lic,
        precinct_abbrv,
        precinct_desc,
        municipality_abbrv,
        municipality_desc,
        ward_abbrv,
        ward_desc,
        cong_dist_abbrv,
        super_court_abbrv,
        judic_dist_abbrv,
        nc_senate_abbrv,
        nc_house_abbrv,
        county_commiss_abbrv,
        county_commiss_desc,
        township_abbrv,
        township_desc,
        school_dist_abbrv,
        school_dist_desc,
        fire_dist_abbrv,
        fire_dist_desc,
        water_dist_abbrv,
        water_dist_desc,
        sewer_dist_abbrv,
        sewer_dist_desc,
        sanit_dist_abbrv,
        sanit_dist_desc,
        rescue_dist_abbrv,
        rescue_dist_desc,
        munic_dist_abbrv,
        munic_dist_desc,
        dist_1_abbrv,
        dist_1_desc,
        vtd_abbrv,
        vtd_desc,
        vr_program_id

    from source

)

select * from renamed
