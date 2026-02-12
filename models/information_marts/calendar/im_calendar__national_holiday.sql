WITH
masat_national_holiday AS (
    -- The qualify statement is there in case the sat changes. It will always get the latest data.
    SELECT *
    FROM {{ ref("rv_masat__calendar_national_holiday") }}
    WHERE end_datetime IS null
),

result AS (
    SELECT
        CONCAT(
            ref_calendar.calendar_date_nk,
            "||",
            national_holiday.country,
            "||",
            national_holiday.country_region,
            "||",
            national_holiday.holiday_name
        ) AS pk_calendar_national_holiday_code,
        ref_calendar.calendar_date_nk AS fk_calendar_date_code,
        national_holiday.holiday_name,
        national_holiday.holiday_type,
        national_holiday.holiday_start_date,
        national_holiday.holiday_end_date,
        national_holiday.country_region,
        national_holiday.country
    FROM masat_national_holiday AS national_holiday
    INNER JOIN
        {{ ref('ref__calendar') }} AS ref_calendar
        ON national_holiday.calendar_date_nk = ref_calendar.calendar_date_nk
)

SELECT * FROM result
