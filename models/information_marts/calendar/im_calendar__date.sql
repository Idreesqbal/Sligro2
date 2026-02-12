WITH

last_full_dateparts AS (
    SELECT
        {{ previous_dateparts('four_week_period', 'calendar_date_nk', 'iso_year_of_week', 'iso_four_week_period') }}
            AS previous_year_period,
        {{ previous_dateparts('month', 'calendar_date_nk', 'year_number', 'month_of_year') }}
            AS previous_year_month,
        {{ previous_dateparts('quarter', 'calendar_date_nk', 'year_number', 'quarter_of_year') }}
            AS previous_year_quarter,
        {{ previous_dateparts('week', 'calendar_date_nk', 'iso_year_of_week', 'iso_week_of_year') }}
            AS previous_year_week
    FROM {{ ref('ref__calendar') }}
    WHERE calendar_date_nk = CURRENT_DATE()
),

calendar AS (
    SELECT
        calendar_date_nk AS pk_calendar_date_code,
        day_of_week_name AS day,
        day_of_week_name_short AS day_name_short,
        day_of_month,
        iso_first_day_of_week_name AS first_day_of_the_week,
        day_of_week_iso AS day_of_week,
        day_of_year,
        iso_weekday_weekend_indicator AS weekday_weekend_indicator,
        iso_week_start_date AS week_start_date,
        iso_week_end_date AS week_end_date,
        iso_week_of_year AS week_number,
        month_of_year AS month,
        month_name,
        month_name_short,
        quarter_of_year AS quarter_number,
        year_number AS year,
        iso_year_of_week AS year_of_week,
        last_day_of_month_indicator,
        iso_four_week_period AS four_week_period,

        -- Financial Calendar
        year_number AS financial_year,
        month_of_year AS financial_period,
        -- Financial Week always starts on 1 January.
        -- The 1st week of january always be financial week 1
        --      although if the ISO week number of the 1st week of January belongs to the previous year.
        -- If the ISO week number of the last week of December belongs to the next year, then the financial week = 53.
        -- The rest follows the ISO week number
        IF(
            iso_year_of_week = year_number, iso_week_of_year,
            IF(iso_year_of_week < year_number, 1, 53)
        ) AS financial_week,

        CONCAT('Q', quarter_of_year) AS quarter_name,
        EXTRACT(MONTH FROM iso_week_start_date) = EXTRACT(MONTH FROM iso_week_end_date) AS week_complete_indicator,

        -- Year-datepart combinations
        {{ period_year_combination('week', 'iso_year_of_week', 'iso_week_of_year') }}
            AS week_year,
        {{ period_year_combination('month', 'year_number', 'month_of_year') }}
            AS month_year,
        {{ period_year_combination('quarter', 'year_number', 'quarter_of_year') }}
            AS quarter_year,
        {{ period_year_combination('four_week_period', 'iso_year_of_week', 'iso_four_week_period') }}
            AS four_week_period_year,

        -- Set indicator for last complete period, month, quarter, and week based on the last_full_dateparts
        CONCAT(iso_year_of_week, iso_week_of_year)
        = (SELECT previous_year_week FROM last_full_dateparts) AS last_complete_week_indicator,
        CONCAT(year_number, month_of_year)
        = (SELECT previous_year_month FROM last_full_dateparts) AS last_complete_month_indicator,
        CONCAT(year_number, quarter_of_year)
        = (SELECT previous_year_quarter FROM last_full_dateparts) AS last_complete_quarter_indicator,
        CONCAT(iso_year_of_week, iso_four_week_period)
        = (SELECT previous_year_period FROM last_full_dateparts) AS last_complete_period_indicator
    FROM {{ ref('ref__calendar') }}
)

SELECT *
FROM calendar
