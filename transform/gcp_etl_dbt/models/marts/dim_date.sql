WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2019-01-01' as date)",
        end_date="cast('2030-12-31' as date)"
    ) }}
)

SELECT
    {{ format_date_key('date_day') }}       AS date_key,
    date_day                                AS date,
    EXTRACT(DAY     FROM date_day)          AS day,
    EXTRACT(MONTH   FROM date_day)          AS month,
    EXTRACT(QUARTER FROM date_day)          AS quarter,
    EXTRACT(YEAR    FROM date_day)          AS year,
    EXTRACT(DOW     FROM date_day)          AS day_of_week
FROM date_spine
