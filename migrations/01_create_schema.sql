CREATE DATABASE IF NOT EXISTS bmkg_weather_forecast;
USE bmkg_weather_forecast;
CREATE TABLE IF NOT EXISTS bmkg_weather_forecast.master_locations
(
    adm1 String NOT NULL,
    adm2 String NOT NULL,
    adm3 String NOT NULL,
    adm4 String NOT NULL,
    provinsi String NOT NULL,
    kotkab String NOT NULL,
    kecamatan String NOT NULL,
    desa String NOT NULL,
    lot Float64 NOT NULL,
    lang Float64 NOT NULL,
    timezone String NOT NULL,
    tipe String NOT NULL,
) ENGINE = MergeTree
ORDER BY (adm4);

CREATE TABLE IF NOT EXISTS bmkg_weather_forecast.forecasts
(
    location_code String NOT NULL,
    timezone String NOT NULL,
    forecast_code String NOT NULL,
    datetime DateTime NOT NULL,
    t Int8 NOT NULL,
    tcc Int8 NOT NULL,
    tp Float32 NOT NULL,
    weather Int8 NOT NULL,
    weather_desc String NOT NULL,
    weather_desc_en String NOT NULL,
    wd_deg Int16 NOT NULL,
    wd String NOT NULL,
    wd_to String NOT NULL,
    ws Float32 NOT NULL,
    hu Int8 NOT NULL,
    vs Int32 NOT NULL,
    vs_text String NOT NULL,
    time_index String NOT NULL,
    analysis_date Datetime NOT NULL,
    image String NOT NULL,
    utc_datetime Datetime NOT NULL,
    local_datetime Datetime NOT NULL
) ENGINE = ReplacingMergeTree(analysis_date)
ORDER BY (location_code, local_datetime);