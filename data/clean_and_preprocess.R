library(readr)
library(dplyr)
library(tidyverse)
#define necessary variables such as state voting labels, presidential years
consistent_blue <- c("California", "Oregon", "Washington",
                     "Minnesota", "Illinois", "New York", "Vermont",
                     "New Jersey", "Maine", "Rhode Island", "Maryland",
                     "Delaware", "District of Columbia", "Conneticut",
                     "Hawaii", "Massachusetts")

consistent_red <- c("Alaska", "Alabama", "Arkansas", "Idaho",
                    "Kansas", "Kentucky", "Louisiana", "Missouri",
                    "Mississippi", "Montana", "Nebraska", "North Dakota",
                    "Oklahoma", "South Carolina", "South Dakota", "Tennesse",
                    "Texas", "Utah", "Wyoming", "West Virginia")

united_states <- c("United States")
all_consistent <- union(consistent_blue, consistent_red)
all_consistent <- union(all_consistent, united_states)

biden_years <- c(2024, 2023, 2022, 2021)
trump_years <- c(2020, 2019, 2018, 2017)
obama_years <- c(2016, 2015, 2014, 2013,
                 2012, 2011, 2010, 2009)
bush_years <- c(2008, 2007, 2006, 2005,
                2004, 2003, 2002, 2001)

#cleaning and preparing unemployment dataset
unemployement <- read_csv("data/raw/unemployement_rate_by_state.csv")

unemployement_filtered <- unemployement |>
  filter(State %in% all_consistent) |>
  pivot_longer(cols = `2024`:`2001`,
               names_to = 'Year',
               values_to = 'unemployement_rate')|>
  mutate(
    Vote = case_when(
      State %in% consistent_red ~ "Republican",
      State %in% consistent_blue ~ "Democrat",
      State == 'United States' ~ "United States"),
    administration = case_when(
      Year %in% biden_years ~ "Biden",
      Year %in% trump_years ~ "Trump",
      Year %in% obama_years ~ "Obama",
      Year %in% bush_years ~ "Bush"

    )
  )|> select(-"2000")

unemployement_filtered$Year <- as.Date(paste0(unemployement_filtered$Year,
                                              "-01-01"))

state_metrics <- unemployement_filtered |>
  group_by(administration, State) |>
  reframe(
    start_rate = unemployement_rate[which.min(Year)],
    end_rate   = unemployement_rate[which.max(Year)],
    difference = end_rate - start_rate,
    percent_change = (end_rate - start_rate) / end_rate,
    Vote = first(Vote)
  )

us_metrics <- unemployement_filtered |>
  filter(State == "United States") |>
  group_by(administration) |>
  reframe(
    US_start = unemployement_rate[which.min(Year)],
    US_end   = unemployement_rate[which.max(Year)],
    US_dif = US_end - US_start
  )

unemployement_clean <- state_metrics |>
  left_join(us_metrics, by = "administration") |>
  filter(State != "United States") |>
  mutate(
    dif_from_us = round(difference - US_dif, 3)
  ) |> distinct()


write_csv(unemployement_clean, "data/clean_preprocessed/unemployement_clean.csv")




## cleaning and preparing median income data set
#define data set specific dates to include month and day
to_dates <- function(yrs) as.Date(sprintf("%d-01-01", yrs))

biden_years <- to_dates(2021:2024)
trump_years <- to_dates(2017:2020)
obama_years <- to_dates(2009:2016)
bush_years  <- to_dates(2001:2008)


state_income <- read_csv("data/raw/all_states_median_income.csv")


state_income2 <- state_income |>
  mutate(
    vote = case_when(
      State %in% consistent_red ~ "Republican",
      State %in% consistent_blue ~ "Democrat",
      State == "United States" ~ "United States"
    ),
    administration = case_when(
      observation_date %in% biden_years ~ "Biden",
      observation_date %in% trump_years ~ "Trump",
      observation_date %in% obama_years ~ "Obama",
      observation_date %in% bush_years ~ "Bush"
    )
  ) |>
  group_by(State) |>
  mutate(
    avg_median      = mean(median_income, na.rm = TRUE),
    `2001_income`   = median_income[which.min(observation_date)],
    `2024_income`   = median_income[which.max(observation_date)],
    Year = observation_date
  ) |>
  ungroup()

state_metrics <- state_income2 |>
  filter(State != "United States") |>
  group_by(administration, State) |>
  reframe(
    starting_median = median_income[which.min(observation_date)],
    ending_median   = median_income[which.max(observation_date)],
    difference      = ending_median - starting_median,
    pct_change      = difference / starting_median,
    vote            = first(vote),
    avg_median_income = mean(median_income),
    `2001`   = first(`2001_income`),
    `2024`   = first(`2024_income`),
    median_income = median_income,
    Year = observation_date
  )

US_metrics <- state_income2|>
  filter(State == "United States") |>
  group_by(administration) |>
  reframe(
    US_start = median_income[which.min(observation_date)],
    US_end   = median_income[which.max(observation_date)],
    US_dif = US_end - US_start
    )

median_income_clean <- state_metrics |>
  left_join(US_metrics, by = "administration") |>
  filter(State != "United States") |>
  mutate(
    dif_from_us = round(difference - US_dif, 3)
  ) |> distinct()


write_csv(median_income_clean, "data/clean_preprocessed/median_income_clean.csv")

# clean and prepare the last data set

biden_years <- c(2024, 2023, 2022, 2021)
trump_years <- c(2020, 2019, 2018, 2017)
obama_years <- c(2016, 2015, 2014, 2013,
                 2012, 2011, 2010, 2009)
bush_years <- c(2008, 2007, 2006, 2005,
                2004, 2003, 2002, 2001)

various_factors <- read_csv("data/raw/state_factors.csv")

various_factors_clean <- various_factors |>
  mutate(across(`2001`:`2024`, ~na_if(.x, "(NA)")))

various_factors_clean1 <- various_factors_clean |>
  select(-`1998`, -`1999`, -`2000`) |>
  filter(!is.na(LineCode)) |>
  select(-LineCode) |>
  pivot_longer(
    cols = `2001`:`2024`,
    names_to = "Year",
    values_to = "Value"
  ) |>

  mutate(
    Year = trimws(Year),
    Year = as.numeric(Year)
  ) |>

  pivot_wider(
    names_from  = Description,
    values_from = Value
  ) |>
  rename(
    "State" = "GeoName",
    RGDP    = `Real GDP (millions of chained 2017 dollars) 1`,
    RPI     = `Real personal income (millions of constant (2017) dollars) 2`,
    RPCE    = `Real PCE (millions of constant (2017) dollars) 3`,
    GDP         = `Gross domestic product (GDP)`,
    PI          = `Personal income`,
    DPI         = `Disposable personal income`,
    PCE         = `Personal consumption expenditures`,
    RPCPI  = `Real per capita personal income 4`,
    RPCPCE = `Real per capita PCE 5`,
    PCPI       = `Per capita personal income 6`,
    PCDPI      = `Per capita disposable personal income 7`,
    PCPCE      = `Per capita personal consumption expenditures (PCE) 8`,
    RPP         = `Regional price parities (RPPs) 9`,
    IRPD        = `Implicit regional price deflator 10`,
    `Employ.`        = `Total employment (number of jobs)`
  )

various_factors_clean2 <- various_factors_clean1 |>
  filter(State %in% all_consistent | State == "United States") |>
  mutate(across(c(PCPI, PCDPI, PCPCE), ~ as.numeric(.))) |>

  mutate(
    administration = case_when(
      Year %in% biden_years ~ "Biden",
      Year %in% trump_years ~ "Trump",
      Year %in% obama_years ~ "Obama",
      Year %in% bush_years ~ "Bush"
    ),
    vote = case_when(
      State %in% consistent_red ~ "Republican",
      State %in% consistent_blue ~ "Democrat"
    ),
    presidental_party = case_when(
      administration %in% c("Biden", "Obama") ~ "Democrat",
      administration %in% c("Trump", "Bush")  ~ "Republican"
    ),
    Date = ymd(paste0(Year, "-01-01"))
  )

state_metrics <- various_factors_clean2 |>
  group_by(administration, State) |>
  arrange(Date, .by_group = TRUE) |>
  reframe(
    start_PC_PI   = first(PCPI),
    end_PC_PI     = last(PCPI),
    dif_PC_PI     = end_PC_PI - start_PC_PI,

    start_PC_DPI  = first(PCDPI),
    end_PC_DPI    = last(PCDPI),
    dif_PC_DPI    = end_PC_DPI - start_PC_DPI,

    start_PC_PCE  = first(PCPCE),
    end_PC_PCE    = last(PCPCE),
    dif_PC_PCE    = end_PC_PCE - start_PC_PCE,

    vote = first(vote),
    administration = first(administration),
    presidental_party = first(presidental_party),
    State = first(State)
  )

US_metrics <- various_factors_clean2 |>
  filter(State == "United States") |>
  group_by(administration) |>
  reframe(
    US_start_PC_PI   = first(PCPI),
    US_end_PC_PI     = last(PCPI),
    US_dif_PC_PI     = US_end_PC_PI - US_start_PC_PI,

    US_start_PC_DPI  = first(PCDPI),
    US_end_PC_DPI    = last(PCDPI),
    US_dif_PC_DPI    = US_end_PC_DPI - US_start_PC_DPI,

    US_start_PC_PCE  = first(PCPCE),
    US_end_PC_PCE    = last(PCPCE),
    US_dif_PC_PCE    = US_end_PC_PCE - US_start_PC_PCE
  )

various_factors <- state_metrics |>
  left_join(US_metrics, by = "administration") |>
  filter(State != "United States") |>
  mutate(
    PC_PI_dif  = round(dif_PC_PI  - US_dif_PC_PI, 3),
    PC_DPI_dif = round(dif_PC_DPI - US_dif_PC_DPI, 3),
    PC_PCE_dif = round(dif_PC_PCE - US_dif_PC_PCE, 3)
  ) |>
  distinct()

write_csv(various_factors_clean1, "data/clean_preprocessed/various_factors_clean_NA.csv")
write_csv(various_factors,       "data/clean_preprocessed/various_factors_clean.csv")
