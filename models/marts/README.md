# Medical Telegram Analytics - Data Marts

## Star Schema Design

### Fact Table
**`fct_messages`** - Contains individual Telegram messages with metrics
- **Granularity**: One row per message
- **Metrics**: Views, forwards, engagement score
- **Foreign Keys**: Links to dim_channels and dim_dates

### Dimension Tables
**`dim_channels`** - Telegram channel information
- Channel categorization (Pharmaceutical, Cosmetics, etc.)
- Activity metrics (total posts, average views)
- Time statistics (first/last post dates)

**`dim_dates`** - Date dimension for time analysis
- Calendar attributes (day, week, month, quarter, year)
- Business flags (weekend, business day)
- Seasonal information

## Business Questions Answered
This star schema enables answering:
1. Which channel types get the most engagement?
2. What days/times get the most views?
3. How has posting volume changed over time?
4. Do images increase message engagement?
5. What are the top-performing channels?

## Data Quality Rules
1. No future-dated messages
2. Non-negative view counts
3. Valid channel categorizations
4. Complete foreign key relationships