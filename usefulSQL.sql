---Create Database: NewsSummaryDB---
CREATE DATABASE "NewsSummaryDB"
    WITH
    OWNER = "personalFinanceWebsiteAdmin"
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.utf8'
    LC_CTYPE = 'en_US.utf8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;
---Create Database: NewsSummaryDB---

---Create public."News" to store raw news scraped using Selenium
CREATE TABLE IF NOT EXISTS public."News"
(
    link text COLLATE pg_catalog."default",
    "newsTitle" text COLLATE pg_catalog."default",
    "newsDescription" text COLLATE pg_catalog."default",
    "newsSource" text COLLATE pg_catalog."default",
    "newsPublishTime" timestamp without time zone,
    ticker text COLLATE pg_catalog."default"
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."News"
    OWNER to "personalFinanceWebsiteAdmin";
---Create public."News" to store raw news scraped using Selenium

---Create public."NewsSummary"---
CREATE TABLE IF NOT EXISTS public."NewsSummary"
(
    link text COLLATE pg_catalog."default",
    "newsTitle" text COLLATE pg_catalog."default",
    "newsDescription" text COLLATE pg_catalog."default",
    "newsSource" text COLLATE pg_catalog."default",
    "newsPublishTime" timestamp without time zone,
    tickers text[] COLLATE pg_catalog."default",
    "newsSentiment" double precision,
    CONSTRAINT "UNIQUE_LINK_CONSTRAINT" UNIQUE (link)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."NewsSummary"
    OWNER to "personalFinanceWebsiteAdmin";
---Create public."NewsSummary"---

---New Processing if table public."NewsSummary" exists---
INSERT into public."NewsSummary"
SELECT link, "newsTitle", "newsDescription", "newsSource", MIN("newsPublishTime") as "newsPublishTime", array_agg(array[ticker]) as "tickers", CAST(NULL AS double precision) as "newsSentiment"
from public."News"
where "newsPublishTime" >= '2025-07-15 00:00:00' and "newsPublishTime" < '2025-07-16 00:00:00'
group by link, "newsTitle", "newsDescription", "newsSource";
---New Processing if table public."NewsSummary" exists---

---Get back records with a particular ticker in tickers array---
SELECT link, "newsTitle", "newsDescription", "newsSource", "newsPublishTime", "tickers", "newsSentiment"
FROM public."NewsSummary"
WHERE 'TSLA'=ANY ("tickers");
---Get back records with ticket in tickers array---

---Get back News that are published from the beginning of today---
SELECT DISTINCT(link, "newsTitle", "newsDescription", "newsSource", "newsPublishTime", "tickers", "newsSentiment")
FROM public."NewsSummary"
WHERE "newsPublishTime" >= '2025-07-15 00:00:00' and "newsPublishTime" < '2025-07-16 00:00:00';
---Get back News  with ticket in tickers array that is published at the end of the day---

---Remove Duplicate from public."NewsSummary"---
DELETE FROM public."NewsSummary" a
USING public."NewsSummary" b
WHERE a."newsPublishTime" < b."newsPublishTime"
AND a.link = b.link;
---Remove Duplicate---