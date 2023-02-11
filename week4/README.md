## Week 4 Overview

* [DE Zoomcamp 4.1.1 - Analytics Engineering Basics](#de-zoomcamp-411---analytics-engineering-basics)
* [DE Zoomcamp 4.1.2 - What is dbt](#de-zoomcamp-412---what-is-dbt)
* [DE Zoomcamp 4.2.1 - Start Your dbt Project: BigQuery and dbt Cloud](#de-zoomcamp-421---start-your-dbt-project-bigquery-and-dbt-cloud)

## [DE Zoomcamp 4.1.1 - Analytics Engineering Basics](https://www.youtube.com/watch?v=uF76d5EmdtU&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=33)

![](./img/etl_vs_elt.png)

**Kimball's Dimensional Modeling:** is an approach used for analytics, that prioritizes understandability and query performance over redundant data. The databases store denormalized data following [star](https://en.wikipedia.org/wiki/Star_schema) or [snowflake](https://en.wikipedia.org/wiki/Snowflake_schema) schemas. The elements of dimensional modeling are:

* **Fact tables**, that store measurements, metrics or facts related to the business process that occurred at some particular moment.

* **Dimensions tables**, which provide context or attributes to the fact tables. [Kleppman (2017)](https://www.google.com.br/books/edition/Designing_Data_Intensive_Applications/p1heDgAAQBAJ?hl=en&gbpv=0) arguments that dimension tables contain the who, what, where, when, how and why of the events registered in the fact tables.

## [DE Zoomcamp 4.1.2 - What is dbt](https://www.youtube.com/watch?v=4eCouvVOJUw&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=34)

Dbt is an acronym for Data Build Tool. It is a tool for the transformation of raw data into a format that allows us to perform analyses and to expose such data to the stakeholders and business managers.

![](./img/dbt.png)

## [DE Zoomcamp 4.2.1 - Start Your dbt Project: BigQuery and dbt Cloud](https://www.youtube.com/watch?v=iMxh6s_wL4Q&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=35)

