# dbt_macros
This project is a collection of macros and other code snippets I have found to be useful in building a data warehouse with dbt.

The CORE folder deals with functions and behaviors that can be called as pre-hooks and post-hooks, or as part of generating models in their entirety, and have special use cases.

The QUERY_FUNCTIONS folder deals with handy functions that can be called within a query to standardize data cleansing operations and other calculations throughout your environment.

Eventually I'll add other folders and models so this repository can be imported directly into a dbt project, but for now, copy and paste the code into your own project's macros folder for best performance.
