## Initial Data:
>Several CSV files. The number of files can be quite large (up to 100,000).
>The number of rows within each file can reach up to several million.
>Each file contains 5 columns: product ID (integer), Name (string), Condition (string), State (string), Price (float).
>The same product IDs may occur more than once in different CSV files and in the same CSV file.

## Task:
>Write a console utility using Java programming language that allows getting a selection of the cheapest 1000 products from the input CSV files, but don’t include the same product more than 20 times. Use parallel processing to increase performance.
 
## Utility Result:
>Output CSV file that meets the following criteria:
>no more than 1000 objects sorted by Price from all files;
>no more than 20 objects for each product ID.
