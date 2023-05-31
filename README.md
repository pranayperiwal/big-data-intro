# big-data-intro
Using Hadoop and Spark for some basic exercises

Task 1: Preprocess data. Process the provided user query logs (search_data.sample). Strip the clickUrls in the query log using Hadoop to leave only a specific part (the url before the first ‘/’) of the clickUrls. 

Example input: zhidao.baidu.com/question/48881311
Example output: zhidao.baidu.com

Output from the MapReduce operation:

<img width="283" alt="image" src="https://github.com/pranayperiwal/big-data-intro/assets/56786002/4c64e5c0-dbcf-441f-921d-b770d17871f4">


Task 2: Rank the tokens that appear most often in the queried url. Tokenlize the clickUrls in the query log, then rank them according to the number of times they appear. The output should be the top ten tokens and the number of times they appear.

Output:

<img width="439" alt="image" src="https://github.com/pranayperiwal/big-data-intro/assets/56786002/14ad08eb-e641-4ac0-bd11-e2f614b246e8">


Task 3: Rank the time period (by minute) with the most queries. Count the number of query at each minute, then rank them from more to less. The output should be the top ten time period (by minute) with most queries and the number of queries during that time period. 

Output:

<img width="348" alt="image" src="https://github.com/pranayperiwal/big-data-intro/assets/56786002/242db2a9-152c-4952-9283-439b2a2f63bd">
