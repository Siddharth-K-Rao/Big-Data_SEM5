# BIG DATA SEM 5 assignments
These assignments were done by **[Revanth](https://github.com/RevanthBabuPN)**, **[Navneeth](https://github.com/NavneethSH)**, **[Siddharth](https://github.com/Siddharth-K-Rao)** and **[me](https://github.com/sreyansb)**.

## Assignment1
This was for searching a word in plane-carriers.ndjson file.

In the task 1, in the mapper, we checked for validity of records and simply saw if the given word was recognised or unrecognised and falling on a weekend and then grouped them in the reducer, giving the output as the number of records having the word as recognised and not recognised.

In the task 2, in the mapper, we checked for the validity of the records and also if the given draawing had the right euclidean distance for a given word. We then did the grouping, reduction in the reducer.

## Assignment2
This assignment was done for page rank calculation.

We were given a dataset containing the edges between from_node and to_node and we had to find the final convergence of the pageranks i.e. **A*v=v1**.


