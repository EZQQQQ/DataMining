# DataMining
# Project 1
# Task 1: Sales Analysis

# Task 2: People You Might Know

## Q2: Spark Pipeline Overview
- Load and structure friendship data into RDD.
- Analyze user relationships.
- Generate friend recommendations based on mutual connections.

## Q2: Recommendations for User IDs
- Output recommendations for specific user IDs (10, 152, 288, 603, 714, 1525, 2434, 2681).
- Recommendations based on mutual friends, ordered by decreasing count.
- Include all second-degree friends if < 10.
- Empty list for users with no friends.
- Resolve ties by ascending user ID.

# Project 2: Products you might buy

# Question (c): Identify Pairs of Items

## Task
- Identify pairs (X, Y) with support ≥ 100.
- Compute confidence scores for association rules X ⇒ Y and Y ⇒ X.
- Sort rules by decreasing confidence, breaking ties lexicographically.
- List the top 5 rules.

## Approach
- Use the Apriori algorithm to find frequent itemsets.
- Generate pairs with support ≥ 100.
- Calculate confidence scores for association rules.
- Sort and list the top 5 rules.

# Question (d): Identify Item Triples

## Task
- Identify triples (X, Y, Z) with support ≥ 100.
- Compute confidence scores for rules (X, Y) ⇒ Z, (X, Z) ⇒ Y, and (Y, Z) ⇒ X.
- Sort rules by decreasing confidence, breaking ties lexicographically.
- List the top 5 rules.

## Approach
- Extend the Apriori algorithm for triples.
- Generate triples with support ≥ 100.
- Calculate confidence scores for association rules.
- Sort and list the top 5 rules.

# Project 3: PageRank and HITS implementation

## PageRank Implementation

### Overview
- Use Spark to implement the iterative PageRank algorithm on a directed graph.
- Matrix M represents the transition probabilities between nodes.
- Iteratively update the PageRank vector until convergence.

### Approach
1. **Initialize**: Set the initial PageRank vector \(r(0)\) to \(1/n\).
2. **Iteration**: For each iteration (\(i\)), update \(r(i)\) using the PageRank equation.
3. **Spark RDD Processing**: Represent matrix \(M\) as an RDD for efficient distributed processing.
4. **Top and Bottom Nodes**: After 40 iterations, identify the top 5 and bottom 5 nodes based on PageRank scores.
5. **Results**: Report the node IDs for both top and bottom nodes.

## HITS Implementation

### Overview
- Implement the HITS algorithm (Hyperlink-Induced Topic Search) using Spark.
- Use link matrix \(L\) to calculate hubbiness (\(h\)) and authority (\(a\)) vectors.
- Iterate the process for 40 iterations to converge to stable scores.

### Approach
1. **Initialize Hubbiness**: Set the initial hubbiness vector (\(h\)) to a vector of all 1's.
2. **Compute Authority**: Calculate \(a\) using \(L^T h\) and normalize the vector.
3. **Compute Hubbiness**: Calculate \(h\) using \(L a\) and normalize the vector.
4. **Spark RDD Processing**: Represent link matrix \(L\) as an RDD for efficient distributed processing.
5. **Top and Bottom Nodes**: After 40 iterations, identify top 5 and bottom 5 nodes for hubbiness and authority scores.
6. **Results**: Report node IDs for both highest and lowest hubbiness and authority scores.

# Project 4: K-Means Clustering and Recommender Systems

## Overview

This project involves implementing clustering algorithms, specifically k-means, and exploring recommender systems using Spark. The tasks include evaluating different distance metrics, initialization strategies, and collaborative filtering methods.

## 1. Implementing k-means on Spark

### High-Level Overview

The objective is to implement the k-means algorithm using Spark and evaluate its performance with different distance metrics (Euclidean and Manhattan) and initialization strategies.

### Approach

1. **Dataset and Initialization**:
   - Load the dataset and initialize centroids using two different strategies (c1.txt and c2.txt).
2. **Euclidean Distance Metrics**:
   - Compute the cost function φ(i) for each iteration using the Euclidean distance metric.
   - Plot the cost function φ(i) vs. iterations for both initialization strategies.
   - Calculate the percentage change in cost after 10 iterations.
3. **Manhattan Distance Metrics**:
   - Compute the cost function ψ(i) for each iteration using the Manhattan distance metric.
   - Plot the cost function ψ(i) vs. iterations for both initialization strategies.
   - Calculate the percentage change in cost after 10 iterations.
4. **Analysis**:
   - Interpret the results, comparing the impact of different distance metrics and initialization strategies on convergence and cost.

## 2. Recommender Systems

### High-Level Overview

This part explores user-item bipartite graphs and collaborative filtering methods for recommender systems. It includes applying matrix operations to compute user and item similarity matrices, as well as recommendation matrices.

### Approach

1. **User-Item Bipartite Graph**:
   - Define the non-normalized user similarity matrix T = R × RT.
   - Explain the meaning of diagonal elements Tii and off-diagonal elements Tij.

2. **Item and User Similarity Matrices**:
   - Derive expressions for item similarity matrix Si and user similarity matrix Su.
   - Utilize matrix operations involving R, P, and Q.

3. **Recommendation Matrices**:
   - Define recommendation matrices Γ for both item-item and user-user collaborative filtering.
   - Provide expressions for Γ in terms of R, P, and Q.

4. **Real Dataset Experiment**:
   - Use the provided dataset and compute matrices P and Q.
   - Compute recommendation matrices Γ for user-user and item-item collaborative filtering.
   - Identify the top 5 TV shows with the highest similarity scores for Alex in both cases.

5. **Analysis**:
   - Interpret the results, discussing the effectiveness of collaborative filtering and the impact of different methods.
