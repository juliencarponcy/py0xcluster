# py0xcluster

py0xCluster is a package dedicated to perform exploratory data analysis and machine learning tasks on DEX activity (Decentralized Exhanges) and web3 data.

This is for now an educational project for myself with the aim of performing fun data-science projects around blockchain data gathered through the Graph Network (https://thegraph.com/)

## Status

- Classes and methods functional to identify most active Pools and retrieve all the swaps / deposits / withdraws events of these pools. All data pre-processed and formatted to DataFrames. 

## Target objectives

- Establish meaningful grouping of address by clustering DEX traders and LP
    - Feature Extraction:
        - TBD but based on mint/swap/burn data from messari subgraphs entities
        - with or without balances at swap time (web3py fetch balance at block)
        - EOA vs Contracts
    - Dimensionality reduction:
        - UMAP / tSNE or PCA / ICA
    - Clustering:
        - DBSCAN
        - silhouette evaluation
    - Visualization:
        - scatter plot with color-coded returns? (TBD)

## Secondary objective: identify which group has the most profitable activity

- Triggered Average of price by swap in/out by group of addresses

- Predict future returns based on the activity of previously clustered groups of addresses

## ML overall approach:

- Decide whether adopting time-series vs tabular approach (preference for the first one)
- Compute time-series based on extracted features and certain kernels / windowing
- Begin by classification approach of expected future (down-bad / neurtral / up-strong)
- Extend to regression

## Random list of potential features:

### Accounts

### Relative to a pool:
- z-scored (clarify how) difference of price 24h? after swap -> could be target independant variable

- nb of events (z-scored to other addresses on same pool)
- average swap size (z-scored/pool)
- average deposit size (z-scored/pool)
- average withdraw size (z-score/pool)

### Account only

- Total nb of positions: swapCount, depositCount, withdrawCount
- ratio? of nb of: swaps / (deposits + withdraws)

### Account - Position
- nb of (liquid) pools interacted with
- % of events (likely swaps) happening in the same block (possibly identical to MEV bots?)
- % of Limit order on uni-v3 (one deposit amout = 0)

### Account - Web3

- is contract?
- Normalized balance (compared to other users) at time of events

# Roadmap:

## Easy / To implement first

### Aggregation / Feature computation

- Aggregate unique addresses
- Implement Account-only query
- First Web3 requests (is_contract / ETH balance)

### First plots

- First features distribution
- PCA/ICA -> t-SNE

## Next, not immediate priority

- Pool clustering / identify easy-best features

### Data Management

- Store/Retrieve to/from SQLite?
- Consider parquet / feather / hdf5

## Secondary, nice to do

### Package

- Update and test requirements / setup
- Document classes and methods with nicely formatted docstrings to future build of the doc

### Performance

- Evaluate performance, profiling, and try improving inefficient / slow bits
