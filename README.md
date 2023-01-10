# py0xcluster

This is for now an educational project for myself with the aim of performing fun data-science projects around blockchain data gathered through the Graph Network (https://thegraph.com/)

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

**Secondary objective: identify which group has the most profitable activity**

- Triggered Average of price by swap in/out by group of addresses

- Predict future returns based on the activity of previously clustered groups of addresses
