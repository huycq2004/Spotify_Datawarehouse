# Spotify Data Warehouse

This project creates a data warehouse for Spotify song data. It reads data from a CSV file, processes it, and loads it into a PostgreSQL database using a star schema.

## Features

*   Extracts data from a large CSV file.
*   Transforms and cleans the data.
*   Loads the data into a PostgreSQL data warehouse.
*   Uses a star schema with dimension and fact tables.
*   Keeps database credentials secure using a `.env` file.

## How to Run

1.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
2.  **Set up the database:**
    *   Make sure you have a PostgreSQL server running.
    *   Create a database (e.g., `spotify_data_warehouse`).
    *   Create a `.env` file with your database connection details.
3.  **Run the script:**
    ```bash
    python create_warehouse.py
    ```

## Project Structure

```
D:\DataWH\
├── .git/                     # Git version control directory
├── .env                      # Stores sensitive environment variables (e.g., database credentials)
├── .gitignore                # Specifies intentionally untracked files to ignore by Git
├── connect_to_postgre.py     # Contains functions to test the PostgreSQL database connection
├── create_warehouse.py       # Script to create the data warehouse schema and load data from the CSV
├── query_data.py             # Script to query specific information (e.g., top Japanese artists/songs) from the data warehouse
├── README.md                 # Project documentation, including setup, features, and file descriptions
├── requirements.txt          # Lists Python dependencies required for the project
└── visualize_data.py         # Script to generate data visualizations (e.g., bar charts) from the data warehouse
```
