# ETL Pipeline for Baseball Data

This project implements an ETL (Extract, Transform, Load) pipeline using Apache Spark for processing baseball data from the **Lahman Baseball Database**. The pipeline extracts data from a MySQL database, performs necessary transformations using Spark, and stores the results in **MinIO**, which serves as the S3-compatible object storage.

## Project Structure
```
├── app/
│   ├── etl.py
│   ├── average_salary.py
│   ├── allstar_appearance.py
│   ├── top_10_pitchers.py
│   ├── first_last_place.py
│   ├── requirements.txt
│   ├── test.py
│   ├── schema_config.json
│   ├── Dockerfile
│   └── utils.py
├── db/
│   └── lahman2016.sql
├── docker-compose.yaml
├── streaming_vs_batch.md
└── README.md
```

### Description of key files:

- **`etl.py`**: Contains the main ETL process (Extract, Transform, Load). This script orchestrates the reading, transforming, and saving of data.
- **`....py`**: Defines the transformation logic for the individual tasks (e.g., average salary, Hall of Fame pitcher analysis, etc.).
- **`utils.py`**: Helper functions for reading data from MySQL, saving the transformed data to MinIO, and other repetitive tasks.
- **`lahman2016.sql`**: SQL schema for creating the Lahman 2016 database and importing the baseball data.
- **`test.py`**: Contains unit tests for validating the transformed data. This script verifies that the output datasets have the correct column names, data types, no null values, and unique keys.
- **`schema_config.json`**: A JSON file that defines the expected schema (column names and data types) for each transformed dataset. This is used in test.py to ensure consistency and correctness in the data output.
- **`streaming_vs_batch.md`**: A markdown file explaining the differences between **batch processing** and **stream processing**, their use cases, advantages, and trade-offs. This document helps justify the choice of batch processing for this project.

## Assumptions on the Data

- **Player Classification**: Players are classified as either infielders or pitchers based on the majority of games played. A player who has played more games as a pitcher than an infielder is considered a pitcher, and vice versa.
- **Hall of Fame Induction**: A player’s Hall of Fame induction year is determined by the first year they played as a pitcher after being inducted into the Hall of Fame.
- **Top 10 Pitchers**: The "Top 10 Pitchers" are ranked based on their performance (ERA, win/loss percentage) in both the regular season and post-season, with averages calculated for both seasons. Only players who participated in both the regular season and post-season are considered for the ranking.
In addition, Not every year has exactly 10 pitchers who participated in both the regular season and the postseason for the same team. In cases where fewer than 10 such players exist for a given year, the aggregation is performed on the available players.
- **Data Processing**: The data is processed in a **batch-oriented** manner rather than streaming, as the dataset is historical and aggregated over multiple years.

## Process Overview

### 1. **Extraction**
Data is extracted from the **MySQL database** using JDBC. The specific SQL query selects only the relevant columns needed for each transformation step. Data is read in batch mode, ensuring that only the necessary data is loaded into Spark.

### 2. **Transformation**
The transformation logic is divided into four parts:
- **Part 1**: Calculate the **average salary** of infielders and pitchers per year.
- **Part 2**: Calculate **Hall of Fame** pitchers' **number of all-star appearances**, **average ERA** in all-star years, and **induction year**.
- **Part 3**: Calculate the **average ERA** and **win/loss percentage** of the top 10 pitchers in the **regular and post-season**.
- **Part 4**: List the **first and last place teams** for each year and their **number of at-bats**.

Each of these tasks reads the necessary data from MySQL, performs the appropriate calculations, and then returns the transformed data as a **Spark DataFrame**.

### 3. **Loading**
Once the transformations are complete, the results are saved as **CSV files** in **MinIO**, an S3-compatible object storage service. The processed files are uploaded to the specified bucket in MinIO for future consumption.


#### Why MinIO?

MinIO was chosen as the storage solution for this project due to the following reasons:
1. **S3 Compatibility**: MinIO is fully compatible with Amazon S3's API. This makes it an ideal choice for projects that need to integrate with cloud storage solutions like AWS but want to avoid the costs associated with using AWS S3. MinIO provides the same functionality at a fraction of the cost by running in an on-premises or private cloud environment.
2. **Cost-Effective**: MinIO is a lightweight, open-source object storage service that can be self-hosted. Since this project has constraints and we are not using AWS S3, MinIO offers a cost-effective alternative to store and retrieve large amounts of data in an object storage format.
3. **Scalability**: MinIO is designed for high scalability. As the dataset grows or more data is processed, MinIO can handle large volumes of data seamlessly, ensuring that our ETL pipeline remains efficient even with larger datasets.
4. **Ease of Setup**: MinIO is easy to set up and integrate with various data processing tools. It works perfectly with the Spark framework for writing CSV files, and its compatibility with the S3 API allows for easy integration with any S3-compatible service in the future.

For these reasons, MinIO was selected to handle our storage needs and ensure efficient, cost-effective, and scalable storage for our processed data.

#### Fallback Storage: Local CSV Files
Since MinIO did not work correctly on my local machine, the ETL pipeline also supports saving CSV files to the local filesystem as a backup.
By default, CSV files will be stored in: `/app/output/`
Each transformation step saves a uniquely named CSV file to this directory. If a file with the same name already exists, it will be overwritten instead of being deleted along with other files.

## Setup and Running

### 1. **Running the Application with Docker**

This project leverages **Docker** and **Docker Compose** to orchestrate the services required for the ETL pipeline, including:
- MySQL database with the lahman2016 schema.
- MinIO for object storage.
- Spark for data processing.

The necessary environment variables for MySQL and MinIO configurations are managed directly within the docker-compose.yaml and Dockerfile settings.

To launch the entire environment with all the services, navigate to the root directory of the project and run:

```bash
docker-compose up --build -d
```

This command will:

- Build the Docker images defined in the `Dockerfile`.
- Start the services defined in `docker-compose.yaml`, including MySQL, MinIO, and the Python application.

To stop all services after they have been run, you can use:
```bash
docker-compose down
```

### 2. **Running Specific Modules**
To run a specific transformation module, such as `etl.py`, execute:
```bash
docker-compose run --rm app python app/etl.py
```
This command will run the ETL process and execute the script inside the `app` module, allowing you to process the data with the specific module.

## Conclusion
This ETL pipeline is designed to process historical baseball data, perform necessary transformations, and store the results in MinIO. The use of Docker and Docker Compose makes it easy to deploy and run the application with all the necessary dependencies and services.
If you encounter any issues or need to modify the pipeline, the project is fully modularized, allowing for easy updates and improvements.