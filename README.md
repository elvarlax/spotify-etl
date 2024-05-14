# Spotify End-To-End Data Pipeline Project

### Introduction
In this project, we will build an end-to-end ETL (Extract, Transform, Load) pipeline using the Spotify API on Azure and Snowflake. The pipeline will retrieve data from the Spotify API, transform it to a desired format, and load it into Azure Data Lake Storage Gen2. From there, we will use Snowflake for further processing and analysis of the transformed data. Snowflake's cloud-based data warehousing platform provides scalable and efficient analytics capabilities, allowing us to derive valuable insights from the Spotify data. This project aims to demonstrate the integration of Azure services with Snowflake for building robust data pipelines and performing advanced analytics on music-related data.

### Architecture
![Architecture Diagram](https://github.com/elvarlax/spotify-etl/blob/main/data_pipeline.jpg)

## Entity-Relationship (ER) Model
![Data Model](https://github.com/elvarlax/spotify-etl/blob/main/er_model.jpg)

## Dimensional Model
![Dimensional Model](https://github.com/elvarlax/spotify-etl/blob/main/dimensional_model.jpg)

### About Dataset/API
The Spotify API offers access to extensive music data, including artist details, album information, and song attributes. It's a valuable resource for developers creating music-related applications and services. - [Spotify API](https://developer.spotify.com/documentation/web-api)

### Services Used
1. **Azure Data Lake Storage Gen2:** Azure Data Lake Storage Gen2, a cloud-based service by Microsoft Azure, merges the scalability and cost-effectiveness of Azure Blob Storage with analytics capabilities. Featuring a hierarchical namespace for efficient data organization and seamless integration with Azure Active Directory for security, it handles both structured and unstructured data. Ideal for analytics, machine learning, and big data tasks, it empowers organizations to derive valuable insights while ensuring scalability, security, and cost-efficiency.

2. **Azure Functions**: Azure Functions, a service by Microsoft Azure, enables developers to deploy event-driven applications seamlessly. With it, you write small, trigger-based functions in various languages like C#, JavaScript, Python, or TypeScript. These functions respond to HTTP requests, timers, storage events, and more. Azure Functions automatically scales with demand, reducing operational overhead and fostering innovation by abstracting infrastructure management.

3. **Snowflake**: Snowflake is a cloud-based data warehousing platform designed for the cloud. It offers a fully managed service that allows you to store and analyze your data using SQL. With its unique architecture, Snowflake separates storage and compute, enabling you to scale resources independently based on your needs. Snowflake supports diverse workloads, including data warehousing, data lakes, and real-time analytics, making it a versatile solution for modern data analytics needs. Within Snowflake, you can leverage services like Snowpipe for data ingestion, warehouses for query processing, and Snowflake's native SQL for analytics.

4. **Power BI:** Power BI is a Microsoft business analytics service that helps users visualize data, create interactive reports and dashboards, and share insights. It connects to various data sources, offers intuitive visualization tools, and supports advanced analytics like predictive modeling. With collaboration features and cloud-based accessibility, Power BI enables users to make data-driven decisions easily.

### Install Packages
```
pip install azure-functions
pip install azure-storage-file-datalake
pip install spotipy
pip install pandas
```

### Project Execution Flow

1. **Extract Data from API:** Retrieve data from the Spotify API.

2. **Timer Trigger (every 1 hour):** Schedule extraction code to run hourly.

3. **Run Extract Code:** Execute code to fetch data from the Spotify API.

4. **Store Raw Data:** Store the extracted raw data to Azure Data Lake Storage Gen2.

5. **Blob Trigger:** Activate when new raw data is stored in Azure Data Lake Storage Gen2.

6. **Transform and Load Code:** Automatically transform and load raw data to Azure Data Lake Storage Gen2.

7. **Load Transformed Data to Snowflake with Snowpipe:** Automatically load transformed data to Snowflake with Snowpipe.

8. **Query Using Snowflake:** Analyze dimensions and facts data with Snowflake.