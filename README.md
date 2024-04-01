# Spotify End-To-End Data Pipeline Project

### Introduction
In this project, we will build ETL (Extract, Transform, Load) pipeline using the Spotify API on Azure. The pipeline will retrieve data from the Spotify API, transform it to a desired format, and load it to an Azure data lake storage.

### Architecture
![Architecture Diagram](https://github.com/elvarlax/spotify-etl/blob/main/spotify_data_pipeline.jpg)

### About Dataset/API
The Spotify API offers access to extensive music data, including artist details, album information, and song attributes. It's a valuable resource for developers creating music-related applications and services. - [Spotify API](https://developer.spotify.com/documentation/web-api)

### Services Used
1. **Azure Data Lake Storage Gen2:** Azure Data Lake Storage Gen2, a cloud-based service by Microsoft Azure, merges the scalability and cost-effectiveness of Azure Blob Storage with analytics capabilities. Featuring a hierarchical namespace for efficient data organization and seamless integration with Azure Active Directory for security, it handles both structured and unstructured data. Ideal for analytics, machine learning, and big data tasks, it empowers organizations to derive valuable insights while ensuring scalability, security, and cost-efficiency.

2. **Azure Functions**: Azure Functions, a service by Microsoft Azure, enables developers to deploy event-driven applications seamlessly. With it, you write small, trigger-based functions in various languages like C#, JavaScript, Python, or TypeScript. These functions respond to HTTP requests, timers, storage events, and more. Azure Functions automatically scales with demand, reducing operational overhead and fostering innovation by abstracting infrastructure management.

3. **Azure Synapse Analytics**: Azure Synapse Analytics, a Microsoft Azure service, merges data warehousing, big data analytics, and integration into one platform. It enables real-time analysis and visualization of large volumes of structured and unstructured data from diverse sources. With on-demand resources, seamless integration with Azure Machine Learning and Power BI, and tight coupling with Azure Data Lake Storage and Azure Databricks, Synapse Analytics empowers organizations to extract valuable insights and make data-driven decisions efficiently.

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

7. **Query Using Synapse Analytics:** Analyze transformed data with Azure Synapse Analytics.
