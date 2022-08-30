# NodeJS Ingest Demo

The application showcases ingest performance of SingleStore by loading random bulk data in parallel.

This project was generated with NodeJS 18, result is a Docker image.

## Running the Demo

### Quickstart: SingleStoreDB Cloud
1. [Sign up][try-free] for $500 in free managed service credits.
1. Create a S-2 sized cluster in [the portal][portal]
1. Gather the following details from the connection section of Portal.

| Key      | Value                       | Docker Option |
|----------|-----------------------------|---------------|
| Host     | from CLUSTER_CONNECTION_URL | **DBHOST**    |
| Username | admin                       | **DBUSER**    |
| Password | CLUSTER_ADMIN_PASSWORD      | **DBPASS**    |


### Run the Docker Image
1. Obtain Server - can use AWS
1. Install Docker - `sudo apt install docker.io nmon -y`
1. Add Docker Group to User `sudo usermod -a -G docker ubuntu`
1. Relogin for the user to gain access to Docker.
1. Make a local copy of the application code found on [github][github] by `git clone https://github.com/JohnRTurner/oltpnodejs.git`
1. Build the Docker image `docker build oltpnodejs -t oltpnodejs`
1. Run the Image `docker run -d --name oltpnodejs -e DBPASS=XXXXXXXX -t oltpnodejs`
   1. Note: Add option[s] from below chart with -e
1. View the logs `docker logs -f oltpnode`
   

| Option        | Description                            | Default                                                                         |
|---------------|----------------------------------------|---------------------------------------------------------------------------------|
| **DBHOST**    | Cluster Server                         | svc-3f97fbaa-99ad-4b51-bdab-b44e14848132-dml.aws-virginia-2.svc.singlestore.com |
| **DBUSER**    | Cluster Username                       | admin                                                                           |
| **DBPASS**    | Cluster Password                       |                                                                                 |
| DBDATABASE    | Database Name                          | tpchtest                                                                        | 
| THREADS       | Number of Threads or Batches to Run    | 8                                                                               |
| TOTALSIZE     | Rows to Process for everything         | 800000                                                                          |
| SENDSIZE      | Rows to Commit per Transaction         | 10000                                                                           |         
| LINEOLTPTEST  | Run the Lineitem OLTP Test 0=off 1=on  | 0                                                                               |  
| LINEBATCHTEST | Run the Lineitem Batch Test 0=off 1=on | 0                                                                               |
| JSONTEST      | Lineitem Table Type 0=column 1=JSON    | 0                                                                               |
| ORDERTEST     | Run the Order Test                     | 1                                                                               |
| TNAME         | Table name for the Order Test          | order_v3                                                                        |

## Code Description
Can view the code on [github][github]

| Filename                        | Description                                       | 
|---------------------------------|---------------------------------------------------|
| apps.js                         | Main module takes parameters and calls tests      |
| lineitem/lineitem.js            | wrapper of database calls for lineitem table      |
| lineitem/lineitemjson.js        | wrapper of database calls for lineitem JSON table |
| lineitem/lineitemoltptest.js    | controller to run the lineitem OLTP               |
| lineitem/lineitembatchtest.js   | controller to run the lineitem batch              | 
| lineitem/inslineitemworker.js   | single worker thread for lineitem batch           | 
| order_3whales/orderbatchtest.js | controller to run the order batch                 | 
| order_3whales/insorderworker.js | single worker thread for order batch              |
| README.md                       | This file                                         |
| Dockerfile                      | Files not to copy to the repository               |
| .dockerignore                   | File to generate docker image                     |
| package.json                    | List of libraries to create this program          |
| jackage-lock.json               | Version of libraries used                         |



[try-free]: https://www.singlestore.com/try-free/
[portal]: https://portal.singlestore.com/
[github]: https://github.com/JohnRTurner/oltpnodejs
