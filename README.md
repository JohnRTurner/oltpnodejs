# NodeJS Ingest Demo

The application showcases the ingest performance of SingleStore.
This project was generated with NodeJS version 16.13.2.  The result is a Docker image.

## Running the Test

### Quickstart: SingleStoreDB Cloud
1. [Sign up][try-free] for $500 in free managed service credits.
1. Create a S-2 sized cluster in [the portal][portal]
1. Gather the following details from the connection section of Portal.

| Key       | Value                    |
|-----------|--------------------------|
| Host      | from CLUSTER_CONNECTION_URL |
| Username  | admin                    |
| Password  | CLUSTER_ADMIN_PASSWORD   |

### Run the Docker Image
1. Obtain Server - can use AWS
1. Install Docker - `sudo apt install docker.io`
1. Add Docker Group to User `sudo usermod -a -G docker ubuntu`
1. Relogin for the user to gain access to Docker.
1. Make a local copy of the application code found on [github][github]
1. Build the Docker image `docker build -t jrt13a/jrt13a_priv:oltpnode .`
1. Run the Image `docker run -d --name oltpnode -e THREADS=8 jrt13a/jrt13a_priv:oltpnode`
   1. Note: Add option[s] from below chart with -e
1. View the logs `docker logs -f oltpnode`
   

| Option     | Description                          | Default                                                                         |
|------------|--------------------------------------|---------------------------------------------------------------------------------|
| HOST       | Cluster Server                       | svc-3f97fbaa-99ad-4b51-bdab-b44e14848132-dml.aws-virginia-2.svc.singlestore.com |
| USER       | Cluster Username                     | admin                                                                           |
| PASSWORD   | Cluster Password                     |                                                                                 |
| DATABASE   | Database Name                        | tpchtest                                                                        | 
| THREADS    | Number of Threads or Batches to Run  | 8                                                                               |
| BATCHSIZE  | Rows to Process per Thread           | 100000                                                                          |
| SENDSIZE   | Rows to Commit per Transaction       | 10000                                                                           |         

## View the Code
Can view the code on [github][github]

| Filename             | Description                                     | 
|----------------------|-------------------------------------------------|
| README.md            | This File                                       |
| apps.js              | Main Module                                     |
| inslineitemjsonworker.js | single worker thread                            | 
| lineitem.js          | wrapper of calling functions for lineitem table |
| Dockerfile           | Files not to copy to the repository             |
| .dockerignore        | File to generate docker image                   |
| package.json         | List of libraries to create this program        |
| jackage-lock.json    | Version of libraries used                       |



[try-free]: https://www.singlestore.com/try-free/
[portal]: https://portal.singlestore.com/
[github]: https://johnrturner.github.io
