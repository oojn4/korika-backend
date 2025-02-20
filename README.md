## Dockerizing and Running the Application


### Prerequisites
- Docker installed on your machine.
- Docker CLI

1. Build the Docker Image<br>
Open a terminal and navigate to the project directory. Run the following command to build the Docker image:

```bash
docker build -t myapp:latest .
```

2. Run the Docker Container<br>
After building the image, run the container using the following command:

```bash
docker run -d -p 5000:5000 --name myapp_container myapp:latest
```

This command will start the container in detached mode and map port 5000 of the container to port 5000 on your host machine.

3. Access the Application<br>
Open your web browser and navigate to `http://localhost:5000` to access the application.


4. Stopping the Application<br>
Docker CLI: To stop the container, run:

```bash
docker stop myapp_container
```

