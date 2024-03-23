# Stop and remove the existing container
docker stop app-1
docker rm app-1

# Build the Docker image
docker build -t my-app .

# Run the Docker container
docker run -d --name app-1 my-app

# Display the logs
docker logs -f app-1