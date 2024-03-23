# Use an official Node.js runtime as the base image
FROM node:21-bullseye

# Set the working directory in the Docker image
WORKDIR /usr/src/app

# Copy package.json and package-lock.json into the Docker image
COPY package*.json ./

# Install the project dependencies
RUN npm install

# Copy the rest of the project files into the Docker image
COPY . .

# Expose port 3000 for the application
EXPOSE 3000

# Run the application
CMD [ "node", "main.js" ]