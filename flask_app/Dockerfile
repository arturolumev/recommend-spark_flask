# Use the Python base image
FROM python:3.8

# Set the working directory in the container
WORKDIR /app

# Copy the requirements.txt file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the Flask application code
COPY . .

# Expose the Flask port
EXPOSE 5000

# Command to run the Flask application
CMD ["python", "app.py"]
