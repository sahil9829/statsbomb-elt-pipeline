# 1. Start with a lightweight version of Python
FROM python:3.14-slim

# 2. Set the working directory inside the container
WORKDIR /app

# 3. Copy our requirements file into the container
COPY requirements.txt .

# 4. Install the required libraries inside the container
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copy all our scripts and folders into the container
COPY . .

# 6. Tell the container what to do when it wakes up
CMD ["python", "scripts/extract_statsbomb.py"]