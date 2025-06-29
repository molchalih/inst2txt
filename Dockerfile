# Step 1: Base Image - Use the specified CUDA 12.8.1 image
FROM nvidia/cuda:12.8.1-devel-ubuntu22.04

# Step 2: Set up the environment
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1
ENV TZ=Etc/UTC

# Step 3: Install system dependencies, including Python 3.10 (default for Ubuntu 22.04) and ffmpeg
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3.10 \
    python3.10-dev \
    python3-pip \
    python3.10-venv \
    ffmpeg \
    git \
    && rm -rf /var/lib/apt/lists/*

# Link python3 to python3.10
RUN ln -sf /usr/bin/python3.10 /usr/bin/python3

# Step 4: Set up the working directory
WORKDIR /app

# Step 5: Install Python dependencies
# First, copy only the requirements file to leverage Docker layer caching
COPY requirements.txt .

# Install the dependencies using the nightly PyTorch build for CUDA 12.8
# The --pre flag is necessary to install pre-release/nightly packages.
RUN pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir packaging && \
    pip3 install --no-cache-dir --pre torch --extra-index-url https://download.pytorch.org/whl/nightly/cu128 && \
    pip3 install --no-cache-dir --pre -r requirements.txt --extra-index-url https://download.pytorch.org/whl/nightly/cu128

# Step 6: Copy the rest of the application code
COPY . .

# Step 7: Expose the necessary port if your application has a web server (optional)
# EXPOSE 8000

# Step 8: Define the default command to run when the container starts
# This can be overridden. For example, to run the main pipeline.
CMD ["python3", "main.py"] 