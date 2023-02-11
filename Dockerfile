# Use Python 3.11 as the base image
FROM python:3.11

# Set the working directory
WORKDIR /scraper
ENV PYTHONPATH "."

# Install the required packages
COPY requirements.txt /scraper/
RUN pip install -r requirements.txt

# Copy the publisher and consumer scripts
COPY ./ /scraper/
# Set the entrypoint
ENTRYPOINT ["python"]
# Set the default command to run the publisher and consumer
CMD ["/scraper/main.py"]