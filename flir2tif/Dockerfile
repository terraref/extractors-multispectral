FROM clowder/pyclowder:2
MAINTAINER Max Burnette <mburnet2@illinois.edu>

# Create user with necessary user ID for writing permissions on Roger
RUN useradd -u 49044 extractor \
    && mkdir /home/extractor \
    && mkdir -p /home/extractor/sites/ua-mac/raw_data \
    && mkdir -p /home/extractor/sites/ua-mac/Level_1/flir2tif \
    && chown -R extractor /home/extractor

# Install necessary requirements
RUN apt-get -y update \
    && apt-get install -y -q python-gdal \
        libpng-dev \
        libjpeg8-dev \
        libfreetype6-dev \
        python-matplotlib \
    && pip install --upgrade python-dateutil \
        numpy \
        pillow \
        matplotlib \
        influxdb \
        urllib3

# command to run when starting docker
COPY entrypoint.sh extractor_info.json *.py /home/extractor/

USER extractor
ENTRYPOINT ["/home/extractor/entrypoint.sh"]
CMD ["extractor"]

# Setup environment variables. These are passed into the container. You can change
# these to your setup. If RABBITMQ_URI is not set, it will try and use the rabbitmq
# server that is linked into the container. MAIN_SCRIPT is set to the script to be
# executed by entrypoint.sh
ENV RABBITMQ_EXCHANGE="terra" \
    RABBITMQ_VHOST="%2F" \
    RABBITMQ_QUEUE="terra.multispectral.flir2tif" \
    MAIN_SCRIPT="terra_flir2tif.py"
