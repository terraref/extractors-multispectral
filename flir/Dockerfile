FROM ubuntu
MAINTAINER Max Burnette <mburnet2@illinois.edu>

# Install any programs needed
RUN useradd -u 47852 extractor \
    && apt-get -y update \
    && apt-get install -y -q build-essential git python python-pip python-gdal libnetcdf-dev \
                             libpng-dev libjpeg8-dev libfreetype6-dev \
    && usr/bin/yes | apt-get build-dep python-matplotlib \
    && rm -rf /var/lib/apt/lists/* \
    && pip install pika requests urllib3 numpy pillow matplotlib \
    && mkdir /home/extractor \
    && chown -R extractor /home/extractor \
    && cd /home/extractor \
    && git clone https://opensource.ncsa.illinois.edu/bitbucket/scm/cats/pyclowder.git \
    && cd pyclowder \
    && python setup.py install

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
    RABBITMQ_QUEUE="terra.flir.converter" \
    MAIN_SCRIPT="terra.flir.py"
