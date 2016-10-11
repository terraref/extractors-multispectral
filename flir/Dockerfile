FROM ubuntu
MAINTAINER Max Burnette <mburnet2@illinois.edu>

# ------------------------
# From /flirExtractor directory:
#     docker build -f Dockerfile -t terra-flirExtractor .
#     docker run -p 9000:9000 -e RABBITMQ_URI=amqp://user:pass@<ip>/%2f terra-flirExtractor
# ------------------------

# Install any programs needed
RUN useradd -u 47852 flirExtractor \
    && apt-get -y update \
    && apt-get install -y -q build-essential git python python-pip python-gdal \
    && rm -rf /var/lib/apt/lists/* \
    && pip install pika requests numpy scipy pillow urllib3 \
    && cd /home \
    && mkdir flirExtractor \
    && chown -R flirExtractor /home/flirExtractor \
    && git clone https://opensource.ncsa.illinois.edu/bitbucket/scm/cats/pyclowder.git \
    && cd pyclowder \
    && git checkout local-mount-path-downloading \
    && python setup.py install

# command to run when starting docker
COPY entrypoint.sh extractor_info.json *.py /home/flirExtractor/

USER flirExtractor
ENTRYPOINT ["/home/flirExtractor/entrypoint.sh"]
CMD ["extractor"]

# Setup environment variables. These are passed into the container. You can change
# these to your setup. If RABBITMQ_URI is not set, it will try and use the rabbitmq
# server that is linked into the container. MAIN_SCRIPT is set to the script to be
# executed by entrypoint.sh
ENV RABBITMQ_EXCHANGE="terra" \
    RABBITMQ_VHOST="%2F" \
    RABBITMQ_QUEUE="terra.flirExtractor" \
    MAIN_SCRIPT="terra_flirExtractor.py"
