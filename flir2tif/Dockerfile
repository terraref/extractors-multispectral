FROM terraref/terrautils:1.4
MAINTAINER Max Burnette <mburnet2@illinois.edu>

# TODO: Dockerhub automated builds from tags

# Create user with necessary user ID for writing permissions on filesystem
RUN useradd -u 49044 extractor

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
    MAIN_SCRIPT="terra_flir2tif.py" \
    CLOWDER_SPACE="5bdc8f174f0cb2fdaaf3148e"
