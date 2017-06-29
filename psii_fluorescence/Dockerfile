FROM terraref/terrautils
MAINTAINER Solmaz Hajmohammadi <Solmaz.hajmohammadi@lemanatec.de>

# Install any programs needed
RUN useradd -u 49044 extractor \
    && mkdir -p /home/extractor/sites/ua-mac/raw_data \
    && mkdir -p /home/extractor/sites/ua-mac/Level_1/ps2_fluorescence \
    && chown -R extractor /home/extractor

RUN apt-get -q -y update \
    && apt-get install -y \
        octave \
        octave-pkg-dev \
        aptitude \
        $(aptitude search ?description\(octave-forge\) | awk '{print $2}') \
        $( apt-cache search octave-forge | awk '{printf $1; printf " "}' ) \
    && apt-get update

RUN octave --eval  " pkg install -forge image"

# command to run when starting docker
COPY entrypoint.sh extractor_info.json *.py *.m /home/extractor/

USER extractor
ENTRYPOINT ["/home/extractor/entrypoint.sh"]
CMD ["extractor"]

# Setup environment variables. These are passed into the container. You can change
# these to your setup. If RABBITMQ_URI is not set, it will try and use the rabbitmq
# server that is linked into the container. MAIN_SCRIPT is set to the script to be
# executed by entrypoint.sh
ENV RABBITMQ_EXCHANGE="terra" \
    RABBITMQ_VHOST="%2F" \
    RABBITMQ_QUEUE="terra.multispectral.ps2fluorescence" \
    MOUNTED_PATHS='{"home/clowder/sites":"/home/extractor/sites"}' \
    MAIN_SCRIPT="terra_ps2fluorescence.py"
