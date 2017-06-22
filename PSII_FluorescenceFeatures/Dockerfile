FROM  ubuntu
MAINTAINER Solmaz Hajmohammadi <Solmaz.hajmohammadi@lemanatec.de>

# Install any programs needed
RUN useradd -u 1000 extractor \
    && apt-get -y update \
    && apt-get install -y -q build-essential git python python-dev python-pip \
    && apt-get install -y netcat wget \
    && pip install --upgrade pip \
    && rm -rf /var/lib/apt/lists/* \
    && pip install pika requests numpy urllib3 enum pyyaml \
    && mkdir /home/extractor \
    && chown -R extractor /home/extractor \
    && cd /home/extractor \
    && git clone https://opensource.ncsa.illinois.edu/bitbucket/scm/cats/pyclowder2.git \
    && cd pyclowder2 \
    && python setup.py install 

RUN apt-get update
RUN apt-get install -y octave
RUN apt-get install -y octave-pkg-dev
RUN apt-get install -y aptitude
RUN aptitude install -y $(aptitude search ?description\(octave-forge\) | awk '{print $2}')
RUN apt-get install -y $( apt-cache search octave-forge | awk '{printf $1; printf " "}' )

RUN apt-get update

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
    RABBITMQ_QUEUE="terra.ps2" \
    RABBITMQ_URI="" \
MAIN_SCRIPT="terra_ps2.py"
