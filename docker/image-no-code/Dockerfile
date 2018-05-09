FROM ubuntu:16.04

#Install Occopus dependencies
RUN apt-get update && apt-get install -y \
    python \
    python-dev \
    python-pip \
    curl \
    && rm -rf /var/lib/apt/lists/*

#Install Occopus from source
RUN curl https://raw.githubusercontent.com/micado-scale/component-policy-keeper/master/requirements.txt -o policy_keeper_requirements.txt 
RUN pip install --upgrade pip
RUN pip install -r policy_keeper_requirements.txt 

COPY docker-entrypoint.sh /usr/local/bin
ENTRYPOINT ["docker-entrypoint.sh"]

#WARNING: This image does not contain the policy keeper code, you must plugin as volume
CMD /policykeeper/policy_keeper.py --srv

