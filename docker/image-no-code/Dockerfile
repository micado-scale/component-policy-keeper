FROM python:2.7-slim

#Install Occopus dependencies
RUN apt-get update && apt-get install -y \
    curl && \
    curl https://raw.githubusercontent.com/micado-scale/component-policy-keeper/master/requirements.txt -o policy_keeper_requirements.txt && \
    pip install --upgrade pip && \
    pip install -r policy_keeper_requirements.txt && \
    apt-get remove --purge -y curl && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/* && \
    rm -rf /root/.cache

COPY docker-entrypoint.sh /usr/local/bin
ENTRYPOINT ["docker-entrypoint.sh"]

#WARNING: This image does not contain the policy keeper code, you must plugin as volume
CMD /policykeeper/policy_keeper.py --srv

