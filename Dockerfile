FROM python:2.7-slim

COPY . /policykeeper
WORKDIR /policykeeper

RUN pip install --upgrade pip && \
    pip install -r requirements.txt

CMD /policykeeper/policy_keeper.py --srv

