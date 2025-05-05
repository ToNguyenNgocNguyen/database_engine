# For more information, please refer to https://aka.ms/vscode-docker-python
FROM python:3.11-slim

# Warning: A port below 1024 has been exposed. This requires the image to run as a root user which is not a best practice.
# For more information, please refer to https://aka.ms/vscode-docker-python-user-rights`
# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1


WORKDIR /app
COPY . /app

RUN python -m pip install .

CMD ["/bin/bash", "-c", "while true; do sleep 3600; done"]