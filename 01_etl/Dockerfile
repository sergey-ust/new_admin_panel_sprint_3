
FROM python:3.9-alpine

WORKDIR /app


# set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN apk update &&\
    apk --no-cache add gettext


COPY requirements.txt requirements.txt

# install dependencies
RUN pip install --no-cache-dir --upgrade pip &&\
 pip install --no-cache-dir -r requirements.txt

COPY . .

# create entrypoint for DB waiting
RUN sed -i 's/\r$//g' /app/entrypoint.sh &&\
    chmod +x /app/entrypoint.sh


CMD ["/app/entrypoint.sh", "python", "main.py"]
