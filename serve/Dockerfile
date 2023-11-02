FROM public.ecr.aws/lambda/python:3.10

COPY . ${LAMBDA_TASK_ROOT}

RUN yum install -y gcc libxml2-devel libxslt-devel

RUN pip3 install --no-cache-dir -r requirements.txt
CMD [ "app.handler" ]