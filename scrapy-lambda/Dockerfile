FROM public.ecr.aws/lambda/python:3.10


# Required for lxml
RUN yum install -y gcc libxml2-devel libxslt-devel
COPY . ${LAMBDA_TASK_ROOT}
RUN pip3 install --no-cache-dir -r requirements.txt
CMD [ "lambda_function.handler" ]
