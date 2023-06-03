FROM jupyter/minimal-notebook

ENV UID=${NB_UID}
ENV GID=${NB_GID}

USER root

RUN mkdir /home/notebooks

WORKDIR /home/notebooks

COPY . .

RUN pip install -r requirements.txt

EXPOSE 8888

CMD jupyter notebook --no-browser --ip 0.0.0.0 --port 8888 --allow-root 
