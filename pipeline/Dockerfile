FROM python:3.13.12-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/ 
# copy just the uv from another docker image
# and put it in the folder /bin
WORKDIR /code
ENV PATH="/code/.venv/bin:$PATH"
# .venv ll be created when us inititate later
# shell will look for the command pythom in /code/.venv/bin first 
# then check what is after : which is the other dirs

COPY pyproject.toml .python-version uv.lock ./
RUN uv sync --locked
# getting files we need to install dependencies
# --locked install the same dependencies everytime



COPY pipeline.py .

ENTRYPOINT ["python","pipeline.py"]




