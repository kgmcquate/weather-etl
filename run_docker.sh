docker run -d \
  -it \
  --name devtest \
  --mount type=bind,source="$(pwd)",target=/var/task/ \
  public.ecr.aws/sam/build-python3.7:latest
